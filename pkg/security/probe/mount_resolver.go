// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache License Version 2.0.
// This product includes software developed at Datadog (https://www.datadoghq.com/).
// Copyright 2016-present Datadog, Inc.

//go:build linux
// +build linux

package probe

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/hashicorp/golang-lru/simplelru"
	"github.com/moby/sys/mountinfo"
	"go.uber.org/atomic"
	"golang.org/x/sys/unix"

	skernel "github.com/DataDog/datadog-agent/pkg/security/ebpf/kernel"
	"github.com/DataDog/datadog-agent/pkg/security/metrics"
	"github.com/DataDog/datadog-agent/pkg/security/secl/model"
	"github.com/DataDog/datadog-agent/pkg/util/kernel"
	"github.com/DataDog/datadog-go/v5/statsd"
)

var (
	// ErrMountNotFound is used when an unknown mount identifier is found
	ErrMountNotFound = errors.New("unknown mount ID")
	// ErrMountUndefined is used when a mount identifier is undefined
	ErrMountUndefined = errors.New("undefined mountID")
)

const (
	deleteDelayTime = 5 * time.Second
)

func parseGroupID(mnt *mountinfo.Info) (uint32, error) {
	// Has optional fields, which is a space separated list of values.
	// Example: shared:2 master:7
	if len(mnt.Optional) > 0 {
		for _, field := range strings.Split(mnt.Optional, " ") {
			target, value, found := strings.Cut(field, ":")
			if found {
				if target == "shared" || target == "master" {
					groupID, err := strconv.ParseUint(value, 10, 32)
					return uint32(groupID), err
				}
			}
		}
	}
	return 0, nil
}

// newMountFromMountInfo - Creates a new MountEvent from parsed MountInfo data
func newMountFromMountInfo(mnt *mountinfo.Info) (*model.Mount, error) {
	groupID, err := parseGroupID(mnt)
	if err != nil {
		return nil, err
	}

	// create a MountEvent out of the parsed MountInfo
	return &model.Mount{
		ParentMountID: uint32(mnt.Parent),
		MountPointStr: mnt.Mountpoint,
		RootStr:       mnt.Root,
		MountID:       uint32(mnt.ID),
		GroupID:       groupID,
		Device:        uint32(unix.Mkdev(uint32(mnt.Major), uint32(mnt.Minor))),
		FSType:        mnt.FSType,
	}, nil
}

type deleteRequest struct {
	mount     *model.Mount
	timeoutAt time.Time
}

// MountResolver represents a cache for mountpoints and the corresponding file systems
type MountResolver struct {
	statsdClient     statsd.ClientInterface
	lock             sync.RWMutex
	mounts           map[uint32]*model.Mount
	devices          map[uint32]map[uint32]*model.Mount
	deleteQueue      []deleteRequest
	overlayPathCache *simplelru.LRU
	parentPathCache  *simplelru.LRU

	// stats
	cacheHitsStats *atomic.Int64
	cacheMissStats *atomic.Int64
	procHitsStats  *atomic.Int64
	procMissStats  *atomic.Int64
}

// SyncCache - Snapshots the current mount points of the system by reading through /proc/[pid]/mountinfo.
func (mr *MountResolver) SyncCache(pid uint32) error {
	mr.lock.Lock()
	defer mr.lock.Unlock()

	return mr.syncCache(pid)
}

func (mr *MountResolver) syncCache(pid uint32) error {
	mnts, err := kernel.ParseMountInfoFile(int32(pid))
	if err != nil {
		pErr, ok := err.(*os.PathError)
		if !ok {
			return err
		}
		return pErr
	}

	for _, mnt := range mnts {
		if _, exists := mr.mounts[uint32(mnt.ID)]; exists {
			continue
		}

		m, err := newMountFromMountInfo(mnt)
		if err != nil {
			return err
		}

		mr.insert(*m)
	}

	return nil
}

func (mr *MountResolver) deleteChildren(parent *model.Mount) {
	for _, mount := range mr.mounts {
		if mount.ParentMountID == parent.MountID {
			if _, exists := mr.mounts[mount.MountID]; exists {
				mr.delete(mount)
			}
		}
	}
}

// deleteDevice deletes MountEvent sharing the same device id for overlay fs mount
func (mr *MountResolver) deleteDevice(mount *model.Mount) {
	if !mount.IsOverlayFS() {
		return
	}

	for _, deviceMount := range mr.devices[mount.Device] {
		if mount.Device == deviceMount.Device && mount.MountID != deviceMount.MountID {
			mr.delete(deviceMount)
		}
	}
}

func (mr *MountResolver) delete(mount *model.Mount) {
	mr.clearCacheForMountID(mount.MountID)
	delete(mr.mounts, mount.MountID)

	mounts, exists := mr.devices[mount.Device]
	if exists {
		delete(mounts, mount.MountID)
	}

	mr.deleteChildren(mount)
	mr.deleteDevice(mount)
}

// Delete a mount from the cache
func (mr *MountResolver) Delete(mountID uint32) error {
	mr.lock.Lock()
	defer mr.lock.Unlock()

	mr.clearCacheForMountID(mountID)

	mount, exists := mr.mounts[mountID]
	if !exists {
		return ErrMountNotFound
	}

	mr.deleteQueue = append(mr.deleteQueue, deleteRequest{mount: mount, timeoutAt: time.Now().Add(deleteDelayTime)})

	return nil
}

// GetFilesystem returns the name of the filesystem
func (mr *MountResolver) GetFilesystem(mountID, pid uint32) string {
	mr.lock.Lock()
	defer mr.lock.Unlock()

	mount, err := mr.resolveMount(mountID, pid)
	if err != nil {
		return ""
	}

	return mount.GetFSType()
}

// Get returns a mount event from the mount id
func (mr *MountResolver) Get(mountID, pid uint32) *model.Mount {
	mr.lock.Lock()
	defer mr.lock.Unlock()

	mount, _ := mr.resolveMount(mountID, pid)
	return mount
}

// Insert a new mount point in the cache
func (mr *MountResolver) Insert(e model.MountEvent) error {
	mr.lock.Lock()
	defer mr.lock.Unlock()

	if e.MountPointPathResolutionError != nil || e.RootPathResolutionError != nil {
		// do not insert an invalid value
		return fmt.Errorf("couldn't insert mount_id %d: mount_point_error:%v root_error:%v", e.MountID, e.MountPointPathResolutionError, e.RootPathResolutionError)
	}

	mr.insert(e.Mount)

	return nil
}

func (mr *MountResolver) insert(e model.Mount) {
	// umount the previous one if exists
	if prev, ok := mr.mounts[e.MountID]; ok {
		mr.delete(prev)
	}

	// Retrieve the parent paths and strip it from the event
	p, ok := mr.mounts[e.ParentMountID]
	if ok {
		prefix := mr.getParentPath(p.MountID)
		if len(prefix) > 0 && prefix != "/" {
			e.MountPointStr = strings.TrimPrefix(e.MountPointStr, prefix)
		}
	}

	deviceMounts := mr.devices[e.Device]
	if deviceMounts == nil {
		deviceMounts = make(map[uint32]*model.Mount)
		mr.devices[e.Device] = deviceMounts
	}
	deviceMounts[e.MountID] = &e

	mr.mounts[e.MountID] = &e
}

func (mr *MountResolver) _getParentPath(mountID uint32, cache map[uint32]bool) string {
	mount, exists := mr.mounts[mountID]
	if !exists {
		return ""
	}

	mountPointStr := mount.MountPointStr

	if _, exists := cache[mountID]; exists {
		return ""
	}
	cache[mountID] = true

	if mount.ParentMountID != 0 {
		p := mr._getParentPath(mount.ParentMountID, cache)
		if p == "" {
			return mountPointStr
		}

		if p != "/" && !strings.HasPrefix(mount.MountPointStr, p) {
			mountPointStr = p + mount.MountPointStr
		}
	}

	return mountPointStr
}

func (mr *MountResolver) getParentPath(mountID uint32) string {
	if entry, found := mr.parentPathCache.Get(mountID); found {
		return entry.(string)
	}

	path := mr._getParentPath(mountID, map[uint32]bool{})
	mr.parentPathCache.Add(mountID, path)
	return path
}

func (mr *MountResolver) _getAncestor(mount *model.Mount, cache map[uint32]bool) *model.Mount {
	if _, exists := cache[mount.MountID]; exists {
		return nil
	}
	cache[mount.MountID] = true

	parent, ok := mr.mounts[mount.ParentMountID]
	if !ok {
		return nil
	}

	if grandParent := mr._getAncestor(parent, cache); grandParent != nil {
		return grandParent
	}

	return parent
}

func (mr *MountResolver) getAncestor(mount *model.Mount) *model.Mount {
	return mr._getAncestor(mount, map[uint32]bool{})
}

// getOverlayPath uses deviceID to find overlay path
func (mr *MountResolver) getOverlayPath(mount *model.Mount) string {
	if entry, found := mr.overlayPathCache.Get(mount.MountID); found {
		return entry.(string)
	}

	if ancestor := mr.getAncestor(mount); ancestor != nil {
		mount = ancestor
	}

	for _, deviceMount := range mr.devices[mount.Device] {
		if mount.MountID != deviceMount.MountID && deviceMount.IsOverlayFS() {
			if p := mr.getParentPath(deviceMount.MountID); p != "" {
				mr.overlayPathCache.Add(mount.MountID, p)
				return p
			}
		}
	}

	return ""
}

func (mr *MountResolver) dequeue(now time.Time) {
	mr.lock.Lock()

	var i int
	var req deleteRequest

	for i != len(mr.deleteQueue) {
		req = mr.deleteQueue[i]
		if req.timeoutAt.After(now) {
			break
		}

		// check if not already replaced
		if prev := mr.mounts[req.mount.MountID]; prev == req.mount {
			mr.delete(req.mount)
		}

		// clear cache anyway
		mr.clearCacheForMountID(req.mount.MountID)

		i++
	}

	if i >= len(mr.deleteQueue) {
		mr.deleteQueue = mr.deleteQueue[0:0]
	} else if i > 0 {
		mr.deleteQueue = mr.deleteQueue[i:]
	}

	mr.lock.Unlock()
}

func (mr *MountResolver) clearCacheForMountID(mountID uint32) {
	mr.parentPathCache.Remove(mountID)
	mr.overlayPathCache.Remove(mountID)
}

// Start starts the resolver
func (mr *MountResolver) Start(ctx context.Context) {
	go func() {
		ticker := time.NewTicker(2 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case now := <-ticker.C:
				mr.dequeue(now)
			case <-ctx.Done():
				return
			}
		}
	}()
}

func (mr *MountResolver) resolveMount(mountID, pid uint32) (*model.Mount, error) {
	if mountID == 0 {
		return nil, ErrMountUndefined
	}

	mount, ok := mr.mounts[mountID]

	if !ok {
		mr.cacheMissStats.Inc()
		if pid != 0 {
			if err := mr.syncCache(pid); err != nil {
				return nil, err
			}
			mount = mr.mounts[mountID]
			if mount != nil {
				mr.procHitsStats.Inc()
			} else {
				mr.procMissStats.Inc()
			}
		}
	} else {
		// stats
		mr.cacheHitsStats.Inc()
	}

	if mount == nil {
		return nil, ErrMountNotFound
	}

	return mount, nil
}

// GetMountPath returns the path of a mount identified by its mount ID. The first path is the container mount path if
// it exists, the second parameter is the mount point path, and the third parameter is the root path.
func (mr *MountResolver) GetMountPath(mountID, pid uint32) (string, string, string, error) {
	mr.lock.Lock()
	defer mr.lock.Unlock()

	mount, err := mr.resolveMount(mountID, pid)
	if err != nil {
		return "", "", "", nil
	}

	return mr.getOverlayPath(mount), mr.getParentPath(mountID), mount.RootStr, nil
}

func (mr *MountResolver) GetMountPointFullPath(mountID uint32) string {
	if mountID == 0 {
		return ""
	}
	mr.lock.RLock()
	defer mr.lock.RUnlock()

	return mr.getParentPath(mountID)
}

func getMountIDOffset(probe *Probe) uint64 {
	offset := uint64(284)

	switch {
	case probe.kernelVersion.IsSuseKernel() || probe.kernelVersion.Code >= skernel.Kernel5_12:
		offset = 292
	case probe.kernelVersion.Code != 0 && probe.kernelVersion.Code < skernel.Kernel4_13:
		offset = 268
	}

	return offset
}

func getVFSLinkDentryPosition(probe *Probe) uint64 {
	position := uint64(2)

	if probe.kernelVersion.Code != 0 && probe.kernelVersion.Code >= skernel.Kernel5_12 {
		position = 3
	}

	return position
}

func getVFSMKDirDentryPosition(probe *Probe) uint64 {
	position := uint64(2)

	if probe.kernelVersion.Code != 0 && probe.kernelVersion.Code >= skernel.Kernel5_12 {
		position = 3
	}

	return position
}

func getVFSLinkTargetDentryPosition(probe *Probe) uint64 {
	position := uint64(3)

	if probe.kernelVersion.Code != 0 && probe.kernelVersion.Code >= skernel.Kernel5_12 {
		position = 4
	}

	return position
}

func getVFSSetxattrDentryPosition(probe *Probe) uint64 {
	position := uint64(1)

	if probe.kernelVersion.Code != 0 && probe.kernelVersion.Code >= skernel.Kernel5_12 {
		position = 2
	}

	return position
}

func getVFSRemovexattrDentryPosition(probe *Probe) uint64 {
	position := uint64(1)

	if probe.kernelVersion.Code != 0 && probe.kernelVersion.Code >= skernel.Kernel5_12 {
		position = 2
	}

	return position
}

func getVFSRenameInputType(probe *Probe) uint64 {
	inputType := uint64(1)

	if probe.kernelVersion.Code != 0 && probe.kernelVersion.Code >= skernel.Kernel5_12 {
		inputType = 2
	}

	return inputType
}

// SendStats sends metrics about the current state of the namespace resolver
func (mr *MountResolver) SendStats() error {
	mr.lock.RLock()
	defer mr.lock.RUnlock()

	if err := mr.statsdClient.Count(metrics.MetricMountResolverHits, mr.cacheHitsStats.Swap(0), []string{metrics.CacheTag}, 1.0); err != nil {
		return err
	}

	if err := mr.statsdClient.Count(metrics.MetricMountResolverMiss, mr.cacheMissStats.Swap(0), []string{metrics.CacheTag}, 1.0); err != nil {
		return err
	}

	if err := mr.statsdClient.Count(metrics.MetricMountResolverHits, mr.procHitsStats.Swap(0), []string{metrics.ProcFSTag}, 1.0); err != nil {
		return err
	}

	if err := mr.statsdClient.Count(metrics.MetricMountResolverMiss, mr.procMissStats.Swap(0), []string{metrics.ProcFSTag}, 1.0); err != nil {
		return err
	}

	return mr.statsdClient.Gauge(metrics.MetricMountResolverCacheSize, float64(len(mr.mounts)), []string{}, 1.0)
}

// NewMountResolver instantiates a new mount resolver
func NewMountResolver(statsdClient statsd.ClientInterface) (*MountResolver, error) {
	overlayPathCache, err := simplelru.NewLRU(256, nil)
	if err != nil {
		return nil, err
	}

	parentPathCache, err := simplelru.NewLRU(256, nil)
	if err != nil {
		return nil, err
	}

	return &MountResolver{
		statsdClient:     statsdClient,
		lock:             sync.RWMutex{},
		devices:          make(map[uint32]map[uint32]*model.Mount),
		mounts:           make(map[uint32]*model.Mount),
		overlayPathCache: overlayPathCache,
		parentPathCache:  parentPathCache,
		cacheHitsStats:   atomic.NewInt64(0),
		procHitsStats:    atomic.NewInt64(0),
		cacheMissStats:   atomic.NewInt64(0),
		procMissStats:    atomic.NewInt64(0),
	}, nil
}
