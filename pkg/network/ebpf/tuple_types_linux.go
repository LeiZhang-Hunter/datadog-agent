// Code generated by cmd/cgo -godefs; DO NOT EDIT.
// cgo -godefs -- -fsigned-char /git/datadog-agent/pkg/network/ebpf/tuple_types.go

package ebpf

type ConnType uint32

const (
	UDP ConnType = 0x0
	TCP ConnType = 0x1
)

type ConnFamily uint32

const (
	IPv4 ConnFamily = 0x0
	IPv6 ConnFamily = 0x2
)

type ConnDirection uint8

const (
	Unknown  ConnDirection = 0x0
	Incoming ConnDirection = 0x1
	Outgoing ConnDirection = 0x2
)
