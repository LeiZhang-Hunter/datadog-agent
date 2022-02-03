// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache License Version 2.0.
// This product includes software developed at Datadog (https://www.datadoghq.com/).
// Copyright 2016-present Datadog, Inc.

package aggregator

import (
	"fmt"
	"sync"
	"time"

	"github.com/DataDog/datadog-agent/pkg/aggregator/tags"
	"github.com/DataDog/datadog-agent/pkg/collector/check"
	"github.com/DataDog/datadog-agent/pkg/config"
	"github.com/DataDog/datadog-agent/pkg/config/resolver"
	"github.com/DataDog/datadog-agent/pkg/containerlifecycle"
	"github.com/DataDog/datadog-agent/pkg/epforwarder"
	"github.com/DataDog/datadog-agent/pkg/forwarder"
	"github.com/DataDog/datadog-agent/pkg/metrics"
	"github.com/DataDog/datadog-agent/pkg/serializer"
	"github.com/DataDog/datadog-agent/pkg/util/log"
)

// DemultiplexerInstance is a shared global demultiplexer instance.
// Initialized by InitAndStartAgentDemultiplexer or InitAndStartServerlessDemultiplexer,
// could be nil otherwise.
//
// The plan is to deprecated this global instance at some point.
var demultiplexerInstance Demultiplexer

var demultiplexerInstanceMu sync.Mutex

// Demultiplexer is composed of multiple samplers (check and time/dogstatsd)
// a shared forwarder, the event platform forwarder, orchestrator data buffers
// and other data that need to be sent to the forwarders.
// DemultiplexerOptions let you configure which forwarders have to be started.
type Demultiplexer interface {
	// General

	Run()
	Stop(flush bool)

	// Aggregation API

	// AddTimeSample sends a MetricSample to the time sampler.
	// In sharded implementation, the metric is sent to the first time sampler.
	AddTimeSample(sample metrics.MetricSample)
	// AddTimeSampleBatch sends a batch of MetricSample to the given time
	// sampler shard.
	// Implementation not supporting sharding may ignore the `shard` parameter.
	AddTimeSampleBatch(shard TimeSamplerID, samples metrics.MetricSampleBatch)
	// AddCheckSample adds check sample sent by a check from one of the collectors into a check sampler pipeline.
	AddCheckSample(sample metrics.MetricSample)
	// ForceFlushToSerializer flushes all the aggregated data from the samplers to
	// the serialization/forwarding parts.
	ForceFlushToSerializer(start time.Time, waitForSerializer bool)
	// GetMetricSamplePool returns a shared resource used in the whole DogStatsD
	// pipeline to re-use metric samples slices: the server is getting a slice
	// and filling it with samples, the rest of the pipeline process them the
	// end of line (the time sampler) is putting back the slice in the pool.
	// Main idea is to reduce the garbage generated by slices allocation.
	GetMetricSamplePool() *metrics.MetricSamplePool
	// GetDogStatsDPipelinesCount returns how many time sampling pipeline are
	// running for the DogStatsD samples.
	// Implementation not implementing sharding must return 1.
	GetDogStatsDPipelinesCount() int

	// Aggregator returns an aggregator that anyone can use. This method exists
	// to keep compatibility with existing code while introducing the Demultiplexer,
	// however, the plan is to remove it anytime soon.
	//
	// Deprecated.
	Aggregator() *BufferedAggregator
	// Serializer returns a serializer that anyone can use. This method exists
	// to keep compatibility with existing code while introducing the Demultiplexer,
	// however, the plan is to remove it anytime soon.
	//
	// Deprecated.
	Serializer() serializer.MetricSerializer

	// Senders API, mainly used by collectors/checks

	GetSender(id check.ID) (Sender, error)
	SetSender(sender Sender, id check.ID) error
	DestroySender(id check.ID)
	GetDefaultSender() (Sender, error)
	ChangeAllSendersDefaultHostname(hostname string)
	cleanSenders()
}

// AgentDemultiplexer is the demultiplexer implementation for the main Agent
type AgentDemultiplexer struct {
	m sync.Mutex

	// options are the options with which the demultiplexer has been created
	options    DemultiplexerOptions
	aggregator *BufferedAggregator
	dataOutputs
	*senders

	// sharded statsd time samplers
	statsd
}

// DemultiplexerOptions are the options used to initialize a Demultiplexer.
type DemultiplexerOptions struct {
	SharedForwarderOptions         *forwarder.Options
	UseNoopEventPlatformForwarder  bool
	UseEventPlatformForwarder      bool
	UseOrchestratorForwarder       bool
	UseContainerLifecycleForwarder bool
	FlushInterval                  time.Duration

	DontStartForwarders bool // unit tests don't need the forwarders to be instanciated
}

type statsd struct {
	// how many sharded statsdSamplers exists.
	// len(statsdSamplers) would return the same result but having it stored
	// it will provide more explicit visiblility / no extra function call for
	// every metric to distribute.
	pipelinesCount int
	samplers       []*TimeSampler
	// shared metric sample pool between the dogstatsd server & the time sampler
	metricSamplePool *metrics.MetricSamplePool
}

type forwarders struct {
	shared             *forwarder.DefaultForwarder
	orchestrator       *forwarder.DefaultForwarder
	eventPlatform      epforwarder.EventPlatformForwarder
	containerLifecycle *forwarder.DefaultForwarder
}

type dataOutputs struct {
	forwarders       forwarders
	sharedSerializer serializer.MetricSerializer
}

// DefaultDemultiplexerOptions returns the default options to initialize a Demultiplexer.
func DefaultDemultiplexerOptions(options *forwarder.Options) DemultiplexerOptions {
	if options == nil {
		options = forwarder.NewOptions(nil)
	}

	return DemultiplexerOptions{
		SharedForwarderOptions:         options,
		FlushInterval:                  DefaultFlushInterval,
		UseEventPlatformForwarder:      true,
		UseOrchestratorForwarder:       true,
		UseContainerLifecycleForwarder: false,
	}
}

// InitAndStartAgentDemultiplexer creates a new Demultiplexer and runs what's necessary
// in goroutines. As of today, only the embedded BufferedAggregator needs a separate goroutine.
// In the future, goroutines will be started for the event platform forwarder and/or orchestrator forwarder.
func InitAndStartAgentDemultiplexer(options DemultiplexerOptions, hostname string) *AgentDemultiplexer {
	demultiplexerInstanceMu.Lock()
	defer demultiplexerInstanceMu.Unlock()

	demux := initAgentDemultiplexer(options, hostname)

	if demultiplexerInstance != nil {
		log.Warn("A DemultiplexerInstance is already existing but InitAndStartAgentDemultiplexer has been called again. Current instance will be overridden")
	}
	demultiplexerInstance = demux

	go demux.Run()
	return demux
}

func initAgentDemultiplexer(options DemultiplexerOptions, hostname string) *AgentDemultiplexer {

	// prepare the multiple forwarders
	// -------------------------------

	log.Debugf("Creating forwarders")
	// orchestrator forwarder
	var orchestratorForwarder *forwarder.DefaultForwarder
	if options.UseOrchestratorForwarder {
		orchestratorForwarder = buildOrchestratorForwarder()
	}

	// event platform forwarder
	var eventPlatformForwarder epforwarder.EventPlatformForwarder
	if options.UseNoopEventPlatformForwarder {
		eventPlatformForwarder = epforwarder.NewNoopEventPlatformForwarder()
	} else if options.UseEventPlatformForwarder {
		eventPlatformForwarder = epforwarder.NewEventPlatformForwarder()
	}

	// setup the container lifecycle events forwarder
	var containerLifecycleForwarder *forwarder.DefaultForwarder
	if options.UseContainerLifecycleForwarder {
		containerLifecycleForwarder = containerlifecycle.NewForwarder()
	}

	sharedForwarder := forwarder.NewDefaultForwarder(options.SharedForwarderOptions)

	// prepare the serializer
	// ----------------------

	sharedSerializer := serializer.NewSerializer(sharedForwarder, orchestratorForwarder, containerLifecycleForwarder)

	// prepare the embedded aggregator
	// --

	agg := InitAggregatorWithFlushInterval(sharedSerializer, eventPlatformForwarder, hostname, options.FlushInterval)

	// statsd samplers
	// ---------------

	bufferSize := config.Datadog.GetInt("aggregator_buffer_size")
	metricSamplePool := metrics.NewMetricSamplePool(MetricSamplePoolBatchSize)

	statsdPipelinesCount := config.Datadog.GetInt("dogstatsd_pipeline_count")
	if statsdPipelinesCount <= 0 {
		statsdPipelinesCount = 1
	}

	statsdSamplers := make([]*TimeSampler, statsdPipelinesCount)

	for i := 0; i < statsdPipelinesCount; i++ {
		tagsStore := tags.NewStore(config.Datadog.GetBool("aggregator_use_tags_store"), fmt.Sprintf("timesampler #%d", i))
		// NOTE(remy): we can consider that the orchestrator forwarder and the
		// container lifecycle fwder aren't useful here and having them nil
		// could probably be considered
		serializer := serializer.NewSerializer(sharedForwarder, orchestratorForwarder,
			containerLifecycleForwarder)
		statsdSamplers[i] = NewTimeSampler(TimeSamplerID(i), bucketSize, options.FlushInterval, metricSamplePool,
			bufferSize, serializer, tagsStore, agg.flushAndSerializeInParallel)
	}

	// --

	demux := &AgentDemultiplexer{
		options: options,

		// Input
		aggregator: agg,

		// Output
		dataOutputs: dataOutputs{

			forwarders: forwarders{
				shared:             sharedForwarder,
				orchestrator:       orchestratorForwarder,
				eventPlatform:      eventPlatformForwarder,
				containerLifecycle: containerLifecycleForwarder,
			},

			sharedSerializer: sharedSerializer,
		},

		senders: newSenders(agg),

		// statsd time samplers
		statsd: statsd{
			pipelinesCount:   statsdPipelinesCount,
			samplers:         statsdSamplers,
			metricSamplePool: metricSamplePool,
		},
	}

	return demux
}

// AddAgentStartupTelemetry adds a startup event and count (in a time sampler)
// to be sent on the next flush.
func (d *AgentDemultiplexer) AddAgentStartupTelemetry(agentVersion string) {
	if agentVersion != "" {
		d.AddTimeSample(metrics.MetricSample{
			Name:       fmt.Sprintf("datadog.%s.started", d.aggregator.agentName),
			Value:      1,
			Tags:       d.aggregator.tags(true),
			Host:       d.aggregator.hostname,
			Mtype:      metrics.CountType,
			SampleRate: 1,
			Timestamp:  0,
		})

		if d.aggregator.hostname != "" {
			// Send startup event only when we have a valid hostname
			d.aggregator.eventIn <- metrics.Event{
				Text:           fmt.Sprintf("Version %s", agentVersion),
				SourceTypeName: "System",
				Host:           d.aggregator.hostname,
				EventType:      "Agent Startup",
			}
		}
	}
}

// Run runs all demultiplexer parts
func (d *AgentDemultiplexer) Run() {
	if !d.options.DontStartForwarders {
		log.Debugf("Starting forwarders")

		// orchestrator forwarder
		if d.forwarders.orchestrator != nil {
			d.forwarders.orchestrator.Start() //nolint:errcheck
		} else {
			log.Debug("not starting the orchestrator forwarder")
		}

		// event platform forwarder
		if d.forwarders.eventPlatform != nil {
			d.forwarders.eventPlatform.Start()
		} else {
			log.Debug("not starting the event platform forwarder")
		}

		// container lifecycle forwarder
		if d.forwarders.containerLifecycle != nil {
			if err := d.forwarders.containerLifecycle.Start(); err != nil {
				log.Errorf("error starting container lifecycle forwarder: %w", err)
			}
		} else {
			log.Debug("not starting the container lifecycle forwarder")
		}

		// shared forwarder
		if d.forwarders.shared != nil {
			d.forwarders.shared.Start() //nolint:errcheck
		} else {
			log.Debug("not starting the shared forwarder")
		}
		log.Debug("Forwarders started")
	}

	if d.options.UseContainerLifecycleForwarder {
		d.aggregator.contLcycleDequeueOnce.Do(func() { go d.aggregator.dequeueContainerLifecycleEvents() })
	}

	d.aggregator.run() // this is the blocking call
}

// Stop stops the demultiplexer.
// Resources are released, the instance should not be used after a call to `Stop()`.
func (d *AgentDemultiplexer) Stop(flush bool) {
	d.m.Lock()
	defer d.m.Unlock()

	if d.aggregator != nil {
		d.aggregator.Stop(flush)
	}
	d.aggregator = nil

	if !d.options.DontStartForwarders {
		if d.dataOutputs.forwarders.orchestrator != nil {
			d.dataOutputs.forwarders.orchestrator.Stop()
			d.dataOutputs.forwarders.orchestrator = nil
		}
		if d.dataOutputs.forwarders.eventPlatform != nil {
			d.dataOutputs.forwarders.eventPlatform.Stop()
			d.dataOutputs.forwarders.eventPlatform = nil
		}
		if d.dataOutputs.forwarders.containerLifecycle != nil {
			d.dataOutputs.forwarders.containerLifecycle.Stop()
			d.dataOutputs.forwarders.containerLifecycle = nil
		}
		if d.dataOutputs.forwarders.shared != nil {
			d.dataOutputs.forwarders.shared.Stop()
			d.dataOutputs.forwarders.shared = nil
		}
	}

	d.dataOutputs.sharedSerializer = nil
	d.senders = nil
	demultiplexerInstance = nil
}

// ForceFlushToSerializer flushes all data from the aggregator and time samplers
// to the serializer.
// Safe to call from multiple threads.
func (d *AgentDemultiplexer) ForceFlushToSerializer(start time.Time, waitForSerializer bool) {
	d.m.Lock()
	defer d.m.Unlock()

	// flush the time samplers
	// ----------------------

	if waitForSerializer {
		wg := sync.WaitGroup{}
		for _, sampler := range d.statsd.samplers {
			wg.Add(1)
			// order the flush to the time sampler, and wait, in a different routine
			go func(sampler *TimeSampler, wg *sync.WaitGroup) {
				sampler.Flush(start, true)
				wg.Done()
			}(sampler, &wg)
		}
		// wait for all samplers to have finished their flush
		wg.Wait()
	} else {
		for _, sampler := range d.statsd.samplers {
			sampler.Flush(start, false)
		}
	}

	// flush the aggregator (check samplers)
	// -------------------------------------

	if d.aggregator != nil {
		d.aggregator.Flush(start, waitForSerializer)
	}

	addFlushTime("MainFlushTime", int64(time.Since(start)))
	aggregatorNumberOfFlush.Add(1)
}

// AddTimeSampleBatch adds a batch of MetricSample into the given time sampler shard.
// If you have to submit a single metric sample see `AddTimeSample`.
func (d *AgentDemultiplexer) AddTimeSampleBatch(shard TimeSamplerID, samples metrics.MetricSampleBatch) {
	// distribute the samples on the different statsd samplers using a channel
	// (in the time sampler implementation) for latency reasons:
	// its buffering + the fact that it is another goroutine processing the samples,
	// it should get back to the caller as fast as possible once the samples are
	// in the channel.
	d.statsd.samplers[shard].addSamples(samples)
}

// AddTimeSample adds a MetricSample in the first time sampler.
func (d *AgentDemultiplexer) AddTimeSample(sample metrics.MetricSample) {
	batch := d.GetMetricSamplePool().GetBatch()
	batch[0] = sample
	d.statsd.samplers[0].addSamples(batch[:1])
}

// AddCheckSample adds check sample sent by a check from one of the collectors into a check sampler pipeline.
func (d *AgentDemultiplexer) AddCheckSample(sample metrics.MetricSample) {
	panic("not implemented yet.")
}

// GetDogStatsDPipelinesCount returns how many sampling pipeline are running for
// the DogStatsD samples.
func (d *AgentDemultiplexer) GetDogStatsDPipelinesCount() int {
	return d.statsd.pipelinesCount
}

// Serializer returns a serializer that anyone can use. This method exists
// to keep compatibility with existing code while introducing the Demultiplexer,
// however, the plan is to remove it anytime soon.
//
// Deprecated.
func (d *AgentDemultiplexer) Serializer() serializer.MetricSerializer {
	return d.dataOutputs.sharedSerializer
}

// Aggregator returns an aggregator that anyone can use. This method exists
// to keep compatibility with existing code while introducing the Demultiplexer,
// however, the plan is to remove it anytime soon.
//
// Deprecated.
func (d *AgentDemultiplexer) Aggregator() *BufferedAggregator {
	return d.aggregator
}

// GetMetricSamplePool returns a shared resource used in the whole DogStatsD
// pipeline to re-use metric samples slices: the server is getting a slice
// and filling it with samples, the rest of the pipeline process them the
// end of line (the time sampler) is putting back the slice in the pool.
// Main idea is to reduce the garbage generated by slices allocation.
func (d *AgentDemultiplexer) GetMetricSamplePool() *metrics.MetricSamplePool {
	return d.statsd.metricSamplePool
}

// ------------------------------

// ServerlessDemultiplexer is a simple demultiplexer used by the serverless flavor of the Agent
type ServerlessDemultiplexer struct {
	// shared metric sample pool between the dogstatsd server & the time sampler
	metricSamplePool *metrics.MetricSamplePool

	serializer    *serializer.Serializer
	forwarder     *forwarder.SyncForwarder
	statsdSampler *TimeSampler

	flushLock *sync.Mutex

	aggregator *BufferedAggregator
	*senders
}

// InitAndStartServerlessDemultiplexer creates and starts new Demultiplexer for the serverless agent.
func InitAndStartServerlessDemultiplexer(domainResolvers map[string]resolver.DomainResolver, hostname string, forwarderTimeout time.Duration) *ServerlessDemultiplexer {
	bufferSize := config.Datadog.GetInt("aggregator_buffer_size")
	forwarder := forwarder.NewSyncForwarder(domainResolvers, forwarderTimeout)
	serializer := serializer.NewSerializer(forwarder, nil, nil)
	aggregator := InitAggregator(serializer, nil, hostname)
	metricSamplePool := metrics.NewMetricSamplePool(MetricSamplePoolBatchSize)
	tagsStore := tags.NewStore(config.Datadog.GetBool("aggregator_use_tags_store"), "timesampler")
	statsdSampler := NewTimeSampler(TimeSamplerID(0), bucketSize, DefaultFlushInterval, metricSamplePool, bufferSize,
		serializer, tagsStore, flushAndSerializeInParallel{enabled: false})

	demux := &ServerlessDemultiplexer{
		aggregator:       aggregator,
		serializer:       serializer,
		forwarder:        forwarder,
		statsdSampler:    statsdSampler,
		metricSamplePool: metricSamplePool,
		senders:          newSenders(aggregator),
		flushLock:        &sync.Mutex{},
	}

	demultiplexerInstance = demux

	go demux.Run()

	return demux
}

// Run runs all demultiplexer parts
func (d *ServerlessDemultiplexer) Run() {
	if d.forwarder != nil {
		d.forwarder.Start() //nolint:errcheck
		log.Debug("Forwarder started")
	} else {
		log.Debug("not starting the forwarder")
	}

	log.Debug("Demultiplexer started")
	d.aggregator.run()
}

// Stop stops the wrapped aggregator and the forwarder.
func (d *ServerlessDemultiplexer) Stop(flush bool) {
	d.aggregator.Stop(flush)

	if d.forwarder != nil {
		d.forwarder.Stop()
	}
}

// ForceFlushToSerializer flushes all data from the time sampler to the serializer.
func (d *ServerlessDemultiplexer) ForceFlushToSerializer(start time.Time, waitForSerializer bool) {
	d.flushLock.Lock()
	defer d.flushLock.Unlock()
	d.statsdSampler.Flush(start, waitForSerializer)
}

// AddTimeSample send a MetricSample to the TimeSampler.
func (d *ServerlessDemultiplexer) AddTimeSample(sample metrics.MetricSample) {
	d.flushLock.Lock()
	defer d.flushLock.Unlock()
	batch := d.GetMetricSamplePool().GetBatch()
	batch[0] = sample
	d.statsdSampler.addSamples(batch[:1])
}

// AddTimeSampleBatch send a MetricSampleBatch to the TimeSampler.
// The ServerlessDemultiplexer is not using sharding in its DogStatsD pipeline,
// the `shard` parameter is ignored.
// In the Serverless Agent, consider using `AddTimeSample` instead.
func (d *ServerlessDemultiplexer) AddTimeSampleBatch(shard TimeSamplerID, samples metrics.MetricSampleBatch) {
	d.flushLock.Lock()
	defer d.flushLock.Unlock()
	d.statsdSampler.addSamples(samples)
}

// GetDogStatsDPipelinesCount returns how many sampling pipeline are running for
// the DogStatsD samples.
func (d *ServerlessDemultiplexer) GetDogStatsDPipelinesCount() int {
	return 1
}

// AddCheckSample doesn't do anything in the Serverless Agent implementation.
func (d *ServerlessDemultiplexer) AddCheckSample(sample metrics.MetricSample) {
	panic("not implemented.")
}

// Serializer returns the shared serializer
func (d *ServerlessDemultiplexer) Serializer() serializer.MetricSerializer {
	return d.serializer
}

// Aggregator returns the main buffered aggregator
func (d *ServerlessDemultiplexer) Aggregator() *BufferedAggregator {
	return d.aggregator
}

// GetMetricSamplePool returns a shared resource used in the whole DogStatsD
// pipeline to re-use metric samples slices: the server is getting a slice
// and filling it with samples, the rest of the pipeline process them the
// end of line (the time sampler) is putting back the slice in the pool.
// Main idea is to reduce the garbage generated by slices allocation.
func (d *ServerlessDemultiplexer) GetMetricSamplePool() *metrics.MetricSamplePool {
	return d.metricSamplePool
}
