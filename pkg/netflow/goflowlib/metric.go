package goflowlib

import (
	"fmt"
	"github.com/DataDog/datadog-agent/pkg/metrics"
	promClient "github.com/prometheus/client_model/go"
)

type remapperType func(string) string

type mappedMetric struct {
	name           string
	allowedTagKeys []string
	valueRemapper  map[string]remapperType
	keyRemapper    map[string]string
	extraTags      []string
}

func (m mappedMetric) isAllowedTagKey(tagKey string) bool {
	for _, allowedTagKey := range m.allowedTagKeys {
		if tagKey == allowedTagKey {
			return true
		}
	}
	return false
}

var typeMapper = map[string]string{
	"NetFlowV5": "netflow5",
	// TODO: more
}

var flowsetMapper = map[string]string{
	"DataFlowSet": "data_flow_set",
	// TODO: more
}

var netflowVersionMapper = map[string]string{
	"5": "netflow5",
	// TODO: more
}

var sflowVersionMapper = map[string]string{
	"5": "sflow5",
}

// metricNameMapping maps goflow prometheus metrics to datadog netflow telemetry metrics
var metricNameMapping = map[string]mappedMetric{
	"flow_decoder_count": mappedMetric{
		name:           "decoder.messages",
		allowedTagKeys: []string{"name", "worker"},
		valueRemapper: map[string]remapperType{
			"name": remapGoflowType,
		},
	},
	"flow_decoder_error_count": mappedMetric{
		name:           "decoder.errors",
		allowedTagKeys: []string{"name", "worker"},
		valueRemapper: map[string]remapperType{
			"name": remapGoflowType,
		},
	},
	"flow_process_nf_count": mappedMetric{
		name:           "processor.flows",
		allowedTagKeys: []string{"router", "version"},
		valueRemapper: map[string]remapperType{
			"version": remapNetFlowVersion,
		},
		keyRemapper: map[string]string{
			"router":  "device_ip",
			"version": "flow_type",
		},
	},
	"flow_process_nf_flowset_sum": mappedMetric{
		name:           "processor.flowsets",
		allowedTagKeys: []string{"router", "type", "version"},
		valueRemapper: map[string]remapperType{
			"type":    remapFlowset,
			"version": remapNetFlowVersion,
		},
		keyRemapper: map[string]string{
			"router":  "device_ip",
			"version": "flow_type",
		},
	},
	"flow_traffic_bytes": mappedMetric{
		name:           "traffic.bytes",
		allowedTagKeys: []string{"local_port", "remote_ip", "name"},
		keyRemapper: map[string]string{
			"local_port": "listener_port",
			"remote_ip":  "device_ip",
			"name":       "flow_type",
		},
		valueRemapper: map[string]remapperType{
			"name": remapGoflowType,
		},
	},
	"flow_traffic_packets": mappedMetric{
		name:           "traffic.packets",
		allowedTagKeys: []string{"local_port", "remote_ip", "name"},
		keyRemapper: map[string]string{
			"local_port": "listener_port",
			"remote_ip":  "device_ip",
			"name":       "flow_type",
		},
		valueRemapper: map[string]remapperType{
			"name": remapGoflowType,
		},
	},
	"flow_process_sf_count": mappedMetric{
		name:           "processor.flows",
		allowedTagKeys: []string{"router", "version"},
		valueRemapper: map[string]remapperType{
			"version": remapSFlowVersion,
		},
		keyRemapper: map[string]string{
			"router":  "device_ip",
			"version": "flow_type",
		},
	},
	"flow_process_sf_errors_count": mappedMetric{
		name:           "processor.errors",
		allowedTagKeys: []string{"router", "error"},
		keyRemapper: map[string]string{
			"router": "device_ip",
		},
	},
}

func remapGoflowType(goflowType string) string {
	return typeMapper[goflowType]
}

func remapFlowset(flowset string) string {
	return flowsetMapper[flowset]
}

func remapNetFlowVersion(version string) string {
	return netflowVersionMapper[version]
}
func remapSFlowVersion(version string) string {
	return sflowVersionMapper[version]
}

func ConvertMetric(metric *promClient.Metric, metricFamily *promClient.MetricFamily) (metrics.MetricType, string, float64, []string, error) {
	var ddMetricType metrics.MetricType
	origMetricName := metricFamily.GetName()
	aMappedMetric, ok := metricNameMapping[origMetricName]
	if !ok {
		return 0, "", 0, nil, fmt.Errorf("metric mapping not found for %s", origMetricName)
	}
	var floatValue float64
	if metricFamily.GetType() == promClient.MetricType_COUNTER {
		floatValue = metric.GetCounter().GetValue()
	}
	promMetricType := metricFamily.GetType()
	switch promMetricType {
	case promClient.MetricType_COUNTER:
		floatValue = metric.GetCounter().GetValue()
		ddMetricType = metrics.MonotonicCountType
	case promClient.MetricType_GAUGE:
		floatValue = metric.GetGauge().GetValue()
		ddMetricType = metrics.GaugeType
	default:
		name := promClient.MetricType_name[int32(promMetricType)]
		return 0, "", 0, nil, fmt.Errorf("metric type `%s` (%d) not supported", name, promMetricType)
	}
	var tags []string
	for _, labelPair := range metric.GetLabel() {
		tagKey := labelPair.GetName()
		if !aMappedMetric.isAllowedTagKey(tagKey) {
			continue
		}
		tagVal := labelPair.GetValue()
		valueRemapper, ok := aMappedMetric.valueRemapper[tagKey]
		if ok {
			tagVal = valueRemapper(tagVal)
		}
		newKey, ok := aMappedMetric.keyRemapper[tagKey]
		if ok {
			tagKey = newKey
		}

		if tagKey != "" && tagVal != "" {
			tags = append(tags, tagKey+":"+tagVal)
		}
	}
	if len(aMappedMetric.extraTags) > 0 {
		tags = append(tags, aMappedMetric.extraTags...)
	}
	return ddMetricType, aMappedMetric.name, floatValue, tags, nil
}
