package goflowlib

import (
	"fmt"
	promClient "github.com/prometheus/client_model/go"
)

type remapperType func(string) string

type mappedMetric struct {
	name          string
	allowedTags   []string
	valueRemapper map[string]remapperType
	keyRemapper   map[string]string
}

var typeMapper = map[string]string{
	"NetFlowV5": "netflow5",
	// TODO: more
}

var flowsetMapper = map[string]string{
	"DataFlowSet": "data_flow_set",
	// TODO: more
}

// metricNameMapping maps goflow prometheus metrics to datadog netflow telemetry metrics
var metricNameMapping = map[string]mappedMetric{
	"flow_decoder_count": mappedMetric{
		name:        "decoder.count",
		allowedTags: []string{"name", "worker"},
		valueRemapper: map[string]remapperType{
			"name": remapGoflowType,
		},
	},
	"flow_process_nf_count": mappedMetric{
		name:        "process_netflow.count",
		allowedTags: []string{"router", "version"},
		keyRemapper: map[string]string{
			"router": "device_ip",
		},
	},
	"flow_process_nf_flowset_sum": mappedMetric{
		name:        "process_netflow_flowset.count",
		allowedTags: []string{"router", "type", "version"},
		valueRemapper: map[string]remapperType{
			"type": remapFlowset,
		},
		keyRemapper: map[string]string{
			"router": "device_ip",
		},
	},
	"flow_traffic_bytes": mappedMetric{
		name:        "traffic.bytes",
		allowedTags: []string{"local_port", "remote_ip", "type"},
		keyRemapper: map[string]string{
			"local_port": "listener_port",
			"remote_ip":  "device_ip",
		},
		valueRemapper: map[string]remapperType{
			"name": remapGoflowType,
		},
	},
	"flow_traffic_packets": mappedMetric{
		name:        "traffic.packets",
		allowedTags: []string{"local_port", "remote_ip", "type"},
		keyRemapper: map[string]string{
			"local_port": "listener_port",
			"remote_ip":  "device_ip",
		},
		valueRemapper: map[string]remapperType{
			"name": remapGoflowType,
		},
	},
}

func remapGoflowType(goflowType string) string {
	return typeMapper[goflowType]
}

func remapFlowset(flowset string) string {
	return flowsetMapper[flowset]
}

func ConvertMetric(metric *promClient.Metric, metricFamily *promClient.MetricFamily) (string, float64, []string, error) {
	origMetricName := metricFamily.GetName()
	aMappedMetric, ok := metricNameMapping[origMetricName]
	if !ok {
		return "", 0, nil, fmt.Errorf("metric mapping not found for %s", origMetricName)
	}
	var floatValue float64
	if metricFamily.GetType() == promClient.MetricType_COUNTER {
		floatValue = metric.GetCounter().GetValue()
	}
	metricType := metricFamily.GetType()
	switch metricType {
	case promClient.MetricType_COUNTER:
		floatValue = metric.GetCounter().GetValue()
	case promClient.MetricType_GAUGE:
		floatValue = metric.GetGauge().GetValue()
	default:
		name := promClient.MetricType_name[int32(metricType)]
		return "", 0, nil, fmt.Errorf("metric type `%s` (%d) not supported", name, metricType)
	}
	var tags []string
	for _, labelPair := range metric.GetLabel() {
		name := labelPair.GetName()
		// TODO: use allowedTags
		// TODO: use keyRemapper
		// TODO: use valueRemapper
		val := labelPair.GetValue()
		if name != "" && val != "" {
			tags = append(tags, name+":"+val)
		}
	}
	return aMappedMetric.name, floatValue, tags, nil
}
