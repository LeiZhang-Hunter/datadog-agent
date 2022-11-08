package goflowlib

import (
	"fmt"
	promClient "github.com/prometheus/client_model/go"
)

//// metricNameMapping maps goflow prometheus metrics to datadog netflow telemetry metrics
//var metricNameMapping = map[string]string{
//	"flow_decoder_count":          "decoder.count",
//	"flow_process_nf_count":       "process_netflow.count",
//	"flow_process_nf_flowset_sum": "process_netflow_flowset.count",
//	"flow_traffic_bytes":          "traffic.bytes",
//	"flow_traffic_packets":        "traffic.packets",
//}

// metricNameMapping maps goflow prometheus metrics to datadog netflow telemetry metrics
var allowedMetrics = map[string]bool{
	"flow_decoder_count":          true,
	"flow_process_nf_count":       true,
	"flow_process_nf_flowset_sum": true,
	"flow_traffic_bytes":          true,
	"flow_traffic_packets":        true,
}

func ConvertMetric(metric *promClient.Metric, metricFamily *promClient.MetricFamily) (string, float64, []string, error) {
	metricName := metricFamily.GetName()
	allowed := allowedMetrics[metricName]
	if allowed {
		return "", 0, nil, fmt.Errorf("metric mapping not found for %s", metricName)
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
	case promClient.MetricType_SUMMARY:
		// TODO support this
		return "", 0, nil, fmt.Errorf("summary metric type not supported for %s", metricName)
	case promClient.MetricType_HISTOGRAM:
		// TODO support this
		return "", 0, nil, fmt.Errorf("histogram metric type not supported for %s", metricName)
	case promClient.MetricType_GAUGE_HISTOGRAM:
		// TODO support this
		return "", 0, nil, fmt.Errorf("gauge histogram metric type not supported for %s", metricName)
	default:
		return "", 0, nil, fmt.Errorf("unexpected metric type `untyped` for %s, type %v", metricName, metricType)
	}
	var tags []string
	for _, labelPair := range metric.GetLabel() {
		name := labelPair.GetName()
		val := labelPair.GetValue()
		if name != "" && val != "" {
			tags = append(tags, name+":"+val)
		}
	}
	return metricName, floatValue, tags, nil
}
