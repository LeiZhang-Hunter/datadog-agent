package goflowlib

import (
	promClient "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestConvertMetric(t *testing.T) {
	tests := []struct {
		name          string
		metric        *promClient.Metric
		metricFamily  *promClient.MetricFamily
		expectedName  string
		expectedValue float64
		expectedTags  []string
		expectedErr   string
	}{
		{
			name: "FEATURE ignore non allowed field",
			metricFamily: &promClient.MetricFamily{
				Name: strToPtr("flow_decoder_count"),
			},
			metric: &promClient.Metric{
				Counter: &promClient.Counter{Value: float64ToPtr(10)},
				Label: []*promClient.LabelPair{
					{Name: strToPtr("worker"), Value: strToPtr("1")},
					{Name: strToPtr("notAllowedField"), Value: strToPtr("1")},
				},
			},
			expectedName:  "decoder.messages",
			expectedValue: 10.0,
			expectedTags:  []string{"worker:1"},
			expectedErr:   "",
		},
		{
			name: "FEATURE valueRemapper",
			metricFamily: &promClient.MetricFamily{
				Name: strToPtr("flow_decoder_count"),
			},
			metric: &promClient.Metric{
				Counter: &promClient.Counter{Value: float64ToPtr(10)},
				Label: []*promClient.LabelPair{
					{Name: strToPtr("name"), Value: strToPtr("NetFlowV5")},
					{Name: strToPtr("worker"), Value: strToPtr("1")},
					{Name: strToPtr("notAllowedField"), Value: strToPtr("1")},
				},
			},
			expectedName:  "decoder.messages",
			expectedValue: 10.0,
			expectedTags:  []string{"name:netflow5", "worker:1"},
			expectedErr:   "",
		},
		{
			name: "FEATURE keyRemapper",
			metricFamily: &promClient.MetricFamily{
				Name: strToPtr("flow_process_nf_count"),
			},
			metric: &promClient.Metric{
				Counter: &promClient.Counter{Value: float64ToPtr(10)},
				Label: []*promClient.LabelPair{
					{Name: strToPtr("router"), Value: strToPtr("1.2.3.4")},
					{Name: strToPtr("version"), Value: strToPtr("5")},
				},
			},
			expectedName:  "processor.flows",
			expectedValue: 10.0,
			expectedTags:  []string{"device_ip:1.2.3.4", "flow_type:netflow5"},
			expectedErr:   "",
		},
		{
			name: "METRIC flow_decoder_count",
			metricFamily: &promClient.MetricFamily{
				Name: strToPtr("flow_decoder_count"),
			},
			metric: &promClient.Metric{
				Counter: &promClient.Counter{Value: float64ToPtr(10)},
				Label: []*promClient.LabelPair{
					{Name: strToPtr("name"), Value: strToPtr("NetFlowV5")},
					{Name: strToPtr("worker"), Value: strToPtr("1")},
				},
			},
			expectedName:  "decoder.messages",
			expectedValue: 10.0,
			expectedTags:  []string{"name:netflow5", "worker:1"},
			expectedErr:   "",
		},
		{
			name: "METRIC flow_decoder_error_count",
			metricFamily: &promClient.MetricFamily{
				Name: strToPtr("flow_decoder_error_count"),
			},
			metric: &promClient.Metric{
				Counter: &promClient.Counter{Value: float64ToPtr(10)},
				Label: []*promClient.LabelPair{
					{Name: strToPtr("name"), Value: strToPtr("NetFlowV5")},
					{Name: strToPtr("worker"), Value: strToPtr("1")},
				},
			},
			expectedName:  "decoder.errors",
			expectedValue: 10.0,
			expectedTags:  []string{"name:netflow5", "worker:1"},
			expectedErr:   "",
		},
		{
			name: "METRIC flow_process_nf_count",
			metricFamily: &promClient.MetricFamily{
				Name: strToPtr("flow_process_nf_count"),
			},
			metric: &promClient.Metric{
				Counter: &promClient.Counter{Value: float64ToPtr(10)},
				Label: []*promClient.LabelPair{
					{Name: strToPtr("router"), Value: strToPtr("1.2.3.4")},
					{Name: strToPtr("version"), Value: strToPtr("5")},
				},
			},
			expectedName:  "processor.flows",
			expectedValue: 10.0,
			expectedTags:  []string{"device_ip:1.2.3.4", "flow_type:netflow5"},
			expectedErr:   "",
		},
		{
			name: "METRIC flow_process_nf_flowset_sum",
			metricFamily: &promClient.MetricFamily{
				Name: strToPtr("flow_process_nf_flowset_sum"),
			},
			metric: &promClient.Metric{
				Counter: &promClient.Counter{Value: float64ToPtr(10)},
				Label: []*promClient.LabelPair{
					{Name: strToPtr("router"), Value: strToPtr("1.2.3.4")},
					{Name: strToPtr("type"), Value: strToPtr("DataFlowSet")},
					{Name: strToPtr("version"), Value: strToPtr("5")},
				},
			},
			expectedName:  "processor.flowsets",
			expectedValue: 10.0,
			expectedTags:  []string{"device_ip:1.2.3.4", "flow_type:netflow5", "type:data_flow_set"},
			expectedErr:   "",
		},
		{
			name: "METRIC flow_traffic_bytes",
			metricFamily: &promClient.MetricFamily{
				Name: strToPtr("flow_traffic_bytes"),
			},
			metric: &promClient.Metric{
				Counter: &promClient.Counter{Value: float64ToPtr(10)},
				Label: []*promClient.LabelPair{
					{Name: strToPtr("remote_ip"), Value: strToPtr("1.2.3.4")},
					{Name: strToPtr("local_port"), Value: strToPtr("2000")},
					{Name: strToPtr("name"), Value: strToPtr("NetFlowV5")},
				},
			},
			expectedName:  "traffic.bytes",
			expectedValue: 10.0,
			expectedTags:  []string{"device_ip:1.2.3.4", "listener_port:2000", "flow_type:netflow5"},
			expectedErr:   "",
		},
		{
			name: "METRIC flow_traffic_packets",
			metricFamily: &promClient.MetricFamily{
				Name: strToPtr("flow_traffic_packets"),
			},
			metric: &promClient.Metric{
				Counter: &promClient.Counter{Value: float64ToPtr(10)},
				Label: []*promClient.LabelPair{
					{Name: strToPtr("remote_ip"), Value: strToPtr("1.2.3.4")},
					{Name: strToPtr("local_port"), Value: strToPtr("2000")},
					{Name: strToPtr("name"), Value: strToPtr("NetFlowV5")},
				},
			},
			expectedName:  "traffic.packets",
			expectedValue: 10.0,
			expectedTags:  []string{"device_ip:1.2.3.4", "listener_port:2000", "flow_type:netflow5"},
			expectedErr:   "",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			name, value, tags, err := ConvertMetric(tt.metric, tt.metricFamily)
			assert.Equal(t, tt.expectedName, name)
			assert.Equal(t, tt.expectedValue, value)
			assert.ElementsMatch(t, tt.expectedTags, tags)
			if err != nil {
				assert.EqualError(t, err, tt.expectedErr)
			}
		})
	}
}

func strToPtr(s string) *string {
	return &s
}

func float64ToPtr(s float64) *float64 {
	return &s
}
