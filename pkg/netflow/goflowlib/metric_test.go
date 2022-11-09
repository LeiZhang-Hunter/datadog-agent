package goflowlib

import (
	"github.com/DataDog/datadog-agent/pkg/metrics"
	promClient "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/assert"
	"testing"
)

func strToPtr(s string) *string {
	return &s
}

func float64ToPtr(s float64) *float64 {
	return &s
}

func TestConvertMetric(t *testing.T) {
	tests := []struct {
		name               string
		metric             *promClient.Metric
		metricFamily       *promClient.MetricFamily
		expectedMetricType metrics.MetricType
		expectedName       string
		expectedValue      float64
		expectedTags       []string
		expectedErr        string
	}{
		{
			name: "FEATURE ignore non allowed field",
			metricFamily: &promClient.MetricFamily{
				Name: strToPtr("flow_decoder_count"),
				Type: promClient.MetricType_COUNTER.Enum(),
			},
			metric: &promClient.Metric{
				Counter: &promClient.Counter{Value: float64ToPtr(10)},
				Label: []*promClient.LabelPair{
					{Name: strToPtr("worker"), Value: strToPtr("1")},
					{Name: strToPtr("notAllowedField"), Value: strToPtr("1")},
				},
			},
			expectedMetricType: metrics.MonotonicCountType,
			expectedName:       "decoder.messages",
			expectedValue:      10.0,
			expectedTags:       []string{"worker:1"},
			expectedErr:        "",
		},
		{
			name: "FEATURE valueRemapper",
			metricFamily: &promClient.MetricFamily{
				Name: strToPtr("flow_decoder_count"),
				Type: promClient.MetricType_COUNTER.Enum(),
			},
			metric: &promClient.Metric{
				Counter: &promClient.Counter{Value: float64ToPtr(10)},
				Label: []*promClient.LabelPair{
					{Name: strToPtr("name"), Value: strToPtr("NetFlowV5")},
					{Name: strToPtr("worker"), Value: strToPtr("1")},
					{Name: strToPtr("notAllowedField"), Value: strToPtr("1")},
				},
			},
			expectedMetricType: metrics.MonotonicCountType,
			expectedName:       "decoder.messages",
			expectedValue:      10.0,
			expectedTags:       []string{"name:netflow5", "worker:1"},
			expectedErr:        "",
		},
		{
			name: "FEATURE keyRemapper",
			metricFamily: &promClient.MetricFamily{
				Name: strToPtr("flow_process_nf_count"),
				Type: promClient.MetricType_COUNTER.Enum(),
			},
			metric: &promClient.Metric{
				Counter: &promClient.Counter{Value: float64ToPtr(10)},
				Label: []*promClient.LabelPair{
					{Name: strToPtr("router"), Value: strToPtr("1.2.3.4")},
					{Name: strToPtr("version"), Value: strToPtr("5")},
				},
			},
			expectedMetricType: metrics.MonotonicCountType,
			expectedName:       "processor.flows",
			expectedValue:      10.0,
			expectedTags:       []string{"device_ip:1.2.3.4", "flow_type:netflow5"},
			expectedErr:        "",
		},
		{
			name: "FEATURE submit MonotonicCountType",
			metricFamily: &promClient.MetricFamily{
				Name: strToPtr("flow_process_nf_count"),
				Type: promClient.MetricType_COUNTER.Enum(),
			},
			metric: &promClient.Metric{
				Counter: &promClient.Counter{Value: float64ToPtr(10)},
				Label: []*promClient.LabelPair{
					{Name: strToPtr("router"), Value: strToPtr("1.2.3.4")},
					{Name: strToPtr("version"), Value: strToPtr("5")},
				},
			},
			expectedMetricType: metrics.MonotonicCountType,
			expectedName:       "processor.flows",
			expectedValue:      10.0,
			expectedTags:       []string{"device_ip:1.2.3.4", "flow_type:netflow5"},
			expectedErr:        "",
		},
		{
			name: "FEATURE submit GaugeType",
			metricFamily: &promClient.MetricFamily{
				Name: strToPtr("flow_process_nf_count"),
				Type: promClient.MetricType_GAUGE.Enum(),
			},
			metric: &promClient.Metric{
				Gauge: &promClient.Gauge{Value: float64ToPtr(10)},
				Label: []*promClient.LabelPair{
					{Name: strToPtr("router"), Value: strToPtr("1.2.3.4")},
					{Name: strToPtr("version"), Value: strToPtr("5")},
				},
			},
			expectedMetricType: metrics.GaugeType,
			expectedName:       "processor.flows",
			expectedValue:      10.0,
			expectedTags:       []string{"device_ip:1.2.3.4", "flow_type:netflow5"},
			expectedErr:        "",
		},
		// TODO: test error cases
		{
			name: "METRIC flow_decoder_count",
			metricFamily: &promClient.MetricFamily{
				Name: strToPtr("flow_decoder_count"),
				Type: promClient.MetricType_COUNTER.Enum(),
			},
			metric: &promClient.Metric{
				Counter: &promClient.Counter{Value: float64ToPtr(10)},
				Label: []*promClient.LabelPair{
					{Name: strToPtr("name"), Value: strToPtr("NetFlowV5")},
					{Name: strToPtr("worker"), Value: strToPtr("1")},
				},
			},
			expectedMetricType: metrics.MonotonicCountType,
			expectedName:       "decoder.messages",
			expectedValue:      10.0,
			expectedTags:       []string{"name:netflow5", "worker:1"},
			expectedErr:        "",
		},
		{
			name: "METRIC flow_decoder_error_count",
			metricFamily: &promClient.MetricFamily{
				Name: strToPtr("flow_decoder_error_count"),
				Type: promClient.MetricType_COUNTER.Enum(),
			},
			metric: &promClient.Metric{
				Counter: &promClient.Counter{Value: float64ToPtr(10)},
				Label: []*promClient.LabelPair{
					{Name: strToPtr("name"), Value: strToPtr("NetFlowV5")},
					{Name: strToPtr("worker"), Value: strToPtr("1")},
				},
			},
			expectedMetricType: metrics.MonotonicCountType,
			expectedName:       "decoder.errors",
			expectedValue:      10.0,
			expectedTags:       []string{"name:netflow5", "worker:1"},
			expectedErr:        "",
		},
		{
			name: "METRIC flow_process_nf_count",
			metricFamily: &promClient.MetricFamily{
				Name: strToPtr("flow_process_nf_count"),
				Type: promClient.MetricType_COUNTER.Enum(),
			},
			metric: &promClient.Metric{
				Counter: &promClient.Counter{Value: float64ToPtr(10)},
				Label: []*promClient.LabelPair{
					{Name: strToPtr("router"), Value: strToPtr("1.2.3.4")},
					{Name: strToPtr("version"), Value: strToPtr("5")},
				},
			},
			expectedMetricType: metrics.MonotonicCountType,
			expectedName:       "processor.flows",
			expectedValue:      10.0,
			expectedTags:       []string{"device_ip:1.2.3.4", "flow_type:netflow5"},
			expectedErr:        "",
		},
		{
			name: "METRIC flow_process_nf_flowset_sum",
			metricFamily: &promClient.MetricFamily{
				Name: strToPtr("flow_process_nf_flowset_sum"),
				Type: promClient.MetricType_COUNTER.Enum(),
			},
			metric: &promClient.Metric{
				Counter: &promClient.Counter{Value: float64ToPtr(10)},
				Label: []*promClient.LabelPair{
					{Name: strToPtr("router"), Value: strToPtr("1.2.3.4")},
					{Name: strToPtr("type"), Value: strToPtr("DataFlowSet")},
					{Name: strToPtr("version"), Value: strToPtr("5")},
				},
			},
			expectedMetricType: metrics.MonotonicCountType,
			expectedName:       "processor.flowsets",
			expectedValue:      10.0,
			expectedTags:       []string{"device_ip:1.2.3.4", "flow_type:netflow5", "type:data_flow_set"},
			expectedErr:        "",
		},
		{
			name: "METRIC flow_traffic_bytes",
			metricFamily: &promClient.MetricFamily{
				Name: strToPtr("flow_traffic_bytes"),
				Type: promClient.MetricType_COUNTER.Enum(),
			},
			metric: &promClient.Metric{
				Counter: &promClient.Counter{Value: float64ToPtr(10)},
				Label: []*promClient.LabelPair{
					{Name: strToPtr("remote_ip"), Value: strToPtr("1.2.3.4")},
					{Name: strToPtr("local_port"), Value: strToPtr("2000")},
					{Name: strToPtr("name"), Value: strToPtr("NetFlowV5")},
				},
			},
			expectedMetricType: metrics.MonotonicCountType,
			expectedName:       "traffic.bytes",
			expectedValue:      10.0,
			expectedTags:       []string{"device_ip:1.2.3.4", "listener_port:2000", "flow_type:netflow5"},
			expectedErr:        "",
		},
		{
			name: "METRIC flow_traffic_packets",
			metricFamily: &promClient.MetricFamily{
				Name: strToPtr("flow_traffic_packets"),
				Type: promClient.MetricType_COUNTER.Enum(),
			},
			metric: &promClient.Metric{
				Counter: &promClient.Counter{Value: float64ToPtr(10)},
				Label: []*promClient.LabelPair{
					{Name: strToPtr("remote_ip"), Value: strToPtr("1.2.3.4")},
					{Name: strToPtr("local_port"), Value: strToPtr("2000")},
					{Name: strToPtr("name"), Value: strToPtr("NetFlowV5")},
				},
			},
			expectedMetricType: metrics.MonotonicCountType,
			expectedName:       "traffic.packets",
			expectedValue:      10.0,
			expectedTags:       []string{"device_ip:1.2.3.4", "listener_port:2000", "flow_type:netflow5"},
			expectedErr:        "",
		},
		{
			name: "METRIC flow_process_sf_count",
			metricFamily: &promClient.MetricFamily{
				Name: strToPtr("flow_process_sf_count"),
				Type: promClient.MetricType_COUNTER.Enum(),
			},
			metric: &promClient.Metric{
				Counter: &promClient.Counter{Value: float64ToPtr(10)},
				Label: []*promClient.LabelPair{
					{Name: strToPtr("router"), Value: strToPtr("1.2.3.4")},
					{Name: strToPtr("version"), Value: strToPtr("5")},
				},
			},
			expectedMetricType: metrics.MonotonicCountType,
			expectedName:       "processor.flows",
			expectedValue:      10.0,
			expectedTags:       []string{"device_ip:1.2.3.4", "flow_type:sflow5"},
			expectedErr:        "",
		},
		{
			name: "METRIC flow_process_sf_errors_count",
			metricFamily: &promClient.MetricFamily{
				Name: strToPtr("flow_process_sf_errors_count"),
				Type: promClient.MetricType_COUNTER.Enum(),
			},
			metric: &promClient.Metric{
				Counter: &promClient.Counter{Value: float64ToPtr(10)},
				Label: []*promClient.LabelPair{
					{Name: strToPtr("router"), Value: strToPtr("1.2.3.4")},
					{Name: strToPtr("error"), Value: strToPtr("some-error")},
				},
			},
			expectedMetricType: metrics.MonotonicCountType,
			expectedName:       "processor.errors",
			expectedValue:      10.0,
			expectedTags:       []string{"device_ip:1.2.3.4", "error:some-error"},
			expectedErr:        "",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			metricType, name, value, tags, err := ConvertMetric(tt.metric, tt.metricFamily)
			assert.Equal(t, tt.expectedMetricType, metricType)
			assert.Equal(t, tt.expectedName, name)
			assert.Equal(t, tt.expectedValue, value)
			assert.ElementsMatch(t, tt.expectedTags, tags)
			if err != nil {
				assert.EqualError(t, err, tt.expectedErr)
			}
		})
	}
}
