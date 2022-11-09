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
			expectedName:  "decoder.messages_decoded",
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
			expectedName:  "decoder.messages_decoded",
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
			expectedTags:  []string{"device_ip:1.2.3.4", "version:netflow5"},
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
			expectedName:  "decoder.messages_decoded",
			expectedValue: 10.0,
			expectedTags:  []string{"name:netflow5", "worker:1"},
			expectedErr:   "",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			name, value, tags, err := ConvertMetric(tt.metric, tt.metricFamily)
			assert.Equal(t, tt.expectedName, name)
			assert.Equal(t, tt.expectedValue, value)
			assert.Equal(t, tt.expectedTags, tags)
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
