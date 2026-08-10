// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package completenessexporter

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/config/configoptional"
	"go.opentelemetry.io/collector/confmap/confmaptest"
	"go.opentelemetry.io/collector/confmap/xconfmap"
	"go.opentelemetry.io/collector/exporter/exporterhelper"

	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/completenessexporter/internal/metadata"
)

func TestLoadConfig(t *testing.T) {
	t.Parallel()

	biggerQueue := exporterhelper.NewDefaultQueueConfig()
	biggerQueue.QueueSize = 5000

	tests := []struct {
		id           component.ID
		expected     component.Config
		errorMessage string
	}{
		{
			id: component.NewIDWithName(metadata.Type, ""),
			expected: &Config{
				Segment:         "daemonset-collector",
				BucketAttribute: defaultBucketAttribute,
				Exporter: WrappedExporter{
					Type:   "otlp",
					Config: map[string]any{"endpoint": "central-collector:4317"},
				},
				QueueConfig: configoptional.Some(exporterhelper.NewDefaultQueueConfig()),
			},
		},
		{
			id: component.NewIDWithName(metadata.Type, "full"),
			expected: &Config{
				Segment:         "central-collector",
				BucketAttribute: "my.custom.bucket",
				Exporter: WrappedExporter{
					Type:   "clickhouse",
					Config: map[string]any{"endpoint": "tcp://clickhouse:9000"},
				},
				QueueConfig: configoptional.Some(biggerQueue),
			},
		},
		{
			id:           component.NewIDWithName(metadata.Type, "missing_segment"),
			errorMessage: "segment must be set",
		},
		{
			id:           component.NewIDWithName(metadata.Type, "missing_exporter"),
			errorMessage: "exporter::type must be set",
		},
		{
			id:           component.NewIDWithName(metadata.Type, "empty_bucket_attribute"),
			errorMessage: "bucket_attribute must not be empty",
		},
	}

	for _, tt := range tests {
		t.Run(tt.id.String(), func(t *testing.T) {
			cm, err := confmaptest.LoadConf(filepath.Join("testdata", "config.yaml"))
			require.NoError(t, err)

			factory := NewFactory()
			cfg := factory.CreateDefaultConfig()

			sub, err := cm.Sub(tt.id.String())
			require.NoError(t, err)
			require.NoError(t, sub.Unmarshal(cfg))

			if tt.expected == nil {
				assert.ErrorContains(t, xconfmap.Validate(cfg), tt.errorMessage)
				return
			}
			assert.NoError(t, xconfmap.Validate(cfg))
			assert.Equal(t, tt.expected, cfg)
		})
	}
}

func TestValidateRejectsInvalidExporterType(t *testing.T) {
	cfg := createDefaultConfig().(*Config)
	cfg.Segment = "a-segment"
	cfg.Exporter.Type = "not a type"
	assert.ErrorContains(t, cfg.Validate(), "is not a valid component type")
}

func TestDefaultConfig(t *testing.T) {
	cfg := createDefaultConfig().(*Config)
	assert.Equal(t, defaultBucketAttribute, cfg.BucketAttribute)
	assert.True(t, cfg.QueueConfig.HasValue(), "the queue this exporter takes over from the wrapped one is on by default")
	// Neither the segment nor the exporter to wrap have a meaningful default.
	assert.Error(t, cfg.Validate())
}
