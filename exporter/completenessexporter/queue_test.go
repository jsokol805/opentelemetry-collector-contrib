// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package completenessexporter

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/config/configoptional"
	"go.opentelemetry.io/collector/exporter/exporterhelper"
	"go.opentelemetry.io/collector/exporter/otlpexporter"
)

type QueueAtTopLevel struct {
	Endpoint string
	Queue    configoptional.Optional[exporterhelper.QueueBatchConfig] `mapstructure:"sending_queue"`
}

type embeddedQueue struct {
	QueueAtTopLevel `mapstructure:",squash"`
	Extra           string
}

type nestedQueue struct {
	Inner     QueueAtTopLevel
	Pointer   *QueueAtTopLevel
	Untouched configoptional.Optional[exporterhelper.BatchConfig] `mapstructure:"batch"`
}

type withoutQueue struct {
	Endpoint string
}

func someQueue() configoptional.Optional[exporterhelper.QueueBatchConfig] {
	return configoptional.Some(exporterhelper.NewDefaultQueueConfig())
}

func TestDisableQueue(t *testing.T) {
	t.Run("top level", func(t *testing.T) {
		cfg := &QueueAtTopLevel{Endpoint: "localhost", Queue: someQueue()}
		assert.True(t, disableQueue(cfg))
		assert.False(t, cfg.Queue.HasValue())
		assert.Equal(t, "localhost", cfg.Endpoint)
	})

	t.Run("embedded struct", func(t *testing.T) {
		cfg := &embeddedQueue{QueueAtTopLevel: QueueAtTopLevel{Queue: someQueue()}}
		assert.True(t, disableQueue(cfg))
		assert.False(t, cfg.Queue.HasValue())
	})

	t.Run("nested structs and pointers", func(t *testing.T) {
		cfg := &nestedQueue{
			Inner:     QueueAtTopLevel{Queue: someQueue()},
			Pointer:   &QueueAtTopLevel{Queue: someQueue()},
			Untouched: configoptional.Some(exporterhelper.BatchConfig{MinSize: 10}),
		}
		assert.True(t, disableQueue(cfg))
		assert.False(t, cfg.Inner.Queue.HasValue())
		assert.False(t, cfg.Pointer.Queue.HasValue())
		assert.True(t, cfg.Untouched.HasValue(), "only the sending queue may be touched")
	})

	t.Run("nil pointer", func(t *testing.T) {
		cfg := &nestedQueue{Inner: QueueAtTopLevel{Queue: someQueue()}}
		assert.True(t, disableQueue(cfg))
		assert.False(t, cfg.Inner.Queue.HasValue())
	})

	t.Run("no queue at all", func(t *testing.T) {
		assert.False(t, disableQueue(&withoutQueue{Endpoint: "localhost"}))
	})

	t.Run("not a pointer to a struct", func(t *testing.T) {
		assert.False(t, disableQueue(QueueAtTopLevel{Queue: someQueue()}))
		assert.False(t, disableQueue((*QueueAtTopLevel)(nil)))
	})
}

// The queue is looked up by type, so it has to be found in the configuration of
// a real exporter and not only in the shapes made up above.
func TestDisableQueueOnRealExporterConfig(t *testing.T) {
	cfg := otlpexporter.NewFactory().CreateDefaultConfig()
	require.True(t, disableQueue(cfg), "the sending queue of the otlp exporter must be found")

	assert.False(t, cfg.(*otlpexporter.Config).QueueConfig.HasValue())
}
