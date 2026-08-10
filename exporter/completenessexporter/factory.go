// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package completenessexporter // import "github.com/open-telemetry/opentelemetry-collector-contrib/exporter/completenessexporter"

import (
	"context"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/config/configoptional"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/exporter/exporterhelper"

	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/completenessexporter/internal/metadata"
)

// NewFactory returns a new factory for the completeness exporter.
func NewFactory() exporter.Factory {
	return exporter.NewFactory(
		metadata.Type,
		createDefaultConfig,
		exporter.WithLogs(createLogsExporter, metadata.LogsStability),
	)
}

func createDefaultConfig() component.Config {
	// The wrapped exporter loses its own queue, so this one defaults to the
	// standard queue settings to keep the pipeline behaving the same way.
	queue := exporterhelper.NewDefaultQueueConfig()
	// Except for the overflow behavior: dropping records the moment the backend
	// cannot keep up defeats the purpose of measuring completeness, so the queue
	// pushes back on the pipeline instead.
	queue.BlockOnOverflow = true

	return &Config{
		BucketAttribute: defaultBucketAttribute,
		QueueConfig:     configoptional.Some(queue),
	}
}

func createLogsExporter(
	ctx context.Context,
	set exporter.Settings,
	cfg component.Config,
) (exporter.Logs, error) {
	oCfg := cfg.(*Config)
	exp, err := newCompletenessExporter(set, oCfg)
	if err != nil {
		return nil, err
	}
	logs, err := exporterhelper.NewLogs(
		ctx,
		set,
		cfg,
		exp.pushLogs,
		exporterhelper.WithStart(exp.start),
		exporterhelper.WithShutdown(exp.shutdown),
		// The wrapped exporter may modify the data it is given.
		exporterhelper.WithCapabilities(consumer.Capabilities{MutatesData: true}),
		// Timeouts and retries stay with the wrapped exporter, which is where the
		// backend specific settings live.
		exporterhelper.WithTimeout(exporterhelper.TimeoutConfig{Timeout: 0}),
		exporterhelper.WithQueue(oCfg.QueueConfig),
	)
	if err != nil {
		return nil, err
	}
	return queueAware{Logs: logs, exp: exp}, nil
}
