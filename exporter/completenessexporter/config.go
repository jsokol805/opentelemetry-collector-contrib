// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package completenessexporter // import "github.com/open-telemetry/opentelemetry-collector-contrib/exporter/completenessexporter"

import (
	"errors"
	"fmt"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/config/configoptional"
	"go.opentelemetry.io/collector/exporter/exporterhelper"
)

// defaultBucketAttribute is the log record attribute the exporter reads the
// ingestion time bucket from. It matches the attribute name suggested in the
// README for the "create" side of the pipeline.
const defaultBucketAttribute = "pipeline.ingestion_bucket"

// Config holds the configuration of the completeness exporter.
type Config struct {
	// Segment identifies the pipeline segment this exporter reports for. It is
	// emitted as the "segment" attribute of the acknowledgment metric, and is
	// what makes the metrics of several collectors distinguishable from each
	// other. This setting is required.
	Segment string `mapstructure:"segment"`

	// BucketAttribute is the name of the log record attribute holding the
	// ingestion time bucket. Records that do not carry the attribute are counted
	// under the "unknown" bucket.
	BucketAttribute string `mapstructure:"bucket_attribute"`

	// Exporter describes the exporter to wrap.
	Exporter WrappedExporter `mapstructure:"exporter"`

	// QueueConfig is the sending queue of this exporter. It sits in front of the
	// acknowledgment point, which is what keeps the acknowledgments meaningful:
	// the queue of the wrapped exporter is disabled, so records are counted when
	// the backend accepted them and not when they were enqueued.
	QueueConfig configoptional.Optional[exporterhelper.QueueBatchConfig] `mapstructure:"sending_queue"`

	// prevent unkeyed literal initialization
	_ struct{}
}

// WrappedExporter identifies and configures the exporter that does the actual
// sending.
type WrappedExporter struct {
	// Type is the component type of the exporter to wrap, for example "otlp" or
	// "clickhouse". It must be part of the collector build.
	Type string `mapstructure:"type"`

	// Config is the configuration of the wrapped exporter, exactly as it would be
	// written under its own key in the `exporters` section. It is validated when
	// the collector starts, because the configuration of a component type that is
	// only known at runtime cannot be checked earlier.
	Config map[string]any `mapstructure:"config"`

	// prevent unkeyed literal initialization
	_ struct{}
}

// Validate checks that the configuration is valid.
func (cfg *Config) Validate() error {
	if cfg.Segment == "" {
		return errors.New("segment must be set to a non-empty identifier of the pipeline segment")
	}
	if cfg.BucketAttribute == "" {
		return errors.New("bucket_attribute must not be empty")
	}
	if cfg.Exporter.Type == "" {
		return errors.New("exporter::type must be set to the type of the exporter to wrap")
	}
	if _, err := component.NewType(cfg.Exporter.Type); err != nil {
		return fmt.Errorf("exporter::type %q is not a valid component type: %w", cfg.Exporter.Type, err)
	}
	return nil
}
