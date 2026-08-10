// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package completenessexporter // import "github.com/open-telemetry/opentelemetry-collector-contrib/exporter/completenessexporter"

import (
	"context"
	"errors"
	"fmt"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/confmap"
	"go.opentelemetry.io/collector/confmap/xconfmap"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/service/hostcapabilities"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/completenessexporter/internal/metadata"
)

const (
	// unknownBucket is the value reported for log records that do not carry the
	// configured bucket attribute.
	unknownBucket = "unknown"

	bucketAttrKey  = "bucket"
	segmentAttrKey = "segment"

	// sendingQueueKey is the configuration key of the sending queue, which this
	// exporter takes over from the exporter it wraps.
	sendingQueueKey = "sending_queue"
)

type completenessExporter struct {
	set              exporter.Settings
	cfg              *Config
	innerType        component.Type
	segment          attribute.KeyValue
	telemetryBuilder *metadata.TelemetryBuilder

	// inner is the wrapped exporter. It is created on start, because the factory
	// of a component type that is only known from the configuration can only be
	// looked up on the host.
	inner exporter.Logs
}

func newCompletenessExporter(set exporter.Settings, cfg *Config) (*completenessExporter, error) {
	innerType, err := component.NewType(cfg.Exporter.Type)
	if err != nil {
		return nil, fmt.Errorf("invalid exporter::type %q: %w", cfg.Exporter.Type, err)
	}
	telemetryBuilder, err := metadata.NewTelemetryBuilder(set.TelemetrySettings)
	if err != nil {
		return nil, err
	}
	return &completenessExporter{
		set:              set,
		cfg:              cfg,
		innerType:        innerType,
		segment:          attribute.String(segmentAttrKey, cfg.Segment),
		telemetryBuilder: telemetryBuilder,
	}, nil
}

// start looks up the factory of the wrapped exporter on the host, builds it from
// the nested configuration and starts it.
func (e *completenessExporter) start(ctx context.Context, host component.Host) error {
	factories, ok := host.(hostcapabilities.ComponentFactory)
	if !ok {
		return errors.New("the collector host does not expose component factories, the exporter to wrap cannot be created")
	}
	factory, ok := factories.GetFactory(component.KindExporter, e.innerType).(exporter.Factory)
	if !ok {
		return fmt.Errorf("exporter type %q is not part of this collector build", e.innerType)
	}

	innerCfg, err := e.wrappedConfig(factory)
	if err != nil {
		return err
	}

	innerSet := e.set
	innerSet.ID = e.wrappedID()
	inner, err := factory.CreateLogs(ctx, innerSet, innerCfg)
	if err != nil {
		return fmt.Errorf("cannot create the wrapped %q exporter: %w", e.innerType, err)
	}
	e.inner = inner

	return inner.Start(ctx, host)
}

// wrappedConfig builds the configuration of the wrapped exporter and takes its
// sending queue over.
func (e *completenessExporter) wrappedConfig(factory exporter.Factory) (component.Config, error) {
	innerCfg := factory.CreateDefaultConfig()
	if err := confmap.NewFromStringMap(e.cfg.Exporter.Config).Unmarshal(innerCfg); err != nil {
		return nil, fmt.Errorf("cannot load the configuration of the wrapped %q exporter: %w", e.innerType, err)
	}
	if err := xconfmap.Validate(innerCfg); err != nil {
		return nil, fmt.Errorf("invalid configuration for the wrapped %q exporter: %w", e.innerType, err)
	}

	if _, ok := e.cfg.Exporter.Config[sendingQueueKey]; ok {
		e.set.Logger.Warn(
			"The sending queue configured on the wrapped exporter is ignored. Configure it on the completeness exporter instead, so that records are counted after the queue.",
			zap.Stringer("wrapped_exporter", e.innerType),
		)
	}
	if disableQueue(innerCfg) {
		e.set.Logger.Info(
			"Disabled the sending queue of the wrapped exporter, its calls now block until the backend accepted the data",
			zap.Stringer("wrapped_exporter", e.innerType),
		)
	} else {
		e.set.Logger.Warn(
			"The wrapped exporter has no sending queue that could be disabled. If it acknowledges data before it reached the backend, so does this exporter.",
			zap.Stringer("wrapped_exporter", e.innerType),
		)
	}
	return innerCfg, nil
}

// wrappedID is the component ID the wrapped exporter reports its own internal
// telemetry under.
func (e *completenessExporter) wrappedID() component.ID {
	name := metadata.Type.String()
	if e.set.ID.Name() != "" {
		name += "_" + e.set.ID.Name()
	}
	return component.NewIDWithName(e.innerType, name)
}

func (e *completenessExporter) shutdown(ctx context.Context) error {
	e.telemetryBuilder.Shutdown()
	if e.inner == nil {
		return nil
	}
	return e.inner.Shutdown(ctx)
}

// pushLogs hands the batch to the wrapped exporter and, only once that exporter
// reported success, counts the records it contained per ingestion bucket. It is
// called after this exporter's sending queue, so a success means the data was
// accepted by the backend.
func (e *completenessExporter) pushLogs(ctx context.Context, ld plog.Logs) error {
	// The records have to be counted before the batch is handed over: once the
	// wrapped exporter got it, the data may be modified or recycled.
	counts := e.countByBucket(ld)

	if err := e.inner.ConsumeLogs(ctx, ld); err != nil {
		return err
	}

	for bucket, count := range counts {
		e.telemetryBuilder.CompletenessAcks.Add(ctx, count, metric.WithAttributes(
			attribute.String(bucketAttrKey, bucket),
			e.segment,
		))
	}
	return nil
}

// countByBucket returns the number of log records in the batch, keyed by the
// value of the bucket attribute.
func (e *completenessExporter) countByBucket(ld plog.Logs) map[string]int64 {
	counts := make(map[string]int64)
	resourceLogs := ld.ResourceLogs()
	for i := 0; i < resourceLogs.Len(); i++ {
		rl := resourceLogs.At(i)
		resourceBucket := e.lookupBucket(rl.Resource().Attributes(), unknownBucket)
		scopeLogs := rl.ScopeLogs()
		for j := 0; j < scopeLogs.Len(); j++ {
			sl := scopeLogs.At(j)
			scopeBucket := e.lookupBucket(sl.Scope().Attributes(), resourceBucket)
			logRecords := sl.LogRecords()
			for k := 0; k < logRecords.Len(); k++ {
				counts[e.lookupBucket(logRecords.At(k).Attributes(), scopeBucket)]++
			}
		}
	}
	return counts
}

// lookupBucket returns the bucket attribute of the given attribute map, or
// fallback if the map does not hold a usable value. Record attributes take
// precedence over scope attributes, which take precedence over resource
// attributes.
func (e *completenessExporter) lookupBucket(attrs pcommon.Map, fallback string) string {
	val, ok := attrs.Get(e.cfg.BucketAttribute)
	if !ok || val.Type() == pcommon.ValueTypeEmpty {
		return fallback
	}
	if bucket := val.AsString(); bucket != "" {
		return bucket
	}
	return fallback
}
