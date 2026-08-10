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
	"go.opentelemetry.io/collector/consumer/consumererror"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/exporter/exporterhelper"
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
	reasonAttrKey  = "reason"

	// Reasons why records were given up on, reported on the drops metric.
	//
	// reasonSendFailed is the wrapped exporter failing to send a whole batch,
	// reasonPartiallyRejected the records of a batch it reported as failed, and
	// reasonQueueFull a batch the sending queue of this exporter could not accept.
	reasonSendFailed        = "send_failed"
	reasonPartiallyRejected = "partially_rejected"
	reasonQueueFull         = "queue_full"

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

// pushLogs hands the batch to the wrapped exporter and counts the records it
// contained per ingestion bucket: the delivered ones as acknowledgments, the
// ones that were given up on as drops. It is called after this exporter's
// sending queue, so an acknowledgment means the data was accepted by the
// backend.
func (e *completenessExporter) pushLogs(ctx context.Context, ld plog.Logs) error {
	// The records have to be counted before the batch is handed over: once the
	// wrapped exporter got it, the data may be modified or recycled.
	counts := e.countByBucket(ld)

	err := e.inner.ConsumeLogs(ctx, ld)
	if err == nil {
		e.recordAcks(ctx, counts)
		return nil
	}

	// An exporter may report exactly which records it could not send. The rest of
	// the batch did reach the backend and is acknowledged.
	var partial consumererror.Logs
	if errors.As(err, &partial) {
		rejected := e.countByBucket(partial.Data())
		e.recordDrops(ctx, rejected, reasonPartiallyRejected)
		e.recordAcks(ctx, subtractCounts(counts, rejected))
		return err
	}

	// This exporter does not retry, that is left to the wrapped exporter, so an
	// error means the batch is gone.
	e.recordDrops(ctx, counts, reasonSendFailed)
	return err
}

func (e *completenessExporter) recordAcks(ctx context.Context, counts map[string]int64) {
	for bucket, count := range counts {
		if count <= 0 {
			continue
		}
		e.telemetryBuilder.CompletenessAcks.Add(ctx, count, metric.WithAttributes(
			attribute.String(bucketAttrKey, bucket),
			e.segment,
		))
	}
}

func (e *completenessExporter) recordDrops(ctx context.Context, counts map[string]int64, reason string) {
	for bucket, count := range counts {
		if count <= 0 {
			continue
		}
		e.telemetryBuilder.CompletenessDrops.Add(ctx, count, metric.WithAttributes(
			attribute.String(bucketAttrKey, bucket),
			e.segment,
			attribute.String(reasonAttrKey, reason),
		))
	}
}

// subtractCounts returns the records of counts that are not in rejected.
func subtractCounts(counts, rejected map[string]int64) map[string]int64 {
	delivered := make(map[string]int64, len(counts))
	for bucket, count := range counts {
		delivered[bucket] = count - rejected[bucket]
	}
	return delivered
}

// queueAware accounts for the records this exporter's own sending queue refused
// to accept. Those never reach pushLogs, so they have to be counted in front of
// the queue.
type queueAware struct {
	exporter.Logs
	exp *completenessExporter
}

func (q queueAware) ConsumeLogs(ctx context.Context, ld plog.Logs) error {
	err := q.Logs.ConsumeLogs(ctx, ld)
	if errors.Is(err, exporterhelper.ErrQueueIsFull) {
		q.exp.recordDrops(ctx, q.exp.countByBucket(ld), reasonQueueFull)
	}
	return err
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
