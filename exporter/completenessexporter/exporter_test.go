// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package completenessexporter

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/config/configoptional"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/exporter/exporterhelper"
	"go.opentelemetry.io/collector/exporter/exportertest"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.opentelemetry.io/otel/sdk/metric/metricdata/metricdatatest"

	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/completenessexporter/internal/metadata"
	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/completenessexporter/internal/metadatatest"
)

const (
	testSegment = "test-collector"
	fakeType    = "fake"
	acksMetric  = "otel_completeness_acks"
)

func exportertestSettings() exporter.Settings {
	return exportertest.NewNopSettings(metadata.Type)
}

// fakeConfig is shaped like the configuration of a regular exporter: it carries
// a sending queue the completeness exporter is expected to take over.
type fakeConfig struct {
	QueueConfig configoptional.Optional[exporterhelper.QueueBatchConfig] `mapstructure:"sending_queue"`
	Endpoint    string                                                   `mapstructure:"endpoint"`
}

// fakeExporter stands in for the wrapped exporter. It records what it was built
// with and what it was asked to send, and fails on demand.
type fakeExporter struct {
	mu       sync.Mutex
	cfg      *fakeConfig
	consumed []plog.Logs
	err      error
}

func (f *fakeExporter) setErr(err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.err = err
}

func (f *fakeExporter) sendCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.consumed)
}

func (f *fakeExporter) receivedConfig() *fakeConfig {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.cfg
}

func (f *fakeExporter) push(_ context.Context, ld plog.Logs) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.consumed = append(f.consumed, ld)
	return f.err
}

func (f *fakeExporter) factory() exporter.Factory {
	return exporter.NewFactory(
		component.MustNewType(fakeType),
		func() component.Config {
			return &fakeConfig{QueueConfig: configoptional.Some(exporterhelper.NewDefaultQueueConfig())}
		},
		exporter.WithLogs(func(ctx context.Context, set exporter.Settings, cfg component.Config) (exporter.Logs, error) {
			f.mu.Lock()
			f.cfg = cfg.(*fakeConfig)
			queue := f.cfg.QueueConfig
			f.mu.Unlock()
			return exporterhelper.NewLogs(ctx, set, cfg, f.push, exporterhelper.WithQueue(queue))
		}, component.StabilityLevelDevelopment),
	)
}

// factoryHost is a host that exposes component factories, like the collector
// service does.
type factoryHost struct {
	component.Host
	factory exporter.Factory
}

func newFactoryHost(factory exporter.Factory) component.Host {
	return factoryHost{Host: componenttest.NewNopHost(), factory: factory}
}

func (h factoryHost) GetFactory(kind component.Kind, componentType component.Type) component.Factory {
	if kind != component.KindExporter || h.factory == nil || componentType != h.factory.Type() {
		return nil
	}
	return h.factory
}

// testConfig wraps the fake exporter without a queue of its own, so that
// ConsumeLogs returns only once the fake exporter is done.
func testConfig() *Config {
	cfg := createDefaultConfig().(*Config)
	cfg.Segment = testSegment
	cfg.Exporter.Type = fakeType
	cfg.QueueConfig = configoptional.None[exporterhelper.QueueBatchConfig]()
	return cfg
}

func newTestExporter(t *testing.T, cfg *Config, fake *fakeExporter) (exporter.Logs, *componenttest.Telemetry) {
	t.Helper()
	tel := componenttest.NewTelemetry()
	t.Cleanup(func() { require.NoError(t, tel.Shutdown(context.Background())) }) //nolint:usetesting // the test context is already canceled during cleanup

	exp, err := createLogsExporter(t.Context(), metadatatest.NewSettings(tel), cfg)
	require.NoError(t, err)
	require.NoError(t, exp.Start(t.Context(), newFactoryHost(fake.factory())))
	t.Cleanup(func() { require.NoError(t, exp.Shutdown(context.Background())) }) //nolint:usetesting // the test context is already canceled during cleanup
	return exp, tel
}

// logsWithBuckets builds a batch holding one resource with one scope, and one
// log record per given bucket value. An empty bucket value means the record
// carries no bucket attribute at all.
func logsWithBuckets(buckets ...string) plog.Logs {
	ld := plog.NewLogs()
	sl := ld.ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty()
	for _, bucket := range buckets {
		lr := sl.LogRecords().AppendEmpty()
		lr.Body().SetStr("a log record")
		if bucket != "" {
			lr.Attributes().PutStr(defaultBucketAttribute, bucket)
		}
	}
	return ld
}

// ignoreVolatile ignores what this exporter has no control over: the timestamps
// and the exemplars picked up from the span exporterhelper opens around a send.
var ignoreVolatile = []metricdatatest.Option{
	metricdatatest.IgnoreTimestamp(),
	metricdatatest.IgnoreExemplars(),
}

func ackDataPoint(bucket string, value int64) metricdata.DataPoint[int64] {
	return metricdata.DataPoint[int64]{
		Value: value,
		Attributes: attribute.NewSet(
			attribute.String(bucketAttrKey, bucket),
			attribute.String(segmentAttrKey, testSegment),
		),
	}
}

func TestAcksAfterWrappedExporterDelivered(t *testing.T) {
	fake := &fakeExporter{}
	exp, tel := newTestExporter(t, testConfig(), fake)

	require.NoError(t, exp.ConsumeLogs(t.Context(), logsWithBuckets(
		"2026-08-10T21:22:00Z",
		"2026-08-10T21:22:00Z",
		"2026-08-10T21:23:00Z",
	)))
	require.NoError(t, exp.ConsumeLogs(t.Context(), logsWithBuckets("2026-08-10T21:23:00Z")))

	assert.Equal(t, 2, fake.sendCount())
	metadatatest.AssertEqualCompletenessAcks(t, tel, []metricdata.DataPoint[int64]{
		ackDataPoint("2026-08-10T21:22:00Z", 2),
		ackDataPoint("2026-08-10T21:23:00Z", 2),
	}, ignoreVolatile...)
}

func TestNoAcksWhenWrappedExporterFails(t *testing.T) {
	sendErr := errors.New("clickhouse rejected the batch")
	fake := &fakeExporter{err: sendErr}
	exp, tel := newTestExporter(t, testConfig(), fake)

	require.ErrorIs(t, exp.ConsumeLogs(t.Context(), logsWithBuckets("2026-08-10T21:22:00Z")), sendErr)

	_, err := tel.GetMetric(acksMetric)
	assert.Error(t, err, "no acknowledgment must be recorded for a failed export")
}

// The point of wrapping the exporter instead of counting in front of it: with a
// queue in the pipeline, the pipeline call returns long before the data reached
// the backend, and only the delivery may be counted.
func TestAcksFollowDeliveryNotEnqueueing(t *testing.T) {
	sendErr := errors.New("backend is down")
	fake := &fakeExporter{err: sendErr}

	cfg := createDefaultConfig().(*Config)
	cfg.Segment = testSegment
	cfg.Exporter.Type = fakeType
	// An asynchronous queue on this exporter, and the wrapped exporter asking for
	// one as well.
	cfg.QueueConfig = configoptional.Some(noBatchQueue())
	cfg.Exporter.Config = map[string]any{"sending_queue": map[string]any{"queue_size": 100}}

	exp, tel := newTestExporter(t, cfg, fake)

	// The queue accepts the batch without waiting for the backend.
	require.NoError(t, exp.ConsumeLogs(t.Context(), logsWithBuckets("2026-08-10T21:22:00Z")))
	require.Eventually(t, func() bool { return fake.sendCount() == 1 }, 5*time.Second, 10*time.Millisecond)

	_, err := tel.GetMetric(acksMetric)
	require.Error(t, err, "the enqueued batch was never delivered, so nothing may be acknowledged")

	// Once the backend recovers, the delivered records are counted.
	fake.setErr(nil)
	require.NoError(t, exp.ConsumeLogs(t.Context(), logsWithBuckets("2026-08-10T21:23:00Z")))
	require.Eventually(t, func() bool {
		_, err := tel.GetMetric(acksMetric)
		return err == nil
	}, 5*time.Second, 10*time.Millisecond)

	metadatatest.AssertEqualCompletenessAcks(t, tel, []metricdata.DataPoint[int64]{
		ackDataPoint("2026-08-10T21:23:00Z", 1),
	}, ignoreVolatile...)
}

func TestWrappedExporterQueueIsTakenOver(t *testing.T) {
	fake := &fakeExporter{}
	cfg := testConfig()
	cfg.Exporter.Config = map[string]any{
		"endpoint":      "localhost:4317",
		"sending_queue": map[string]any{"queue_size": 42},
	}
	newTestExporter(t, cfg, fake)

	innerCfg := fake.receivedConfig()
	require.NotNil(t, innerCfg)
	assert.False(t, innerCfg.QueueConfig.HasValue(), "the wrapped exporter must not queue, this exporter does")
	assert.Equal(t, "localhost:4317", innerCfg.Endpoint, "the rest of the configuration must be passed through")
}

func TestStartErrors(t *testing.T) {
	tests := []struct {
		name        string
		cfg         func(*Config)
		host        component.Host
		expectedErr string
	}{
		{
			name:        "host without component factories",
			host:        componenttest.NewNopHost(),
			expectedErr: "does not expose component factories",
		},
		{
			name:        "unknown exporter type",
			cfg:         func(cfg *Config) { cfg.Exporter.Type = "notbuiltin" },
			expectedErr: `exporter type "notbuiltin" is not part of this collector build`,
		},
		{
			name:        "invalid configuration of the wrapped exporter",
			cfg:         func(cfg *Config) { cfg.Exporter.Config = map[string]any{"not_a_field": true} },
			expectedErr: "cannot load the configuration of the wrapped",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := testConfig()
			if tt.cfg != nil {
				tt.cfg(cfg)
			}
			fake := &fakeExporter{}
			exp, err := createLogsExporter(t.Context(), exportertestSettings(), cfg)
			require.NoError(t, err)

			host := tt.host
			if host == nil {
				host = newFactoryHost(fake.factory())
			}
			assert.ErrorContains(t, exp.Start(t.Context(), host), tt.expectedErr)
			assert.NoError(t, exp.Shutdown(t.Context()))
		})
	}
}

func TestCountsRecordsWithoutBucketAsUnknown(t *testing.T) {
	fake := &fakeExporter{}
	exp, tel := newTestExporter(t, testConfig(), fake)

	require.NoError(t, exp.ConsumeLogs(t.Context(), logsWithBuckets("", "", "2026-08-10T21:22:00Z")))

	metadatatest.AssertEqualCompletenessAcks(t, tel, []metricdata.DataPoint[int64]{
		ackDataPoint(unknownBucket, 2),
		ackDataPoint("2026-08-10T21:22:00Z", 1),
	}, ignoreVolatile...)
}

func TestCustomBucketAttribute(t *testing.T) {
	fake := &fakeExporter{}
	cfg := testConfig()
	cfg.BucketAttribute = "my.bucket"
	exp, tel := newTestExporter(t, cfg, fake)

	ld := plog.NewLogs()
	sl := ld.ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty()
	sl.LogRecords().AppendEmpty().Attributes().PutStr("my.bucket", "bucket-1")
	// The default attribute is not the configured one, so it is ignored.
	sl.LogRecords().AppendEmpty().Attributes().PutStr(defaultBucketAttribute, "bucket-2")

	require.NoError(t, exp.ConsumeLogs(t.Context(), ld))

	metadatatest.AssertEqualCompletenessAcks(t, tel, []metricdata.DataPoint[int64]{
		ackDataPoint("bucket-1", 1),
		ackDataPoint(unknownBucket, 1),
	}, ignoreVolatile...)
}

// Processors such as groupbyattrs may move a common record attribute up to the
// resource or scope, so those levels are used as a fallback.
func TestBucketAttributePrecedence(t *testing.T) {
	fake := &fakeExporter{}
	exp, tel := newTestExporter(t, testConfig(), fake)

	ld := plog.NewLogs()
	rl := ld.ResourceLogs().AppendEmpty()
	rl.Resource().Attributes().PutStr(defaultBucketAttribute, "resource-bucket")

	// Inherits the resource bucket.
	rl.ScopeLogs().AppendEmpty().LogRecords().AppendEmpty()

	// The scope bucket wins over the resource one, and the record bucket over both.
	sl := rl.ScopeLogs().AppendEmpty()
	sl.Scope().Attributes().PutStr(defaultBucketAttribute, "scope-bucket")
	sl.LogRecords().AppendEmpty()
	sl.LogRecords().AppendEmpty().Attributes().PutStr(defaultBucketAttribute, "record-bucket")

	require.NoError(t, exp.ConsumeLogs(t.Context(), ld))

	metadatatest.AssertEqualCompletenessAcks(t, tel, []metricdata.DataPoint[int64]{
		ackDataPoint("resource-bucket", 1),
		ackDataPoint("scope-bucket", 1),
		ackDataPoint("record-bucket", 1),
	}, ignoreVolatile...)
}

func TestNonStringBucketAttribute(t *testing.T) {
	fake := &fakeExporter{}
	exp, tel := newTestExporter(t, testConfig(), fake)

	ld := plog.NewLogs()
	sl := ld.ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty()
	sl.LogRecords().AppendEmpty().Attributes().PutInt(defaultBucketAttribute, 1770758520)
	sl.LogRecords().AppendEmpty().Attributes().PutEmpty(defaultBucketAttribute)
	sl.LogRecords().AppendEmpty().Attributes().PutStr(defaultBucketAttribute, "")

	require.NoError(t, exp.ConsumeLogs(t.Context(), ld))

	metadatatest.AssertEqualCompletenessAcks(t, tel, []metricdata.DataPoint[int64]{
		ackDataPoint("1770758520", 1),
		ackDataPoint(unknownBucket, 2),
	}, ignoreVolatile...)
}

func TestEmptyBatch(t *testing.T) {
	fake := &fakeExporter{}
	exp, tel := newTestExporter(t, testConfig(), fake)

	require.NoError(t, exp.ConsumeLogs(t.Context(), plog.NewLogs()))

	_, err := tel.GetMetric(acksMetric)
	assert.Error(t, err, "an empty batch must not record anything")
}

func TestWrappedExporterID(t *testing.T) {
	tests := []struct {
		id       component.ID
		expected string
	}{
		{id: component.MustNewID("completeness"), expected: "fake/completeness"},
		{id: component.MustNewIDWithName("completeness", "central"), expected: "fake/completeness_central"},
	}
	for _, tt := range tests {
		t.Run(tt.expected, func(t *testing.T) {
			set := exportertestSettings()
			set.ID = tt.id
			exp, err := newCompletenessExporter(set, testConfig())
			require.NoError(t, err)
			assert.Equal(t, tt.expected, exp.wrappedID().String())
		})
	}
}

// noBatchQueue is the default queue without the batcher, so that a single record
// is handed to the wrapped exporter without waiting for the flush timeout.
func noBatchQueue() exporterhelper.QueueBatchConfig {
	queue := exporterhelper.NewDefaultQueueConfig()
	queue.Batch = configoptional.None[exporterhelper.BatchConfig]()
	return queue
}
