// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package completenessexporter

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/completenessexporter/internal/metadata"
)

func TestNewFactory(t *testing.T) {
	factory := NewFactory()
	assert.Equal(t, metadata.Type, factory.Type())
	assert.Equal(t, metadata.LogsStability, factory.LogsStability())
}

func TestCreateLogs(t *testing.T) {
	exp, err := NewFactory().CreateLogs(t.Context(), exportertestSettings(), testConfig())
	require.NoError(t, err)
	require.NotNil(t, exp)
	// The wrapped exporter is only built on start, so nothing was created yet.
	assert.NoError(t, exp.Shutdown(t.Context()))
}

func TestCreateLogsRejectsInvalidWrappedType(t *testing.T) {
	cfg := testConfig()
	cfg.Exporter.Type = "not a type"
	_, err := NewFactory().CreateLogs(t.Context(), exportertestSettings(), cfg)
	assert.ErrorContains(t, err, "invalid exporter::type")
}
