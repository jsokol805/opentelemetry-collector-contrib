// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//go:generate make mdatagen

// Package completenessexporter wraps another logs exporter and counts the log
// records it confirmed as delivered, grouped by the ingestion time bucket the
// records carry.
package completenessexporter // import "github.com/open-telemetry/opentelemetry-collector-contrib/exporter/completenessexporter"
