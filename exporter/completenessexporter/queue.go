// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package completenessexporter // import "github.com/open-telemetry/opentelemetry-collector-contrib/exporter/completenessexporter"

import (
	"reflect"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/config/configoptional"
	"go.opentelemetry.io/collector/exporter/exporterhelper"
)

// maxConfigDepth bounds the search for the sending queue in nested
// configuration structs.
const maxConfigDepth = 8

var optionalQueueType = reflect.TypeFor[configoptional.Optional[exporterhelper.QueueBatchConfig]]()

// disableQueue turns the sending queue of an exporter configuration off, so that
// the calls to that exporter block until the data was accepted by the backend
// instead of returning as soon as it was enqueued. The queueing is done by the
// completeness exporter instead, in front of the point where records are
// counted.
//
// The queue is a standard `exporterhelper` setting, but exporters are free to
// place it anywhere in their configuration struct, so it is looked up by type.
// It reports whether a queue was found.
func disableQueue(cfg component.Config) bool {
	val := reflect.ValueOf(cfg)
	if val.Kind() != reflect.Pointer || val.IsNil() {
		return false
	}
	return disableQueueIn(val.Elem(), 0)
}

func disableQueueIn(val reflect.Value, depth int) bool {
	if depth > maxConfigDepth || val.Kind() != reflect.Struct {
		return false
	}
	found := false
	for i := 0; i < val.NumField(); i++ {
		field := val.Field(i)
		if field.Type() == optionalQueueType {
			// Queues reached through an unexported field cannot be set. Leaving
			// them alone is reported to the caller, which warns about it.
			if !field.CanSet() {
				continue
			}
			field.Set(reflect.ValueOf(configoptional.None[exporterhelper.QueueBatchConfig]()))
			found = true
			continue
		}
		switch field.Kind() {
		case reflect.Struct:
			found = disableQueueIn(field, depth+1) || found
		case reflect.Pointer:
			if !field.IsNil() {
				found = disableQueueIn(field.Elem(), depth+1) || found
			}
		default:
		}
	}
	return found
}
