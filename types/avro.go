package types

import (
	"encoding/base64"
	"fmt"
	"math/big"
	"strconv"
	"strings"
	"time"

	"github.com/goccy/go-json"
	"github.com/vantaboard/go-googlesqlite"
	bigqueryv2 "google.golang.org/api/bigquery/v2"
)

type AVROSchema struct {
	Namespace string             `json:"namespace"`
	Name      string             `json:"name"`
	Type      string             `json:"type"`
	Fields    []*AVROFieldSchema `json:"fields"`
}

type AVROFieldSchema struct {
	Type *AVROType `json:"type"`
	Name string    `json:"name"`
}

type AVROType struct {
	TypeSchema *bigqueryv2.TableFieldSchema
	Namespace  string // Namespace for record types
}

func (t *AVROType) Key() string {
	switch Type(t.TypeSchema.Type).FieldType() {
	case FieldInteger:
		return "long"
	case FieldBoolean:
		return "boolean"
	case FieldFloat:
		return "double"
	case FieldString:
		return "string"
	case FieldBytes:
		return "bytes"
	case FieldDate:
		// Date: goavro uses compound name for unions (standard AVRO logical type)
		return "int.date"
	case FieldDatetime:
		// Datetime: goavro uses base type for unions (non-standard AVRO type)
		return "string"
	case FieldTime:
		// Time: goavro uses compound name for unions (to be verified)
		return "long.time-micros"
	case FieldTimestamp:
		// Timestamp: goavro uses compound name for unions
		return "long.timestamp-micros"
	case FieldJSON:
		return "string"
	case FieldRecord:
		// For records, include namespace if present
		if t.Namespace != "" {
			return fmt.Sprintf("%s.%s", t.Namespace, t.TypeSchema.Name)
		}
		return t.TypeSchema.Name
	case FieldNumeric:
		// Decimal: goavro uses compound name for unions
		return "bytes.decimal"
	case FieldBignumeric:
		// Decimal: goavro uses compound name for unions
		return "bytes.decimal"
	}
	return ""
}

func (t *AVROType) CastValue(v string) (interface{}, error) {
	switch Type(t.TypeSchema.Type).FieldType() {
	case FieldInteger:
		return strconv.ParseInt(v, 10, 64)
	case FieldBoolean:
		return strconv.ParseBool(v)
	case FieldFloat:
		return strconv.ParseFloat(v, 64)
	case FieldString:
		return v, nil
	case FieldJSON:
		return v, nil
	case FieldBytes:
		// Bytes are stored as base64 in BigQuery JSON API, decode to raw bytes
		decoded, err := base64.StdEncoding.DecodeString(v)
		if err != nil {
			return nil, fmt.Errorf("failed to decode base64 bytes: %w", err)
		}
		return decoded, nil
	case FieldNumeric, FieldBignumeric:
		r := new(big.Rat)
		r.SetString(v)
		return r, nil
	case FieldDate:
		return parseDate(v)
	case FieldDatetime:
		if t, err := time.Parse("2006-01-02T15:04:05.999999", v); err == nil {
			return t, nil
		}
		return time.Parse("2006-01-02 15:04:05.999999", v)
	case FieldTime:
		return parseTime(v)
	case FieldTimestamp:
		return googlesqlite.TimeFromTimestampValue(v)
	}
	return v, nil
}

// AVROPrimitiveValue converts a string value to its AVRO primitive type
// for encoding. Unlike CastValue which returns Go native types, this returns
// the underlying AVRO primitive type (string, int, long, bytes) that goavro expects.
func (t *AVROType) AVROPrimitiveValue(v string) (interface{}, error) {
	switch Type(t.TypeSchema.Type).FieldType() {
	case FieldInteger:
		return strconv.ParseInt(v, 10, 64)
	case FieldBoolean:
		return strconv.ParseBool(v)
	case FieldFloat:
		return strconv.ParseFloat(v, 64)
	case FieldString:
		return v, nil
	case FieldJSON:
		return v, nil
	case FieldBytes:
		// Bytes are stored as base64 in BigQuery JSON API, decode to raw bytes for AVRO
		decoded, err := base64.StdEncoding.DecodeString(v)
		if err != nil {
			return nil, fmt.Errorf("failed to decode base64 bytes: %w", err)
		}
		return decoded, nil
	case FieldNumeric, FieldBignumeric:
		// For decimal logical types, goavro expects *big.Rat
		r := new(big.Rat)
		if _, ok := r.SetString(v); !ok {
			return nil, fmt.Errorf("failed to parse numeric value: %s", v)
		}
		return r, nil
	case FieldDate:
		// Date is encoded as int (days since Unix epoch)
		parsed, err := time.Parse("2006-01-02", v)
		if err != nil {
			return nil, err
		}
		// Calculate days since Unix epoch (1970-01-01)
		days := int32(parsed.Unix() / 86400)
		return days, nil
	case FieldDatetime:
		// Datetime is encoded as string (not parsed to time.Time)
		return v, nil
	case FieldTime:
		// Time is encoded as long (microseconds since midnight)
		parsed, err := time.Parse("15:04:05.999999", v)
		if err != nil {
			parsed, err = time.Parse("15:04:05", v)
			if err != nil {
				return nil, err
			}
		}
		// Calculate microseconds since midnight
		micros := int64(parsed.Hour())*3600000000 +
			int64(parsed.Minute())*60000000 +
			int64(parsed.Second())*1000000 +
			int64(parsed.Nanosecond())/1000
		return micros, nil
	case FieldTimestamp:
		// Timestamp is encoded as long (microseconds since Unix epoch)
		t, err := googlesqlite.TimeFromTimestampValue(v)
		if err != nil {
			return nil, err
		}
		return t.UnixMicro(), nil
	}
	return v, nil
}

func (t *AVROType) MarshalJSON() ([]byte, error) {
	b, err := marshalAVROType(t.TypeSchema)
	if err != nil {
		return nil, err
	}
	typ := json.RawMessage(b)
	// Normalize mode to uppercase for comparison
	mode := Mode(strings.ToUpper(t.TypeSchema.Mode))
	switch mode {
	case RepeatedMode:
		return json.Marshal(map[string]interface{}{
			"type":  "array",
			"items": typ,
		})
	case RequiredMode:
		return json.Marshal(typ)
	default:
		return json.Marshal([]interface{}{
			"null",
			typ,
		})
	}
}

func marshalAVROType(t *bigqueryv2.TableFieldSchema) ([]byte, error) {
	switch Type(t.Type).FieldType() {
	case FieldInteger:
		return []byte(`"long"`), nil
	case FieldBoolean:
		return []byte(`"boolean"`), nil
	case FieldFloat:
		return []byte(`"double"`), nil
	case FieldString:
		return []byte(`"string"`), nil
	case FieldBytes:
		return []byte(`"bytes"`), nil
	case FieldDate:
		return json.Marshal(map[string]string{
			"type":        "int",
			"logicalType": "date",
		})
	case FieldDatetime:
		return json.Marshal(map[string]string{
			"type":        "string",
			"logicalType": "datetime",
		})
	case FieldTime:
		return json.Marshal(map[string]string{
			"type":        "long",
			"logicalType": "time-micros",
		})
	case FieldTimestamp:
		return json.Marshal(map[string]string{
			"type":        "long",
			"logicalType": "timestamp-micros",
		})
	case FieldJSON:
		return json.Marshal(map[string]string{
			"type":    "string",
			"sqlType": "JSON",
		})
	case FieldRecord:
		var fields []interface{}
		// Note: Calling TableFieldSchemasToAVRO (without namespace) for nested fields
		// because we want to preserve the namespace-less schema structure
		for _, field := range TableFieldSchemasToAVRO(t.Fields) {
			b, err := json.Marshal(field.Type)
			if err != nil {
				return nil, err
			}
			fields = append(fields, map[string]interface{}{
				"name": field.Name,
				"type": json.RawMessage(b),
			})
		}
		// Nested records should NOT have a namespace, they inherit from the parent
		return json.Marshal(map[string]interface{}{
			"type":   "record",
			"name":   t.Name,
			"fields": fields,
		})
	case FieldNumeric:
		return json.Marshal(map[string]interface{}{
			"type":        "bytes",
			"logicalType": "decimal",
			"precision":   38,
			"scale":       9,
		})
	case FieldBignumeric:
		return json.Marshal(map[string]interface{}{
			"type":        "bytes",
			"logicalType": "decimal",
			"precision":   77,
			"scale":       38,
		})
	case FieldGeography:
		return json.Marshal(map[string]string{
			"type":    "string",
			"sqlType": "GEOGRAPHY",
		})
	case FieldInterval:
		return json.Marshal(map[string]string{
			"type":    "string",
			"sqlType": "INTERVAL",
		})
	}
	return nil, fmt.Errorf("unsupported avro type %s", t.Type)
}

// sanitizeAVROName replaces characters that are invalid in AVRO names (must be [A-Za-z0-9_])
// BigQuery allows hyphens in project/dataset/table IDs, but AVRO does not
func sanitizeAVROName(name string) string {
	return strings.ReplaceAll(name, "-", "_")
}

func TableToAVRO(t *bigqueryv2.Table) *AVROSchema {
	// Sanitize project and dataset IDs for AVRO namespace
	namespace := fmt.Sprintf("%s.%s",
		sanitizeAVROName(t.TableReference.ProjectId),
		sanitizeAVROName(t.TableReference.DatasetId))
	return &AVROSchema{
		Namespace: namespace,
		Name:      sanitizeAVROName(t.TableReference.TableId),
		Type:      "record",
		Fields:    tableFieldSchemasToAVROWithNamespace(t.Schema.Fields, namespace),
	}
}

func TableFieldSchemasToAVRO(fields []*bigqueryv2.TableFieldSchema) []*AVROFieldSchema {
	return tableFieldSchemasToAVROWithNamespace(fields, "")
}

func tableFieldSchemasToAVROWithNamespace(fields []*bigqueryv2.TableFieldSchema, namespace string) []*AVROFieldSchema {
	ret := make([]*AVROFieldSchema, 0, len(fields))
	for _, field := range fields {
		ret = append(ret, tableFieldSchemaToAVROWithNamespace(field, namespace))
	}
	return ret
}

func TableFieldSchemaToAVRO(s *bigqueryv2.TableFieldSchema) *AVROFieldSchema {
	return tableFieldSchemaToAVROWithNamespace(s, "")
}

func tableFieldSchemaToAVROWithNamespace(s *bigqueryv2.TableFieldSchema, namespace string) *AVROFieldSchema {
	return &AVROFieldSchema{
		Type: &AVROType{
			TypeSchema: s,
			Namespace:  namespace,
		},
		Name: s.Name,
	}
}
