package types

import (
	"testing"

	bigqueryv2 "google.golang.org/api/bigquery/v2"
)

func TestValidateDataAgainstSchema(t *testing.T) {
	schema := &bigqueryv2.TableSchema{
		Fields: []*bigqueryv2.TableFieldSchema{
			{Name: "field1", Type: "STRING"},
			{Name: "field2", Type: "INTEGER"},
		},
	}

	t.Run("all valid fields", func(t *testing.T) {
		data := Data{
			{"field1": "value1", "field2": 123},
			{"field1": "value2", "field2": 456},
		}

		errors := ValidateDataAgainstSchema(schema, data)
		if len(errors) != 0 {
			t.Errorf("expected no errors for valid data, got %d errors", len(errors))
		}
	})

	t.Run("unknown field in single row", func(t *testing.T) {
		data := Data{
			{"field1": "value1", "unknown_field": "bad"},
		}

		errors := ValidateDataAgainstSchema(schema, data)
		if len(errors) != 1 {
			t.Fatalf("expected 1 error, got %d", len(errors))
		}
		if errors[0].RowIndex != 0 {
			t.Errorf("expected row index 0, got %d", errors[0].RowIndex)
		}
		if errors[0].FieldName != "unknown_field" {
			t.Errorf("expected field name 'unknown_field', got '%s'", errors[0].FieldName)
		}
	})

	t.Run("unknown fields in multiple rows", func(t *testing.T) {
		data := Data{
			{"field1": "value1", "bad_field1": "error"},
			{"field1": "value2"},
			{"field2": 123, "bad_field2": "error"},
		}

		errors := ValidateDataAgainstSchema(schema, data)
		if len(errors) != 2 {
			t.Fatalf("expected 2 errors (rows 0 and 2), got %d", len(errors))
		}

		// Check that we got errors for the correct rows
		rowsWithErrors := make(map[int]string)
		for _, e := range errors {
			rowsWithErrors[e.RowIndex] = e.FieldName
		}

		if _, ok := rowsWithErrors[0]; !ok {
			t.Error("expected error for row 0")
		}
		if _, ok := rowsWithErrors[2]; !ok {
			t.Error("expected error for row 2")
		}
		if _, ok := rowsWithErrors[1]; ok {
			t.Error("did not expect error for row 1 (valid row)")
		}
	})

	t.Run("only one error per row", func(t *testing.T) {
		data := Data{
			{"field1": "value1", "bad1": "error", "bad2": "error", "bad3": "error"},
		}

		errors := ValidateDataAgainstSchema(schema, data)
		if len(errors) != 1 {
			t.Errorf("expected exactly 1 error (one per row), got %d", len(errors))
		}
	})

	t.Run("empty data", func(t *testing.T) {
		data := Data{}

		errors := ValidateDataAgainstSchema(schema, data)
		if len(errors) != 0 {
			t.Errorf("expected no errors for empty data, got %d", len(errors))
		}
	})

	t.Run("empty schema allows nothing", func(t *testing.T) {
		emptySchema := &bigqueryv2.TableSchema{
			Fields: []*bigqueryv2.TableFieldSchema{},
		}
		data := Data{
			{"any_field": "value"},
		}

		errors := ValidateDataAgainstSchema(emptySchema, data)
		if len(errors) != 1 {
			t.Errorf("expected 1 error for data with empty schema, got %d", len(errors))
		}
	})

	t.Run("partial fields allowed", func(t *testing.T) {
		data := Data{
			{"field1": "value1"}, // Only field1, missing field2 - should be valid
		}

		errors := ValidateDataAgainstSchema(schema, data)
		if len(errors) != 0 {
			t.Errorf("expected no errors when providing subset of schema fields, got %d", len(errors))
		}
	})
}

func TestValidateDataAgainstSchema_NestedFields(t *testing.T) {
	schema := &bigqueryv2.TableSchema{
		Fields: []*bigqueryv2.TableFieldSchema{
			{Name: "id", Type: "INTEGER"},
			{Name: "payload", Type: "JSON"},
			{
				Name: "record",
				Type: "RECORD",
				Fields: []*bigqueryv2.TableFieldSchema{
					{Name: "known", Type: "STRING"},
				},
			},
			{
				Name: "records",
				Type: "RECORD",
				Mode: "REPEATED",
				Fields: []*bigqueryv2.TableFieldSchema{
					{Name: "known", Type: "STRING"},
				},
			},
		},
	}

	t.Run("JSON column accepts arbitrary object keys", func(t *testing.T) {
		data := Data{
			{"payload": map[string]interface{}{"any": "object", "nested": map[string]interface{}{"a": 1}}},
		}

		errors := ValidateDataAgainstSchema(schema, data)
		if len(errors) != 0 {
			t.Errorf("expected no errors for object value in JSON column, got %v", errors)
		}
	})

	t.Run("known nested fields are valid", func(t *testing.T) {
		data := Data{
			{
				"record":  map[string]interface{}{"known": "value"},
				"records": []interface{}{map[string]interface{}{"known": "value"}},
			},
		}

		errors := ValidateDataAgainstSchema(schema, data)
		if len(errors) != 0 {
			t.Errorf("expected no errors for known nested fields, got %v", errors)
		}
	})

	t.Run("unknown key in STRUCT column is reported with dotted path", func(t *testing.T) {
		data := Data{
			{"record": map[string]interface{}{"unknown": "value"}},
		}

		errors := ValidateDataAgainstSchema(schema, data)
		if len(errors) != 1 {
			t.Fatalf("expected 1 error for unknown nested field, got %d", len(errors))
		}
		if errors[0].FieldName != "record.unknown" {
			t.Errorf("expected field name %q, got %q", "record.unknown", errors[0].FieldName)
		}
	})

	t.Run("unknown key in REPEATED STRUCT element is reported", func(t *testing.T) {
		data := Data{
			{"records": []interface{}{map[string]interface{}{"unknown": "value"}}},
		}

		errors := ValidateDataAgainstSchema(schema, data)
		if len(errors) != 1 {
			t.Fatalf("expected 1 error for unknown nested field, got %d", len(errors))
		}
		if errors[0].FieldName != "records.unknown" {
			t.Errorf("expected field name %q, got %q", "records.unknown", errors[0].FieldName)
		}
	})
}

func TestNewTableWithSchema_JSONColumn(t *testing.T) {
	tableSchema := &bigqueryv2.Table{
		TableReference: &bigqueryv2.TableReference{
			TableId: "test_table",
		},
		Schema: &bigqueryv2.TableSchema{
			Fields: []*bigqueryv2.TableFieldSchema{
				{Name: "payload", Type: "JSON"},
				{Name: "payloads", Type: "JSON", Mode: "REPEATED"},
			},
		},
	}

	t.Run("object value is stored as serialized JSON", func(t *testing.T) {
		data := Data{
			{"payload": map[string]interface{}{"any": "object"}},
		}

		table, err := NewTableWithSchema(tableSchema, data)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		got, ok := table.Data[0]["payload"].(string)
		if !ok {
			t.Fatalf("expected serialized JSON string, got %T", table.Data[0]["payload"])
		}
		if got != `{"any":"object"}` {
			t.Errorf("expected %q, got %q", `{"any":"object"}`, got)
		}
	})

	t.Run("array value is stored as serialized JSON", func(t *testing.T) {
		data := Data{
			{"payload": []interface{}{1, "two", nil}},
		}

		table, err := NewTableWithSchema(tableSchema, data)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		got, ok := table.Data[0]["payload"].(string)
		if !ok {
			t.Fatalf("expected serialized JSON string, got %T", table.Data[0]["payload"])
		}
		if got != `[1,"two",null]` {
			t.Errorf("expected %q, got %q", `[1,"two",null]`, got)
		}
	})

	t.Run("pre-serialized string value is passed through", func(t *testing.T) {
		data := Data{
			{"payload": `{"any":"object"}`},
		}

		table, err := NewTableWithSchema(tableSchema, data)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if got := table.Data[0]["payload"]; got != `{"any":"object"}` {
			t.Errorf("expected %q, got %v", `{"any":"object"}`, got)
		}
	})

	t.Run("repeated JSON column serializes each element", func(t *testing.T) {
		data := Data{
			{"payloads": []interface{}{
				map[string]interface{}{"a": 1},
				`{"b":2}`,
			}},
		}

		table, err := NewTableWithSchema(tableSchema, data)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		values, ok := table.Data[0]["payloads"].([]interface{})
		if !ok {
			t.Fatalf("expected slice, got %T", table.Data[0]["payloads"])
		}
		if len(values) != 2 {
			t.Fatalf("expected 2 elements, got %d", len(values))
		}
		if values[0] != `{"a":1}` {
			t.Errorf("expected %q, got %v", `{"a":1}`, values[0])
		}
		if values[1] != `{"b":2}` {
			t.Errorf("expected %q, got %v", `{"b":2}`, values[1])
		}
	})
}

func TestNewTableWithSchema_StructUnknownKey(t *testing.T) {
	tableSchema := &bigqueryv2.Table{
		TableReference: &bigqueryv2.TableReference{
			TableId: "test_table",
		},
		Schema: &bigqueryv2.TableSchema{
			Fields: []*bigqueryv2.TableFieldSchema{
				{
					Name: "record",
					Type: "RECORD",
					Fields: []*bigqueryv2.TableFieldSchema{
						{Name: "known", Type: "STRING"},
					},
				},
			},
		},
	}

	t.Run("unknown nested key returns error instead of panicking", func(t *testing.T) {
		data := Data{
			{"record": map[string]interface{}{"known": "v", "unknown": "v"}},
		}

		_, err := NewTableWithSchema(tableSchema, data)
		if err == nil {
			t.Fatal("expected error for unknown nested field, got nil")
		}
	})
}

func TestNewTableWithSchema_SkipsUnknownFields(t *testing.T) {
	tableSchema := &bigqueryv2.Table{
		TableReference: &bigqueryv2.TableReference{
			TableId: "test_table",
		},
		Schema: &bigqueryv2.TableSchema{
			Fields: []*bigqueryv2.TableFieldSchema{
				{Name: "valid_field", Type: "STRING"},
			},
		},
	}

	t.Run("skips unknown fields silently", func(t *testing.T) {
		data := Data{
			{"valid_field": "value", "unknown_field": "should_be_skipped"},
		}

		table, err := NewTableWithSchema(tableSchema, data)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if len(table.Data) != 1 {
			t.Fatalf("expected 1 row, got %d", len(table.Data))
		}

		// Check that unknown_field was skipped
		if _, exists := table.Data[0]["unknown_field"]; exists {
			t.Error("expected unknown_field to be skipped")
		}

		// Check that valid_field is present
		if _, exists := table.Data[0]["valid_field"]; !exists {
			t.Error("expected valid_field to be present")
		}
	})
}
