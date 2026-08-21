package types

import (
	"strings"

	"github.com/go-playground/validator/v10"
	bigqueryv2 "google.golang.org/api/bigquery/v2"
)

func TypeValidation(fl validator.FieldLevel) bool {
	return Type(fl.Field().String()).ZetaSQLTypeKind().String() != ""
}

func ModeValidation(fl validator.FieldLevel) bool {
	mode := fl.Field().String()
	if mode == "" {
		return true
	}
	switch strings.ToLower(mode) {
	case strings.ToLower(string(NullableMode)):
		return true
	case strings.ToLower(string(RequiredMode)):
		return true
	case strings.ToLower(string(RepeatedMode)):
		return true
	}
	return false
}

func RegisterTypeValidation(v *validator.Validate) {
	v.RegisterValidation("type", TypeValidation)
	v.RegisterValidation("mode", ModeValidation)
}

// FieldValidationError represents a validation error for a specific field in a row.
type FieldValidationError struct {
	RowIndex  int
	FieldName string
}

// ValidateDataAgainstSchema validates that all fields in the data exist in the
// schema, including keys nested inside RECORD/STRUCT values. It returns a list
// of validation errors, one per row that has an unknown field.
// Only one unknown field is reported per row (matching BigQuery's behavior).
func ValidateDataAgainstSchema(schema *bigqueryv2.TableSchema, data Data) []FieldValidationError {
	var errors []FieldValidationError
	for rowIdx, row := range data {
		if path, found := findUnknownField(schema.Fields, row, ""); found {
			errors = append(errors, FieldValidationError{
				RowIndex:  rowIdx,
				FieldName: path,
			})
		}
	}
	return errors
}

// findUnknownField returns the dotted path of the first key in row (a row or a
// nested RECORD value) that does not exist in fields. JSON columns have no
// nested schema (no Fields), so their object values are not descended into.
func findUnknownField(fields []*bigqueryv2.TableFieldSchema, row map[string]interface{}, prefix string) (string, bool) {
	fieldMap := make(map[string]*bigqueryv2.TableFieldSchema, len(fields))
	for _, f := range fields {
		fieldMap[f.Name] = f
	}
	for key, value := range row {
		path := key
		if prefix != "" {
			path = prefix + "." + key
		}
		f, exists := fieldMap[key]
		if !exists {
			return path, true
		}
		if len(f.Fields) == 0 {
			continue
		}
		switch v := value.(type) {
		case map[string]interface{}:
			if p, found := findUnknownField(f.Fields, v, path); found {
				return p, true
			}
		case []interface{}:
			for _, elem := range v {
				if m, ok := elem.(map[string]interface{}); ok {
					if p, found := findUnknownField(f.Fields, m, path); found {
						return p, true
					}
				}
			}
		}
	}
	return "", false
}
