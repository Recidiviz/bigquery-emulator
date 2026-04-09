package types

import (
	"strings"

	"github.com/go-playground/validator/v10"
	bigqueryv2 "google.golang.org/api/bigquery/v2"
)

func TypeValidation(fl validator.FieldLevel) bool {
	return Type(fl.Field().String()).TypeKind().String() != ""
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

// ValidateDataAgainstSchema validates that all fields in the data exist in the schema.
// It returns a list of validation errors, one per row that has an unknown field.
// Only one unknown field is reported per row (matching BigQuery's behavior).
func ValidateDataAgainstSchema(schema *bigqueryv2.TableSchema, data Data) []FieldValidationError {
	schemaFields := make(map[string]bool)
	for _, f := range schema.Fields {
		schemaFields[f.Name] = true
	}

	var errors []FieldValidationError
	for rowIdx, row := range data {
		for fieldName := range row {
			if !schemaFields[fieldName] {
				errors = append(errors, FieldValidationError{
					RowIndex:  rowIdx,
					FieldName: fieldName,
				})
				break // Only one error per row (matches BigQuery behavior)
			}
		}
	}
	return errors
}
