package types

import (
	"cloud.google.com/go/bigquery"
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/apache/arrow-go/v18/arrow/decimal256"
	"github.com/vantaboard/go-googlesqlite"
	bigqueryv2 "google.golang.org/api/bigquery/v2"
	"math/big"
	"strconv"
	"strings"
)

func TableToARROW(t *bigqueryv2.Table) (*arrow.Schema, error) {
	fields := make([]arrow.Field, 0, len(t.Schema.Fields))
	for _, field := range t.Schema.Fields {
		f, err := TableFieldToARROW(field)
		if err != nil {
			return nil, err
		}
		fields = append(fields, *f)
	}
	return arrow.NewSchema(fields, nil), nil
}

func TableFieldToARROW(f *bigqueryv2.TableFieldSchema) (*arrow.Field, error) {
	field, err := tableFieldToARROW(f)
	if err != nil {
		return nil, err
	}
	switch Mode(f.Mode) {
	case RepeatedMode:
		return &arrow.Field{
			Name: f.Name,
			Type: arrow.ListOfField(*field),
		}, nil
	case RequiredMode:
		return field, nil
	}
	field.Nullable = true
	return field, nil
}

func tableFieldToARROW(f *bigqueryv2.TableFieldSchema) (*arrow.Field, error) {
	switch FieldType(f.Type) {
	case FieldInteger:
		return &arrow.Field{Name: f.Name, Type: arrow.PrimitiveTypes.Int64}, nil
	case FieldBoolean:
		return &arrow.Field{Name: f.Name, Type: arrow.FixedWidthTypes.Boolean}, nil
	case FieldFloat:
		return &arrow.Field{Name: f.Name, Type: arrow.PrimitiveTypes.Float64}, nil
	case FieldString:
		return &arrow.Field{Name: f.Name, Type: arrow.BinaryTypes.String}, nil
	case FieldBytes:
		return &arrow.Field{Name: f.Name, Type: arrow.BinaryTypes.Binary}, nil
	case FieldDate:
		return &arrow.Field{Name: f.Name, Type: arrow.PrimitiveTypes.Date64}, nil
	case FieldDatetime:
		return &arrow.Field{
			Name: f.Name,
			Type: &arrow.TimestampType{Unit: arrow.Microsecond},
			Metadata: arrow.MetadataFrom(
				map[string]string{
					"ARROW:extension:name": "google:sqlType:datetime",
				},
			),
		}, nil
	case FieldTime:
		return &arrow.Field{Name: f.Name, Type: arrow.FixedWidthTypes.Time64us}, nil
	case FieldTimestamp:
		return &arrow.Field{Name: f.Name, Type: arrow.FixedWidthTypes.Timestamp_us}, nil
	case FieldJSON:
		return &arrow.Field{
			Name: f.Name,
			Type: arrow.BinaryTypes.String,
			Metadata: arrow.MetadataFrom(
				map[string]string{
					"ARROW:extension:name": "google:sqlType:json",
				},
			),
		}, nil
	case FieldRecord:
		fields := make([]arrow.Field, 0, len(f.Fields))
		for _, field := range f.Fields {
			fieldV, err := TableFieldToARROW(field)
			if err != nil {
				return nil, err
			}
			fields = append(fields, *fieldV)
		}
		return &arrow.Field{Name: f.Name, Type: arrow.StructOf(fields...)}, nil
	case FieldNumeric:
		// NUMERIC is a DECIMAL with precision 38, scale 9
		return &arrow.Field{Name: f.Name, Type: &arrow.Decimal128Type{
			Precision: bigquery.NumericPrecisionDigits,
			Scale:     bigquery.NumericScaleDigits,
		}}, nil
	case FieldBignumeric:
		// In BigQuery, BIGNUMERIC is a DECIMAL with precision 76 (partial 77), scale 38
		// Values requiring 77 digits when scaled by 10^38 work fine, including the maximum value (±2^255 / 10^38).
		// These values can technically be encoded into the Arrow format, but most libraries (including arrow-go)
		// raise validation errors when trying to build them.
		// The values returned by the BigQuery Storage Read API raise errors when you try to validate them client side
		// but if you only access their values, it is fine.
		return &arrow.Field{Name: f.Name, Type: &arrow.Decimal256Type{
			Precision: bigquery.BigNumericPrecisionDigits, // 76
			Scale:     bigquery.BigNumericScaleDigits,     // 38
		}}, nil
	case FieldGeography:
		return &arrow.Field{Name: f.Name, Type: arrow.BinaryTypes.String}, nil
	case FieldInterval:
		return &arrow.Field{Name: f.Name, Type: arrow.BinaryTypes.String}, nil
	}
	return nil, fmt.Errorf("unsupported arrow type %s", f.Type)
}

func AppendValueToARROWBuilder(ptrv *string, builder array.Builder) error {
	if ptrv == nil {
		builder.AppendNull()
		return nil
	}
	v := *ptrv
	switch b := builder.(type) {
	case *array.Int64Builder:
		i64, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			return err
		}
		b.Append(i64)
		return nil
	case *array.Float64Builder:
		f64, err := strconv.ParseFloat(v, 64)
		if err != nil {
			return err
		}
		b.Append(f64)
		return nil
	case *array.BooleanBuilder:
		cond, err := strconv.ParseBool(v)
		if err != nil {
			return err
		}
		b.Append(cond)
		return nil
	case *array.StringBuilder:
		b.Append(v)
		return nil
	case *array.BinaryBuilder:
		// Bytes are stored as base64 in BigQuery JSON API, decode to raw bytes
		decoded, err := base64.StdEncoding.DecodeString(v)
		if err != nil {
			return fmt.Errorf("failed to decode base64 bytes: %w", err)
		}
		b.Append(decoded)
		return nil
	case *array.Date64Builder:
		t, err := parseDate(v)
		if err != nil {
			return err
		}
		b.Append(arrow.Date64FromTime(t))
		return nil
	case *array.Time64Builder:
		t, err := parseTime(v)
		if err != nil {
			return err
		}
		b.Append(arrow.Time64(microsecondsSinceMidnight(t)))
		return nil
	case *array.TimestampBuilder:
		// Handle datetime strings
		var t arrow.Timestamp
		if strings.Contains(v, "T") {
			parsed, err := arrow.TimestampFromString(v, arrow.Microsecond)
			if err != nil {
				return err
			}
			t = parsed
		} else {
			parsed, err := googlesqlite.TimeFromTimestampValue(v)
			if err != nil {
				return err
			}
			t = arrow.Timestamp(parsed.UnixMicro())
		}
		b.Append(t)
		return nil
	case *array.Decimal128Builder:
		// NUMERIC type: precision 38, scale 9
		// Parse the string value to a big.Rat, then convert to scaled integer
		rat := new(big.Rat)
		if _, ok := rat.SetString(v); !ok {
			return fmt.Errorf("failed to parse decimal value: %s", v)
		}

		// Scale the value by 10^scale to get the integer representation
		scale := int32(9)
		scaleFactor := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(scale)), nil)

		// Multiply the rational by the scale factor
		scaled := new(big.Rat).Mul(rat, new(big.Rat).SetInt(scaleFactor))

		// Convert to integer (this truncates any remaining fractional part)
		scaledInt := new(big.Int).Div(scaled.Num(), scaled.Denom())

		// Convert to decimal128.Num using Arrow's FromBigInt (handles two's complement correctly)
		b.Append(decimal128.FromBigInt(scaledInt))
		return nil
	case *array.Decimal256Builder:
		// BIGNUMERIC type: precision 76, scale 38
		// NOTE: BigQuery declares decimal256(76, 38) in the schema but doesn't enforce
		// precision during encoding. Values requiring 77 digits when scaled work fine in BigQuery.
		// We bypass Arrow's FromBigInt validation (bitlen > 255 check) and manually construct
		// the Decimal256 using the same logic but without the strict check.

		// Parse as rational number
		rat := new(big.Rat)
		if _, ok := rat.SetString(v); !ok {
			return fmt.Errorf("failed to parse BIGNUMERIC value: %s", v)
		}

		// Scale by 10^38 to get integer representation
		scale := int64(bigquery.BigNumericScaleDigits) // 38
		scaleFactor := new(big.Int).Exp(big.NewInt(10), big.NewInt(scale), nil)
		scaled := new(big.Rat).Mul(rat, new(big.Rat).SetInt(scaleFactor))

		// Convert to integer (truncating any remaining fractional part)
		scaledInt := new(big.Int).Div(scaled.Num(), scaled.Denom())

		// Replicate decimal256.FromBigInt logic without the bitlen > 255 check
		// This matches how Arrow handles two's complement representation
		var num decimal256.Num
		if scaledInt.Sign() == 0 {
			// Zero value, return default
			b.Append(num)
			return nil
		}

		num = decimal256FromScaledInt(scaledInt)

		b.Append(num)
		return nil
	}
	return fmt.Errorf("unexpected builder type %T", builder)
}

func decimal256FromScaledInt(scaledInt *big.Int) decimal256.Num {
	b := scaledInt.FillBytes(make([]byte, 32))

	var limbs [4]uint64

	// Arrow Decimal256 uses little-endian uint64 limbs.
	// BigQuery and BigInt.FillBytes produce big-endian bytes.
	//
	// So the 256-bit structure:
	//   b[0] ... b[31]   (big endian)
	// maps to Arrow limbs:
	//   limbs[0] = low  64 bits
	//   limbs[3] = high 64 bits

	for i := 0; i < 4; i++ {
		// Big-endian slice for limb i:
		start := 32 - (i+1)*8
		end := 32 - i*8

		// Convert this BE 8-byte block into LE uint64
		// Arrow stores each limb as native endian (LE)
		limbs[i] = binary.LittleEndian.Uint64(reverse8(b[start:end]))
	}

	dec := decimal256.New(limbs[3], limbs[2], limbs[1], limbs[0])
	// If negative, negate to get two's complement representation
	if scaledInt.Sign() < 0 {
		dec = dec.Negate()
	}
	return dec
}

func reverse8(b []byte) []byte {
	out := make([]byte, 8)
	for i := 0; i < 8; i++ {
		out[i] = b[7-i]
	}
	return out
}
