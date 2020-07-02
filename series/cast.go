package series

import (
	"fmt"
	"strconv"

	"github.com/apache/arrow/go/arrow"
	"github.com/apache/arrow/go/arrow/array"
)

// CAST
func castToInt32(s Series) Series {
	if s.field.Type == arrow.PrimitiveTypes.Int32 {
		return s
	}

	vals := make([]int32, s.Len())
	f := s.field
	f.Type = arrow.PrimitiveTypes.Int32

	switch s.field.Type {
	case arrow.PrimitiveTypes.Int64:
		is := s.Interface.(*array.Int64)
		for i := 0; i < is.Len(); i++ {
			vals[i] = int32(is.Value(i))
		}
	case arrow.PrimitiveTypes.Float32:
		is := s.Interface.(*array.Float32)
		for i := 0; i < is.Len(); i++ {
			vals[i] = int32(is.Value(i))
		}
	case arrow.PrimitiveTypes.Float64:
		is := s.Interface.(*array.Float64)
		for i := 0; i < is.Len(); i++ {
			vals[i] = int32(is.Value(i))
		}
	case arrow.BinaryTypes.String:
		is := s.Interface.(*array.String)
		for i := 0; i < is.Len(); i++ {
			val, err := strconv.ParseInt(is.Value(i), 10, 32)
			if err != nil {
				panic(fmt.Sprintf("series: cast: %s", err))
			}
			vals[i] = int32(val)
		}
	// case arrow.PrimitiveTypes.Uint64:
	default:
		panic("series: cast: unsupported type")
	}

	return FromInt32(s.pool, f, vals, nil)
}
func castToInt64(s Series) Series {
	if s.field.Type == arrow.PrimitiveTypes.Int64 {
		return s
	}

	vals := make([]int64, s.Len())
	f := s.field
	f.Type = arrow.PrimitiveTypes.Int64

	switch s.field.Type {
	case arrow.PrimitiveTypes.Int32:
		is := s.Interface.(*array.Int32)
		for i := 0; i < is.Len(); i++ {
			vals[i] = int64(is.Value(i))
		}
	case arrow.PrimitiveTypes.Float32:
		is := s.Interface.(*array.Float32)
		for i := 0; i < is.Len(); i++ {
			vals[i] = int64(is.Value(i))
		}
	case arrow.PrimitiveTypes.Float64:
		is := s.Interface.(*array.Float64)
		for i := 0; i < is.Len(); i++ {
			vals[i] = int64(is.Value(i))
		}
	case arrow.BinaryTypes.String:
		is := s.Interface.(*array.String)
		for i := 0; i < is.Len(); i++ {
			val, err := strconv.ParseInt(is.Value(i), 10, 64)
			if err != nil {
				panic(fmt.Sprintf("series: cast: %s", err))
			}
			vals[i] = val
		}
	// case arrow.PrimitiveTypes.Uint64:
	default:
		panic("series: cast: unsupported type")
	}

	return FromInt64(s.pool, f, vals, nil)
}
func castToFloat32(s Series) Series {
	if s.field.Type == arrow.PrimitiveTypes.Float32 {
		return s
	}

	vals := make([]float32, s.Len())
	f := s.field
	f.Type = arrow.PrimitiveTypes.Float32

	switch s.field.Type {
	case arrow.PrimitiveTypes.Int32:
		is := s.Interface.(*array.Int32)
		for i := 0; i < is.Len(); i++ {
			vals[i] = float32(is.Value(i))
		}
	case arrow.PrimitiveTypes.Int64:
		is := s.Interface.(*array.Int64)
		for i := 0; i < is.Len(); i++ {
			vals[i] = float32(is.Value(i))
		}
	case arrow.PrimitiveTypes.Float64:
		is := s.Interface.(*array.Float64)
		for i := 0; i < is.Len(); i++ {
			vals[i] = float32(is.Value(i))
		}
	case arrow.BinaryTypes.String:
		is := s.Interface.(*array.String)
		for i := 0; i < is.Len(); i++ {
			val, err := strconv.ParseFloat(is.Value(i), 32)
			if err != nil {
				panic(fmt.Sprintf("series: cast: %s", err))
			}
			vals[i] = float32(val)
		}
	// case arrow.PrimitiveTypes.Uint64:
	default:
		panic("series: cast: unsupported type")
	}

	return FromFloat32(s.pool, f, vals, nil)
}
func castToFloat64(s Series) Series {
	if s.field.Type == arrow.PrimitiveTypes.Float64 {
		return s
	}

	vals := make([]float64, s.Len())
	f := s.field
	f.Type = arrow.PrimitiveTypes.Float64

	switch s.field.Type {
	case arrow.PrimitiveTypes.Int32:
		is := s.Interface.(*array.Int32)
		for i := 0; i < is.Len(); i++ {
			vals[i] = float64(is.Value(i))
		}
	case arrow.PrimitiveTypes.Int64:
		is := s.Interface.(*array.Int64)
		for i := 0; i < is.Len(); i++ {
			vals[i] = float64(is.Value(i))
		}
	case arrow.PrimitiveTypes.Float32:
		is := s.Interface.(*array.Float32)
		for i := 0; i < is.Len(); i++ {
			vals[i] = float64(is.Value(i))
		}
	case arrow.BinaryTypes.String:
		is := s.Interface.(*array.String)
		for i := 0; i < is.Len(); i++ {
			val, err := strconv.ParseFloat(is.Value(i), 64)
			if err != nil {
				panic(fmt.Sprintf("series: cast: %s", err))
			}
			vals[i] = float64(val)
		}
	// case arrow.PrimitiveTypes.Uint64:
	default:
		panic("series: cast: unsupported type")
	}

	return FromFloat64(s.pool, f, vals, nil)
}
func castToString(s Series) Series {
	if s.field.Type == arrow.BinaryTypes.String {
		return s
	}

	vals := make([]string, s.Len())
	f := s.field
	f.Type = arrow.BinaryTypes.String

	switch s.field.Type {
	case arrow.PrimitiveTypes.Int32:
		is := s.Interface.(*array.Int32)
		for i := 0; i < is.Len(); i++ {
			vals[i] = strconv.FormatInt(int64(is.Value(i)), 10)
		}
	case arrow.PrimitiveTypes.Int64:
		is := s.Interface.(*array.Int64)
		for i := 0; i < is.Len(); i++ {
			vals[i] = strconv.FormatInt(is.Value(i), 10)
		}
	case arrow.PrimitiveTypes.Float32:
		is := s.Interface.(*array.Float32)
		for i := 0; i < is.Len(); i++ {
			vals[i] = strconv.FormatFloat(float64(is.Value(i)), 'f', -1, 32)
		}
	case arrow.PrimitiveTypes.Float64:
		is := s.Interface.(*array.Float64)
		for i := 0; i < is.Len(); i++ {
			vals[i] = strconv.FormatFloat(is.Value(i), 'f', -1, 64)
		}
	// case arrow.PrimitiveTypes.Uint64:
	default:
		panic("series: cast: unsupported type")
	}

	return FromString(s.pool, f, vals, nil)
}
