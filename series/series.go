package series

import (
	"fmt"
	"sort"
	"strconv"

	"github.com/apache/arrow/go/arrow"
	"github.com/apache/arrow/go/arrow/array"
	"github.com/apache/arrow/go/arrow/memory"
	"gonum.org/v1/gonum/floats"
	"gonum.org/v1/gonum/mat"
	"gonum.org/v1/gonum/stat"
)

// TODO(poopoothegorilla): NA vs NULL NAMING CONVENTION?

// Series ...
// TODO: TRY A CHUNKED / COLUMN VERSION OF SERIES
// TODO: ADD PRECISION
type Series struct {
	pool  memory.Allocator
	field arrow.Field
	array.Interface
}

// FromArrow creates a Series from an Arrow array.
func FromArrow(pool memory.Allocator, field arrow.Field, column array.Interface) Series {
	return Series{
		pool:      pool,
		field:     field,
		Interface: column,
	}
}

// FromInterface creates a Series from a slice of supported types.
func FromInterface(pool memory.Allocator, field arrow.Field, vals interface{}, valid []bool) Series {
	switch vs := vals.(type) {
	case []int32:
		return FromInt32(pool, field, vs, valid)
	case []int64:
		return FromInt64(pool, field, vs, valid)
	case []float32:
		return FromFloat32(pool, field, vs, valid)
	case []float64:
		return FromFloat64(pool, field, vs, valid)
	case []interface{}:
		switch field.Type {
		case arrow.PrimitiveTypes.Int32:
			int32s := make([]int32, len(vs))
			for i, v := range vs {
				val, ok := v.(int32)
				if !ok {
					val = 0
				}
				int32s[i] = val
			}
			return FromInt32(pool, field, int32s, valid)
		case arrow.PrimitiveTypes.Int64:
			int64s := make([]int64, len(vs))
			for i, v := range vs {
				val, ok := v.(int64)
				if !ok {
					val = 0
				}
				int64s[i] = val
			}
			return FromInt64(pool, field, int64s, valid)
		case arrow.PrimitiveTypes.Float32:
			float32s := make([]float32, len(vs))
			for i, v := range vs {
				val, ok := v.(float32)
				if !ok {
					val = 0
				}
				float32s[i] = val
			}
			return FromFloat32(pool, field, float32s, valid)
		case arrow.PrimitiveTypes.Float64:
			float64s := make([]float64, len(vs))
			for i, v := range vs {
				val, ok := v.(float64)
				if !ok {
					val = 0
				}
				float64s[i] = val
			}
			return FromFloat64(pool, field, float64s, valid)
		case arrow.BinaryTypes.String:
			strings := make([]string, len(vs))
			for i, v := range vs {
				val, ok := v.(string)
				if !ok {
					val = ""
				}
				strings[i] = val
			}
			return FromString(pool, field, strings, valid)
		default:
			panic(fmt.Sprintf("series: from_interface: unsupported type: %T", field.Type))
		}
	default:
		panic(fmt.Sprintf("series: from_interface: unsupported type: %T", vs))
	}

	return Series{}
}

// FromInt32 creates a Series from a slice of int32 values.
//
// TODO(poopoothegorilla): might be worth creating a pool of builders and
// recycling them.
// TODO(poopoothegorilla): should the arrow.Field be replaced by a string param
// and constructed in the function?
func FromInt32(pool memory.Allocator, field arrow.Field, vals []int32, valid []bool) Series {
	b := array.NewInt32Builder(pool)
	defer b.Release()
	b.AppendValues(vals, valid)

	return Series{
		pool:      pool,
		field:     field,
		Interface: b.NewArray(),
	}
}

// FromInt64 creates a Series from a slice of int64 values.
func FromInt64(pool memory.Allocator, field arrow.Field, vals []int64, valid []bool) Series {
	b := array.NewInt64Builder(pool)
	defer b.Release()
	b.AppendValues(vals, valid)

	return Series{
		pool:      pool,
		field:     field,
		Interface: b.NewArray(),
	}
}

// FromFloat32 creates a Series from a slice of float32 values.
func FromFloat32(pool memory.Allocator, field arrow.Field, vals []float32, valid []bool) Series {
	b := array.NewFloat32Builder(pool)
	defer b.Release()
	b.AppendValues(vals, valid)

	return Series{
		pool:      pool,
		field:     field,
		Interface: b.NewArray(),
	}
}

// FromFloat64 creates a Series from a slice of float64 values.
func FromFloat64(pool memory.Allocator, field arrow.Field, vals []float64, valid []bool) Series {
	b := array.NewFloat64Builder(pool)
	defer b.Release()
	b.AppendValues(vals, valid)

	return Series{
		pool:      pool,
		field:     field,
		Interface: b.NewArray(),
	}
}

// FromString creates a Series from a slice of string values.
func FromString(pool memory.Allocator, field arrow.Field, vals []string, valid []bool) Series {
	b := array.NewStringBuilder(pool)
	defer b.Release()
	b.AppendValues(vals, valid)

	return Series{
		pool:      pool,
		field:     field,
		Interface: b.NewArray(),
	}
}

// Empty creates an empty Series with n elements.
func (s Series) Empty(n int) Series {
	s.Retain()

	valid := make([]bool, n)
	var col array.Interface
	switch s.field.Type {
	case arrow.PrimitiveTypes.Int32:
		vals := make([]int32, n)
		b := array.NewInt32Builder(s.pool)
		defer b.Release()
		b.AppendValues(vals, valid)
		col = b.NewArray()
	case arrow.PrimitiveTypes.Int64:
		vals := make([]int64, n)
		b := array.NewInt64Builder(s.pool)
		defer b.Release()
		b.AppendValues(vals, valid)
		col = b.NewArray()
	case arrow.PrimitiveTypes.Float32:
		vals := make([]float32, n)
		b := array.NewFloat32Builder(s.pool)
		defer b.Release()
		b.AppendValues(vals, valid)
		col = b.NewArray()
	case arrow.PrimitiveTypes.Float64:
		vals := make([]float64, n)
		b := array.NewFloat64Builder(s.pool)
		defer b.Release()
		b.AppendValues(vals, valid)
		col = b.NewArray()
	default:
		panic("series.Empty: unknown type")
	}

	s.Release()
	return FromArrow(s.pool, s.field, col)
}

// Column returns an Arrow array column.
func (s Series) Column() *array.Column {
	s.Retain()
	defer s.Release()

	chunks := array.NewChunked(s.DataType(), []array.Interface{s})
	defer chunks.Release()

	return array.NewColumn(s.field, chunks)
}

// Value returns the value at position i from the Series as an interface.
func (s Series) Value(i int) interface{} {
	s.Retain()

	var v interface{}
	switch s.field.Type {
	case arrow.PrimitiveTypes.Int32:
		v = s.Interface.(*array.Int32).Value(i)
	case arrow.PrimitiveTypes.Int64:
		v = s.Interface.(*array.Int64).Value(i)
	case arrow.PrimitiveTypes.Float32:
		v = s.Interface.(*array.Float32).Value(i)
	case arrow.PrimitiveTypes.Float64:
		v = s.Interface.(*array.Float64).Value(i)
	case arrow.BinaryTypes.String:
		v = s.Interface.(*array.String).Value(i)
	default:
		panic("series.Value: unknown type")
	}

	s.Release()
	return v
}

// Int32 returns the value at position i from the Series as an int32.
func (s Series) Int32(i int) int32 {
	s.Retain()

	var v int32
	switch s.field.Type {
	case arrow.PrimitiveTypes.Int32:
		v = s.Interface.(*array.Int32).Value(i)
	case arrow.PrimitiveTypes.Int64:
		v = int32(s.Interface.(*array.Int64).Value(i))
	case arrow.PrimitiveTypes.Float32:
		v = int32(s.Interface.(*array.Float32).Value(i))
	case arrow.PrimitiveTypes.Float64:
		v = int32(s.Interface.(*array.Float64).Value(i))
	default:
		panic("series.Int32: unknown type")
	}

	s.Release()
	return v
}

// Int64 returns the value at position i from the Series as an int64.
func (s Series) Int64(i int) int64 {
	s.Retain()

	var v int64
	switch s.field.Type {
	case arrow.PrimitiveTypes.Int32:
		v = int64(s.Interface.(*array.Int32).Value(i))
	case arrow.PrimitiveTypes.Int64:
		v = s.Interface.(*array.Int64).Value(i)
	case arrow.PrimitiveTypes.Float32:
		v = int64(s.Interface.(*array.Float32).Value(i))
	case arrow.PrimitiveTypes.Float64:
		v = int64(s.Interface.(*array.Float64).Value(i))
	default:
		panic("series: unknown type")
	}

	s.Release()
	return v
}

// Float32 returns the value at position i from the Series as an float32.
func (s Series) Float32(i int) float32 {
	s.Retain()

	var v float32
	switch s.field.Type {
	case arrow.PrimitiveTypes.Int32:
		v = float32(s.Interface.(*array.Int32).Value(i))
	case arrow.PrimitiveTypes.Int64:
		v = float32(s.Interface.(*array.Int64).Value(i))
	case arrow.PrimitiveTypes.Float32:
		v = s.Interface.(*array.Float32).Value(i)
	case arrow.PrimitiveTypes.Float64:
		v = float32(s.Interface.(*array.Float64).Value(i))
	default:
		panic("series: unknown type")
	}

	s.Release()
	return v
}

// Float64 returns the value at position i from the Series as an float64.
func (s Series) Float64(i int) float64 {
	s.Retain()

	var v float64
	switch s.field.Type {
	case arrow.PrimitiveTypes.Int32:
		v = float64(s.Interface.(*array.Int32).Value(i))
	case arrow.PrimitiveTypes.Int64:
		v = float64(s.Interface.(*array.Int64).Value(i))
	case arrow.PrimitiveTypes.Float32:
		v = float64(s.Interface.(*array.Float32).Value(i))
	case arrow.PrimitiveTypes.Float64:
		v = s.Interface.(*array.Float64).Value(i)
	default:
		panic("series: unknown type")
	}

	s.Release()
	return v
}

// Values returns the values of the Series as a slice of types.
func (s Series) Values() interface{} {
	s.Retain()
	defer s.Release()

	switch s.field.Type {
	case arrow.PrimitiveTypes.Int32:
		return s.Interface.(*array.Int32).Int32Values()
	case arrow.PrimitiveTypes.Int64:
		return s.Interface.(*array.Int64).Int64Values()
	case arrow.PrimitiveTypes.Float32:
		return s.Interface.(*array.Float32).Float32Values()
	case arrow.PrimitiveTypes.Float64:
		return s.Interface.(*array.Float64).Float64Values()
	case arrow.BinaryTypes.String:
		vals := make([]string, s.Len())
		for i := 0; i < s.Len(); i++ {
			vals[i] = s.Interface.(*array.String).Value(i)
		}
		return vals
	default:
		panic("series: unknown type")
	}
}

// StringValues returns the values of the Series as a slice of strings.
func (s Series) StringValues() []string {
	s.Retain()
	defer s.Release()

	var res []string
	switch s.field.Type {
	case arrow.PrimitiveTypes.Int32:
		vs := s.Interface.(*array.Int32).Int32Values()
		for _, v := range vs {
			res = append(res, strconv.FormatInt(int64(v), 10))
		}
	case arrow.PrimitiveTypes.Int64:
		vs := s.Interface.(*array.Int64).Int64Values()
		for _, v := range vs {
			res = append(res, strconv.FormatInt(v, 10))
		}
	case arrow.PrimitiveTypes.Float32:
		vs := s.Interface.(*array.Float32).Float32Values()
		for _, v := range vs {
			res = append(res, strconv.FormatFloat(float64(v), 'f', -1, 32))
		}
	case arrow.PrimitiveTypes.Float64:
		vs := s.Interface.(*array.Float64).Float64Values()
		for _, v := range vs {
			res = append(res, strconv.FormatFloat(v, 'f', -1, 64))
		}
	case arrow.BinaryTypes.String:
		for i := 0; i < s.Len(); i++ {
			res = append(res, s.Interface.(*array.String).Value(i))
		}
	default:
		panic("series: unknown type")
	}
	return res
}

// String returns a representation of the data as a string.
func (s Series) String() string {
	return fmt.Sprintf("%v", s.Interface)
}

// Field returns the Arrow field associated with the Series.
func (s Series) Field() arrow.Field {
	return s.field
}

// Name returns the Series name.
func (s Series) Name() string {
	return s.field.Name
}

//////////////
// NOTE: for gonum Matrix interface
//////////////

// Dims returns the dimensions of the Series as rows and columns.
func (s Series) Dims() (r, c int) {
	return s.Len(), 1
}

// At returns the float64 value at the i position in the Series.
func (s Series) At(i, _ int) float64 {
	return s.AtVec(i)
}

// T returns the transpose of the Series which is itself.
func (s Series) T() mat.Matrix {
	return s
}

//////////////
// NOTE: for gonum Vector interface
//////////////

// AtVec returns a float64 value at position i.
func (s Series) AtVec(i int) float64 {
	s.Retain()
	defer s.Release()

	switch s.field.Type {
	case arrow.PrimitiveTypes.Int32:
		val := s.Interface.(*array.Int32).Value(i)
		return float64(val)
	case arrow.PrimitiveTypes.Int64:
		val := s.Interface.(*array.Int64).Value(i)
		return float64(val)
	case arrow.PrimitiveTypes.Float32:
		val := s.Interface.(*array.Float32).Value(i)
		return float64(val)
	case arrow.PrimitiveTypes.Float64:
		return s.Interface.(*array.Float64).Value(i)
	case arrow.BinaryTypes.String:
		val := s.Interface.(*array.String).Value(i)
		if val == "" {
			return 0
		}
		v, err := strconv.ParseFloat(val, 64)
		if err != nil {
			panic(fmt.Sprintf("series: at_vec: %s", err))
		}
		return v
	default:
		panic("series: at_vec: unknown type")
	}
}

//////////////
// TODO(poopoothegorilla): for gonum RawVectorer interface?
//////////////

// RawVector ...
// func (s Series) RawVector() blas64.Vector {
// }

//////////////
// NOTE: regular API
//////////////

// Cast returns a new series of t type.
func (s Series) Cast(t arrow.DataType) Series {
	s.Retain()
	defer s.Release()

	switch t {
	case arrow.PrimitiveTypes.Int32:
		return castToInt32(s)
	case arrow.PrimitiveTypes.Int64:
		return castToInt64(s)
	case arrow.PrimitiveTypes.Float32:
		return castToFloat32(s)
	case arrow.PrimitiveTypes.Float64:
		return castToFloat64(s)
	case arrow.BinaryTypes.String:
		return castToString(s)
	// case arrow.PrimitiveTypes.Uint64:
	default:
		panic("series: cast: unsupported type")
	}

	return s
}

// Unique returns a new series with only unique values.
func (s Series) Unique() Series {
	s.Retain()
	defer s.Release()

	switch s.field.Type {
	case arrow.BinaryTypes.String:
		vals := stringUnique(s.Interface.(*array.String))
		return FromString(s.pool, s.field, vals, nil)
	case arrow.PrimitiveTypes.Int32:
		vals := int32Unique(s.Interface.(*array.Int32))
		return FromInt32(s.pool, s.field, vals, nil)
	case arrow.PrimitiveTypes.Int64:
		vals := int64Unique(s.Interface.(*array.Int64))
		return FromInt64(s.pool, s.field, vals, nil)
	case arrow.PrimitiveTypes.Float32:
		vals := float32Unique(s.Interface.(*array.Float32))
		return FromFloat32(s.pool, s.field, vals, nil)
	case arrow.PrimitiveTypes.Float64:
		vals := float64Unique(s.Interface.(*array.Float64))
		return FromFloat64(s.pool, s.field, vals, nil)
	// case arrow.PrimitiveTypes.Uint64:
	default:
		panic("series: unique: unsupported type")
	}
}

// FindIndices returns a slice of indices where the value exists.
func (s Series) FindIndices(val interface{}) []int {
	s.Retain()
	defer s.Release()

	var result []int
	switch vals := s.Values().(type) {
	case []int32:
		switch v := val.(type) {
		case string:
			b, err := strconv.ParseInt(v, 10, 32)
			if err != nil {
				panic(fmt.Sprintf("series: find_indices: %s", err))
			}
			val = int32(b)
		}

		for i, v := range vals {
			if v != val || s.IsNull(i) {
				continue
			}
			result = append(result, i)
		}
	case []int64:
		switch v := val.(type) {
		case string:
			b, err := strconv.ParseInt(v, 10, 64)
			if err != nil {
				panic(fmt.Sprintf("series: find_indices: %s", err))
			}
			val = b
		}

		for i, v := range vals {
			if v != val || s.IsNull(i) {
				continue
			}
			result = append(result, i)
		}
	case []float32:
		switch v := val.(type) {
		case string:
			b, err := strconv.ParseFloat(v, 32)
			if err != nil {
				panic(fmt.Sprintf("series: find_indices: %s", err))
			}
			val = float32(b)
		}

		for i, v := range vals {
			if v != val || s.IsNull(i) {
				continue
			}
			result = append(result, i)
		}
	case []float64:
		switch v := val.(type) {
		case string:
			b, err := strconv.ParseFloat(v, 64)
			if err != nil {
				panic(fmt.Sprintf("series: find_indices: %s", err))
			}
			val = b
		}

		for i, v := range vals {
			if v != val || s.IsNull(i) {
				continue
			}
			result = append(result, i)
		}
	case []string:
		for i, v := range vals {
			if v != val || s.IsNull(i) {
				continue
			}
			result = append(result, i)
		}
	default:
		panic("series: find_indices: unknown type")
	}

	return result
}

// NAIndices returns the indices where values are null.
//
// TODO{poopoothegorilla): COULD BE IMPROVED PERFORMANCE WISE... MAYBE USE NULL BITMASK
func (s Series) NAIndices() []int {
	s.Retain()

	var j int
	result := make([]int, s.NullN())
	for i := 0; i < s.Len(); i++ {
		if s.IsNull(i) {
			result[j] = i
			j++
		}
	}

	s.Release()
	return result
}

// IsNA returns a slice of bools indicating positions where values are null.
func (s Series) IsNA() []bool {
	s.Retain()

	result := make([]bool, s.Len())
	for i := 0; i < s.Len(); i++ {
		result[i] = s.IsNull(i)
	}

	s.Release()
	return result
}

// DropIndices returns a Series without values located at the provided indices.
//
// TODO(poopoothegorilla): add DropByMask either as seperate function or choose
// by options.
func (s Series) DropIndices(indices []int) Series {
	s.Retain()
	defer s.Release()

	if !sort.IntsAreSorted(indices) {
		sort.Ints(indices)
	}

	switch s.field.Type {
	case arrow.PrimitiveTypes.Int32:
		vals := int32DropIndices(s.Interface.(*array.Int32), indices)
		return FromInt32(s.pool, s.field, vals, nil)
	case arrow.PrimitiveTypes.Int64:
		vals := int64DropIndices(s.Interface.(*array.Int64), indices)
		return FromInt64(s.pool, s.field, vals, nil)
	case arrow.PrimitiveTypes.Float32:
		vals := float32DropIndices(s.Interface.(*array.Float32), indices)
		return FromFloat32(s.pool, s.field, vals, nil)
	case arrow.PrimitiveTypes.Float64:
		vals := float64DropIndices(s.Interface.(*array.Float64), indices)
		return FromFloat64(s.pool, s.field, vals, nil)
	// case arrow.PrimitiveTypes.Uint64:
	default:
		panic("series: drop_indices: unsupported type")
	}
}

// SelectIndices returns a Series with values only present in the provided
// indices.
func (s Series) SelectIndices(indices []int) Series {
	s.Retain()
	defer s.Release()

	if !sort.IntsAreSorted(indices) {
		sort.Ints(indices)
	}

	switch s.field.Type {
	case arrow.PrimitiveTypes.Int32:
		vals := int32SelectIndices(s.Interface.(*array.Int32), indices)
		return FromInt32(s.pool, s.field, vals, nil)
	case arrow.PrimitiveTypes.Int64:
		vals := int64SelectIndices(s.Interface.(*array.Int64), indices)
		return FromInt64(s.pool, s.field, vals, nil)
	case arrow.PrimitiveTypes.Float32:
		vals := float32SelectIndices(s.Interface.(*array.Float32), indices)
		return FromFloat32(s.pool, s.field, vals, nil)
	case arrow.PrimitiveTypes.Float64:
		vals := float64SelectIndices(s.Interface.(*array.Float64), indices)
		return FromFloat64(s.pool, s.field, vals, nil)
	// case arrow.PrimitiveTypes.Uint64:
	default:
		panic("series: select: unsupported type")
	}
}

// Truncate returns a truncated Series.
func (s Series) Truncate(i, j int64) Series {
	s.Retain()
	defer s.Release()

	ss := Series{
		pool:      s.pool,
		field:     s.field,
		Interface: array.NewSlice(s.Interface, i, j),
	}

	return ss
}

// Sum returns the sum of all values in the Series as a float64 value.
func (s Series) Sum() float64 {
	s.Retain()
	defer s.Release()

	switch s.field.Type {
	case arrow.PrimitiveTypes.Int32:
		val := int32Sum(s.Interface.(*array.Int32))
		return float64(val)
	case arrow.PrimitiveTypes.Int64:
		val := int64Sum(s.Interface.(*array.Int64))
		return float64(val)
	case arrow.PrimitiveTypes.Float32:
		val := float32Sum(s.Interface.(*array.Float32))
		return float64(val)
	case arrow.PrimitiveTypes.Float64:
		return float64Sum(s.Interface.(*array.Float64))
	// case arrow.PrimitiveTypes.Uint64:
	// 	val := uint64Sum(s.Interface.(*array.Uint64))
	// 	return float64(val)
	default:
		return mat.Sum(s)
	}
}

// Magnitude returns the magnitude of the Series as a float64 value.
func (s Series) Magnitude() float64 {
	s.Retain()
	defer s.Release()

	switch s.field.Type {
	case arrow.PrimitiveTypes.Int32:
		return int32Magnitude(s.Interface.(*array.Int32))
	case arrow.PrimitiveTypes.Int64:
		return int64Magnitude(s.Interface.(*array.Int64))
	case arrow.PrimitiveTypes.Float32:
		return float32Magnitude(s.Interface.(*array.Float32))
	case arrow.PrimitiveTypes.Float64:
		return float64Magnitude(s.Interface.(*array.Float64))
	// case arrow.PrimitiveTypes.Uint64:
	// 	val := uint64Magnitude(s.Interface.(*array.Uint64))
	// 	return float64(val)
	default:
		return mat.Norm(s, 2)
	}
}

// STD returns the standard deviation of the Series as a float64 value.
func (s Series) STD() float64 {
	s.Retain()
	defer s.Release()

	switch s.field.Type {
	case arrow.PrimitiveTypes.Int32:
		vals := make([]float64, s.Len())
		for i, v := range s.Interface.(*array.Int32).Int32Values() {
			vals[i] = float64(v)
		}
		return stat.StdDev(vals, nil)
	case arrow.PrimitiveTypes.Int64:
		vals := make([]float64, s.Len())
		for i, v := range s.Interface.(*array.Int64).Int64Values() {
			vals[i] = float64(v)
		}
		return stat.StdDev(vals, nil)
	case arrow.PrimitiveTypes.Float32:
		vals := make([]float64, s.Len())
		for i, v := range s.Interface.(*array.Float32).Float32Values() {
			vals[i] = float64(v)
		}
		return stat.StdDev(vals, nil)
	case arrow.PrimitiveTypes.Float64:
		vals := s.Interface.(*array.Float64).Float64Values()
		return stat.StdDev(vals, nil)
	// case arrow.PrimitiveTypes.Uint64:
	// 	v := s.Interface.(*array.Uint64).Uint64Values()
	// 	vv := ss.Interface.(*array.Uint64).Uint64Values()
	default:
		panic("series: std: unsupported type")
	}
}

// Rename returns a Series with a new name.
func (s Series) Rename(name string) Series {
	s.field.Name = name
	return s
}

// Map applies the fn function to all values in the s Series and returns a Series
// with the resulting values.
func (s Series) Map(fn func(interface{}) interface{}) Series {
	vals := s.Values()
	switch vs := vals.(type) {
	case []int32:
		result := make([]int32, s.Len())
		for i, v := range vs {
			result[i] = fn(v).(int32)
		}
		return FromInt32(s.pool, s.field, result, nil)
	case []int64:
		result := make([]int64, s.Len())
		for i, v := range vs {
			result[i] = fn(v).(int64)
		}
		return FromInt64(s.pool, s.field, result, nil)
	case []float32:
		result := make([]float32, s.Len())
		for i, v := range vs {
			result[i] = fn(v).(float32)
		}
		return FromFloat32(s.pool, s.field, result, nil)
	case []float64:
		result := make([]float64, s.Len())
		for i, v := range vs {
			result[i] = fn(v).(float64)
		}
		return FromFloat64(s.pool, s.field, result, nil)
	default:
		panic("series: map: unsupported type")
	}
}

// Condition represents a function which can be applied to any value and return
// a boolean.
type Condition func(interface{}) bool

// Where returns a Series where values satisfy any of the conditions.
func (s Series) Where(cs ...Condition) Series {
	s.Retain()
	defer s.Release()

	switch s.field.Type {
	case arrow.PrimitiveTypes.Int32:
		vals := make([]int32, 0, s.Len())
		for _, v := range s.Interface.(*array.Int32).Int32Values() {
			for _, conditionFunc := range cs {
				if conditionFunc(v) {
					vals = append(vals, v)
					break
				}
			}
		}
		return FromInt32(s.pool, s.field, vals, nil)
	case arrow.PrimitiveTypes.Int64:
		vals := make([]int64, 0, s.Len())
		for _, v := range s.Interface.(*array.Int64).Int64Values() {
			for _, conditionFunc := range cs {
				if conditionFunc(v) {
					vals = append(vals, v)
					break
				}
			}
		}
		return FromInt64(s.pool, s.field, vals, nil)
	case arrow.PrimitiveTypes.Float32:
		vals := make([]float32, 0, s.Len())
		for _, v := range s.Interface.(*array.Float32).Float32Values() {
			for _, conditionFunc := range cs {
				if conditionFunc(v) {
					vals = append(vals, v)
					break
				}
			}
		}
		return FromFloat32(s.pool, s.field, vals, nil)
	case arrow.PrimitiveTypes.Float64:
		vals := make([]float64, 0, s.Len())
		for _, v := range s.Interface.(*array.Float64).Float64Values() {
			for _, conditionFunc := range cs {
				if conditionFunc(v) {
					vals = append(vals, v)
					break
				}
			}
		}
		return FromFloat64(s.pool, s.field, vals, nil)
	// // case arrow.PrimitiveTypes.Uint64:
	// // 	v := s.Interface.(*array.Uint64).Uint64Values()
	// // 	vv := ss.Interface.(*array.Uint64).Uint64Values()
	default:
		panic("series: where: unsupported type")
	}
}

// Head returns a Series with n values.
func (s Series) Head(n int) Series {
	s.Retain()
	defer s.Release()

	if s.Len() < n {
		n = s.Len()
	}

	return Series{
		pool:      s.pool,
		field:     s.field,
		Interface: array.NewSlice(s.Interface, 0, int64(n)),
	}
}

// SortValues returns a Series with sorted values.
//
// TODO(poopoothegorilla): add sort options
func (s Series) SortValues() Series {
	s.Retain()
	defer s.Release()

	switch s.field.Type {
	case arrow.PrimitiveTypes.Int32:
		vv := make([]int, s.Len())
		for i, v := range s.Interface.(*array.Int32).Int32Values() {
			vv[i] = int(v)
		}
		sort.Ints(vv)
		sortedVals := make([]int32, s.Len())
		for i, v := range vv {
			sortedVals[i] = int32(v)
		}

		return FromInt32(s.pool, s.field, sortedVals, nil)
	case arrow.PrimitiveTypes.Int64:
		vv := make([]int, s.Len())
		for i, v := range s.Interface.(*array.Int64).Int64Values() {
			vv[i] = int(v)
		}
		sort.Ints(vv)
		sortedVals := make([]int64, s.Len())
		for i, v := range vv {
			sortedVals[i] = int64(v)
		}

		return FromInt64(s.pool, s.field, sortedVals, nil)
	case arrow.PrimitiveTypes.Float32:
		vv := make([]float64, s.Len())
		for i, v := range s.Interface.(*array.Float32).Float32Values() {
			vv[i] = float64(v)
		}
		sort.Float64s(vv)
		sortedVals := make([]float32, s.Len())
		for i, v := range vv {
			sortedVals[i] = float32(v)
		}

		return FromFloat32(s.pool, s.field, sortedVals, nil)
	case arrow.PrimitiveTypes.Float64:
		vals := s.Interface.(*array.Float64).Float64Values()
		sort.Float64s(vals)
		return FromFloat64(s.pool, s.field, vals, nil)
	default:
		panic("series: sort_values: unknown type")
	}
}

// DropNA returns a Series without null values.
func (s Series) DropNA() Series {
	s.Retain()
	defer s.Release()

	switch s.field.Type {
	case arrow.PrimitiveTypes.Int32:
		vals := make([]int32, 0, s.Len()-s.NullN())
		is := s.Interface.(*array.Int32)
		for i := 0; i < is.Len(); i++ {
			if is.IsNull(i) {
				continue
			}
			vals = append(vals, is.Value(i))
		}
		return FromInt32(s.pool, s.field, vals, nil)
	case arrow.PrimitiveTypes.Int64:
		vals := make([]int64, 0, s.Len()-s.NullN())
		is := s.Interface.(*array.Int64)
		for i := 0; i < is.Len(); i++ {
			if is.IsNull(i) {
				continue
			}
			vals = append(vals, is.Value(i))
		}
		return FromInt64(s.pool, s.field, vals, nil)
	case arrow.PrimitiveTypes.Float32:
		vals := make([]float32, 0, s.Len()-s.NullN())
		is := s.Interface.(*array.Float32)
		for i := 0; i < is.Len(); i++ {
			if is.IsNull(i) {
				continue
			}
			vals = append(vals, is.Value(i))
		}
		return FromFloat32(s.pool, s.field, vals, nil)
	case arrow.PrimitiveTypes.Float64:
		vals := make([]float64, 0, s.Len()-s.NullN())
		is := s.Interface.(*array.Float64)
		for i := 0; i < is.Len(); i++ {
			if is.IsNull(i) {
				continue
			}
			vals = append(vals, is.Value(i))
		}
		return FromFloat64(s.pool, s.field, vals, nil)
	default:
		panic("series: drop_na: unsupported type")
	}
}

// Dot returns the Dot product of all values in the Series as a float64 value.
func (s Series) Dot(ss Series) float64 {
	s.Retain()
	defer s.Release()
	ss.Retain()
	defer ss.Release()

	if s.Len() != ss.Len() {
		panic("series: dot: series lengths do not match")
	}
	if s.field.Type != ss.field.Type {
		panic("series: dot: series types do not match")
	}

	switch s.field.Type {
	case arrow.PrimitiveTypes.Int32:
		v := s.Interface.(*array.Int32)
		vv := ss.Interface.(*array.Int32)
		return float64(int32Dot(v, vv))
	case arrow.PrimitiveTypes.Int64:
		v := s.Interface.(*array.Int64)
		vv := ss.Interface.(*array.Int64)
		return float64(int64Dot(v, vv))
	case arrow.PrimitiveTypes.Float32:
		v := s.Interface.(*array.Float32)
		vv := ss.Interface.(*array.Float32)
		return float64(float32Dot(v, vv))
	case arrow.PrimitiveTypes.Float64:
		v := s.Interface.(*array.Float64).Float64Values()
		vv := ss.Interface.(*array.Float64).Float64Values()
		return floats.Dot(v, vv)
	// case arrow.PrimitiveTypes.Uint64:
	// 	v := s.Interface.(*array.Uint64)
	// 	vv := ss.Interface.(*array.Uint64)
	// return float64(uint64Dot(v, vv))
	default:
		return mat.Dot(s, ss)
	}
}

// Abs returns a Series with all absolute values.
//
// TODO(poopoothegorilla): need to pass valids into new Series.
func (s Series) Abs() Series {
	s.Retain()
	defer s.Release()

	switch s.field.Type {
	case arrow.PrimitiveTypes.Int32:
		v := s.Interface.(*array.Int32)
		f := arrow.Field{Name: s.field.Name, Type: arrow.PrimitiveTypes.Int32}
		return FromInt32(s.pool, f, int32Abs(v), nil)
	case arrow.PrimitiveTypes.Int64:
		v := s.Interface.(*array.Int64)
		f := arrow.Field{Name: s.field.Name, Type: arrow.PrimitiveTypes.Int64}
		return FromInt64(s.pool, f, int64Abs(v), nil)
	case arrow.PrimitiveTypes.Float32:
		v := s.Interface.(*array.Float32)
		f := arrow.Field{Name: s.field.Name, Type: arrow.PrimitiveTypes.Float32}
		return FromFloat32(s.pool, f, float32Abs(v), nil)
	case arrow.PrimitiveTypes.Float64:
		v := s.Interface.(*array.Float64)
		f := arrow.Field{Name: s.field.Name, Type: arrow.PrimitiveTypes.Float64}
		return FromFloat64(s.pool, f, float64Abs(v), nil)
	// case arrow.PrimitiveTypes.Uint64:
	// 	v := s.Interface.(*array.Uint64)
	// return float64(uint64Square(v))
	default:
		panic("series: square: unsupported type")
	}
}

// Min returns the minimum value of the Series as a float64.
func (s Series) Min() float64 {
	s.Retain()
	defer s.Release()

	switch s.field.Type {
	case arrow.PrimitiveTypes.Int32:
		v := s.Interface.(*array.Int32)
		return int32Min(v)
	case arrow.PrimitiveTypes.Int64:
		v := s.Interface.(*array.Int64)
		return int64Min(v)
	case arrow.PrimitiveTypes.Float32:
		v := s.Interface.(*array.Float32)
		return float32Min(v)
	case arrow.PrimitiveTypes.Float64:
		v := s.Interface.(*array.Float64)
		return float64Min(v)
	default:
		panic("series: min: unsupported type")
	}
}

// Max returns the maximum value of the Series as a float64.
func (s Series) Max() float64 {
	s.Retain()
	defer s.Release()

	switch s.field.Type {
	case arrow.PrimitiveTypes.Int32:
		v := s.Interface.(*array.Int32)
		return int32Max(v)
	case arrow.PrimitiveTypes.Int64:
		v := s.Interface.(*array.Int64)
		return int64Max(v)
	case arrow.PrimitiveTypes.Float32:
		v := s.Interface.(*array.Float32)
		return float32Max(v)
	case arrow.PrimitiveTypes.Float64:
		v := s.Interface.(*array.Float64)
		return float64Max(v)
	default:
		panic("series: max: unsupported type")
	}
}

// Mean retuns the mean of the Series as a float64.
func (s Series) Mean() float64 {
	s.Retain()
	defer s.Release()

	switch s.field.Type {
	case arrow.PrimitiveTypes.Int32:
		v := s.Interface.(*array.Int32)
		return int32Mean(v)
	case arrow.PrimitiveTypes.Int64:
		v := s.Interface.(*array.Int64)
		return int64Mean(v)
	case arrow.PrimitiveTypes.Float32:
		v := s.Interface.(*array.Float32)
		return float32Mean(v)
	case arrow.PrimitiveTypes.Float64:
		v := s.Interface.(*array.Float64)
		return float64Mean(v)
	default:
		panic("series: mean: unsupported type")
	}
}

// Median returns the median value of the Series as a float64.
//
// TODO(poopoothegorilla): inefficient could be improved
func (s Series) Median() float64 {
	s.Retain()
	defer s.Release()

	s2 := s.SortValues()
	defer s2.Release()

	m := s2.Len() / 2
	rv := s2.AtVec(m)
	if s2.Len()%2 != 0 {
		return rv
	}
	lv := s2.AtVec(m - 1)
	return (lv + rv) / float64(2)
}

// Square returns a Series with all values squared.
func (s Series) Square() Series {
	s.Retain()
	defer s.Release()

	switch s.field.Type {
	case arrow.PrimitiveTypes.Int32:
		v := s.Interface.(*array.Int32)
		f := arrow.Field{Name: s.field.Name, Type: arrow.PrimitiveTypes.Int64}
		return FromInt64(s.pool, f, int32Square(v), nil)
	case arrow.PrimitiveTypes.Int64:
		v := s.Interface.(*array.Int64)
		f := arrow.Field{Name: s.field.Name, Type: arrow.PrimitiveTypes.Int64}
		return FromInt64(s.pool, f, int64Square(v), nil)
	case arrow.PrimitiveTypes.Float32:
		v := s.Interface.(*array.Float32)
		f := arrow.Field{Name: s.field.Name, Type: arrow.PrimitiveTypes.Float64}
		return FromFloat64(s.pool, f, float32Square(v), nil)
	case arrow.PrimitiveTypes.Float64:
		v := s.Interface.(*array.Float64)
		f := arrow.Field{Name: s.field.Name, Type: arrow.PrimitiveTypes.Float64}
		return FromFloat64(s.pool, f, float64Square(v), nil)
	// case arrow.PrimitiveTypes.Uint64:
	// 	v := s.Interface.(*array.Uint64)
	// return float64(uint64Square(v))
	default:
		panic("series: square: unsupported type")
	}
}

// Sqrt returns a Series with the square root of all values.
func (s Series) Sqrt() Series {
	s.Retain()
	defer s.Release()

	var vals []float64
	switch s.field.Type {
	case arrow.PrimitiveTypes.Int32:
		v := s.Interface.(*array.Int32)
		vals = int32Sqrt(v)
	case arrow.PrimitiveTypes.Int64:
		v := s.Interface.(*array.Int64)
		vals = int64Sqrt(v)
	case arrow.PrimitiveTypes.Float32:
		v := s.Interface.(*array.Float32)
		vals = float32Sqrt(v)
	case arrow.PrimitiveTypes.Float64:
		v := s.Interface.(*array.Float64)
		vals = float64Sqrt(v)
	// case arrow.PrimitiveTypes.Uint64:
	// 	v := s.Interface.(*array.Uint64)
	// return float64(uint64Square(v))
	default:
		panic("series: sqrt: unsupported type")
	}

	f := arrow.Field{Name: s.field.Name, Type: arrow.PrimitiveTypes.Float64}
	return FromFloat64(s.pool, f, vals, nil)
}

// Add adds two equal length and type Series and returns the resulting Series.
func (s Series) Add(ss Series) Series {
	s.Retain()
	defer s.Release()
	ss.Retain()
	defer ss.Release()

	if s.Len() != ss.Len() {
		panic("series: add: series lengths do not match")
	}
	if s.field.Type != ss.field.Type {
		panic("series: add: series types do not match")
	}

	switch s.field.Type {
	case arrow.PrimitiveTypes.Int32:
		v := s.Interface.(*array.Int32)
		vv := ss.Interface.(*array.Int32)
		return FromInt32(s.pool, s.field, int32Add(v, vv), nil)
	case arrow.PrimitiveTypes.Int64:
		v := s.Interface.(*array.Int64)
		vv := ss.Interface.(*array.Int64)
		return FromInt64(s.pool, s.field, int64Add(v, vv), nil)
	case arrow.PrimitiveTypes.Float32:
		v := s.Interface.(*array.Float32)
		vv := ss.Interface.(*array.Float32)
		return FromFloat32(s.pool, s.field, float32Add(v, vv), nil)
	case arrow.PrimitiveTypes.Float64:
		v := s.Interface.(*array.Float64).Float64Values()
		vv := ss.Interface.(*array.Float64).Float64Values()
		dst := make([]float64, s.Len())
		floats.SubTo(dst, v, vv)
		return FromFloat64(s.pool, s.field, floats.AddTo(dst, v, vv), nil)
	// case arrow.PrimitiveTypes.Uint64:
	// 	v := s.Interface.(*array.Uint64).Uint64Values()
	// 	vv := ss.Interface.(*array.Uint64).Uint64Values()
	default:
		panic("series: add: unsupported type")
	}
}

// Subtract subtracts two equal length and type Series and returns the resulting
// Series.
func (s Series) Subtract(ss Series) Series {
	s.Retain()
	defer s.Release()
	ss.Retain()
	defer ss.Release()

	if s.Len() != ss.Len() {
		panic("series: subtract: series lengths do not match")
	}
	if s.field.Type != ss.field.Type {
		panic("series: subtract: series types do not match")
	}

	switch s.field.Type {
	case arrow.PrimitiveTypes.Int32:
		v := s.Interface.(*array.Int32)
		vv := ss.Interface.(*array.Int32)
		return FromInt32(s.pool, s.field, int32Subtract(v, vv), nil)
	case arrow.PrimitiveTypes.Int64:
		v := s.Interface.(*array.Int64)
		vv := ss.Interface.(*array.Int64)
		return FromInt64(s.pool, s.field, int64Subtract(v, vv), nil)
	case arrow.PrimitiveTypes.Float32:
		v := s.Interface.(*array.Float32)
		vv := ss.Interface.(*array.Float32)
		return FromFloat32(s.pool, s.field, float32Subtract(v, vv), nil)
	case arrow.PrimitiveTypes.Float64:
		v := s.Interface.(*array.Float64).Float64Values()
		vv := ss.Interface.(*array.Float64).Float64Values()
		dst := make([]float64, s.Len())
		floats.SubTo(dst, v, vv)
		return FromFloat64(s.pool, s.field, dst, nil)
	// case arrow.PrimitiveTypes.Uint64:
	// 	v := s.Interface.(*array.Uint64)
	// 	vv := ss.Interface.(*array.Uint64)
	// return FromUint64(s.field, uint64Subtract(v, vv), s.pool)
	default:
		panic("series: subtract: unsupported type")
	}
}

// Append returns a Series with the values from the ss Series appended to the s
// Series.
func (s Series) Append(ss Series) Series {
	s.Retain()
	defer s.Release()
	ss.Retain()
	defer ss.Release()

	if s.field.Type != ss.field.Type {
		panic("series: append: series types do not match")
	}

	switch s.field.Type {
	case arrow.PrimitiveTypes.Int32:
		v := s.Interface.(*array.Int32).Int32Values()
		vv := ss.Interface.(*array.Int32).Int32Values()
		return FromInt32(s.pool, s.field, append(v, vv...), nil)
	case arrow.PrimitiveTypes.Int64:
		v := s.Interface.(*array.Int64).Int64Values()
		vv := ss.Interface.(*array.Int64).Int64Values()
		return FromInt64(s.pool, s.field, append(v, vv...), nil)
	case arrow.PrimitiveTypes.Float32:
		v := s.Interface.(*array.Float32).Float32Values()
		vv := ss.Interface.(*array.Float32).Float32Values()
		return FromFloat32(s.pool, s.field, append(v, vv...), nil)
	case arrow.PrimitiveTypes.Float64:
		v := s.Interface.(*array.Float64).Float64Values()
		vv := ss.Interface.(*array.Float64).Float64Values()
		return FromFloat64(s.pool, s.field, append(v, vv...), nil)
	default:
		panic("series: append: unsupported type")
	}
}
