package dataframe_test

import (
	"encoding/csv"
	"math"
	"strings"
	"testing"

	"github.com/apache/arrow/go/arrow"
	"github.com/apache/arrow/go/arrow/array"
	"github.com/apache/arrow/go/arrow/memory"
	"github.com/poopoothegorilla/fastframe/dataframe"
	"github.com/poopoothegorilla/fastframe/series"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gonum.org/v1/gonum/mat"
)

var (
	_ array.Table = &dataframe.DataFrame{}
	_ mat.Matrix  = &dataframe.DataFrame{}
)

// TODO(poopoothegorilla): this needs to have a test case with multiple records
// being added.
func TestNewFromRecords(t *testing.T) {
	tests := []struct {
		scenario string

		inRecords func(memory.Allocator) []array.Record

		expNumCols   int
		expNumRows   int
		exp          []interface{}
		expNAIndices [][]int
	}{
		{
			// TODO: CHANGE THE FIELDS TO BE ALL TYPES
			scenario: "valid",
			inRecords: func(pool memory.Allocator) []array.Record {
				fields := []arrow.Field{
					arrow.Field{Name: "f1-i32", Type: arrow.PrimitiveTypes.Int32},
					arrow.Field{Name: "f2-i64", Type: arrow.PrimitiveTypes.Int64},
					arrow.Field{Name: "f3-f32", Type: arrow.PrimitiveTypes.Float32},
					arrow.Field{Name: "f4-f64", Type: arrow.PrimitiveTypes.Float64},
				}
				schema := arrow.NewSchema(fields, nil)

				var result []array.Record
				var cols []array.Interface

				bi32 := array.NewInt32Builder(pool)
				defer bi32.Release()
				bi64 := array.NewInt64Builder(pool)
				defer bi64.Release()
				bf32 := array.NewFloat32Builder(pool)
				defer bf32.Release()
				bf64 := array.NewFloat64Builder(pool)
				defer bf64.Release()

				bi32.AppendValues(
					[]int32{33, 44, 55, 66, 77},
					[]bool{true, true, false, true, true},
				)
				bi64.AppendValues(
					[]int64{333, 444, 555, 666, 777},
					[]bool{true, false, true, false, true},
				)
				bf32.AppendValues(
					[]float32{3333, 4444, 5555, 6666, 7777},
					[]bool{false, true, true, true, false},
				)
				bf64.AppendValues(
					[]float64{33333, 44444, 55555, 66666, 77777},
					[]bool{false, true, false, true, false},
				)

				arri32 := bi32.NewArray()
				defer arri32.Release()
				arri64 := bi64.NewArray()
				defer arri64.Release()
				arrf32 := bf32.NewArray()
				defer arrf32.Release()
				arrf64 := bf64.NewArray()
				defer arrf64.Release()

				cols = append(cols, arri32)
				cols = append(cols, arri64)
				cols = append(cols, arrf32)
				cols = append(cols, arrf64)

				result = append(result, array.NewRecord(schema, cols, -1))
				return result
			},
			expNumCols: 4,
			expNumRows: 5,
			exp: []interface{}{
				[]int32{33, 44, 55, 66, 77},
				[]int64{333, 444, 555, 666, 777},
				[]float32{3333, 4444, 5555, 6666, 7777},
				[]float64{33333, 44444, 55555, 66666, 77777},
			},
			expNAIndices: [][]int{
				[]int{2},
				[]int{1, 3},
				[]int{0, 4},
				[]int{0, 2, 4},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.scenario, func(t *testing.T) {
			pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
			defer pool.AssertSize(t, 0)

			inRecords := tt.inRecords(pool)
			for i := range inRecords {
				defer inRecords[i].Release()
			}

			act := dataframe.NewFromRecords(pool, inRecords)
			defer act.Release()

			numR, numC := act.Dims()
			require.Equal(t, tt.expNumCols, numC)
			require.Equal(t, tt.expNumRows, numR)

			for i := range tt.exp {
				assert.Equal(t, tt.exp[i], act.Series(i).Values())
				assert.Equal(t, tt.expNAIndices[i], act.Series(i).NAIndices())
			}
		})
	}
}

func TestNewFromSeries(t *testing.T) {
	tests := []struct {
		scenario string

		inSeries func(memory.Allocator) []series.Series

		expNumCols int
		expNumRows int
		exp        []interface{}
	}{
		{
			// TODO: CHANGE THE FIELDS TO BE ALL TYPES
			scenario: "valid",
			inSeries: func(pool memory.Allocator) []series.Series {
				var result []series.Series
				f1 := arrow.Field{Name: "f1-i64", Type: arrow.PrimitiveTypes.Int32}
				vals1 := []int32{1, 2, 3, 4, 5}
				f2 := arrow.Field{Name: "f2-i64", Type: arrow.PrimitiveTypes.Int64}
				vals2 := []int64{11, 22, 33, 44, 55}
				f3 := arrow.Field{Name: "f3-f32", Type: arrow.PrimitiveTypes.Float32}
				vals3 := []float32{111, 222, 333, 444, 555}
				f4 := arrow.Field{Name: "f4-f64", Type: arrow.PrimitiveTypes.Float64}
				vals4 := []float64{1111, 2222, 3333, 4444, 5555}

				result = append(result, series.FromInt32(pool, f1, vals1, nil))
				result = append(result, series.FromInt64(pool, f2, vals2, nil))
				result = append(result, series.FromFloat32(pool, f3, vals3, nil))
				result = append(result, series.FromFloat64(pool, f4, vals4, nil))

				return result
			},
			expNumCols: 4,
			expNumRows: 5,
			exp: []interface{}{
				[]int32{1, 2, 3, 4, 5},
				[]int64{11, 22, 33, 44, 55},
				[]float32{111, 222, 333, 444, 555},
				[]float64{1111, 2222, 3333, 4444, 5555},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.scenario, func(t *testing.T) {
			pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
			defer pool.AssertSize(t, 0)

			inSeries := tt.inSeries(pool)
			for i := range inSeries {
				defer inSeries[i].Release()
			}

			act := dataframe.NewFromSeries(pool, inSeries)
			defer act.Release()

			numR, numC := act.Dims()
			require.Equal(t, tt.expNumCols, numC)
			require.Equal(t, tt.expNumRows, numR)

			for i := range tt.exp {
				assert.Equal(t, tt.exp[i], act.Series(i).Values())
			}
		})
	}
}

func TestNewFromCSV(t *testing.T) {
	tests := []struct {
		scenario string

		inCSV string

		expNumCols int
		expNumRows int
		exp        []interface{}
	}{
		{
			// TODO: CHANGE THE FIELDS TO BE ALL TYPES
			scenario: "valid",
			inCSV: `"f1-f64","f2-f64","f3-f64","f4-f64","f5-str"
1,11,111,1111,john
2,22,222,2222,jim
3,33,333,3333,julie
4,44,444,4444,alex
5,55,555,5555,shaydul`,
			expNumCols: 5,
			expNumRows: 5,
			exp: []interface{}{
				[]string{"1", "2", "3", "4", "5"},
				[]string{"11", "22", "33", "44", "55"},
				[]string{"111", "222", "333", "444", "555"},
				[]string{"1111", "2222", "3333", "4444", "5555"},
				[]string{"john", "jim", "julie", "alex", "shaydul"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.scenario, func(t *testing.T) {
			pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
			defer pool.AssertSize(t, 0)

			inCSVReader := csv.NewReader(strings.NewReader(tt.inCSV))

			act := dataframe.NewFromCSV(pool, inCSVReader, -1, nil)
			defer act.Release()

			numR, numC := act.Dims()
			require.Equal(t, tt.expNumCols, numC)
			require.Equal(t, tt.expNumRows, numR)

			for i := range tt.exp {
				assert.Equal(t, tt.exp[i], act.Series(i).Values())
			}
		})
	}
}

func TestSubtract(t *testing.T) {
	tests := []struct {
		scenario string

		inSeries  func(memory.Allocator) []series.Series
		inSeries2 func(memory.Allocator) []series.Series

		expNumCols int
		expNumRows int
		exp        []interface{}
	}{
		{
			scenario: "subtract dataframes",
			inSeries: func(pool memory.Allocator) []series.Series {
				var result []series.Series
				f1 := arrow.Field{Name: "f1-i64", Type: arrow.PrimitiveTypes.Int32}
				vals1 := []int32{1, 2, 3, 4, 5}
				f2 := arrow.Field{Name: "f2-i64", Type: arrow.PrimitiveTypes.Int64}
				vals2 := []int64{11, 22, 33, 44, 55}
				f3 := arrow.Field{Name: "f3-f32", Type: arrow.PrimitiveTypes.Float32}
				vals3 := []float32{111, 222, 333, 444, 555}
				f4 := arrow.Field{Name: "f4-f64", Type: arrow.PrimitiveTypes.Float64}
				vals4 := []float64{1111, 2222, 3333, 4444, 5555}

				result = append(result, series.FromInt32(pool, f1, vals1, nil))
				result = append(result, series.FromInt64(pool, f2, vals2, nil))
				result = append(result, series.FromFloat32(pool, f3, vals3, nil))
				result = append(result, series.FromFloat64(pool, f4, vals4, nil))

				return result
			},
			inSeries2: func(pool memory.Allocator) []series.Series {
				var result []series.Series
				f1 := arrow.Field{Name: "f1-i64", Type: arrow.PrimitiveTypes.Int32}
				vals1 := []int32{1, 2, 3, 4, 5}
				f2 := arrow.Field{Name: "f2-i64", Type: arrow.PrimitiveTypes.Int64}
				vals2 := []int64{11, 22, 33, 44, 55}
				f3 := arrow.Field{Name: "f3-f32", Type: arrow.PrimitiveTypes.Float32}
				vals3 := []float32{111, 222, 333, 444, 555}
				f4 := arrow.Field{Name: "f4-f64", Type: arrow.PrimitiveTypes.Float64}
				vals4 := []float64{1111, 2222, 3333, 4444, 5555}

				result = append(result, series.FromInt32(pool, f1, vals1, nil))
				result = append(result, series.FromInt64(pool, f2, vals2, nil))
				result = append(result, series.FromFloat32(pool, f3, vals3, nil))
				result = append(result, series.FromFloat64(pool, f4, vals4, nil))

				return result
			},
			expNumCols: 4,
			expNumRows: 5,
			exp: []interface{}{
				[]int32{0, 0, 0, 0, 0},
				[]int64{0, 0, 0, 0, 0},
				[]float32{0, 0, 0, 0, 0},
				[]float64{0, 0, 0, 0, 0},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.scenario, func(t *testing.T) {
			pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
			defer pool.AssertSize(t, 0)

			inCols := tt.inSeries(pool)
			for i := range inCols {
				defer inCols[i].Release()
			}
			inCols2 := tt.inSeries2(pool)
			for i := range inCols2 {
				defer inCols2[i].Release()
			}

			act := dataframe.NewFromSeries(pool, inCols)
			defer act.Release()
			act2 := dataframe.NewFromSeries(pool, inCols2)
			defer act2.Release()

			actAdd := act.Subtract(act2)
			defer actAdd.Release()

			numR, numC := actAdd.Dims()
			require.Equal(t, tt.expNumCols, numC)
			require.Equal(t, tt.expNumRows, numR)

			for i := range tt.exp {
				assert.Equal(t, tt.exp[i], actAdd.Series(i).Values())
			}
		})
	}
}

func TestAdd(t *testing.T) {
	tests := []struct {
		scenario string

		inSeries  func(memory.Allocator) []series.Series
		inSeries2 func(memory.Allocator) []series.Series

		expNumCols int
		expNumRows int
		exp        []interface{}
	}{
		{
			scenario: "add dataframes",
			inSeries: func(pool memory.Allocator) []series.Series {
				var result []series.Series
				f1 := arrow.Field{Name: "f1-i64", Type: arrow.PrimitiveTypes.Int32}
				vals1 := []int32{1, 2, 3, 4, 5}
				f2 := arrow.Field{Name: "f2-i64", Type: arrow.PrimitiveTypes.Int64}
				vals2 := []int64{11, 22, 33, 44, 55}
				f3 := arrow.Field{Name: "f3-f32", Type: arrow.PrimitiveTypes.Float32}
				vals3 := []float32{111, 222, 333, 444, 555}
				f4 := arrow.Field{Name: "f4-f64", Type: arrow.PrimitiveTypes.Float64}
				vals4 := []float64{1111, 2222, 3333, 4444, 5555}

				result = append(result, series.FromInt32(pool, f1, vals1, nil))
				result = append(result, series.FromInt64(pool, f2, vals2, nil))
				result = append(result, series.FromFloat32(pool, f3, vals3, nil))
				result = append(result, series.FromFloat64(pool, f4, vals4, nil))

				return result
			},
			inSeries2: func(pool memory.Allocator) []series.Series {
				var result []series.Series
				f1 := arrow.Field{Name: "f1-i64", Type: arrow.PrimitiveTypes.Int32}
				vals1 := []int32{1, 2, 3, 4, 5}
				f2 := arrow.Field{Name: "f2-i64", Type: arrow.PrimitiveTypes.Int64}
				vals2 := []int64{11, 22, 33, 44, 55}
				f3 := arrow.Field{Name: "f3-f32", Type: arrow.PrimitiveTypes.Float32}
				vals3 := []float32{111, 222, 333, 444, 555}
				f4 := arrow.Field{Name: "f4-f64", Type: arrow.PrimitiveTypes.Float64}
				vals4 := []float64{1111, 2222, 3333, 4444, 5555}

				result = append(result, series.FromInt32(pool, f1, vals1, nil))
				result = append(result, series.FromInt64(pool, f2, vals2, nil))
				result = append(result, series.FromFloat32(pool, f3, vals3, nil))
				result = append(result, series.FromFloat64(pool, f4, vals4, nil))

				return result
			},
			expNumCols: 4,
			expNumRows: 5,
			exp: []interface{}{
				[]int32{2, 4, 6, 8, 10},
				[]int64{22, 44, 66, 88, 110},
				[]float32{222, 444, 666, 888, 1110},
				[]float64{2222, 4444, 6666, 8888, 11110},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.scenario, func(t *testing.T) {
			pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
			defer pool.AssertSize(t, 0)

			inCols := tt.inSeries(pool)
			for i := range inCols {
				defer inCols[i].Release()
			}
			inCols2 := tt.inSeries2(pool)
			for i := range inCols2 {
				defer inCols2[i].Release()
			}

			act := dataframe.NewFromSeries(pool, inCols)
			defer act.Release()
			act2 := dataframe.NewFromSeries(pool, inCols2)
			defer act2.Release()

			actAdd := act.Add(act2)
			defer actAdd.Release()

			numR, numC := actAdd.Dims()
			require.Equal(t, tt.expNumCols, numC)
			require.Equal(t, tt.expNumRows, numR)

			for i := range tt.exp {
				assert.Equal(t, tt.exp[i], actAdd.Series(i).Values())
			}
		})
	}
}

func TestAppendRecords(t *testing.T) {
	// tests := []struct {
	// 	scenario string
	//
	// 	inSeries  func(memory.Allocator) []series.Series
	// 	inRecords func(memory.Allocator) []array.Record
	//
	// 	expNumCols int
	// 	expNumRows int
	// 	exp        []interface{}
	// }{
	// 	{
	// 		scenario: "fixed size append",
	// 		inSeries: func(pool memory.Allocator) []series.Series {
	// 			var result []series.Series
	// 			f1 := arrow.Field{Name: "f1-i32", Type: arrow.PrimitiveTypes.Int32}
	// 			vals1 := []int32{1, 2, 3, 4, 5}
	// 			f2 := arrow.Field{Name: "f2-i64", Type: arrow.PrimitiveTypes.Int64}
	// 			vals2 := []int64{0, 11, 22, 33, 44}
	// 			f3 := arrow.Field{Name: "f3-f32", Type: arrow.PrimitiveTypes.Float32}
	// 			vals3 := []float32{0, 1, 0.5, 1.1, 0.7}
	// 			f4 := arrow.Field{Name: "f4-f64", Type: arrow.PrimitiveTypes.Float64}
	// 			vals4 := []float64{1, 0, 1.1, 0.5, 1.2}
	// 			f5 := arrow.Field{Name: "f5-f64", Type: arrow.PrimitiveTypes.Float64}
	// 			vals5 := []float64{20, 21, 20.5, 21.1, 20.7}
	//
	// 			result = append(result, series.FromInt32(f1, vals1, pool))
	// 			result = append(result, series.FromInt64(f2, vals2, pool))
	// 			result = append(result, series.FromFloat32(f3, vals3, pool))
	// 			result = append(result, series.FromFloat64(f4, vals4, pool))
	// 			result = append(result, series.FromFloat64(f5, vals5, pool))
	//
	// 			return result
	// 		},
	// 		inRecords: func(pool memory.Allocator) []array.Record {
	// 			fields := []arrow.Field{
	// 				arrow.Field{Name: "f1-i32", Type: arrow.PrimitiveTypes.Int32},
	// 				arrow.Field{Name: "f2-i64", Type: arrow.PrimitiveTypes.Int64},
	// 				arrow.Field{Name: "f3-f32", Type: arrow.PrimitiveTypes.Float32},
	// 				arrow.Field{Name: "f4-f64", Type: arrow.PrimitiveTypes.Float64},
	// 				arrow.Field{Name: "f5-f64", Type: arrow.PrimitiveTypes.Float64},
	// 			}
	// 			schema := arrow.NewSchema(fields, nil)
	//
	// 			var result []array.Record
	// 			var cols []array.Interface
	//
	// 			bi32 := array.NewInt32Builder(pool)
	// 			defer bi32.Release()
	// 			bi64 := array.NewInt64Builder(pool)
	// 			defer bi64.Release()
	// 			bf32 := array.NewFloat32Builder(pool)
	// 			defer bf32.Release()
	// 			bf64 := array.NewFloat64Builder(pool)
	// 			defer bf64.Release()
	//
	// 			vi32 := []int32{33, 44, 55, 66, 77}
	// 			vi64 := []int64{333, 444, 555, 666, 777}
	// 			vf32 := []float32{3333, 4444, 5555, 6666, 7777}
	// 			vf64 := []float64{33333, 44444, 55555, 66666, 77777}
	// 			v2f64 := []float64{33333, 44444, 55555, 66666, 77777}
	//
	// 			bi32.AppendValues(vi32, nil)
	// 			arri32 := bi32.NewArray()
	// 			defer arri32.Release()
	//
	// 			bi64.AppendValues(vi64, nil)
	// 			arri64 := bi64.NewArray()
	// 			defer arri64.Release()
	//
	// 			bf32.AppendValues(vf32, nil)
	// 			arrf32 := bf32.NewArray()
	// 			defer arrf32.Release()
	//
	// 			bf64.AppendValues(vf64, nil)
	// 			arrf64 := bf64.NewArray()
	// 			defer arrf64.Release()
	//
	// 			bf64.AppendValues(v2f64, nil)
	// 			arr2f64 := bf64.NewArray()
	// 			defer arr2f64.Release()
	//
	// 			cols = append(cols, arri32)
	// 			cols = append(cols, arri64)
	// 			cols = append(cols, arrf32)
	// 			cols = append(cols, arrf64)
	// 			cols = append(cols, arr2f64)
	//
	// 			result = append(result, array.NewRecord(schema, cols, -1))
	// 			return result
	// 		},
	// 		expNumCols: 5,
	// 		expNumRows: 10,
	// 		exp: []interface{}{
	// 			[]int32{1, 2, 3, 4, 5, 33, 44, 55, 66, 77},
	// 			[]int64{0, 11, 22, 33, 44, 333, 444, 555, 666, 777},
	// 			[]float32{0, 1, 0.5, 1.1, 0.7, 3333, 4444, 5555, 6666, 7777},
	// 			[]float64{1, 0, 1.1, 0.5, 1.2, 33333, 44444, 55555, 66666, 77777},
	// 			[]float64{20, 21, 20.5, 21.1, 20.7, 33333, 44444, 55555, 66666, 77777},
	// 		},
	// 	},
	// }
	//
	// for _, tt := range tests {
	// 	t.Run(tt.scenario, func(t *testing.T) {
	// 		pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	// 		defer pool.AssertSize(t, 0)
	//
	// 		inCols := tt.inSeries(pool)
	// 		for i := range inCols {
	// 			defer inCols[i].Release()
	// 		}
	// 		inRecords := tt.inRecords(pool)
	// 		for i := range inRecords {
	// 			defer inRecords[i].Release()
	// 		}
	//
	// 		act := dataframe.NewFromSeries(inCols, pool)
	// 		defer act.Release()
	// 		actAppend := act.AppendRecords(inRecords)
	// 		defer actAppend.Release()
	//
	// 		numR, numC := actAppend.Dims()
	// 		require.Equal(t, tt.expNumCols, numC)
	// 		require.Equal(t, tt.expNumRows, numR)
	//
	// 		for i := range tt.exp {
	// 			assert.Equal(t, tt.exp[i], actAppend.Series(i).Values())
	// 		}
	// 	})
	// }
}

func TestAppendSeries(t *testing.T) {
	tests := []struct {
		scenario string

		inSeries  func(memory.Allocator) []series.Series
		inSeries2 func(memory.Allocator) []series.Series

		expNumCols int
		expNumRows int
		exp        []interface{}
	}{
		{
			scenario: "append series",
			inSeries: func(pool memory.Allocator) []series.Series {
				var result []series.Series
				f1 := arrow.Field{Name: "f1-i64", Type: arrow.PrimitiveTypes.Int32}
				vals1 := []int32{1, 2, 3, 4, 5}
				f2 := arrow.Field{Name: "f2-i64", Type: arrow.PrimitiveTypes.Int64}
				vals2 := []int64{11, 22, 33, 44, 55}
				f3 := arrow.Field{Name: "f3-f32", Type: arrow.PrimitiveTypes.Float32}
				vals3 := []float32{111, 222, 333, 444, 555}
				f4 := arrow.Field{Name: "f4-f64", Type: arrow.PrimitiveTypes.Float64}
				vals4 := []float64{1111, 2222, 3333, 4444, 5555}

				result = append(result, series.FromInt32(pool, f1, vals1, nil))
				result = append(result, series.FromInt64(pool, f2, vals2, nil))
				result = append(result, series.FromFloat32(pool, f3, vals3, nil))
				result = append(result, series.FromFloat64(pool, f4, vals4, nil))

				return result
			},
			inSeries2: func(pool memory.Allocator) []series.Series {
				var result []series.Series
				f1 := arrow.Field{Name: "f5-i64", Type: arrow.PrimitiveTypes.Int32}
				vals1 := []int32{91, 92, 93, 94, 95}
				f2 := arrow.Field{Name: "f6-i64", Type: arrow.PrimitiveTypes.Int64}
				vals2 := []int64{911, 922, 933, 944, 955}
				f3 := arrow.Field{Name: "f7-f32", Type: arrow.PrimitiveTypes.Float32}
				vals3 := []float32{9111, 9222, 9333, 9444, 9555}
				f4 := arrow.Field{Name: "f8-f64", Type: arrow.PrimitiveTypes.Float64}
				vals4 := []float64{91111, 92222, 93333, 94444, 95555}

				result = append(result, series.FromInt32(pool, f1, vals1, nil))
				result = append(result, series.FromInt64(pool, f2, vals2, nil))
				result = append(result, series.FromFloat32(pool, f3, vals3, nil))
				result = append(result, series.FromFloat64(pool, f4, vals4, nil))

				return result
			},
			expNumCols: 8,
			expNumRows: 5,
			exp: []interface{}{
				[]int32{1, 2, 3, 4, 5},
				[]int64{11, 22, 33, 44, 55},
				[]float32{111, 222, 333, 444, 555},
				[]float64{1111, 2222, 3333, 4444, 5555},
				[]int32{91, 92, 93, 94, 95},
				[]int64{911, 922, 933, 944, 955},
				[]float32{9111, 9222, 9333, 9444, 9555},
				[]float64{91111, 92222, 93333, 94444, 95555},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.scenario, func(t *testing.T) {
			pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
			defer pool.AssertSize(t, 0)

			inCols := tt.inSeries(pool)
			for i := range inCols {
				defer inCols[i].Release()
			}
			inCols2 := tt.inSeries2(pool)
			for i := range inCols2 {
				defer inCols2[i].Release()
			}

			df := dataframe.NewFromSeries(pool, inCols)
			defer df.Release()

			act := df.AppendSeries(inCols2)
			defer act.Release()

			numR, numC := act.Dims()
			require.Equal(t, tt.expNumCols, numC)
			require.Equal(t, tt.expNumRows, numR)

			for i := range tt.exp {
				assert.Equal(t, tt.exp[i], act.Series(i).Values())
			}
		})
	}
}

func TestSelectColumnsByNames(t *testing.T) {
	tests := []struct {
		scenario string

		inSeries      func(memory.Allocator) []series.Series
		inColumnNames []string

		expNumCols int
		expNumRows int
		exp        []interface{}
	}{
		{
			scenario: "select columns",
			inSeries: func(pool memory.Allocator) []series.Series {
				var result []series.Series
				f1 := arrow.Field{Name: "f1-f64", Type: arrow.PrimitiveTypes.Float64}
				vals1 := []float64{1, 1.1, 1.2, 0, 0.5}
				f2 := arrow.Field{Name: "f2-f64", Type: arrow.PrimitiveTypes.Float64}
				vals2 := []float64{0, 0.5, 1, 1.1, 1.2}
				f3 := arrow.Field{Name: "f3-f64", Type: arrow.PrimitiveTypes.Float64}
				vals3 := []float64{0, 1, 0.5, 1.1, 0.7}
				f4 := arrow.Field{Name: "f4-f64", Type: arrow.PrimitiveTypes.Float64}
				vals4 := []float64{1, 0, 1.1, 0.5, 1.2}
				f5 := arrow.Field{Name: "f5-f64", Type: arrow.PrimitiveTypes.Float64}
				vals5 := []float64{20, 21, 20.5, 21.1, 20.7}

				result = append(result, series.FromFloat64(pool, f1, vals1, nil))
				result = append(result, series.FromFloat64(pool, f2, vals2, nil))
				result = append(result, series.FromFloat64(pool, f3, vals3, nil))
				result = append(result, series.FromFloat64(pool, f4, vals4, nil))
				result = append(result, series.FromFloat64(pool, f5, vals5, nil))

				return result
			},
			inColumnNames: []string{"f3-f64", "f5-f64"},
			expNumCols:    2,
			expNumRows:    5,
			exp: []interface{}{
				[]float64{0, 1, 0.5, 1.1, 0.7},
				[]float64{20, 21, 20.5, 21.1, 20.7},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.scenario, func(t *testing.T) {
			pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
			defer pool.AssertSize(t, 0)

			inCols := tt.inSeries(pool)
			for i := range inCols {
				defer inCols[i].Release()
			}
			act := dataframe.NewFromSeries(pool, inCols)
			defer act.Release()
			actDrop := act.SelectColumnsByNames(tt.inColumnNames)
			defer actDrop.Release()

			numR, numC := actDrop.Dims()
			require.Equal(t, tt.expNumCols, numC)
			require.Equal(t, tt.expNumRows, numR)

			for i := range tt.exp {
				assert.Equal(t, tt.exp[i], actDrop.Series(i).Values())
			}
		})
	}
}

func TestDropColumnsByIndices(t *testing.T) {
	tests := []struct {
		scenario string

		inSeries        func(memory.Allocator) []series.Series
		inColumnIndices []int

		expNumCols int
		expNumRows int
		exp        []interface{}
	}{
		{
			scenario: "drop columns",
			inSeries: func(pool memory.Allocator) []series.Series {
				var result []series.Series
				f1 := arrow.Field{Name: "f1-f64", Type: arrow.PrimitiveTypes.Float64}
				vals1 := []float64{1, 1.1, 1.2, 0, 0.5}
				f2 := arrow.Field{Name: "f2-f64", Type: arrow.PrimitiveTypes.Float64}
				vals2 := []float64{0, 0.5, 1, 1.1, 1.2}
				f3 := arrow.Field{Name: "f3-f64", Type: arrow.PrimitiveTypes.Float64}
				vals3 := []float64{0, 1, 0.5, 1.1, 0.7}
				f4 := arrow.Field{Name: "f4-f64", Type: arrow.PrimitiveTypes.Float64}
				vals4 := []float64{1, 0, 1.1, 0.5, 1.2}
				f5 := arrow.Field{Name: "f5-f64", Type: arrow.PrimitiveTypes.Float64}
				vals5 := []float64{20, 21, 20.5, 21.1, 20.7}

				result = append(result, series.FromFloat64(pool, f1, vals1, nil))
				result = append(result, series.FromFloat64(pool, f2, vals2, nil))
				result = append(result, series.FromFloat64(pool, f3, vals3, nil))
				result = append(result, series.FromFloat64(pool, f4, vals4, nil))
				result = append(result, series.FromFloat64(pool, f5, vals5, nil))

				return result
			},
			inColumnIndices: []int{2, 4},
			expNumCols:      3,
			expNumRows:      5,
			exp: []interface{}{
				[]float64{1, 1.1, 1.2, 0, 0.5},
				[]float64{0, 0.5, 1, 1.1, 1.2},
				[]float64{1, 0, 1.1, 0.5, 1.2},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.scenario, func(t *testing.T) {
			pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
			defer pool.AssertSize(t, 0)

			inCols := tt.inSeries(pool)
			for i := range inCols {
				defer inCols[i].Release()
			}
			act := dataframe.NewFromSeries(pool, inCols)
			defer act.Release()
			actDrop := act.DropColumnsByIndices(tt.inColumnIndices)
			defer actDrop.Release()

			numR, numC := actDrop.Dims()
			require.Equal(t, tt.expNumCols, numC)
			require.Equal(t, tt.expNumRows, numR)

			for i := range tt.exp {
				assert.Equal(t, tt.exp[i], actDrop.Series(i).Values())
			}
		})
	}
}

func TestDropColumnsByNames(t *testing.T) {
	tests := []struct {
		scenario string

		inSeries      func(memory.Allocator) []series.Series
		inColumnNames []string

		expNumCols int
		expNumRows int
		exp        []interface{}
	}{
		{
			scenario: "drop columns",
			inSeries: func(pool memory.Allocator) []series.Series {
				var result []series.Series
				f1 := arrow.Field{Name: "f1-f64", Type: arrow.PrimitiveTypes.Float64}
				vals1 := []float64{1, 1.1, 1.2, 0, 0.5}
				f2 := arrow.Field{Name: "f2-f64", Type: arrow.PrimitiveTypes.Float64}
				vals2 := []float64{0, 0.5, 1, 1.1, 1.2}
				f3 := arrow.Field{Name: "f3-f64", Type: arrow.PrimitiveTypes.Float64}
				vals3 := []float64{0, 1, 0.5, 1.1, 0.7}
				f4 := arrow.Field{Name: "f4-f64", Type: arrow.PrimitiveTypes.Float64}
				vals4 := []float64{1, 0, 1.1, 0.5, 1.2}
				f5 := arrow.Field{Name: "f5-f64", Type: arrow.PrimitiveTypes.Float64}
				vals5 := []float64{20, 21, 20.5, 21.1, 20.7}

				result = append(result, series.FromFloat64(pool, f1, vals1, nil))
				result = append(result, series.FromFloat64(pool, f2, vals2, nil))
				result = append(result, series.FromFloat64(pool, f3, vals3, nil))
				result = append(result, series.FromFloat64(pool, f4, vals4, nil))
				result = append(result, series.FromFloat64(pool, f5, vals5, nil))

				return result
			},
			inColumnNames: []string{"f3-f64", "f5-f64"},
			expNumCols:    3,
			expNumRows:    5,
			exp: []interface{}{
				[]float64{1, 1.1, 1.2, 0, 0.5},
				[]float64{0, 0.5, 1, 1.1, 1.2},
				[]float64{1, 0, 1.1, 0.5, 1.2},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.scenario, func(t *testing.T) {
			pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
			defer pool.AssertSize(t, 0)

			inCols := tt.inSeries(pool)
			for i := range inCols {
				defer inCols[i].Release()
			}
			act := dataframe.NewFromSeries(pool, inCols)
			defer act.Release()
			actDrop := act.DropColumnsByNames(tt.inColumnNames)
			defer actDrop.Release()

			numR, numC := actDrop.Dims()
			require.Equal(t, tt.expNumCols, numC)
			require.Equal(t, tt.expNumRows, numR)

			for i := range tt.exp {
				assert.Equal(t, tt.exp[i], actDrop.Series(i).Values())
			}
		})
	}
}

func TestDropRowsByIndices(t *testing.T) {
	tests := []struct {
		scenario string

		inSeries  func(memory.Allocator) []series.Series
		inIndices []int

		expNumCols int
		expNumRows int
		exp        []interface{}
	}{
		{
			scenario: "drop rows",
			inSeries: func(pool memory.Allocator) []series.Series {
				var result []series.Series
				f1 := arrow.Field{Name: "f1-i32", Type: arrow.PrimitiveTypes.Int32}
				vals1 := []int32{1, 2, 3, 4, 5}
				f2 := arrow.Field{Name: "f2-i64", Type: arrow.PrimitiveTypes.Int64}
				vals2 := []int64{0, 1, 2, 3, 4}
				f3 := arrow.Field{Name: "f3-f32", Type: arrow.PrimitiveTypes.Float32}
				vals3 := []float32{0, 1, 0.5, 1.1, 0.7}
				f4 := arrow.Field{Name: "f4-f64", Type: arrow.PrimitiveTypes.Float64}
				vals4 := []float64{1, 0, 1.1, 0.5, 1.2}

				result = append(result, series.FromInt32(pool, f1, vals1, nil))
				result = append(result, series.FromInt64(pool, f2, vals2, nil))
				result = append(result, series.FromFloat32(pool, f3, vals3, nil))
				result = append(result, series.FromFloat64(pool, f4, vals4, nil))

				return result
			},
			inIndices:  []int{2, 4},
			expNumCols: 4,
			expNumRows: 3,
			exp: []interface{}{
				[]int32{1, 2, 4},
				[]int64{0, 1, 3},
				[]float32{0, 1, 1.1},
				[]float64{1, 0, 0.5},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.scenario, func(t *testing.T) {
			pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
			defer pool.AssertSize(t, 0)

			inCols := tt.inSeries(pool)
			for i := range inCols {
				defer inCols[i].Release()
			}
			act := dataframe.NewFromSeries(pool, inCols)
			defer act.Release()
			actDrop := act.DropRowsByIndices(tt.inIndices)
			defer actDrop.Release()

			numR, numC := actDrop.Dims()
			require.Equal(t, tt.expNumCols, numC)
			require.Equal(t, tt.expNumRows, numR)

			for i := range tt.exp {
				assert.Equal(t, tt.exp[i], actDrop.Series(i).Values())
			}
		})
	}
}

func TestSelectRowsByIndices(t *testing.T) {
	tests := []struct {
		scenario string

		inSeries  func(memory.Allocator) []series.Series
		inIndices []int

		expNumCols int
		expNumRows int
		exp        []interface{}
	}{
		{
			scenario: "select rows",
			inSeries: func(pool memory.Allocator) []series.Series {
				var result []series.Series
				f1 := arrow.Field{Name: "f1-i32", Type: arrow.PrimitiveTypes.Int32}
				vals1 := []int32{1, 2, 3, 4, 5}
				f2 := arrow.Field{Name: "f2-i64", Type: arrow.PrimitiveTypes.Int64}
				vals2 := []int64{0, 1, 2, 3, 4}
				f3 := arrow.Field{Name: "f3-f32", Type: arrow.PrimitiveTypes.Float32}
				vals3 := []float32{0, 1, 0.5, 1.1, 0.7}
				f4 := arrow.Field{Name: "f4-f64", Type: arrow.PrimitiveTypes.Float64}
				vals4 := []float64{1, 0, 1.1, 0.5, 1.2}

				result = append(result, series.FromInt32(pool, f1, vals1, nil))
				result = append(result, series.FromInt64(pool, f2, vals2, nil))
				result = append(result, series.FromFloat32(pool, f3, vals3, nil))
				result = append(result, series.FromFloat64(pool, f4, vals4, nil))

				return result
			},
			inIndices:  []int{2, 4},
			expNumCols: 4,
			expNumRows: 2,
			exp: []interface{}{
				[]int32{3, 5},
				[]int64{2, 4},
				[]float32{0.5, 0.7},
				[]float64{1.1, 1.2},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.scenario, func(t *testing.T) {
			pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
			defer pool.AssertSize(t, 0)

			inCols := tt.inSeries(pool)
			for i := range inCols {
				defer inCols[i].Release()
			}
			df := dataframe.NewFromSeries(pool, inCols)
			defer df.Release()
			act := df.SelectRowsByIndices(tt.inIndices)
			defer act.Release()

			numR, numC := act.Dims()
			require.Equal(t, tt.expNumCols, numC)
			require.Equal(t, tt.expNumRows, numR)

			for i := range tt.exp {
				assert.Equal(t, tt.exp[i], act.Series(i).Values())
			}
		})
	}
}

func TestLeftJoinEM(t *testing.T) {
	tests := []struct {
		scenario string

		inDataFrame   func(memory.Allocator) dataframe.DataFrame
		inSeriesName  string
		inDataFrame2  func(memory.Allocator) dataframe.DataFrame
		inSeriesName2 string

		expNumCols   int
		expNumRows   int
		exp          []interface{}
		expNAIndices [][]int
	}{
		{
			scenario: "left join dataframes",
			inDataFrame: func(pool memory.Allocator) dataframe.DataFrame {
				ss := []series.Series{
					series.FromInt64(
						pool,
						arrow.Field{Name: "f1-i64", Type: arrow.PrimitiveTypes.Int64},
						[]int64{1, 2, 3, 4, 5, 11},
						nil,
					),
					series.FromInt32(
						pool,
						arrow.Field{Name: "f2-i32", Type: arrow.PrimitiveTypes.Int32},
						[]int32{11, 22, 33, 44, 55, 1111},
						nil,
					),
				}
				for _, s := range ss {
					defer s.Release()
				}

				return dataframe.NewFromSeries(pool, ss)
			},
			inSeriesName: "f1-i64",
			inDataFrame2: func(pool memory.Allocator) dataframe.DataFrame {
				ss := []series.Series{
					series.FromInt64(
						pool,
						arrow.Field{Name: "f1-i64", Type: arrow.PrimitiveTypes.Int64},
						[]int64{1, 2, 2, 5, 8, 9, 10},
						nil,
					),
					series.FromInt32(
						pool,
						arrow.Field{Name: "f3-i32", Type: arrow.PrimitiveTypes.Int32},
						[]int32{111, 222, 202020, 555, 888, 999, 101010},
						nil,
					),
				}
				for _, s := range ss {
					defer s.Release()
				}

				return dataframe.NewFromSeries(pool, ss)
			},
			inSeriesName2: "f1-i64",
			expNumCols:    3,
			expNumRows:    7,
			exp: []interface{}{
				[]int64{1, 2, 2, 3, 4, 5, 11},
				[]int32{11, 22, 22, 33, 44, 55, 1111},
				[]int32{111, 222, 202020, 0, 0, 555, 0},
			},
			expNAIndices: [][]int{
				[]int{},
				[]int{},
				[]int{3, 4, 6},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.scenario, func(t *testing.T) {
			pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
			defer pool.AssertSize(t, 0)

			act := tt.inDataFrame(pool)
			defer act.Release()
			act2 := tt.inDataFrame2(pool)
			defer act2.Release()

			actJoin := dataframe.LeftJoinEM(act, tt.inSeriesName, act2, tt.inSeriesName2)
			defer actJoin.Release()

			numR, numC := actJoin.Dims()
			require.Equal(t, tt.expNumCols, numC)
			require.Equal(t, tt.expNumRows, numR)

			for i := range tt.exp {
				assert.Equal(t, tt.exp[i], actJoin.Series(i).Values())
				assert.Equal(t, tt.expNAIndices[i], actJoin.Series(i).NAIndices())
			}
		})
	}
}

func TestLeftJoin(t *testing.T) {
	tests := []struct {
		scenario string

		inDataFrame   func(memory.Allocator) dataframe.DataFrame
		inSeriesName  string
		inDataFrame2  func(memory.Allocator) dataframe.DataFrame
		inSeriesName2 string

		expNumCols   int
		expNumRows   int
		exp          []interface{}
		expNAIndices [][]int
	}{
		{
			scenario: "left join dataframes",
			inDataFrame: func(pool memory.Allocator) dataframe.DataFrame {
				ss := []series.Series{
					series.FromInt64(
						pool,
						arrow.Field{Name: "f1-i64", Type: arrow.PrimitiveTypes.Int64},
						[]int64{1, 2, 3, 4, 5, 11},
						nil,
					),
					series.FromInt32(
						pool,
						arrow.Field{Name: "f2-i32", Type: arrow.PrimitiveTypes.Int32},
						[]int32{11, 22, 33, 44, 55, 1111},
						nil,
					),
				}
				for _, s := range ss {
					defer s.Release()
				}

				return dataframe.NewFromSeries(pool, ss)
			},
			inSeriesName: "f1-i64",
			inDataFrame2: func(pool memory.Allocator) dataframe.DataFrame {
				ss := []series.Series{
					series.FromInt64(
						pool,
						arrow.Field{Name: "f1-i64", Type: arrow.PrimitiveTypes.Int64},
						[]int64{1, 2, 2, 5, 8, 9, 10},
						nil,
					),
					series.FromInt32(
						pool,
						arrow.Field{Name: "f3-i32", Type: arrow.PrimitiveTypes.Int32},
						[]int32{111, 222, 202020, 555, 888, 999, 101010},
						nil,
					),
				}
				for _, s := range ss {
					defer s.Release()
				}

				return dataframe.NewFromSeries(pool, ss)
			},
			inSeriesName2: "f1-i64",
			expNumCols:    3,
			expNumRows:    7,
			exp: []interface{}{
				[]int64{1, 2, 2, 3, 4, 5, 11},
				[]int32{11, 22, 22, 33, 44, 55, 1111},
				[]int32{111, 222, 202020, 0, 0, 555, 0},
			},
			expNAIndices: [][]int{
				[]int{},
				[]int{},
				[]int{3, 4, 6},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.scenario, func(t *testing.T) {
			pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
			defer pool.AssertSize(t, 0)

			act := tt.inDataFrame(pool)
			defer act.Release()
			act2 := tt.inDataFrame2(pool)
			defer act2.Release()

			actJoin := dataframe.LeftJoin(act, tt.inSeriesName, act2, tt.inSeriesName2)
			defer actJoin.Release()

			numR, numC := actJoin.Dims()
			require.Equal(t, tt.expNumCols, numC)
			require.Equal(t, tt.expNumRows, numR)

			for i := range tt.exp {
				assert.Equal(t, tt.exp[i], actJoin.Series(i).Values())
				assert.Equal(t, tt.expNAIndices[i], actJoin.Series(i).NAIndices())
			}
		})
	}
}

func TestRightJoin(t *testing.T) {
	tests := []struct {
		scenario string

		inDataFrame   func(memory.Allocator) dataframe.DataFrame
		inSeriesName  string
		inDataFrame2  func(memory.Allocator) dataframe.DataFrame
		inSeriesName2 string

		expNumCols   int
		expNumRows   int
		exp          []interface{}
		expNAIndices [][]int
	}{
		{
			scenario: "right join dataframes",
			inDataFrame: func(pool memory.Allocator) dataframe.DataFrame {
				ss := []series.Series{
					series.FromInt64(
						pool,
						arrow.Field{Name: "f1-i64", Type: arrow.PrimitiveTypes.Int64},
						[]int64{1, 2, 3, 4, 5, 11},
						nil,
					),
					series.FromInt32(
						pool,
						arrow.Field{Name: "f2-i32", Type: arrow.PrimitiveTypes.Int32},
						[]int32{11, 22, 33, 44, 55, 1111},
						nil,
					),
				}
				for _, s := range ss {
					defer s.Release()
				}

				return dataframe.NewFromSeries(pool, ss)
			},
			inSeriesName: "f1-i64",
			inDataFrame2: func(pool memory.Allocator) dataframe.DataFrame {
				ss := []series.Series{
					series.FromInt64(
						pool,
						arrow.Field{Name: "f1-i64", Type: arrow.PrimitiveTypes.Int64},
						[]int64{1, 2, 2, 5, 8, 9, 10},
						nil,
					),
					series.FromInt32(
						pool,
						arrow.Field{Name: "f3-i32", Type: arrow.PrimitiveTypes.Int32},
						[]int32{111, 222, 202020, 555, 888, 999, 101010},
						nil,
					),
				}
				for _, s := range ss {
					defer s.Release()
				}

				return dataframe.NewFromSeries(pool, ss)
			},
			inSeriesName2: "f1-i64",
			expNumCols:    3,
			expNumRows:    7,
			exp: []interface{}{
				[]int64{1, 2, 2, 3, 4, 5, 11},
				[]int32{11, 22, 22, 33, 44, 55, 1111},
				[]int32{111, 222, 202020, 0, 0, 555, 0},
			},
			expNAIndices: [][]int{
				[]int{},
				[]int{},
				[]int{3, 4, 6},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.scenario, func(t *testing.T) {
			pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
			defer pool.AssertSize(t, 0)

			act := tt.inDataFrame(pool)
			defer act.Release()
			act2 := tt.inDataFrame2(pool)
			defer act2.Release()

			actJoin := dataframe.RightJoin(act2, tt.inSeriesName2, act, tt.inSeriesName)
			defer actJoin.Release()

			numR, numC := actJoin.Dims()
			require.Equal(t, tt.expNumCols, numC)
			require.Equal(t, tt.expNumRows, numR)

			for i := range tt.exp {
				assert.Equal(t, tt.exp[i], actJoin.Series(i).Values())
				assert.Equal(t, tt.expNAIndices[i], actJoin.Series(i).NAIndices())
			}
		})
	}
}

func TestMax(t *testing.T) {
	tests := []struct {
		scenario string

		inSeries func(memory.Allocator) []series.Series

		expMax float64
	}{
		{
			scenario: "max value from dataframe",
			inSeries: func(pool memory.Allocator) []series.Series {
				var result []series.Series
				f1 := arrow.Field{Name: "f1-f64", Type: arrow.PrimitiveTypes.Float64}
				vals1 := []float64{1, 1.1, 1.2, 0, 0.5}
				f2 := arrow.Field{Name: "f2-f64", Type: arrow.PrimitiveTypes.Float64}
				vals2 := []float64{0, 0.5, 1, 1.1, 1.2}
				f3 := arrow.Field{Name: "f3-f64", Type: arrow.PrimitiveTypes.Float64}
				vals3 := []float64{0, 1, 0.5, 1.1, 0.7}
				f4 := arrow.Field{Name: "f4-f64", Type: arrow.PrimitiveTypes.Float64}
				vals4 := []float64{1, 0, 1.1, 0.5, 1.2}
				f5 := arrow.Field{Name: "f5-f64", Type: arrow.PrimitiveTypes.Float64}
				vals5 := []float64{20, 21, 20.5, 21.1, 20.7}

				result = append(result, series.FromFloat64(pool, f1, vals1, nil))
				result = append(result, series.FromFloat64(pool, f2, vals2, nil))
				result = append(result, series.FromFloat64(pool, f3, vals3, nil))
				result = append(result, series.FromFloat64(pool, f4, vals4, nil))
				result = append(result, series.FromFloat64(pool, f5, vals5, nil))

				return result
			},
			expMax: 21.1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.scenario, func(t *testing.T) {
			pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
			defer pool.AssertSize(t, 0)

			inCols := tt.inSeries(pool)
			for i := range inCols {
				defer inCols[i].Release()
			}
			act := dataframe.NewFromSeries(pool, inCols)
			defer act.Release()

			actMax := act.Max()
			assert.Equal(t, tt.expMax, actMax)
		})
	}
}

func TestMin(t *testing.T) {
	tests := []struct {
		scenario string

		inSeries func(memory.Allocator) []series.Series

		expMin float64
	}{
		{
			scenario: "min value from dataframe",
			inSeries: func(pool memory.Allocator) []series.Series {
				var result []series.Series
				f1 := arrow.Field{Name: "f1-f64", Type: arrow.PrimitiveTypes.Float64}
				vals1 := []float64{1, 1.1, -11.2, 0, 0.5}
				f2 := arrow.Field{Name: "f2-f64", Type: arrow.PrimitiveTypes.Float64}
				vals2 := []float64{0, 0.5, -10, 1.1, 1.2}
				f3 := arrow.Field{Name: "f3-f64", Type: arrow.PrimitiveTypes.Float64}
				vals3 := []float64{0, 1, 0.5, -1.1, 0.7}
				f4 := arrow.Field{Name: "f4-f64", Type: arrow.PrimitiveTypes.Float64}
				vals4 := []float64{1, 0, 1.1, 0.5, 1.2}
				f5 := arrow.Field{Name: "f5-f64", Type: arrow.PrimitiveTypes.Float64}
				vals5 := []float64{20, 21, 20.5, 21.1, 20.7}

				result = append(result, series.FromFloat64(pool, f1, vals1, nil))
				result = append(result, series.FromFloat64(pool, f2, vals2, nil))
				result = append(result, series.FromFloat64(pool, f3, vals3, nil))
				result = append(result, series.FromFloat64(pool, f4, vals4, nil))
				result = append(result, series.FromFloat64(pool, f5, vals5, nil))

				return result
			},
			expMin: -11.2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.scenario, func(t *testing.T) {
			pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
			defer pool.AssertSize(t, 0)

			inCols := tt.inSeries(pool)
			for i := range inCols {
				defer inCols[i].Release()
			}
			act := dataframe.NewFromSeries(pool, inCols)
			defer act.Release()

			actMin := act.Min()
			assert.Equal(t, tt.expMin, actMin)
		})
	}
}

func TestMean(t *testing.T) {
	tests := []struct {
		scenario string

		inSeries func(memory.Allocator) []series.Series

		expMean float64
	}{
		{
			scenario: "mean value from dataframe",
			inSeries: func(pool memory.Allocator) []series.Series {
				var result []series.Series
				f1 := arrow.Field{Name: "f1-f64", Type: arrow.PrimitiveTypes.Float64}
				vals1 := []float64{1, 1.1, 1.2, 0, 0.5}
				f2 := arrow.Field{Name: "f2-f64", Type: arrow.PrimitiveTypes.Float64}
				vals2 := []float64{0, 0.5, 1, 1.1, 1.2}
				f3 := arrow.Field{Name: "f3-f64", Type: arrow.PrimitiveTypes.Float64}
				vals3 := []float64{0, 1, 0.5, 1.1, 0.7}
				f4 := arrow.Field{Name: "f4-f64", Type: arrow.PrimitiveTypes.Float64}
				vals4 := []float64{1, 0, 1.1, 0.5, 1.2}
				f5 := arrow.Field{Name: "f5-f64", Type: arrow.PrimitiveTypes.Float64}
				vals5 := []float64{20, 21, 20.5, 21.1, 20.7}

				result = append(result, series.FromFloat64(pool, f1, vals1, nil))
				result = append(result, series.FromFloat64(pool, f2, vals2, nil))
				result = append(result, series.FromFloat64(pool, f3, vals3, nil))
				result = append(result, series.FromFloat64(pool, f4, vals4, nil))
				result = append(result, series.FromFloat64(pool, f5, vals5, nil))

				return result
			},
			expMean: 4.720000000000001,
		},
	}

	for _, tt := range tests {
		t.Run(tt.scenario, func(t *testing.T) {
			pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
			defer pool.AssertSize(t, 0)

			inCols := tt.inSeries(pool)
			for i := range inCols {
				defer inCols[i].Release()
			}
			act := dataframe.NewFromSeries(pool, inCols)
			defer act.Release()

			actMean := act.Mean()
			assert.Equal(t, tt.expMean, actMean)
		})
	}
}

func TestSquare(t *testing.T) {
	tests := []struct {
		scenario string

		inSeries func(memory.Allocator) []series.Series

		expNumCols int
		expNumRows int
		exp        []interface{}
	}{
		{
			scenario: "square",
			inSeries: func(pool memory.Allocator) []series.Series {
				var result []series.Series
				f1 := arrow.Field{Name: "f1-f64", Type: arrow.PrimitiveTypes.Float64}
				vals1 := []float64{1, 2, 3, 4, 5}
				f2 := arrow.Field{Name: "f2-f64", Type: arrow.PrimitiveTypes.Float64}
				vals2 := []float64{11, 22, 33, 44, 55}
				f3 := arrow.Field{Name: "f3-f64", Type: arrow.PrimitiveTypes.Float64}
				vals3 := []float64{111, 222, 333, 444, 555}
				f4 := arrow.Field{Name: "f4-f64", Type: arrow.PrimitiveTypes.Float64}
				vals4 := []float64{1111, 2222, 3333, 4444, 5555}
				f5 := arrow.Field{Name: "f5-f64", Type: arrow.PrimitiveTypes.Float64}
				vals5 := []float64{11111, 22222, 33333, 44444, 55555}

				result = append(result, series.FromFloat64(pool, f1, vals1, nil))
				result = append(result, series.FromFloat64(pool, f2, vals2, nil))
				result = append(result, series.FromFloat64(pool, f3, vals3, nil))
				result = append(result, series.FromFloat64(pool, f4, vals4, nil))
				result = append(result, series.FromFloat64(pool, f5, vals5, nil))

				return result
			},
			expNumCols: 5,
			expNumRows: 5,
			exp: []interface{}{
				[]float64{1 * 1, 2 * 2, 3 * 3, 4 * 4, 5 * 5},
				[]float64{11 * 11, 22 * 22, 33 * 33, 44 * 44, 55 * 55},
				[]float64{111 * 111, 222 * 222, 333 * 333, 444 * 444, 555 * 555},
				[]float64{1111 * 1111, 2222 * 2222, 3333 * 3333, 4444 * 4444, 5555 * 5555},
				[]float64{11111 * 11111, 22222 * 22222, 33333 * 33333, 44444 * 44444, 55555 * 55555},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.scenario, func(t *testing.T) {
			pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
			defer pool.AssertSize(t, 0)

			inCols := tt.inSeries(pool)
			for i := range inCols {
				defer inCols[i].Release()
			}
			act := dataframe.NewFromSeries(pool, inCols)
			defer act.Release()
			actSquare := act.Square()
			defer actSquare.Release()

			numR, numC := actSquare.Dims()
			require.Equal(t, tt.expNumCols, numC)
			require.Equal(t, tt.expNumRows, numR)

			for i := range tt.exp {
				assert.Equal(t, tt.exp[i], actSquare.Series(i).Values())
			}
		})
	}
}

func TestSqrt(t *testing.T) {
	tests := []struct {
		scenario string

		inSeries func(memory.Allocator) []series.Series

		expNumCols int
		expNumRows int
		exp        []interface{}
	}{
		{
			scenario: "sqrt",
			inSeries: func(pool memory.Allocator) []series.Series {
				var result []series.Series
				f1 := arrow.Field{Name: "f1-f64", Type: arrow.PrimitiveTypes.Float64}
				vals1 := []float64{1, 2, 3, 4, 5}
				f2 := arrow.Field{Name: "f2-f64", Type: arrow.PrimitiveTypes.Float64}
				vals2 := []float64{11, 22, 33, 44, 55}
				f3 := arrow.Field{Name: "f3-f64", Type: arrow.PrimitiveTypes.Float64}
				vals3 := []float64{111, 222, 333, 444, 555}
				f4 := arrow.Field{Name: "f4-f64", Type: arrow.PrimitiveTypes.Float64}
				vals4 := []float64{1111, 2222, 3333, 4444, 5555}
				f5 := arrow.Field{Name: "f5-f64", Type: arrow.PrimitiveTypes.Float64}
				vals5 := []float64{11111, 22222, 33333, 44444, 55555}

				result = append(result, series.FromFloat64(pool, f1, vals1, nil))
				result = append(result, series.FromFloat64(pool, f2, vals2, nil))
				result = append(result, series.FromFloat64(pool, f3, vals3, nil))
				result = append(result, series.FromFloat64(pool, f4, vals4, nil))
				result = append(result, series.FromFloat64(pool, f5, vals5, nil))

				return result
			},
			expNumCols: 5,
			expNumRows: 5,
			exp: []interface{}{
				[]float64{math.Sqrt(1), math.Sqrt(2), math.Sqrt(3), math.Sqrt(4), math.Sqrt(5)},
				[]float64{math.Sqrt(11), math.Sqrt(22), math.Sqrt(33), math.Sqrt(44), math.Sqrt(55)},
				[]float64{math.Sqrt(111), math.Sqrt(222), math.Sqrt(333), math.Sqrt(444), math.Sqrt(555)},
				[]float64{math.Sqrt(1111), math.Sqrt(2222), math.Sqrt(3333), math.Sqrt(4444), math.Sqrt(5555)},
				[]float64{math.Sqrt(11111), math.Sqrt(22222), math.Sqrt(33333), math.Sqrt(44444), math.Sqrt(55555)},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.scenario, func(t *testing.T) {
			pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
			defer pool.AssertSize(t, 0)

			inCols := tt.inSeries(pool)
			for i := range inCols {
				defer inCols[i].Release()
			}
			act := dataframe.NewFromSeries(pool, inCols)
			defer act.Release()
			actSqrt := act.Sqrt()
			defer actSqrt.Release()

			numR, numC := actSqrt.Dims()
			require.Equal(t, tt.expNumCols, numC)
			require.Equal(t, tt.expNumRows, numR)

			for i := range tt.exp {
				assert.Equal(t, tt.exp[i], actSqrt.Series(i).Values())
			}
		})
	}
}

func TestSum(t *testing.T) {
	tests := []struct {
		scenario string

		inSeries func(memory.Allocator) []series.Series

		expSum float64
	}{
		{
			scenario: "sum value from dataframe",
			inSeries: func(pool memory.Allocator) []series.Series {
				var result []series.Series
				f1 := arrow.Field{Name: "f1-f64", Type: arrow.PrimitiveTypes.Float64}
				vals1 := []float64{1, 1.1, 1.2, 0, 0.5}
				f2 := arrow.Field{Name: "f2-f64", Type: arrow.PrimitiveTypes.Float64}
				vals2 := []float64{0, 0.5, 1, 1.1, 1.2}
				f3 := arrow.Field{Name: "f3-f64", Type: arrow.PrimitiveTypes.Float64}
				vals3 := []float64{0, 1, 0.5, 1.1, 0.7}
				f4 := arrow.Field{Name: "f4-f64", Type: arrow.PrimitiveTypes.Float64}
				vals4 := []float64{1, 0, 1.1, 0.5, 1.2}
				f5 := arrow.Field{Name: "f5-f64", Type: arrow.PrimitiveTypes.Float64}
				vals5 := []float64{20, 21, 20.5, 21.1, 20.7}

				result = append(result, series.FromFloat64(pool, f1, vals1, nil))
				result = append(result, series.FromFloat64(pool, f2, vals2, nil))
				result = append(result, series.FromFloat64(pool, f3, vals3, nil))
				result = append(result, series.FromFloat64(pool, f4, vals4, nil))
				result = append(result, series.FromFloat64(pool, f5, vals5, nil))

				return result
			},
			expSum: 118,
		},
	}

	for _, tt := range tests {
		t.Run(tt.scenario, func(t *testing.T) {
			pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
			defer pool.AssertSize(t, 0)

			inCols := tt.inSeries(pool)
			for i := range inCols {
				defer inCols[i].Release()
			}
			act := dataframe.NewFromSeries(pool, inCols)
			defer act.Release()

			actSum := act.Sum()
			assert.Equal(t, tt.expSum, actSum)
		})
	}
}

func TestDot(t *testing.T) {
	tests := []struct {
		scenario string

		inSeries func(memory.Allocator) []series.Series
		inRowi   int
		inRowj   int

		expDot float64
	}{
		{
			scenario: "all floats (1.1*1.2)+(0.5*1)+(1*0.5)+(0*1.1)=2.32",
			inSeries: func(pool memory.Allocator) []series.Series {
				var result []series.Series
				f1 := arrow.Field{Name: "f1-f64", Type: arrow.PrimitiveTypes.Float64}
				vals1 := []float64{1, 1.1, 1.2, 0, 0.5}
				f2 := arrow.Field{Name: "f2-f64", Type: arrow.PrimitiveTypes.Float64}
				vals2 := []float64{0, 0.5, 1, 1.1, 1.2}
				f3 := arrow.Field{Name: "f3-f64", Type: arrow.PrimitiveTypes.Float64}
				vals3 := []float64{0, 1, 0.5, 1.1, 0.7}
				f4 := arrow.Field{Name: "f4-f64", Type: arrow.PrimitiveTypes.Float64}
				vals4 := []float64{1, 0, 1.1, 0.5, 1.2}

				result = append(result, series.FromFloat64(pool, f1, vals1, nil))
				result = append(result, series.FromFloat64(pool, f2, vals2, nil))
				result = append(result, series.FromFloat64(pool, f3, vals3, nil))
				result = append(result, series.FromFloat64(pool, f4, vals4, nil))

				return result
			},
			inRowi: 1,
			inRowj: 2,
			expDot: 2.3200000000000003,
		},
		{
			scenario: "mixed types (2*3)+(0*1)+(1*0.5)+(0*1.1)=6.5",
			inSeries: func(pool memory.Allocator) []series.Series {
				var result []series.Series
				f1 := arrow.Field{Name: "f1-i32", Type: arrow.PrimitiveTypes.Int32}
				vals1 := []int32{1, 2, 3, 0, 0}
				f2 := arrow.Field{Name: "f2-i64", Type: arrow.PrimitiveTypes.Int64}
				vals2 := []int64{0, 0, 1, 10, 100}
				f3 := arrow.Field{Name: "f3-f32", Type: arrow.PrimitiveTypes.Float32}
				vals3 := []float32{0, 1, 0.5, 1.1, 0.7}
				f4 := arrow.Field{Name: "f4-f64", Type: arrow.PrimitiveTypes.Float64}
				vals4 := []float64{1, 0, 1.1, 0.5, 1.2}

				result = append(result, series.FromInt32(pool, f1, vals1, nil))
				result = append(result, series.FromInt64(pool, f2, vals2, nil))
				result = append(result, series.FromFloat32(pool, f3, vals3, nil))
				result = append(result, series.FromFloat64(pool, f4, vals4, nil))

				return result
			},
			inRowi: 1,
			inRowj: 2,
			expDot: 6.5,
		},
	}

	for _, tt := range tests {
		t.Run(tt.scenario, func(t *testing.T) {
			pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
			defer pool.AssertSize(t, 0)

			inCols := tt.inSeries(pool)
			for i := range inCols {
				defer inCols[i].Release()
			}
			act := dataframe.NewFromSeries(pool, inCols)
			defer act.Release()
			actDot := act.Dot(tt.inRowi, tt.inRowj)
			assert.Equal(t, tt.expDot, actDot)
		})
	}
}

func TestPivot(t *testing.T) {
	tests := []struct {
		scenario string

		inDataFrame func(memory.Allocator) dataframe.DataFrame
		inIdxName   string
		inColsName  string
		inValsName  string

		expNumCols   int
		expNumRows   int
		exp          []interface{}
		expNAIndices [][]int
		expHeaders   []string
	}{
		{
			scenario: "series pivot dataframe",
			inDataFrame: func(pool memory.Allocator) dataframe.DataFrame {
				ss := []series.Series{
					series.FromInt64(
						pool,
						arrow.Field{Name: "idx", Type: arrow.PrimitiveTypes.Int64},
						[]int64{1, 2, 3, 4, 5, 6},
						nil,
					),
					series.FromInt32(
						pool,
						arrow.Field{Name: "cols", Type: arrow.PrimitiveTypes.Int32},
						[]int32{100, 200, 300, 400, 500, 600},
						nil,
					),
					series.FromInt32(
						pool,
						arrow.Field{Name: "vals", Type: arrow.PrimitiveTypes.Int32},
						[]int32{11, 22, 33, 44, 55, 1111},
						nil,
					),
				}
				for _, s := range ss {
					defer s.Release()
				}

				return dataframe.NewFromSeries(pool, ss)
			},
			inIdxName:  "idx",
			inColsName: "cols",
			inValsName: "vals",
			expNumCols: 7,
			expNumRows: 6,
			exp: []interface{}{
				[]int64{1, 2, 3, 4, 5, 6},
				[]int32{11, 0, 0, 0, 0, 0},
				[]int32{0, 22, 0, 0, 0, 0},
				[]int32{0, 0, 33, 0, 0, 0},
				[]int32{0, 0, 0, 44, 0, 0},
				[]int32{0, 0, 0, 0, 55, 0},
				[]int32{0, 0, 0, 0, 0, 1111},
			},
			expNAIndices: [][]int{
				[]int{},
				[]int{},
				[]int{},
				[]int{},
				[]int{},
				[]int{},
				[]int{},
			},
			expHeaders: []string{"idx", "100", "200", "300", "400", "500", "600"},
		},
		{
			scenario: "csv pivot dataframe",
			inDataFrame: func(pool memory.Allocator) dataframe.DataFrame {
				f := strings.NewReader(`"idx","cols","vals"
1,100,11
2,200,22
3,300,33
4,400,44
5,500,55
6,600,1111`)

				r := csv.NewReader(f)
				return dataframe.NewFromCSV(pool, r, -1, nil)
			},
			inIdxName:  "idx",
			inColsName: "cols",
			inValsName: "vals",
			expNumCols: 7,
			expNumRows: 6,
			exp: []interface{}{
				[]string{"1", "2", "3", "4", "5", "6"},
				[]string{"11", "", "", "", "", ""},
				[]string{"", "22", "", "", "", ""},
				[]string{"", "", "33", "", "", ""},
				[]string{"", "", "", "44", "", ""},
				[]string{"", "", "", "", "55", ""},
				[]string{"", "", "", "", "", "1111"},
			},
			expNAIndices: [][]int{
				[]int{},
				[]int{},
				[]int{},
				[]int{},
				[]int{},
				[]int{},
				[]int{},
			},
			expHeaders: []string{"idx", "100", "200", "300", "400", "500", "600"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.scenario, func(t *testing.T) {
			pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
			defer pool.AssertSize(t, 0)

			act := tt.inDataFrame(pool)
			defer act.Release()

			actPivot := act.Pivot(tt.inIdxName, tt.inColsName, tt.inValsName)
			defer actPivot.Release()

			numR, numC := actPivot.Dims()
			require.Equal(t, tt.expNumCols, numC)
			require.Equal(t, tt.expNumRows, numR)

			for i := range tt.exp {
				assert.Equal(t, tt.exp[i], actPivot.Series(i).Values())
				assert.Equal(t, tt.expNAIndices[i], actPivot.Series(i).NAIndices())
			}
			actHeaders := actPivot.Headers()
			assert.Equal(t, tt.expHeaders, actHeaders)
		})
	}
}
