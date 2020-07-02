package series_test

import (
	"testing"

	"github.com/apache/arrow/go/arrow"
	"github.com/apache/arrow/go/arrow/array"
	"github.com/apache/arrow/go/arrow/memory"
	"github.com/poopoothegorilla/fastframe/series"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gonum.org/v1/gonum/mat"
)

var (
	_ array.Interface = &series.Series{}
	_ mat.Matrix      = &series.Series{}
	_ mat.Vector      = &series.Series{}
)

func TestUnique(t *testing.T) {
	tests := []struct {
		scenario string

		inField  arrow.Field
		inColumn func(memory.Allocator) array.Interface

		expUnique interface{}
	}{
		{
			scenario: "int32 column",
			inField:  arrow.Field{Name: "f1-i32", Type: arrow.PrimitiveTypes.Int32},
			inColumn: func(pool memory.Allocator) array.Interface {
				b := array.NewInt32Builder(pool)
				defer b.Release()

				b.AppendValues([]int32{1, 1, 2, 2, 2, 2, 3, 4, 5, 6}, nil)

				return b.NewArray()
			},
			expUnique: []int32{1, 2, 3, 4, 5, 6},
		},
		{
			scenario: "int64 column",
			inField:  arrow.Field{Name: "f1-i64", Type: arrow.PrimitiveTypes.Int64},
			inColumn: func(pool memory.Allocator) array.Interface {
				b := array.NewInt64Builder(pool)
				defer b.Release()

				b.AppendValues([]int64{1, 1, 2, 2, 2, 2, 3, 4, 5, 6}, nil)

				return b.NewArray()
			},
			expUnique: []int64{1, 2, 3, 4, 5, 6},
		},
		{
			scenario: "float32 column",
			inField:  arrow.Field{Name: "f1-f32", Type: arrow.PrimitiveTypes.Float32},
			inColumn: func(pool memory.Allocator) array.Interface {
				b := array.NewFloat32Builder(pool)
				defer b.Release()

				b.AppendValues([]float32{1, 1, 2, 2, 2, 2, 3, 4, 5, 6}, nil)

				return b.NewArray()
			},
			expUnique: []float32{1, 2, 3, 4, 5, 6},
		},
		{
			scenario: "float64 column",
			inField:  arrow.Field{Name: "f1-f64", Type: arrow.PrimitiveTypes.Float64},
			inColumn: func(pool memory.Allocator) array.Interface {
				b := array.NewFloat64Builder(pool)
				defer b.Release()

				b.AppendValues([]float64{1, 1, 2, 2, 2, 2, 3, 4, 5, 6}, nil)

				return b.NewArray()
			},
			expUnique: []float64{1, 2, 3, 4, 5, 6},
		},
	}

	for _, tt := range tests {
		t.Run(tt.scenario, func(t *testing.T) {
			pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
			defer pool.AssertSize(t, 0)

			actSeries := series.FromArrow(pool, tt.inField, tt.inColumn(pool))
			require.NotNil(t, actSeries)
			defer actSeries.Release()

			actUnique := actSeries.Unique()
			defer actUnique.Release()
			assert.Equal(t, tt.expUnique, actUnique.Values())
		})
	}
}

func TestFindIndices(t *testing.T) {
	tests := []struct {
		scenario string

		inSeries func(pool memory.Allocator) series.Series
		inVal    interface{}

		exp []int
	}{
		{
			scenario: "int32 column",
			inSeries: func(pool memory.Allocator) series.Series {
				field := arrow.Field{Name: "f1-i32", Type: arrow.PrimitiveTypes.Int32}
				vals := []int32{1, 3, 2, 3, 5, 3, 7}
				nulls := []bool{false, true, true, false, true, true, true}
				return series.FromInt32(pool, field, vals, nulls)
			},
			inVal: int32(3),
			exp:   []int{1, 5},
		},
		{
			scenario: "int64 column",
			inSeries: func(pool memory.Allocator) series.Series {
				field := arrow.Field{Name: "f1-i64", Type: arrow.PrimitiveTypes.Int64}
				vals := []int64{1, 3, 2, 3, 5, 3, 7}
				nulls := []bool{false, true, true, false, true, true, true}
				return series.FromInt64(pool, field, vals, nulls)
			},
			inVal: int64(3),
			exp:   []int{1, 5},
		},
		{
			scenario: "float32 column",
			inSeries: func(pool memory.Allocator) series.Series {
				field := arrow.Field{Name: "f1-f32", Type: arrow.PrimitiveTypes.Float32}
				vals := []float32{1, 3, 2, 3, 5, 3, 7}
				nulls := []bool{false, true, true, false, true, true, true}
				return series.FromFloat32(pool, field, vals, nulls)
			},
			inVal: float32(3),
			exp:   []int{1, 5},
		},
		{
			scenario: "float64 column",
			inSeries: func(pool memory.Allocator) series.Series {
				field := arrow.Field{Name: "f1-f64", Type: arrow.PrimitiveTypes.Float64}
				vals := []float64{1, 3, 2, 3, 5, 3, 7}
				nulls := []bool{false, true, true, false, true, true, true}
				return series.FromFloat64(pool, field, vals, nulls)
			},
			inVal: float64(3),
			exp:   []int{1, 5},
		},
	}

	for _, tt := range tests {
		t.Run(tt.scenario, func(t *testing.T) {
			pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
			defer pool.AssertSize(t, 0)

			actSeries := tt.inSeries(pool)
			defer actSeries.Release()

			act := actSeries.FindIndices(tt.inVal)
			assert.Equal(t, tt.exp, act)
		})
	}
}

func TestNAIndices(t *testing.T) {
	tests := []struct {
		scenario string

		inField  arrow.Field
		inColumn func(memory.Allocator) array.Interface

		exp []int
	}{
		{
			scenario: "int32 column",
			inField:  arrow.Field{Name: "f1-i32", Type: arrow.PrimitiveTypes.Int32},
			inColumn: func(pool memory.Allocator) array.Interface {
				b := array.NewInt32Builder(pool)
				defer b.Release()

				b.AppendValues([]int32{1, 2, 3, 4, 5, 6, 7},
					[]bool{true, false, true, false, true, false, true})

				return b.NewArray()
			},
			exp: []int{1, 3, 5},
		},
		{
			scenario: "int64 column",
			inField:  arrow.Field{Name: "f1-i64", Type: arrow.PrimitiveTypes.Int64},
			inColumn: func(pool memory.Allocator) array.Interface {
				b := array.NewInt64Builder(pool)
				defer b.Release()

				b.AppendValues([]int64{1, 2, 3, 4, 5, 6, 7},
					[]bool{true, false, true, false, true, false, true})

				return b.NewArray()
			},
			exp: []int{1, 3, 5},
		},
		{
			scenario: "float32 column",
			inField:  arrow.Field{Name: "f1-f32", Type: arrow.PrimitiveTypes.Float32},
			inColumn: func(pool memory.Allocator) array.Interface {
				b := array.NewFloat32Builder(pool)
				defer b.Release()

				b.AppendValues([]float32{1, 2, 3, 4, 5, 6, 7},
					[]bool{true, false, true, false, true, false, true})

				return b.NewArray()
			},
			exp: []int{1, 3, 5},
		},
		{
			scenario: "float64 column",
			inField:  arrow.Field{Name: "f1-f64", Type: arrow.PrimitiveTypes.Float64},
			inColumn: func(pool memory.Allocator) array.Interface {
				b := array.NewFloat64Builder(pool)
				defer b.Release()

				b.AppendValues([]float64{1, 2, 3, 4, 5, 6, 7},
					[]bool{true, false, true, false, true, false, true})

				return b.NewArray()
			},
			exp: []int{1, 3, 5},
		},
	}

	for _, tt := range tests {
		t.Run(tt.scenario, func(t *testing.T) {
			pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
			defer pool.AssertSize(t, 0)

			actSeries := series.FromArrow(pool, tt.inField, tt.inColumn(pool))
			require.NotNil(t, actSeries)
			defer actSeries.Release()

			act := actSeries.NAIndices()
			assert.Equal(t, tt.exp, act)
		})
	}
}

func TestIsNA(t *testing.T) {
	tests := []struct {
		scenario string

		inField  arrow.Field
		inColumn func(memory.Allocator) array.Interface

		exp []bool
	}{
		{
			scenario: "int32 column",
			inField:  arrow.Field{Name: "f1-i32", Type: arrow.PrimitiveTypes.Int32},
			inColumn: func(pool memory.Allocator) array.Interface {
				b := array.NewInt32Builder(pool)
				defer b.Release()

				b.AppendValues([]int32{1, 2, 3, 4, 5, 6, 7},
					[]bool{true, false, true, false, true, false, true})

				return b.NewArray()
			},
			exp: []bool{false, true, false, true, false, true, false},
		},
		{
			scenario: "int64 column",
			inField:  arrow.Field{Name: "f1-i64", Type: arrow.PrimitiveTypes.Int64},
			inColumn: func(pool memory.Allocator) array.Interface {
				b := array.NewInt64Builder(pool)
				defer b.Release()

				b.AppendValues([]int64{1, 2, 3, 4, 5, 6, 7},
					[]bool{true, false, true, false, true, false, true})

				return b.NewArray()
			},
			exp: []bool{false, true, false, true, false, true, false},
		},
		{
			scenario: "float32 column",
			inField:  arrow.Field{Name: "f1-f32", Type: arrow.PrimitiveTypes.Float32},
			inColumn: func(pool memory.Allocator) array.Interface {
				b := array.NewFloat32Builder(pool)
				defer b.Release()

				b.AppendValues([]float32{1, 2, 3, 4, 5, 6, 7},
					[]bool{true, false, true, false, true, false, true})

				return b.NewArray()
			},
			exp: []bool{false, true, false, true, false, true, false},
		},
		{
			scenario: "float64 column",
			inField:  arrow.Field{Name: "f1-f64", Type: arrow.PrimitiveTypes.Float64},
			inColumn: func(pool memory.Allocator) array.Interface {
				b := array.NewFloat64Builder(pool)
				defer b.Release()

				b.AppendValues([]float64{1, 2, 3, 4, 5, 6, 7},
					[]bool{true, false, true, false, true, false, true})

				return b.NewArray()
			},
			exp: []bool{false, true, false, true, false, true, false},
		},
	}

	for _, tt := range tests {
		t.Run(tt.scenario, func(t *testing.T) {
			pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
			defer pool.AssertSize(t, 0)

			actSeries := series.FromArrow(pool, tt.inField, tt.inColumn(pool))
			require.NotNil(t, actSeries)
			defer actSeries.Release()

			act := actSeries.IsNA()
			assert.Equal(t, tt.exp, act)
		})
	}
}

func TestDropIndices(t *testing.T) {
	tests := []struct {
		scenario string

		inField   arrow.Field
		inColumn  func(memory.Allocator) array.Interface
		inIndices []int

		expDropIndices interface{}
	}{
		{
			scenario: "int32 column",
			inField:  arrow.Field{Name: "f1-i32", Type: arrow.PrimitiveTypes.Int32},
			inColumn: func(pool memory.Allocator) array.Interface {
				b := array.NewInt32Builder(pool)
				defer b.Release()

				b.AppendValues([]int32{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, nil)

				return b.NewArray()
			},
			inIndices:      []int{1, 2, 3, 5, 7, 8},
			expDropIndices: []int32{1, 5, 7, 10},
		},
		{
			scenario: "int64 column",
			inField:  arrow.Field{Name: "f1-i64", Type: arrow.PrimitiveTypes.Int64},
			inColumn: func(pool memory.Allocator) array.Interface {
				b := array.NewInt64Builder(pool)
				defer b.Release()

				b.AppendValues([]int64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, nil)

				return b.NewArray()
			},
			inIndices:      []int{5, 7},
			expDropIndices: []int64{1, 2, 3, 4, 5, 7, 9, 10},
		},
		{
			scenario: "float32 column",
			inField:  arrow.Field{Name: "f1-f32", Type: arrow.PrimitiveTypes.Float32},
			inColumn: func(pool memory.Allocator) array.Interface {
				b := array.NewFloat32Builder(pool)
				defer b.Release()

				b.AppendValues([]float32{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, nil)

				return b.NewArray()
			},
			inIndices:      []int{1, 2, 3, 5, 7, 8},
			expDropIndices: []float32{1, 5, 7, 10},
		},
		{
			scenario: "float64 column",
			inField:  arrow.Field{Name: "f1-f64", Type: arrow.PrimitiveTypes.Float64},
			inColumn: func(pool memory.Allocator) array.Interface {
				b := array.NewFloat64Builder(pool)
				defer b.Release()

				b.AppendValues([]float64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, nil)

				return b.NewArray()
			},
			inIndices:      []int{1, 2, 3, 5, 7, 8},
			expDropIndices: []float64{1, 5, 7, 10},
		},
	}

	for _, tt := range tests {
		t.Run(tt.scenario, func(t *testing.T) {
			pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
			defer pool.AssertSize(t, 0)

			actSeries := series.FromArrow(pool, tt.inField, tt.inColumn(pool))
			require.NotNil(t, actSeries)
			defer actSeries.Release()

			actDropIndices := actSeries.DropIndices(tt.inIndices)
			defer actDropIndices.Release()
			assert.Equal(t, tt.expDropIndices, actDropIndices.Values())
		})
	}
}

func TestSelectIndices(t *testing.T) {
	tests := []struct {
		scenario string

		inField   arrow.Field
		inColumn  func(memory.Allocator) array.Interface
		inIndices []int

		exp interface{}
	}{
		{
			scenario: "int32 column",
			inField:  arrow.Field{Name: "f1-i32", Type: arrow.PrimitiveTypes.Int32},
			inColumn: func(pool memory.Allocator) array.Interface {
				b := array.NewInt32Builder(pool)
				defer b.Release()

				b.AppendValues([]int32{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, nil)

				return b.NewArray()
			},
			inIndices: []int{1, 2, 3, 5, 7, 8},
			exp:       []int32{2, 3, 4, 6, 8, 9},
		},
		{
			scenario: "int64 column",
			inField:  arrow.Field{Name: "f1-i64", Type: arrow.PrimitiveTypes.Int64},
			inColumn: func(pool memory.Allocator) array.Interface {
				b := array.NewInt64Builder(pool)
				defer b.Release()

				b.AppendValues([]int64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, nil)

				return b.NewArray()
			},
			inIndices: []int{5, 7},
			exp:       []int64{6, 8},
		},
		{
			scenario: "float32 column",
			inField:  arrow.Field{Name: "f1-f32", Type: arrow.PrimitiveTypes.Float32},
			inColumn: func(pool memory.Allocator) array.Interface {
				b := array.NewFloat32Builder(pool)
				defer b.Release()

				b.AppendValues([]float32{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, nil)

				return b.NewArray()
			},
			inIndices: []int{1, 2, 3, 5, 7, 8},
			exp:       []float32{2, 3, 4, 6, 8, 9},
		},
		{
			scenario: "float64 column",
			inField:  arrow.Field{Name: "f1-f64", Type: arrow.PrimitiveTypes.Float64},
			inColumn: func(pool memory.Allocator) array.Interface {
				b := array.NewFloat64Builder(pool)
				defer b.Release()

				b.AppendValues([]float64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, nil)

				return b.NewArray()
			},
			inIndices: []int{1, 2, 3, 5, 7, 8},
			exp:       []float64{2, 3, 4, 6, 8, 9},
		},
	}

	for _, tt := range tests {
		t.Run(tt.scenario, func(t *testing.T) {
			pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
			defer pool.AssertSize(t, 0)

			actSeries := series.FromArrow(pool, tt.inField, tt.inColumn(pool))
			require.NotNil(t, actSeries)
			defer actSeries.Release()

			act := actSeries.SelectIndices(tt.inIndices)
			defer act.Release()
			assert.Equal(t, tt.exp, act.Values())
		})
	}
}

func TestHead(t *testing.T) {
	tests := []struct {
		scenario string

		inField  arrow.Field
		inColumn func(memory.Allocator) array.Interface
		inN      int

		expHead interface{}
	}{
		{
			scenario: "int32 column",
			inField:  arrow.Field{Name: "f1-i32", Type: arrow.PrimitiveTypes.Int32},
			inColumn: func(pool memory.Allocator) array.Interface {
				b := array.NewInt32Builder(pool)
				defer b.Release()

				b.AppendValues([]int32{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, nil)

				return b.NewArray()
			},
			inN:     2,
			expHead: []int32{1, 2},
		},
		{
			scenario: "int64 column",
			inField:  arrow.Field{Name: "f1-i64", Type: arrow.PrimitiveTypes.Int64},
			inColumn: func(pool memory.Allocator) array.Interface {
				b := array.NewInt64Builder(pool)
				defer b.Release()

				b.AppendValues([]int64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, nil)

				return b.NewArray()
			},
			inN:     2,
			expHead: []int64{1, 2},
		},
		{
			scenario: "float32 column",
			inField:  arrow.Field{Name: "f1-f32", Type: arrow.PrimitiveTypes.Float32},
			inColumn: func(pool memory.Allocator) array.Interface {
				b := array.NewFloat32Builder(pool)
				defer b.Release()

				b.AppendValues([]float32{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, nil)

				return b.NewArray()
			},
			inN:     2,
			expHead: []float32{1, 2},
		},
		{
			scenario: "float64 column",
			inField:  arrow.Field{Name: "f1-f64", Type: arrow.PrimitiveTypes.Float64},
			inColumn: func(pool memory.Allocator) array.Interface {
				b := array.NewFloat64Builder(pool)
				defer b.Release()

				b.AppendValues([]float64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, nil)

				return b.NewArray()
			},
			inN:     2,
			expHead: []float64{1, 2},
		},
	}

	for _, tt := range tests {
		t.Run(tt.scenario, func(t *testing.T) {
			pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
			defer pool.AssertSize(t, 0)

			actSeries := series.FromArrow(pool, tt.inField, tt.inColumn(pool))
			require.NotNil(t, actSeries)
			defer actSeries.Release()

			actHead := actSeries.Head(tt.inN)
			defer actHead.Release()
			assert.Equal(t, tt.expHead, actHead.Values())
		})
	}
}

func TestTruncate(t *testing.T) {
	tests := []struct {
		scenario string

		inField  arrow.Field
		inColumn func(memory.Allocator) array.Interface
		inI      int64
		inJ      int64

		expTruncate interface{}
	}{
		{
			scenario: "int32 column",
			inField:  arrow.Field{Name: "f1-i32", Type: arrow.PrimitiveTypes.Int32},
			inColumn: func(pool memory.Allocator) array.Interface {
				b := array.NewInt32Builder(pool)
				defer b.Release()

				b.AppendValues([]int32{1, 1, 2, 2, 2, 2, 3, 4, 5, 6}, nil)

				return b.NewArray()
			},
			inI:         2,
			inJ:         7,
			expTruncate: []int32{2, 2, 2, 2, 3},
		},
		{
			scenario: "int64 column",
			inField:  arrow.Field{Name: "f1-i64", Type: arrow.PrimitiveTypes.Int64},
			inColumn: func(pool memory.Allocator) array.Interface {
				b := array.NewInt64Builder(pool)
				defer b.Release()

				b.AppendValues([]int64{1, 1, 2, 2, 2, 2, 3, 4, 5, 6}, nil)

				return b.NewArray()
			},
			inI:         2,
			inJ:         7,
			expTruncate: []int64{2, 2, 2, 2, 3},
		},
		{
			scenario: "float32 column",
			inField:  arrow.Field{Name: "f1-f32", Type: arrow.PrimitiveTypes.Float32},
			inColumn: func(pool memory.Allocator) array.Interface {
				b := array.NewFloat32Builder(pool)
				defer b.Release()

				b.AppendValues([]float32{1, 1, 2, 2, 2, 2, 3, 4, 5, 6}, nil)

				return b.NewArray()
			},
			inI:         2,
			inJ:         7,
			expTruncate: []float32{2, 2, 2, 2, 3},
		},
		{
			scenario: "float64 column",
			inField:  arrow.Field{Name: "f1-f64", Type: arrow.PrimitiveTypes.Float64},
			inColumn: func(pool memory.Allocator) array.Interface {
				b := array.NewFloat64Builder(pool)
				defer b.Release()

				b.AppendValues([]float64{1, 1, 2, 2, 2, 2, 3, 4, 5, 6}, nil)

				return b.NewArray()
			},
			inI:         2,
			inJ:         7,
			expTruncate: []float64{2, 2, 2, 2, 3},
		},
	}

	for _, tt := range tests {
		t.Run(tt.scenario, func(t *testing.T) {
			pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
			defer pool.AssertSize(t, 0)

			actSeries := series.FromArrow(pool, tt.inField, tt.inColumn(pool))
			require.NotNil(t, actSeries)
			defer actSeries.Release()

			actTruncate := actSeries.Truncate(tt.inI, tt.inJ)
			defer actTruncate.Release()
			assert.Equal(t, tt.expTruncate, actTruncate.Values())
		})
	}
}

func TestSum(t *testing.T) {
	tests := []struct {
		scenario string

		inField  arrow.Field
		inColumn func(memory.Allocator) array.Interface

		expSum       float64
		expMagnitude float64
	}{
		{
			scenario: "int32 column",
			inField:  arrow.Field{Name: "f1-i32", Type: arrow.PrimitiveTypes.Int32},
			inColumn: func(pool memory.Allocator) array.Interface {
				b := array.NewInt32Builder(pool)
				defer b.Release()

				b.AppendValues([]int32{1, 1, 2, 2, 2, 2, 3, 4, 5, 6}, nil)

				return b.NewArray()
			},
			expSum:       28,
			expMagnitude: 10.198039027185569,
		},
		{
			scenario: "int64 column",
			inField:  arrow.Field{Name: "f1-i64", Type: arrow.PrimitiveTypes.Int64},
			inColumn: func(pool memory.Allocator) array.Interface {
				b := array.NewInt64Builder(pool)
				defer b.Release()

				b.AppendValues([]int64{1, 1, 2, 2, 2, 2, 3, 4, 5, 6}, nil)

				return b.NewArray()
			},
			expSum:       28,
			expMagnitude: 10.198039027185569,
		},
		{
			scenario: "float32 column",
			inField:  arrow.Field{Name: "f1-f32", Type: arrow.PrimitiveTypes.Float32},
			inColumn: func(pool memory.Allocator) array.Interface {
				b := array.NewFloat32Builder(pool)
				defer b.Release()

				b.AppendValues([]float32{1, 1, 2, 2, 2, 2, 3, 4, 5, 6}, nil)

				return b.NewArray()
			},
			expSum:       28,
			expMagnitude: 10.198039027185569,
		},
		{
			scenario: "float64 column",
			inField:  arrow.Field{Name: "f1-f64", Type: arrow.PrimitiveTypes.Float64},
			inColumn: func(pool memory.Allocator) array.Interface {
				b := array.NewFloat64Builder(pool)
				defer b.Release()

				b.AppendValues([]float64{1, 1, 2, 2, 2, 2, 3, 4, 5, 6}, nil)

				return b.NewArray()
			},
			expSum:       28,
			expMagnitude: 10.198039027185569,
		},
	}

	for _, tt := range tests {
		t.Run(tt.scenario, func(t *testing.T) {
			pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
			defer pool.AssertSize(t, 0)

			actSeries := series.FromArrow(pool, tt.inField, tt.inColumn(pool))
			require.NotNil(t, actSeries)
			defer actSeries.Release()

			actSum := actSeries.Sum()
			assert.Equal(t, tt.expSum, actSum)

			actMagnitude := actSeries.Magnitude()
			assert.Equal(t, tt.expMagnitude, actMagnitude)
		})
	}
}

func TestSTD(t *testing.T) {
	tests := []struct {
		scenario string

		inField  arrow.Field
		inColumn func(memory.Allocator) array.Interface

		expSTD float64
	}{
		{
			scenario: "int32 column",
			inField:  arrow.Field{Name: "f1-i32", Type: arrow.PrimitiveTypes.Int32},
			inColumn: func(pool memory.Allocator) array.Interface {
				b := array.NewInt32Builder(pool)
				defer b.Release()

				b.AppendValues([]int32{1, 1, 2, 2, 2, 2, 3, 4, 5, 6}, nil)

				return b.NewArray()
			},
			expSTD: 1.6865480854231356,
		},
		{
			scenario: "int64 column",
			inField:  arrow.Field{Name: "f1-i64", Type: arrow.PrimitiveTypes.Int64},
			inColumn: func(pool memory.Allocator) array.Interface {
				b := array.NewInt64Builder(pool)
				defer b.Release()

				b.AppendValues([]int64{10, 20, 30, 40, 50, 60}, nil)

				return b.NewArray()
			},
			expSTD: 18.708286933869708,
		},
		{
			scenario: "float32 column",
			inField:  arrow.Field{Name: "f1-f32", Type: arrow.PrimitiveTypes.Float32},
			inColumn: func(pool memory.Allocator) array.Interface {
				b := array.NewFloat32Builder(pool)
				defer b.Release()

				b.AppendValues([]float32{1, 1, 2, 2, 2, 2, 3, 4, 5, 6}, nil)

				return b.NewArray()
			},
			expSTD: 1.6865480854231356,
		},
		{
			scenario: "float64 column",
			inField:  arrow.Field{Name: "f1-f64", Type: arrow.PrimitiveTypes.Float64},
			inColumn: func(pool memory.Allocator) array.Interface {
				b := array.NewFloat64Builder(pool)
				defer b.Release()

				b.AppendValues([]float64{10, 20, 30, 40, 50, 60}, nil)

				return b.NewArray()
			},
			expSTD: 18.708286933869708,
		},
	}

	for _, tt := range tests {
		t.Run(tt.scenario, func(t *testing.T) {
			pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
			defer pool.AssertSize(t, 0)

			actSeries := series.FromArrow(pool, tt.inField, tt.inColumn(pool))
			require.NotNil(t, actSeries)
			defer actSeries.Release()

			actSTD := actSeries.STD()
			assert.Equal(t, tt.expSTD, actSTD)
		})
	}
}

func TestMap(t *testing.T) {
	tests := []struct {
		scenario string

		inField  arrow.Field
		inColumn func(memory.Allocator) array.Interface
		inMapFn  func(interface{}) interface{}

		expValues interface{}
	}{
		{
			scenario: "int32 column",
			inField:  arrow.Field{Name: "f1-i32", Type: arrow.PrimitiveTypes.Int32},
			inColumn: func(pool memory.Allocator) array.Interface {
				b := array.NewInt32Builder(pool)
				defer b.Release()

				b.AppendValues([]int32{1, 1, 2, 2, 2, 2, 3, 4, 5, 6}, nil)

				return b.NewArray()
			},
			inMapFn: func(v interface{}) interface{} {
				return v.(int32) * 2
			},
			expValues: []int32{2, 2, 4, 4, 4, 4, 6, 8, 10, 12},
		},
		{
			scenario: "int64 column",
			inField:  arrow.Field{Name: "f1-i64", Type: arrow.PrimitiveTypes.Int64},
			inColumn: func(pool memory.Allocator) array.Interface {
				b := array.NewInt64Builder(pool)
				defer b.Release()

				b.AppendValues([]int64{1, 1, 2, 2, 2, 2, 3, 4, 5, 6}, nil)

				return b.NewArray()
			},
			inMapFn: func(v interface{}) interface{} {
				return v.(int64) * 2
			},
			expValues: []int64{2, 2, 4, 4, 4, 4, 6, 8, 10, 12},
		},
		{
			scenario: "float32 column",
			inField:  arrow.Field{Name: "f1-f32", Type: arrow.PrimitiveTypes.Float32},
			inColumn: func(pool memory.Allocator) array.Interface {
				b := array.NewFloat32Builder(pool)
				defer b.Release()

				b.AppendValues([]float32{1, 1, 2, 2, 2, 2, 3, 4, 5, 6}, nil)

				return b.NewArray()
			},
			inMapFn: func(v interface{}) interface{} {
				return v.(float32) * 2
			},
			expValues: []float32{2, 2, 4, 4, 4, 4, 6, 8, 10, 12},
		},
		{
			scenario: "float64 column",
			inField:  arrow.Field{Name: "f1-f64", Type: arrow.PrimitiveTypes.Float64},
			inColumn: func(pool memory.Allocator) array.Interface {
				b := array.NewFloat64Builder(pool)
				defer b.Release()

				b.AppendValues([]float64{1, 1, 2, 2, 2, 2, 3, 4, 5, 6}, nil)

				return b.NewArray()
			},
			inMapFn: func(v interface{}) interface{} {
				return v.(float64) * 2
			},
			expValues: []float64{2, 2, 4, 4, 4, 4, 6, 8, 10, 12},
		},
	}

	for _, tt := range tests {
		t.Run(tt.scenario, func(t *testing.T) {
			pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
			defer pool.AssertSize(t, 0)

			actSeries := series.FromArrow(pool, tt.inField, tt.inColumn(pool))
			require.NotNil(t, actSeries)
			defer actSeries.Release()

			actValues := actSeries.Map(tt.inMapFn)
			defer actValues.Release()
			assert.Equal(t, tt.expValues, actValues.Values())
		})
	}
}

func TestWhere(t *testing.T) {
	tests := []struct {
		scenario string

		inField      arrow.Field
		inColumn     func(memory.Allocator) array.Interface
		inConditions []series.Condition

		expValues interface{}
	}{
		{
			scenario: "int32 column",
			inField:  arrow.Field{Name: "f1-i32", Type: arrow.PrimitiveTypes.Int32},
			inColumn: func(pool memory.Allocator) array.Interface {
				b := array.NewInt32Builder(pool)
				defer b.Release()

				b.AppendValues([]int32{1, 1, 2, 2, 2, 2, 3, 4, 5, 6}, nil)

				return b.NewArray()
			},
			inConditions: []series.Condition{
				func(v interface{}) bool {
					if v == int32(2) {
						return true
					}
					return false
				},
			},
			expValues: []int32{2, 2, 2, 2},
		},
		{
			scenario: "int64 column",
			inField:  arrow.Field{Name: "f1-i64", Type: arrow.PrimitiveTypes.Int64},
			inColumn: func(pool memory.Allocator) array.Interface {
				b := array.NewInt64Builder(pool)
				defer b.Release()

				b.AppendValues([]int64{1, 1, 2, 2, 2, 2, 3, 4, 5, 6}, nil)

				return b.NewArray()
			},
			inConditions: []series.Condition{
				func(v interface{}) bool {
					if v == int64(2) {
						return true
					}
					return false
				},
			},
			expValues: []int64{2, 2, 2, 2},
		},
		{
			scenario: "float32 column",
			inField:  arrow.Field{Name: "f1-f32", Type: arrow.PrimitiveTypes.Float32},
			inColumn: func(pool memory.Allocator) array.Interface {
				b := array.NewFloat32Builder(pool)
				defer b.Release()

				b.AppendValues([]float32{1, 1, 2, 2, 2, 2, 3, 4, 5, 6}, nil)

				return b.NewArray()
			},
			inConditions: []series.Condition{
				func(v interface{}) bool {
					if v == float32(2) {
						return true
					}
					return false
				},
			},
			expValues: []float32{2, 2, 2, 2},
		},
		{
			scenario: "float64 column",
			inField:  arrow.Field{Name: "f1-f64", Type: arrow.PrimitiveTypes.Float64},
			inColumn: func(pool memory.Allocator) array.Interface {
				b := array.NewFloat64Builder(pool)
				defer b.Release()

				b.AppendValues([]float64{1, 1, 2, 2, 2, 2, 3, 4, 5, 6}, nil)

				return b.NewArray()
			},
			inConditions: []series.Condition{
				func(v interface{}) bool {
					if v == float64(2) {
						return true
					}
					return false
				},
			},
			expValues: []float64{2, 2, 2, 2},
		},
	}

	for _, tt := range tests {
		t.Run(tt.scenario, func(t *testing.T) {
			pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
			defer pool.AssertSize(t, 0)

			actSeries := series.FromArrow(pool, tt.inField, tt.inColumn(pool))
			require.NotNil(t, actSeries)
			defer actSeries.Release()

			actValues := actSeries.Where(tt.inConditions...)
			defer actValues.Release()
			assert.Equal(t, tt.expValues, actValues.Values())
		})
	}
}

func TestSortValues(t *testing.T) {
	tests := []struct {
		scenario string

		inField  arrow.Field
		inColumn func(memory.Allocator) array.Interface

		expSortValues interface{}
	}{
		{
			scenario: "int32 column",
			inField:  arrow.Field{Name: "f1-i32", Type: arrow.PrimitiveTypes.Int32},
			inColumn: func(pool memory.Allocator) array.Interface {
				b := array.NewInt32Builder(pool)
				defer b.Release()

				b.AppendValues([]int32{4, 2, 7, 1, 3, 5, 6}, nil)

				return b.NewArray()
			},
			expSortValues: []int32{1, 2, 3, 4, 5, 6, 7},
		},
		{
			scenario: "int64 column",
			inField:  arrow.Field{Name: "f1-i64", Type: arrow.PrimitiveTypes.Int64},
			inColumn: func(pool memory.Allocator) array.Interface {
				b := array.NewInt64Builder(pool)
				defer b.Release()

				b.AppendValues([]int64{40, 20, 70, 10, 30, 50, 60}, nil)

				return b.NewArray()
			},
			expSortValues: []int64{10, 20, 30, 40, 50, 60, 70},
		},
		{
			scenario: "float32 column",
			inField:  arrow.Field{Name: "f1-f32", Type: arrow.PrimitiveTypes.Float32},
			inColumn: func(pool memory.Allocator) array.Interface {
				b := array.NewFloat32Builder(pool)
				defer b.Release()

				b.AppendValues([]float32{4, 2, 7, 1, 3, 5, 6}, nil)

				return b.NewArray()
			},
			expSortValues: []float32{1, 2, 3, 4, 5, 6, 7},
		},
		{
			scenario: "float64 column",
			inField:  arrow.Field{Name: "f1-f64", Type: arrow.PrimitiveTypes.Float64},
			inColumn: func(pool memory.Allocator) array.Interface {
				b := array.NewFloat64Builder(pool)
				defer b.Release()

				b.AppendValues([]float64{40, 20, 70, 10, 30, 50, 60}, nil)

				return b.NewArray()
			},
			expSortValues: []float64{10, 20, 30, 40, 50, 60, 70},
		},
	}

	for _, tt := range tests {
		t.Run(tt.scenario, func(t *testing.T) {
			pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
			defer pool.AssertSize(t, 0)

			actSeries := series.FromArrow(pool, tt.inField, tt.inColumn(pool))
			require.NotNil(t, actSeries)
			defer actSeries.Release()

			actSortValues := actSeries.SortValues()
			assert.Equal(t, tt.expSortValues, actSortValues.Values())
			defer actSortValues.Release()
		})
	}
}

func TestDropNA(t *testing.T) {
	tests := []struct {
		scenario string

		inField  arrow.Field
		inColumn func(memory.Allocator) array.Interface

		expDropNA interface{}
	}{
		{
			scenario: "int32 column",
			inField:  arrow.Field{Name: "f1-i32", Type: arrow.PrimitiveTypes.Int32},
			inColumn: func(pool memory.Allocator) array.Interface {
				b := array.NewInt32Builder(pool)
				defer b.Release()

				b.AppendValues([]int32{1, 2, 3, 4, 5, 6, 7},
					[]bool{true, false, true, false, true, false, true})

				return b.NewArray()
			},
			expDropNA: []int32{1, 3, 5, 7},
		},
		{
			scenario: "int64 column",
			inField:  arrow.Field{Name: "f1-i64", Type: arrow.PrimitiveTypes.Int64},
			inColumn: func(pool memory.Allocator) array.Interface {
				b := array.NewInt64Builder(pool)
				defer b.Release()

				b.AppendValues([]int64{1, 2, 3, 4, 5, 6, 7},
					[]bool{true, false, true, false, true, false, true})

				return b.NewArray()
			},
			expDropNA: []int64{1, 3, 5, 7},
		},
		{
			scenario: "float32 column",
			inField:  arrow.Field{Name: "f1-f32", Type: arrow.PrimitiveTypes.Float32},
			inColumn: func(pool memory.Allocator) array.Interface {
				b := array.NewFloat32Builder(pool)
				defer b.Release()

				b.AppendValues([]float32{1, 2, 3, 4, 5, 6, 7},
					[]bool{true, false, true, false, true, false, true})

				return b.NewArray()
			},
			expDropNA: []float32{1, 3, 5, 7},
		},
		{
			scenario: "float64 column",
			inField:  arrow.Field{Name: "f1-f64", Type: arrow.PrimitiveTypes.Float64},
			inColumn: func(pool memory.Allocator) array.Interface {
				b := array.NewFloat64Builder(pool)
				defer b.Release()

				b.AppendValues([]float64{1, 2, 3, 4, 5, 6, 7},
					[]bool{true, false, true, false, true, false, true})

				return b.NewArray()
			},
			expDropNA: []float64{1, 3, 5, 7},
		},
	}

	for _, tt := range tests {
		t.Run(tt.scenario, func(t *testing.T) {
			pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
			defer pool.AssertSize(t, 0)

			actSeries := series.FromArrow(pool, tt.inField, tt.inColumn(pool))
			require.NotNil(t, actSeries)
			defer actSeries.Release()

			actDropNA := actSeries.DropNA()
			assert.Equal(t, tt.expDropNA, actDropNA.Values())
			defer actDropNA.Release()
		})
	}
}

func TestDot(t *testing.T) {
	tests := []struct {
		scenario string

		inField   arrow.Field
		inColumn  func(memory.Allocator) array.Interface
		inField2  arrow.Field
		inColumn2 func(memory.Allocator) array.Interface

		expDot float64
	}{
		{
			scenario: "int32 column",
			inField:  arrow.Field{Name: "f1-i32", Type: arrow.PrimitiveTypes.Int32},
			inColumn: func(pool memory.Allocator) array.Interface {
				b := array.NewInt32Builder(pool)
				defer b.Release()

				b.AppendValues([]int32{1, 2, 3, 4, 5, 6, 7}, nil)

				return b.NewArray()
			},
			inField2: arrow.Field{Name: "f1-i32", Type: arrow.PrimitiveTypes.Int32},
			inColumn2: func(pool memory.Allocator) array.Interface {
				b := array.NewInt32Builder(pool)
				defer b.Release()

				b.AppendValues([]int32{1, 2, 3, 4, 5, 6, 7}, nil)

				return b.NewArray()
			},
			expDot: 140,
		},
		{
			scenario: "int64 column",
			inField:  arrow.Field{Name: "f1-i64", Type: arrow.PrimitiveTypes.Int64},
			inColumn: func(pool memory.Allocator) array.Interface {
				b := array.NewInt64Builder(pool)
				defer b.Release()

				b.AppendValues([]int64{1, 2, 3, 4, 5, 6, 7}, nil)

				return b.NewArray()
			},
			inField2: arrow.Field{Name: "f1-i64", Type: arrow.PrimitiveTypes.Int64},
			inColumn2: func(pool memory.Allocator) array.Interface {
				b := array.NewInt64Builder(pool)
				defer b.Release()

				b.AppendValues([]int64{1, 2, 3, 4, 5, 6, 7}, nil)

				return b.NewArray()
			},
			expDot: 140,
		},
		{
			scenario: "float32 column",
			inField:  arrow.Field{Name: "f1-f32", Type: arrow.PrimitiveTypes.Float32},
			inColumn: func(pool memory.Allocator) array.Interface {
				b := array.NewFloat32Builder(pool)
				defer b.Release()

				b.AppendValues([]float32{1, 2, 3, 4, 5, 6, 7}, nil)

				return b.NewArray()
			},
			inField2: arrow.Field{Name: "f1-f32", Type: arrow.PrimitiveTypes.Float32},
			inColumn2: func(pool memory.Allocator) array.Interface {
				b := array.NewFloat32Builder(pool)
				defer b.Release()

				b.AppendValues([]float32{1, 2, 3, 4, 5, 6, 7}, nil)

				return b.NewArray()
			},
			expDot: 140,
		},
		{
			scenario: "float64 column",
			inField:  arrow.Field{Name: "f1-f64", Type: arrow.PrimitiveTypes.Float64},
			inColumn: func(pool memory.Allocator) array.Interface {
				b := array.NewFloat64Builder(pool)
				defer b.Release()

				b.AppendValues([]float64{1, 2, 3, 4, 5, 6, 7}, nil)

				return b.NewArray()
			},
			inField2: arrow.Field{Name: "f1-f64", Type: arrow.PrimitiveTypes.Float64},
			inColumn2: func(pool memory.Allocator) array.Interface {
				b := array.NewFloat64Builder(pool)
				defer b.Release()

				b.AppendValues([]float64{1, 2, 3, 4, 5, 6, 7}, nil)

				return b.NewArray()
			},
			expDot: 140,
		},
	}

	for _, tt := range tests {
		t.Run(tt.scenario, func(t *testing.T) {
			pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
			defer pool.AssertSize(t, 0)

			actSeries := series.FromArrow(pool, tt.inField, tt.inColumn(pool))
			require.NotNil(t, actSeries)
			defer actSeries.Release()

			actSeries2 := series.FromArrow(pool, tt.inField2, tt.inColumn2(pool))
			require.NotNil(t, actSeries2)
			defer actSeries2.Release()

			actDot := actSeries.Dot(actSeries2)
			assert.Equal(t, tt.expDot, actDot)
		})
	}
}

func TestMin(t *testing.T) {
	tests := []struct {
		scenario string

		inField  arrow.Field
		inColumn func(memory.Allocator) array.Interface

		expMin float64
	}{
		{
			scenario: "int32 column",
			inField:  arrow.Field{Name: "f1-i32", Type: arrow.PrimitiveTypes.Int32},
			inColumn: func(pool memory.Allocator) array.Interface {
				b := array.NewInt32Builder(pool)
				defer b.Release()

				b.AppendValues([]int32{1, 2, 3, -4, 5, 6, 7}, nil)

				return b.NewArray()
			},
			expMin: -4,
		},
		{
			scenario: "int64 column",
			inField:  arrow.Field{Name: "f1-i64", Type: arrow.PrimitiveTypes.Int64},
			inColumn: func(pool memory.Allocator) array.Interface {
				b := array.NewInt64Builder(pool)
				defer b.Release()

				b.AppendValues([]int64{1, 2, 3, -4, 5, 6, 7}, nil)

				return b.NewArray()
			},
			expMin: -4,
		},
		{
			scenario: "float32 column",
			inField:  arrow.Field{Name: "f1-f32", Type: arrow.PrimitiveTypes.Float32},
			inColumn: func(pool memory.Allocator) array.Interface {
				b := array.NewFloat32Builder(pool)
				defer b.Release()

				b.AppendValues([]float32{1, 1, 2, 3, 4, -4, 6, 7}, nil)

				return b.NewArray()
			},
			expMin: -4,
		},
		{
			scenario: "float64 column",
			inField:  arrow.Field{Name: "f1-f64", Type: arrow.PrimitiveTypes.Float64},
			inColumn: func(pool memory.Allocator) array.Interface {
				b := array.NewFloat64Builder(pool)
				defer b.Release()

				b.AppendValues([]float64{1, 1, 2, 3, 4, 0, 6, 7}, nil)

				return b.NewArray()
			},
			expMin: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.scenario, func(t *testing.T) {
			pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
			defer pool.AssertSize(t, 0)

			actSeries := series.FromArrow(pool, tt.inField, tt.inColumn(pool))
			require.NotNil(t, actSeries)
			defer actSeries.Release()

			actMin := actSeries.Min()
			assert.Equal(t, tt.expMin, actMin)
		})
	}
}

func TestMax(t *testing.T) {
	tests := []struct {
		scenario string

		inField  arrow.Field
		inColumn func(memory.Allocator) array.Interface

		expMax float64
	}{
		{
			scenario: "int32 column",
			inField:  arrow.Field{Name: "f1-i32", Type: arrow.PrimitiveTypes.Int32},
			inColumn: func(pool memory.Allocator) array.Interface {
				b := array.NewInt32Builder(pool)
				defer b.Release()

				b.AppendValues([]int32{1, -10, 2, 3, -4, 8, 5, 6, 7}, nil)

				return b.NewArray()
			},
			expMax: 8,
		},
		{
			scenario: "int64 column",
			inField:  arrow.Field{Name: "f1-i64", Type: arrow.PrimitiveTypes.Int64},
			inColumn: func(pool memory.Allocator) array.Interface {
				b := array.NewInt64Builder(pool)
				defer b.Release()

				b.AppendValues([]int64{1, -10, 3, -4, 8, 5, 6, 7}, nil)

				return b.NewArray()
			},
			expMax: 8,
		},
		{
			scenario: "float32 column",
			inField:  arrow.Field{Name: "f1-f32", Type: arrow.PrimitiveTypes.Float32},
			inColumn: func(pool memory.Allocator) array.Interface {
				b := array.NewFloat32Builder(pool)
				defer b.Release()

				b.AppendValues([]float32{1.1, -10, 2, 3, 4, 8, 6, 7}, nil)

				return b.NewArray()
			},
			expMax: 8,
		},
		{
			scenario: "float64 column",
			inField:  arrow.Field{Name: "f1-f64", Type: arrow.PrimitiveTypes.Float64},
			inColumn: func(pool memory.Allocator) array.Interface {
				b := array.NewFloat64Builder(pool)
				defer b.Release()

				b.AppendValues([]float64{1, 1, 2, 3, 4, 8, 6, 7}, nil)

				return b.NewArray()
			},
			expMax: 8,
		},
	}

	for _, tt := range tests {
		t.Run(tt.scenario, func(t *testing.T) {
			pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
			defer pool.AssertSize(t, 0)

			actSeries := series.FromArrow(pool, tt.inField, tt.inColumn(pool))
			require.NotNil(t, actSeries)
			defer actSeries.Release()

			actMax := actSeries.Max()
			assert.Equal(t, tt.expMax, actMax)
		})
	}
}

func TestMean(t *testing.T) {
	tests := []struct {
		scenario string

		inField  arrow.Field
		inColumn func(memory.Allocator) array.Interface

		expMean float64
	}{
		{
			scenario: "int32 column",
			inField:  arrow.Field{Name: "f1-i32", Type: arrow.PrimitiveTypes.Int32},
			inColumn: func(pool memory.Allocator) array.Interface {
				b := array.NewInt32Builder(pool)
				defer b.Release()

				b.AppendValues([]int32{1, 2, 3, 4, 5, 6, 7}, nil)

				return b.NewArray()
			},
			expMean: 4,
		},
		{
			scenario: "int64 column",
			inField:  arrow.Field{Name: "f1-i64", Type: arrow.PrimitiveTypes.Int64},
			inColumn: func(pool memory.Allocator) array.Interface {
				b := array.NewInt64Builder(pool)
				defer b.Release()

				b.AppendValues([]int64{1, 2, 3, 4, 5, 6, 7}, nil)

				return b.NewArray()
			},
			expMean: 4,
		},
		{
			scenario: "float32 column",
			inField:  arrow.Field{Name: "f1-f32", Type: arrow.PrimitiveTypes.Float32},
			inColumn: func(pool memory.Allocator) array.Interface {
				b := array.NewFloat32Builder(pool)
				defer b.Release()

				b.AppendValues([]float32{1, 2, 3, 4, 5, 6, 7}, nil)

				return b.NewArray()
			},
			expMean: 4,
		},
		{
			scenario: "float64 column",
			inField:  arrow.Field{Name: "f1-f64", Type: arrow.PrimitiveTypes.Float64},
			inColumn: func(pool memory.Allocator) array.Interface {
				b := array.NewFloat64Builder(pool)
				defer b.Release()

				b.AppendValues([]float64{1, 2, 3, 4, 5, 6, 7}, nil)

				return b.NewArray()
			},
			expMean: 4,
		},
	}

	for _, tt := range tests {
		t.Run(tt.scenario, func(t *testing.T) {
			pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
			defer pool.AssertSize(t, 0)

			actSeries := series.FromArrow(pool, tt.inField, tt.inColumn(pool))
			require.NotNil(t, actSeries)
			defer actSeries.Release()

			actMean := actSeries.Mean()
			assert.Equal(t, tt.expMean, actMean)
		})
	}
}

func TestMedian(t *testing.T) {
	tests := []struct {
		scenario string

		inField  arrow.Field
		inColumn func(memory.Allocator) array.Interface

		expMedian float64
	}{
		{
			scenario: "int32 column: odd number of rows",
			inField:  arrow.Field{Name: "f1-i32", Type: arrow.PrimitiveTypes.Int32},
			inColumn: func(pool memory.Allocator) array.Interface {
				b := array.NewInt32Builder(pool)
				defer b.Release()

				b.AppendValues([]int32{1, 2, 3, 4, 5, 6, 7}, nil)

				return b.NewArray()
			},
			expMedian: 4,
		},
		{
			scenario: "int32 column: even number of rows",
			inField:  arrow.Field{Name: "f1-i32", Type: arrow.PrimitiveTypes.Int32},
			inColumn: func(pool memory.Allocator) array.Interface {
				b := array.NewInt32Builder(pool)
				defer b.Release()

				b.AppendValues([]int32{1, 2, 3, 4, 5, 6}, nil)

				return b.NewArray()
			},
			expMedian: float64(3+4) / float64(2),
		},
		{
			scenario: "int64 column: odd number of rows",
			inField:  arrow.Field{Name: "f1-i64", Type: arrow.PrimitiveTypes.Int64},
			inColumn: func(pool memory.Allocator) array.Interface {
				b := array.NewInt64Builder(pool)
				defer b.Release()

				b.AppendValues([]int64{1, 2, 3, 4, 5, 6, 7}, nil)

				return b.NewArray()
			},
			expMedian: 4,
		},
		{
			scenario: "int64 column: even number of rows",
			inField:  arrow.Field{Name: "f1-i64", Type: arrow.PrimitiveTypes.Int64},
			inColumn: func(pool memory.Allocator) array.Interface {
				b := array.NewInt64Builder(pool)
				defer b.Release()

				b.AppendValues([]int64{1, 2, 3, 4, 5, 6}, nil)

				return b.NewArray()
			},
			expMedian: float64(3+4) / float64(2),
		},
		{
			scenario: "float32 column: odd number of rows",
			inField:  arrow.Field{Name: "f1-f32", Type: arrow.PrimitiveTypes.Float32},
			inColumn: func(pool memory.Allocator) array.Interface {
				b := array.NewFloat32Builder(pool)
				defer b.Release()

				b.AppendValues([]float32{1, 2, 3, 4, 5, 6, 7}, nil)

				return b.NewArray()
			},
			expMedian: 4,
		},
		{
			scenario: "float32 column: even number of rows",
			inField:  arrow.Field{Name: "f1-f32", Type: arrow.PrimitiveTypes.Float32},
			inColumn: func(pool memory.Allocator) array.Interface {
				b := array.NewFloat32Builder(pool)
				defer b.Release()

				b.AppendValues([]float32{1, 2, 3, 4, 5, 6}, nil)

				return b.NewArray()
			},
			expMedian: float64(3+4) / float64(2),
		},
		{
			scenario: "float64 column: odd number of rows",
			inField:  arrow.Field{Name: "f1-f64", Type: arrow.PrimitiveTypes.Float64},
			inColumn: func(pool memory.Allocator) array.Interface {
				b := array.NewFloat64Builder(pool)
				defer b.Release()

				b.AppendValues([]float64{1, 2, 3, 4, 5, 6, 7}, nil)

				return b.NewArray()
			},
			expMedian: 4,
		},
		{
			scenario: "float64 column: even number of rows",
			inField:  arrow.Field{Name: "f1-f64", Type: arrow.PrimitiveTypes.Float64},
			inColumn: func(pool memory.Allocator) array.Interface {
				b := array.NewFloat64Builder(pool)
				defer b.Release()

				b.AppendValues([]float64{1, 2, 3, 4, 5, 6}, nil)

				return b.NewArray()
			},
			expMedian: float64(3+4) / float64(2),
		},
	}

	for _, tt := range tests {
		t.Run(tt.scenario, func(t *testing.T) {
			pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
			defer pool.AssertSize(t, 0)

			actSeries := series.FromArrow(pool, tt.inField, tt.inColumn(pool))
			require.NotNil(t, actSeries)
			defer actSeries.Release()

			actMedian := actSeries.Median()
			assert.Equal(t, tt.expMedian, actMedian)
		})
	}
}

func TestSquare(t *testing.T) {
	tests := []struct {
		scenario string

		inField  arrow.Field
		inColumn func(memory.Allocator) array.Interface

		expSquare interface{}
	}{
		{
			scenario: "int32 column",
			inField:  arrow.Field{Name: "f1-i32", Type: arrow.PrimitiveTypes.Int32},
			inColumn: func(pool memory.Allocator) array.Interface {
				b := array.NewInt32Builder(pool)
				defer b.Release()

				b.AppendValues([]int32{1, 2, 3, 4, 5, 6, 7}, nil)

				return b.NewArray()
			},
			expSquare: []int64{1, 4, 9, 16, 25, 36, 49},
		},
		{
			scenario: "int64 column",
			inField:  arrow.Field{Name: "f1-i64", Type: arrow.PrimitiveTypes.Int64},
			inColumn: func(pool memory.Allocator) array.Interface {
				b := array.NewInt64Builder(pool)
				defer b.Release()

				b.AppendValues([]int64{1, 2, 3, 4, 5, 6, 7}, nil)

				return b.NewArray()
			},
			expSquare: []int64{1, 4, 9, 16, 25, 36, 49},
		},
		{
			scenario: "float32 column",
			inField:  arrow.Field{Name: "f1-f32", Type: arrow.PrimitiveTypes.Float32},
			inColumn: func(pool memory.Allocator) array.Interface {
				b := array.NewFloat32Builder(pool)
				defer b.Release()

				b.AppendValues([]float32{1, 2, 3, 4, 5, 6, 7}, nil)

				return b.NewArray()
			},
			expSquare: []float64{1, 4, 9, 16, 25, 36, 49},
		},
		{
			scenario: "float64 column",
			inField:  arrow.Field{Name: "f1-f64", Type: arrow.PrimitiveTypes.Float64},
			inColumn: func(pool memory.Allocator) array.Interface {
				b := array.NewFloat64Builder(pool)
				defer b.Release()

				b.AppendValues([]float64{1, 2, 3, 4, 5, 6, 7}, nil)

				return b.NewArray()
			},
			expSquare: []float64{1, 4, 9, 16, 25, 36, 49},
		},
	}

	for _, tt := range tests {
		t.Run(tt.scenario, func(t *testing.T) {
			pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
			defer pool.AssertSize(t, 0)

			actSeries := series.FromArrow(pool, tt.inField, tt.inColumn(pool))
			require.NotNil(t, actSeries)
			defer actSeries.Release()

			actSquare := actSeries.Square()
			assert.Equal(t, tt.expSquare, actSquare.Values())
			defer actSquare.Release()
		})
	}
}

func TestSqrt(t *testing.T) {
	tests := []struct {
		scenario string

		inField  arrow.Field
		inColumn func(memory.Allocator) array.Interface

		expSqrt interface{}
	}{
		{
			scenario: "int32 column",
			inField:  arrow.Field{Name: "f1-i32", Type: arrow.PrimitiveTypes.Int32},
			inColumn: func(pool memory.Allocator) array.Interface {
				b := array.NewInt32Builder(pool)
				defer b.Release()

				b.AppendValues([]int32{1, 4, 9, 16, 25, 36, 49}, nil)

				return b.NewArray()
			},
			expSqrt: []float64{1, 2, 3, 4, 5, 6, 7},
		},
		{
			scenario: "int64 column",
			inField:  arrow.Field{Name: "f1-i64", Type: arrow.PrimitiveTypes.Int64},
			inColumn: func(pool memory.Allocator) array.Interface {
				b := array.NewInt64Builder(pool)
				defer b.Release()

				b.AppendValues([]int64{1, 4, 9, 16, 25, 36, 49}, nil)

				return b.NewArray()
			},
			expSqrt: []float64{1, 2, 3, 4, 5, 6, 7},
		},
		{
			scenario: "float32 column",
			inField:  arrow.Field{Name: "f1-f32", Type: arrow.PrimitiveTypes.Float32},
			inColumn: func(pool memory.Allocator) array.Interface {
				b := array.NewFloat32Builder(pool)
				defer b.Release()

				b.AppendValues([]float32{1, 4, 9, 16, 25, 36, 49}, nil)

				return b.NewArray()
			},
			expSqrt: []float64{1, 2, 3, 4, 5, 6, 7},
		},
		{
			scenario: "float64 column",
			inField:  arrow.Field{Name: "f1-f64", Type: arrow.PrimitiveTypes.Float64},
			inColumn: func(pool memory.Allocator) array.Interface {
				b := array.NewFloat64Builder(pool)
				defer b.Release()

				b.AppendValues([]float64{1, 4, 9, 16, 25, 36, 49}, nil)

				return b.NewArray()
			},
			expSqrt: []float64{1, 2, 3, 4, 5, 6, 7},
		},
	}

	for _, tt := range tests {
		t.Run(tt.scenario, func(t *testing.T) {
			pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
			defer pool.AssertSize(t, 0)

			actSeries := series.FromArrow(pool, tt.inField, tt.inColumn(pool))
			require.NotNil(t, actSeries)
			defer actSeries.Release()

			actSqrt := actSeries.Sqrt()
			assert.Equal(t, tt.expSqrt, actSqrt.Values())
			defer actSqrt.Release()
		})
	}
}

func TestAbs(t *testing.T) {
	tests := []struct {
		scenario string

		inField  arrow.Field
		inColumn func(memory.Allocator) array.Interface

		expAbs interface{}
	}{
		{
			scenario: "int32 column",
			inField:  arrow.Field{Name: "f1-i32", Type: arrow.PrimitiveTypes.Int32},
			inColumn: func(pool memory.Allocator) array.Interface {
				b := array.NewInt32Builder(pool)
				defer b.Release()

				b.AppendValues([]int32{-1, 2, -3, 4, -5, 6, -7}, nil)

				return b.NewArray()
			},
			expAbs: []int32{1, 2, 3, 4, 5, 6, 7},
		},
		{
			scenario: "int64 column",
			inField:  arrow.Field{Name: "f1-i64", Type: arrow.PrimitiveTypes.Int64},
			inColumn: func(pool memory.Allocator) array.Interface {
				b := array.NewInt64Builder(pool)
				defer b.Release()

				b.AppendValues([]int64{-1, 2, -3, 4, -5, 6, -7}, nil)

				return b.NewArray()
			},
			expAbs: []int64{1, 2, 3, 4, 5, 6, 7},
		},
		{
			scenario: "float32 column",
			inField:  arrow.Field{Name: "f1-f32", Type: arrow.PrimitiveTypes.Float32},
			inColumn: func(pool memory.Allocator) array.Interface {
				b := array.NewFloat32Builder(pool)
				defer b.Release()

				b.AppendValues([]float32{-1, 2, -3, 4, -5, 6, -7}, nil)

				return b.NewArray()
			},
			expAbs: []float32{1, 2, 3, 4, 5, 6, 7},
		},
		{
			scenario: "float64 column",
			inField:  arrow.Field{Name: "f1-f64", Type: arrow.PrimitiveTypes.Float64},
			inColumn: func(pool memory.Allocator) array.Interface {
				b := array.NewFloat64Builder(pool)
				defer b.Release()

				b.AppendValues([]float64{-1, 2, -3, 4, -5, 6, -7}, nil)

				return b.NewArray()
			},
			expAbs: []float64{1, 2, 3, 4, 5, 6, 7},
		},
	}

	for _, tt := range tests {
		t.Run(tt.scenario, func(t *testing.T) {
			pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
			defer pool.AssertSize(t, 0)

			actSeries := series.FromArrow(pool, tt.inField, tt.inColumn(pool))
			require.NotNil(t, actSeries)
			defer actSeries.Release()

			actAbs := actSeries.Abs()
			assert.Equal(t, tt.expAbs, actAbs.Values())
			defer actAbs.Release()
		})
	}
}

func TestAdd(t *testing.T) {
	tests := []struct {
		scenario string

		inField   arrow.Field
		inColumn  func(memory.Allocator) array.Interface
		inField2  arrow.Field
		inColumn2 func(memory.Allocator) array.Interface

		expAdd interface{}
	}{
		{
			scenario: "int32 column",
			inField:  arrow.Field{Name: "f1-i32", Type: arrow.PrimitiveTypes.Int32},
			inColumn: func(pool memory.Allocator) array.Interface {
				b := array.NewInt32Builder(pool)
				defer b.Release()

				b.AppendValues([]int32{1, 2, 3, 4, 5, 6, 7}, nil)

				return b.NewArray()
			},
			inField2: arrow.Field{Name: "f1-i32", Type: arrow.PrimitiveTypes.Int32},
			inColumn2: func(pool memory.Allocator) array.Interface {
				b := array.NewInt32Builder(pool)
				defer b.Release()

				b.AppendValues([]int32{1, 2, 3, 4, 5, 6, 7}, nil)

				return b.NewArray()
			},
			expAdd: []int32{2, 4, 6, 8, 10, 12, 14},
		},
		{
			scenario: "int64 column",
			inField:  arrow.Field{Name: "f1-i64", Type: arrow.PrimitiveTypes.Int64},
			inColumn: func(pool memory.Allocator) array.Interface {
				b := array.NewInt64Builder(pool)
				defer b.Release()

				b.AppendValues([]int64{1, 2, 3, 4, 5, 6, 7}, nil)

				return b.NewArray()
			},
			inField2: arrow.Field{Name: "f1-i64", Type: arrow.PrimitiveTypes.Int64},
			inColumn2: func(pool memory.Allocator) array.Interface {
				b := array.NewInt64Builder(pool)
				defer b.Release()

				b.AppendValues([]int64{1, 2, 3, 4, 5, 6, 7}, nil)

				return b.NewArray()
			},
			expAdd: []int64{2, 4, 6, 8, 10, 12, 14},
		},
		{
			scenario: "float32 column",
			inField:  arrow.Field{Name: "f1-f32", Type: arrow.PrimitiveTypes.Float32},
			inColumn: func(pool memory.Allocator) array.Interface {
				b := array.NewFloat32Builder(pool)
				defer b.Release()

				b.AppendValues([]float32{1, 2, 3, 4, 5, 6, 7}, nil)

				return b.NewArray()
			},
			inField2: arrow.Field{Name: "f1-f32", Type: arrow.PrimitiveTypes.Float32},
			inColumn2: func(pool memory.Allocator) array.Interface {
				b := array.NewFloat32Builder(pool)
				defer b.Release()

				b.AppendValues([]float32{1, 2, 3, 4, 5, 6, 7}, nil)

				return b.NewArray()
			},
			expAdd: []float32{2, 4, 6, 8, 10, 12, 14},
		},
		{
			scenario: "float64 column",
			inField:  arrow.Field{Name: "f1-f64", Type: arrow.PrimitiveTypes.Float64},
			inColumn: func(pool memory.Allocator) array.Interface {
				b := array.NewFloat64Builder(pool)
				defer b.Release()

				b.AppendValues([]float64{1, 2, 3, 4, 5, 6, 7}, nil)

				return b.NewArray()
			},
			inField2: arrow.Field{Name: "f1-f64", Type: arrow.PrimitiveTypes.Float64},
			inColumn2: func(pool memory.Allocator) array.Interface {
				b := array.NewFloat64Builder(pool)
				defer b.Release()

				b.AppendValues([]float64{1, 2, 3, 4, 5, 6, 7}, nil)

				return b.NewArray()
			},
			expAdd: []float64{2, 4, 6, 8, 10, 12, 14},
		},
	}

	for _, tt := range tests {
		t.Run(tt.scenario, func(t *testing.T) {
			pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
			defer pool.AssertSize(t, 0)

			actSeries := series.FromArrow(pool, tt.inField, tt.inColumn(pool))
			require.NotNil(t, actSeries)
			defer actSeries.Release()

			actSeries2 := series.FromArrow(pool, tt.inField2, tt.inColumn2(pool))
			require.NotNil(t, actSeries2)
			defer actSeries2.Release()

			actAdd := actSeries.Add(actSeries2)
			assert.Equal(t, tt.expAdd, actAdd.Values())
			defer actAdd.Release()
		})
	}
}

func TestSubtract(t *testing.T) {
	tests := []struct {
		scenario string

		inField   arrow.Field
		inColumn  func(memory.Allocator) array.Interface
		inField2  arrow.Field
		inColumn2 func(memory.Allocator) array.Interface

		expSubtract interface{}
		expPanic    string
	}{
		{
			scenario: "int32 column",
			inField:  arrow.Field{Name: "f1-i32", Type: arrow.PrimitiveTypes.Int32},
			inColumn: func(pool memory.Allocator) array.Interface {
				b := array.NewInt32Builder(pool)
				defer b.Release()

				b.AppendValues([]int32{1, 2, 3, 4, 5, 6, 7}, nil)

				return b.NewArray()
			},
			inField2: arrow.Field{Name: "f1-i32", Type: arrow.PrimitiveTypes.Int32},
			inColumn2: func(pool memory.Allocator) array.Interface {
				b := array.NewInt32Builder(pool)
				defer b.Release()

				b.AppendValues([]int32{0, 4, 1, 1, 1, 1, 27}, nil)

				return b.NewArray()
			},
			expSubtract: []int32{1, -2, 2, 3, 4, 5, -20},
		},
		{
			scenario: "int64 column",
			inField:  arrow.Field{Name: "f1-i64", Type: arrow.PrimitiveTypes.Int64},
			inColumn: func(pool memory.Allocator) array.Interface {
				b := array.NewInt64Builder(pool)
				defer b.Release()

				b.AppendValues([]int64{1, 2, 3, 4, 5, 6, 7}, nil)

				return b.NewArray()
			},
			inField2: arrow.Field{Name: "f1-i64", Type: arrow.PrimitiveTypes.Int64},
			inColumn2: func(pool memory.Allocator) array.Interface {
				b := array.NewInt64Builder(pool)
				defer b.Release()

				b.AppendValues([]int64{0, 4, 1, 1, 1, 1, 27}, nil)

				return b.NewArray()
			},
			expSubtract: []int64{1, -2, 2, 3, 4, 5, -20},
		},
		{
			scenario: "float32 column",
			inField:  arrow.Field{Name: "f1-f32", Type: arrow.PrimitiveTypes.Float32},
			inColumn: func(pool memory.Allocator) array.Interface {
				b := array.NewFloat32Builder(pool)
				defer b.Release()

				b.AppendValues([]float32{1, 2, 3, 4, 5, 6, 7}, nil)

				return b.NewArray()
			},
			inField2: arrow.Field{Name: "f1-f32", Type: arrow.PrimitiveTypes.Float32},
			inColumn2: func(pool memory.Allocator) array.Interface {
				b := array.NewFloat32Builder(pool)
				defer b.Release()

				b.AppendValues([]float32{0, 4, 1, 1, 1, 1, 27}, nil)

				return b.NewArray()
			},
			expSubtract: []float32{1, -2, 2, 3, 4, 5, -20},
		},
		{
			scenario: "float64 column",
			inField:  arrow.Field{Name: "f1-f64", Type: arrow.PrimitiveTypes.Float64},
			inColumn: func(pool memory.Allocator) array.Interface {
				b := array.NewFloat64Builder(pool)
				defer b.Release()

				b.AppendValues([]float64{1, 2, 3, 4, 5, 6, 7}, nil)

				return b.NewArray()
			},
			inField2: arrow.Field{Name: "f1-f64", Type: arrow.PrimitiveTypes.Float64},
			inColumn2: func(pool memory.Allocator) array.Interface {
				b := array.NewFloat64Builder(pool)
				defer b.Release()

				b.AppendValues([]float64{0, 4, 1, 1, 1, 1, 27}, nil)

				return b.NewArray()
			},
			expSubtract: []float64{1, -2, 2, 3, 4, 5, -20},
		},
		{
			scenario: "invalid types",
			inField:  arrow.Field{Name: "f1-f64", Type: arrow.PrimitiveTypes.Float64},
			inColumn: func(pool memory.Allocator) array.Interface {
				b := array.NewFloat64Builder(pool)
				defer b.Release()

				b.AppendValues([]float64{1, 2, 3, 4, 5, 6, 7}, nil)

				return b.NewArray()
			},
			inField2: arrow.Field{Name: "f1-f32", Type: arrow.PrimitiveTypes.Float32},
			inColumn2: func(pool memory.Allocator) array.Interface {
				b := array.NewFloat32Builder(pool)
				defer b.Release()

				b.AppendValues([]float32{0, 4, 1, 1, 1, 1, 27}, nil)

				return b.NewArray()
			},
			expPanic: "series: subtract: series types do not match",
		},
		{
			scenario: "unsupported type",
			inField:  arrow.Field{Name: "f1-u64", Type: arrow.PrimitiveTypes.Uint64},
			inColumn: func(pool memory.Allocator) array.Interface {
				b := array.NewUint64Builder(pool)
				defer b.Release()

				b.AppendValues([]uint64{1, 2, 3, 4, 5, 6, 7}, nil)

				return b.NewArray()
			},
			inField2: arrow.Field{Name: "f1-u64", Type: arrow.PrimitiveTypes.Uint64},
			inColumn2: func(pool memory.Allocator) array.Interface {
				b := array.NewUint64Builder(pool)
				defer b.Release()

				b.AppendValues([]uint64{0, 4, 1, 1, 1, 1, 27}, nil)

				return b.NewArray()
			},
			expPanic: "series: subtract: unsupported type",
		},
	}

	for _, tt := range tests {
		t.Run(tt.scenario, func(t *testing.T) {
			pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
			defer pool.AssertSize(t, 0)

			actSeries := series.FromArrow(pool, tt.inField, tt.inColumn(pool))
			require.NotNil(t, actSeries)
			defer actSeries.Release()

			actSeries2 := series.FromArrow(pool, tt.inField2, tt.inColumn2(pool))
			require.NotNil(t, actSeries2)
			defer actSeries2.Release()

			if tt.expPanic != "" {
				assert.PanicsWithValue(t, tt.expPanic, func() { actSeries.Subtract(actSeries2) })
				return
			}

			actSubtract := actSeries.Subtract(actSeries2)
			assert.Equal(t, tt.expSubtract, actSubtract.Values())
			defer actSubtract.Release()
		})
	}
}

// TODO: MAKE UNSUPPORTED TYPE THAT IS NOT A REAL TYPE
func TestAppend(t *testing.T) {
	tests := []struct {
		scenario string

		inField   arrow.Field
		inColumn  func(memory.Allocator) array.Interface
		inField2  arrow.Field
		inColumn2 func(memory.Allocator) array.Interface

		expAppend interface{}
		expPanic  string
	}{
		{
			scenario: "int32 column",
			inField:  arrow.Field{Name: "f1-i32", Type: arrow.PrimitiveTypes.Int32},
			inColumn: func(pool memory.Allocator) array.Interface {
				b := array.NewInt32Builder(pool)
				defer b.Release()

				b.AppendValues([]int32{1, 2, 3, 4, 5, 6, 7}, nil)

				return b.NewArray()
			},
			inField2: arrow.Field{Name: "f1-i32", Type: arrow.PrimitiveTypes.Int32},
			inColumn2: func(pool memory.Allocator) array.Interface {
				b := array.NewInt32Builder(pool)
				defer b.Release()

				b.AppendValues([]int32{8, 9, 10, 11, 12, 13}, nil)

				return b.NewArray()
			},
			expAppend: []int32{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13},
		},
		{
			scenario: "int64 column",
			inField:  arrow.Field{Name: "f1-i64", Type: arrow.PrimitiveTypes.Int64},
			inColumn: func(pool memory.Allocator) array.Interface {
				b := array.NewInt64Builder(pool)
				defer b.Release()

				b.AppendValues([]int64{1, 2, 3, 4, 5, 6, 7}, nil)

				return b.NewArray()
			},
			inField2: arrow.Field{Name: "f1-i64", Type: arrow.PrimitiveTypes.Int64},
			inColumn2: func(pool memory.Allocator) array.Interface {
				b := array.NewInt64Builder(pool)
				defer b.Release()

				b.AppendValues([]int64{8, 9, 10, 11, 12, 13}, nil)

				return b.NewArray()
			},
			expAppend: []int64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13},
		},
		{
			scenario: "float32 column",
			inField:  arrow.Field{Name: "f1-f32", Type: arrow.PrimitiveTypes.Float32},
			inColumn: func(pool memory.Allocator) array.Interface {
				b := array.NewFloat32Builder(pool)
				defer b.Release()

				b.AppendValues([]float32{1, 2, 3, 4, 5, 6, 7}, nil)

				return b.NewArray()
			},
			inField2: arrow.Field{Name: "f1-f32", Type: arrow.PrimitiveTypes.Float32},
			inColumn2: func(pool memory.Allocator) array.Interface {
				b := array.NewFloat32Builder(pool)
				defer b.Release()

				b.AppendValues([]float32{8, 9, 10, 11, 12, 13}, nil)

				return b.NewArray()
			},
			expAppend: []float32{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13},
		},
		{
			scenario: "float64 column",
			inField:  arrow.Field{Name: "f1-f64", Type: arrow.PrimitiveTypes.Float64},
			inColumn: func(pool memory.Allocator) array.Interface {
				b := array.NewFloat64Builder(pool)
				defer b.Release()

				b.AppendValues([]float64{1, 2, 3, 4, 5, 6, 7}, nil)

				return b.NewArray()
			},
			inField2: arrow.Field{Name: "f1-f64", Type: arrow.PrimitiveTypes.Float64},
			inColumn2: func(pool memory.Allocator) array.Interface {
				b := array.NewFloat64Builder(pool)
				defer b.Release()

				b.AppendValues([]float64{8, 9, 10, 11, 12, 13}, nil)

				return b.NewArray()
			},
			expAppend: []float64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13},
		},
		{
			scenario: "invalid types",
			inField:  arrow.Field{Name: "f1-f64", Type: arrow.PrimitiveTypes.Float64},
			inColumn: func(pool memory.Allocator) array.Interface {
				b := array.NewFloat64Builder(pool)
				defer b.Release()

				b.AppendValues([]float64{1, 2, 3, 4, 5, 6, 7}, nil)

				return b.NewArray()
			},
			inField2: arrow.Field{Name: "f1-f32", Type: arrow.PrimitiveTypes.Float32},
			inColumn2: func(pool memory.Allocator) array.Interface {
				b := array.NewFloat32Builder(pool)
				defer b.Release()

				b.AppendValues([]float32{8, 9, 10, 11, 12, 13}, nil)

				return b.NewArray()
			},
			expPanic: "series: append: series types do not match",
		},
		{
			scenario: "unsupported type",
			inField:  arrow.Field{Name: "f1-u64", Type: arrow.PrimitiveTypes.Uint64},
			inColumn: func(pool memory.Allocator) array.Interface {
				b := array.NewUint64Builder(pool)
				defer b.Release()

				b.AppendValues([]uint64{1, 2, 3, 4, 5, 6, 7}, nil)

				return b.NewArray()
			},
			inField2: arrow.Field{Name: "f1-u64", Type: arrow.PrimitiveTypes.Uint64},
			inColumn2: func(pool memory.Allocator) array.Interface {
				b := array.NewUint64Builder(pool)
				defer b.Release()

				b.AppendValues([]uint64{8, 9, 10, 11, 12, 13}, nil)

				return b.NewArray()
			},
			expPanic: "series: append: unsupported type",
		},
	}

	for _, tt := range tests {
		t.Run(tt.scenario, func(t *testing.T) {
			pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
			defer pool.AssertSize(t, 0)

			actSeries := series.FromArrow(pool, tt.inField, tt.inColumn(pool))
			require.NotNil(t, actSeries)
			defer actSeries.Release()

			actSeries2 := series.FromArrow(pool, tt.inField2, tt.inColumn2(pool))
			require.NotNil(t, actSeries2)
			defer actSeries2.Release()

			if tt.expPanic != "" {
				assert.PanicsWithValue(t, tt.expPanic, func() { actSeries.Append(actSeries2) })
				return
			}

			actAppend := actSeries.Append(actSeries2)
			assert.Equal(t, tt.expAppend, actAppend.Values())
			defer actAppend.Release()
		})
	}
}

func TestCast(t *testing.T) {
	type inDataType struct {
		dataType arrow.DataType
		expVals  interface{}
	}

	tests := []struct {
		scenario string

		inSeries func(memory.Allocator) series.Series

		inDataTypes []inDataType
	}{
		{
			scenario: "int32 column",
			inSeries: func(pool memory.Allocator) series.Series {
				field := arrow.Field{Name: "i32", Type: arrow.PrimitiveTypes.Int32}
				data := []int32{1, 2, 3, 4, 5, 6}
				var valid []bool = nil

				return series.FromInt32(pool, field, data, valid)
			},
			inDataTypes: []inDataType{
				inDataType{dataType: arrow.PrimitiveTypes.Int32, expVals: []int32{1, 2, 3, 4, 5, 6}},
				inDataType{dataType: arrow.PrimitiveTypes.Int64, expVals: []int64{1, 2, 3, 4, 5, 6}},
				inDataType{dataType: arrow.PrimitiveTypes.Float32, expVals: []float32{1, 2, 3, 4, 5, 6}},
				inDataType{dataType: arrow.PrimitiveTypes.Float64, expVals: []float64{1, 2, 3, 4, 5, 6}},
				inDataType{dataType: arrow.BinaryTypes.String, expVals: []string{"1", "2", "3", "4", "5", "6"}},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.scenario, func(t *testing.T) {
			pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
			defer pool.AssertSize(t, 0)

			actSeries := tt.inSeries(pool)
			defer actSeries.Release()

			for _, inDataType := range tt.inDataTypes {
				cast := actSeries.Cast(inDataType.dataType)
				defer cast.Release()

				assert.Equal(t, inDataType.dataType, cast.DataType())
				assert.Equal(t, inDataType.expVals, cast.Values())
			}
		})
	}
}
