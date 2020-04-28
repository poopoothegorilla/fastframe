package dataframe_test

import (
	"fmt"
	"math/rand"
	"strconv"
	"testing"

	"github.com/apache/arrow/go/arrow"
	"github.com/apache/arrow/go/arrow/memory"
	"github.com/poopoothegorilla/fastframe/dataframe"
	"github.com/poopoothegorilla/fastframe/series"
)

func BenchmarkAdd(b *testing.B) {
	vals := []int{10, 100, 1000}

	for _, val := range vals {
		b.Run(fmt.Sprintf("size=%v", val), func(b *testing.B) {
			benchmarkAdd(b, val)
		})
	}
}

func benchmarkAdd(b *testing.B, cols int) {
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(b, 0)
	rows := 2

	columns := make([]series.Series, cols)
	for i := 0; i < cols; i++ {
		vals := make([]float64, rows)
		for j := 0; j < rows; j++ {
			vals[j] = rand.Float64()
		}
		field := arrow.Field{Name: strconv.Itoa(i), Type: arrow.PrimitiveTypes.Float64}
		ss := series.FromFloat64(pool, field, vals, nil)
		defer ss.Release()

		columns[i] = ss
	}

	df := dataframe.NewFromSeries(pool, columns)
	defer df.Release()
	df2 := dataframe.NewFromSeries(pool, columns)
	defer df2.Release()

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		df3 := df.Add(df2)
		df3.Release()
	}
}

func BenchmarkSelectColumnsByNames(b *testing.B) {
	vals := []int{10, 100, 1000}

	for _, val := range vals {
		b.Run(fmt.Sprintf("size=%v", val), func(b *testing.B) {
			benchmarkSelectColumnsByNames(b, val)
		})
	}
}

func benchmarkSelectColumnsByNames(b *testing.B, cols int) {
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(b, 0)
	rows := 2

	columns := make([]series.Series, cols)
	for i := 0; i < cols; i++ {
		vals := make([]float64, rows)
		for j := 0; j < rows; j++ {
			vals[j] = rand.Float64()
		}
		field := arrow.Field{Name: strconv.Itoa(i), Type: arrow.PrimitiveTypes.Float64}
		ss := series.FromFloat64(pool, field, vals, nil)
		defer ss.Release()

		columns[i] = ss
	}

	indices := rand.Perm(cols)
	indices = indices[:cols/2]
	columnNames := make([]string, len(indices))
	for i, v := range indices {
		columnNames[i] = strconv.Itoa(v)
	}

	df := dataframe.NewFromSeries(pool, columns)
	defer df.Release()

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		df2 := df.SelectColumnsByNames(columnNames)
		df2.Release()
	}
}

func BenchmarkDropColumnsByIndices(b *testing.B) {
	vals := []int{10, 100, 1000}

	for _, val := range vals {
		b.Run(fmt.Sprintf("size=%v", val), func(b *testing.B) {
			benchmarkDropColumnsByIndices(b, val)
		})
	}
}

func benchmarkDropColumnsByIndices(b *testing.B, cols int) {
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(b, 0)
	rows := 2

	columns := make([]series.Series, cols)
	for i := 0; i < cols; i++ {
		vals := make([]float64, rows)
		for j := 0; j < rows; j++ {
			vals[j] = rand.Float64()
		}
		field := arrow.Field{Name: strconv.Itoa(i), Type: arrow.PrimitiveTypes.Float64}
		ss := series.FromFloat64(pool, field, vals, nil)
		defer ss.Release()

		columns[i] = ss
	}

	indices := rand.Perm(cols)
	indices = indices[:cols/2]

	df := dataframe.NewFromSeries(pool, columns)
	defer df.Release()

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		df2 := df.DropColumnsByIndices(indices)
		df2.Release()
	}
}

func BenchmarkDropColumnsByNames(b *testing.B) {
	vals := []int{10, 100, 1000}

	for _, val := range vals {
		b.Run(fmt.Sprintf("size=%v", val), func(b *testing.B) {
			benchmarkDropColumnsByNames(b, val)
		})
	}
}

func benchmarkDropColumnsByNames(b *testing.B, cols int) {
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(b, 0)
	rows := 2

	columns := make([]series.Series, cols)
	for i := 0; i < cols; i++ {
		vals := make([]float64, rows)
		for j := 0; j < rows; j++ {
			vals[j] = rand.Float64()
		}
		field := arrow.Field{Name: strconv.Itoa(i), Type: arrow.PrimitiveTypes.Float64}
		ss := series.FromFloat64(pool, field, vals, nil)
		defer ss.Release()

		columns[i] = ss
	}

	indices := rand.Perm(cols)
	indices = indices[:cols/2]
	columnNames := make([]string, len(indices))
	for i, v := range indices {
		columnNames[i] = strconv.Itoa(v)
	}

	df := dataframe.NewFromSeries(pool, columns)
	defer df.Release()

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		df2 := df.DropColumnsByNames(columnNames)
		df2.Release()
	}
}

func BenchmarkDropRowsByIndices(b *testing.B) {
	vals := []int{10, 100, 1000}

	for _, val := range vals {
		b.Run(fmt.Sprintf("size=%v", val), func(b *testing.B) {
			benchmarkDropRowsByIndices(b, val)
		})
	}
}

func benchmarkDropRowsByIndices(b *testing.B, cols int) {
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(b, 0)
	rows := cols

	columns := make([]series.Series, cols)
	for i := 0; i < cols; i++ {
		vals := make([]float64, rows)
		for j := 0; j < rows; j++ {
			vals[j] = rand.Float64()
		}
		field := arrow.Field{Name: strconv.Itoa(i), Type: arrow.PrimitiveTypes.Float64}
		ss := series.FromFloat64(pool, field, vals, nil)
		defer ss.Release()

		columns[i] = ss
	}

	indices := rand.Perm(cols)
	indices = indices[:cols/2]

	df := dataframe.NewFromSeries(pool, columns)
	defer df.Release()

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		df2 := df.DropRowsByIndices(indices)
		df2.Release()
	}
}

func BenchmarkDot(b *testing.B) {
	vals := []int{10, 100, 1000}

	for _, val := range vals {
		b.Run(fmt.Sprintf("size=%v", val), func(b *testing.B) {
			benchmarkDot(b, val)
		})
	}
	for _, val := range vals {
		b.Run(fmt.Sprintf("series/size=%v", val), func(b *testing.B) {
			benchmarkSeriesDot(b, val)
		})
	}
}

func benchmarkDot(b *testing.B, cols int) {
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(b, 0)
	rows := 2

	columns := make([]series.Series, cols)
	for i := 0; i < cols; i++ {
		vals := make([]float64, rows)
		for j := 0; j < rows; j++ {
			vals[j] = rand.Float64()
		}
		field := arrow.Field{Name: strconv.Itoa(i), Type: arrow.PrimitiveTypes.Float64}
		ss := series.FromFloat64(pool, field, vals, nil)
		defer ss.Release()

		columns[i] = ss
	}

	df := dataframe.NewFromSeries(pool, columns)
	defer df.Release()

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		df.Dot(0, 1)
	}
}

func benchmarkSeriesDot(b *testing.B, cols int) {
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(b, 0)
	rows := 2

	columns := make([]series.Series, cols)
	for i := 0; i < cols; i++ {
		vals := make([]float64, rows)
		for j := 0; j < rows; j++ {
			vals[j] = rand.Float64()
		}
		field := arrow.Field{Name: strconv.Itoa(i), Type: arrow.PrimitiveTypes.Float64}
		ss := series.FromFloat64(pool, field, vals, nil)
		defer ss.Release()

		columns[i] = ss
	}

	df := dataframe.NewFromSeries(pool, columns)
	defer df.Release()

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		s := df.Row(0)
		s2 := df.Row(1)
		s.Dot(s2)
		s.Release()
		s2.Release()
	}
}
