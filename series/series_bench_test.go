package series_test

import (
	"fmt"
	"math/rand"
	"testing"

	"github.com/apache/arrow/go/arrow"
	"github.com/apache/arrow/go/arrow/memory"
	gotaseries "github.com/go-gota/gota/series"
	"github.com/poopoothegorilla/fastframe/series"
	"github.com/ptiger10/tada"
)

func BenchmarkAppend(b *testing.B) {
	vals := []int{10, 100, 1000}
	dataTypes := []arrow.DataType{
		arrow.PrimitiveTypes.Int32,
		arrow.PrimitiveTypes.Int64,
		arrow.PrimitiveTypes.Float32,
		arrow.PrimitiveTypes.Float64,
	}

	for _, dataType := range dataTypes {
		for _, val := range vals {
			b.Run(fmt.Sprintf("%s=%v", dataType, val), func(b *testing.B) {
				benchmarkAppend(b, val, dataType)
			})
		}
		for _, val := range vals {
			b.Run(fmt.Sprintf("gota/%s=%v", dataType, val), func(b *testing.B) {
				benchmarkGotaAppend(b, val, dataType)
			})
		}
		for _, val := range vals {
			b.Run(fmt.Sprintf("tada/%s=%v", dataType, val), func(b *testing.B) {
				benchmarkTadaAppend(b, val, dataType)
			})
		}
	}
}

func benchmarkAppend(b *testing.B, numVals int, t arrow.DataType) {
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(b, 0)

	s := newTestSeries(numVals, t, pool, numVals/2)
	defer s.Release()
	s2 := newTestSeries(numVals, t, pool, numVals/2)
	defer s2.Release()

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		s3 := s.Append(s2)
		s3.Release()
	}
}

func benchmarkGotaAppend(b *testing.B, numVals int, tt arrow.DataType) {
	var vals2 interface{}

	switch tt {
	case arrow.PrimitiveTypes.Int32:
		v2 := make([]int32, numVals)
		for j := 0; j < numVals; j++ {
			v2[j] = rand.Int31()
		}
		vals2 = v2
	case arrow.PrimitiveTypes.Int64:
		v2 := make([]int64, numVals)
		for j := 0; j < numVals; j++ {
			v2[j] = rand.Int63()
		}
		vals2 = v2
	case arrow.PrimitiveTypes.Float32:
		v2 := make([]float32, numVals)
		for j := 0; j < numVals; j++ {
			v2[j] = rand.Float32()
		}
		vals2 = v2
	case arrow.PrimitiveTypes.Float64:
		v2 := make([]float64, numVals)
		for j := 0; j < numVals; j++ {
			v2[j] = rand.Float64()
		}
		vals2 = v2
	}
	s := newGotaTestSeries(numVals, tt)

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		s.Append(vals2)
	}
}

func benchmarkTadaAppend(b *testing.B, numVals int, tt arrow.DataType) {
	s := newGotaTestSeries(numVals, tt)
	s2 := newGotaTestSeries(numVals, tt)

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		s.Append(s2)
	}
}

func BenchmarkUnique(b *testing.B) {
	vals := []int{10, 100, 1000}
	dataTypes := []arrow.DataType{
		arrow.PrimitiveTypes.Int32,
		arrow.PrimitiveTypes.Int64,
		arrow.PrimitiveTypes.Float32,
		arrow.PrimitiveTypes.Float64,
	}

	for _, dataType := range dataTypes {
		for _, val := range vals {
			b.Run(fmt.Sprintf("%s=%v", dataType, val), func(b *testing.B) {
				benchmarkUnique(b, val, dataType)
			})
		}
	}
}

func benchmarkUnique(b *testing.B, numVals int, t arrow.DataType) {
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(b, 0)

	s := newTestSeries(numVals, t, pool, numVals/2)
	defer s.Release()

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		s2 := s.Unique()
		s2.Release()
	}
}

func BenchmarkNAIndices(b *testing.B) {
	vals := []int{10, 100, 1000}
	dataTypes := []arrow.DataType{
		arrow.PrimitiveTypes.Int32,
		arrow.PrimitiveTypes.Int64,
		arrow.PrimitiveTypes.Float32,
		arrow.PrimitiveTypes.Float64,
	}

	for _, dataType := range dataTypes {
		for _, val := range vals {
			b.Run(fmt.Sprintf("%s=%v", dataType, val), func(b *testing.B) {
				benchmarkNAIndices(b, val, dataType)
			})
		}
	}
}

func benchmarkNAIndices(b *testing.B, numVals int, t arrow.DataType) {
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(b, 0)

	s := newTestSeries(numVals, t, pool, numVals/2)
	defer s.Release()
	indices := rand.Perm(numVals)
	indices = indices[:numVals/2]

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		s.NAIndices()
	}
}

func BenchmarkDropIndices(b *testing.B) {
	vals := []int{10, 100, 1000}
	dataTypes := []arrow.DataType{
		arrow.PrimitiveTypes.Int32,
		arrow.PrimitiveTypes.Int64,
		arrow.PrimitiveTypes.Float32,
		arrow.PrimitiveTypes.Float64,
	}

	for _, dataType := range dataTypes {
		for _, val := range vals {
			b.Run(fmt.Sprintf("%s=%v", dataType, val), func(b *testing.B) {
				benchmarkDropIndices(b, val, dataType)
			})
		}
	}
}

func benchmarkDropIndices(b *testing.B, numVals int, t arrow.DataType) {
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(b, 0)

	s := newTestSeries(numVals, t, pool, numVals/2)
	defer s.Release()
	indices := rand.Perm(numVals)
	indices = indices[:numVals/2]

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		s2 := s.DropIndices(indices)
		s2.Release()
	}
}

func BenchmarkSelectIndices(b *testing.B) {
	vals := []int{10, 100, 1000}
	dataTypes := []arrow.DataType{
		arrow.PrimitiveTypes.Int32,
		arrow.PrimitiveTypes.Int64,
		arrow.PrimitiveTypes.Float32,
		arrow.PrimitiveTypes.Float64,
	}

	for _, dataType := range dataTypes {
		for _, val := range vals {
			b.Run(fmt.Sprintf("%s=%v", dataType, val), func(b *testing.B) {
				benchmarkSelectIndices(b, val, dataType)
			})
		}
	}
}

func benchmarkSelectIndices(b *testing.B, numVals int, t arrow.DataType) {
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(b, 0)

	s := newTestSeries(numVals, t, pool, numVals/2)
	defer s.Release()
	indices := rand.Perm(numVals)
	indices = indices[:numVals/2]

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		s2 := s.SelectIndices(indices)
		s2.Release()
	}
}

func BenchmarkSum(b *testing.B) {
	vals := []int{10, 100, 1000}
	dataTypes := []arrow.DataType{
		arrow.PrimitiveTypes.Int32,
		arrow.PrimitiveTypes.Int64,
		arrow.PrimitiveTypes.Float32,
		arrow.PrimitiveTypes.Float64,
	}

	for _, dataType := range dataTypes {
		for _, val := range vals {
			b.Run(fmt.Sprintf("%s=%v", dataType, val), func(b *testing.B) {
				benchmarkSum(b, val, dataType)
			})
		}
	}
}

func benchmarkSum(b *testing.B, numVals int, t arrow.DataType) {
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(b, 0)

	s := newTestSeries(numVals, t, pool, numVals/2)
	defer s.Release()

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		s.Sum()
	}
}

func BenchmarkMagnitude(b *testing.B) {
	vals := []int{10, 100, 1000}
	dataTypes := []arrow.DataType{
		arrow.PrimitiveTypes.Int32,
		arrow.PrimitiveTypes.Int64,
		arrow.PrimitiveTypes.Float32,
		arrow.PrimitiveTypes.Float64,
	}

	for _, dataType := range dataTypes {
		for _, val := range vals {
			b.Run(fmt.Sprintf("%s=%v", dataType, val), func(b *testing.B) {
				benchmarkMagnitude(b, val, arrow.PrimitiveTypes.Int32)
			})
		}
	}
}

func benchmarkMagnitude(b *testing.B, numVals int, t arrow.DataType) {
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(b, 0)

	s := newTestSeries(numVals, t, pool, numVals/2)
	defer s.Release()

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		s.Magnitude()
	}
}

func BenchmarkSTD(b *testing.B) {
	vals := []int{10, 100, 1000}
	dataTypes := []arrow.DataType{
		arrow.PrimitiveTypes.Int32,
		arrow.PrimitiveTypes.Int64,
		arrow.PrimitiveTypes.Float32,
		arrow.PrimitiveTypes.Float64,
	}

	for _, dataType := range dataTypes {
		for _, val := range vals {
			b.Run(fmt.Sprintf("%s=%v", dataType, val), func(b *testing.B) {
				benchmarkSTD(b, val, dataType)
			})
		}
		for _, val := range vals {
			b.Run(fmt.Sprintf("gota/%s=%v", dataType, val), func(b *testing.B) {
				benchmarkGotaStdDev(b, val, dataType)
			})
		}
		for _, val := range vals {
			b.Run(fmt.Sprintf("tada/%s=%v", dataType, val), func(b *testing.B) {
				benchmarkTadaStdDev(b, val, dataType)
			})
		}
	}
}

func benchmarkSTD(b *testing.B, numVals int, t arrow.DataType) {
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(b, 0)

	s := newTestSeries(numVals, t, pool, numVals/2)
	defer s.Release()

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		s.STD()
	}
}
func benchmarkGotaStdDev(b *testing.B, numVals int, tt arrow.DataType) {
	s := newGotaTestSeries(numVals, tt)
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		s.StdDev()
	}
}
func benchmarkTadaStdDev(b *testing.B, numVals int, tt arrow.DataType) {
	s := newTadaTestSeries(numVals, tt)
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		s.StdDev()
	}
}

func BenchmarkSortValues(b *testing.B) {
	vals := []int{10, 100, 1000}
	dataTypes := []arrow.DataType{
		arrow.PrimitiveTypes.Int32,
		arrow.PrimitiveTypes.Int64,
		arrow.PrimitiveTypes.Float32,
		arrow.PrimitiveTypes.Float64,
	}

	for _, dataType := range dataTypes {
		for _, val := range vals {
			b.Run(fmt.Sprintf("%s=%v", dataType, val), func(b *testing.B) {
				benchmarkSortValues(b, val, dataType)
			})
		}
	}
}

func benchmarkSortValues(b *testing.B, numVals int, t arrow.DataType) {
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(b, 0)

	s := newTestSeries(numVals, t, pool, numVals/2)
	defer s.Release()

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		s2 := s.SortValues()
		s2.Release()
	}
}

func BenchmarkDot(b *testing.B) {
	vals := []int{10, 100, 1000}
	dataTypes := []arrow.DataType{
		arrow.PrimitiveTypes.Int32,
		arrow.PrimitiveTypes.Int64,
		arrow.PrimitiveTypes.Float32,
		arrow.PrimitiveTypes.Float64,
	}

	for _, dataType := range dataTypes {
		for _, val := range vals {
			b.Run(fmt.Sprintf("%s=%v", dataType, val), func(b *testing.B) {
				benchmarkDot(b, val, dataType)
			})
		}
	}
}

func benchmarkDot(b *testing.B, numVals int, t arrow.DataType) {
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(b, 0)

	s := newTestSeries(numVals, t, pool, numVals/2)
	defer s.Release()
	s2 := newTestSeries(numVals, t, pool, numVals/2)
	defer s2.Release()

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		s.Dot(s2)
	}
}

func BenchmarkAdd(b *testing.B) {
	vals := []int{10, 100, 1000}
	dataTypes := []arrow.DataType{
		arrow.PrimitiveTypes.Int32,
		arrow.PrimitiveTypes.Int64,
		arrow.PrimitiveTypes.Float32,
		arrow.PrimitiveTypes.Float64,
	}

	for _, dataType := range dataTypes {
		for _, val := range vals {
			b.Run(fmt.Sprintf("%s=%v", dataType, val), func(b *testing.B) {
				benchmarkAdd(b, val, dataType)
			})
		}
		for _, val := range vals {
			b.Run(fmt.Sprintf("tada/%s=%v", dataType, val), func(b *testing.B) {
				benchmarkTadaAdd(b, val, dataType)
			})
		}
	}
}

func benchmarkAdd(b *testing.B, numVals int, t arrow.DataType) {
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(b, 0)

	s := newTestSeries(numVals, t, pool, numVals/2)
	defer s.Release()
	s2 := newTestSeries(numVals, t, pool, numVals/2)
	defer s2.Release()

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		s3 := s.Add(s2)
		s3.Release()
	}
}

func benchmarkTadaAdd(b *testing.B, numVals int, t arrow.DataType) {
	s := newTadaTestSeries(numVals, t)
	s2 := newTadaTestSeries(numVals, t)

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		s.Add(s2, true)
	}
}

func BenchmarkMin(b *testing.B) {
	vals := []int{10, 100, 1000}
	dataTypes := []arrow.DataType{
		arrow.PrimitiveTypes.Int32,
		arrow.PrimitiveTypes.Int64,
		arrow.PrimitiveTypes.Float32,
		arrow.PrimitiveTypes.Float64,
	}

	for _, dataType := range dataTypes {
		for _, val := range vals {
			b.Run(fmt.Sprintf("%s=%v", dataType, val), func(b *testing.B) {
				benchmarkMin(b, val, dataType)
			})
		}
		for _, val := range vals {
			b.Run(fmt.Sprintf("gota/%s=%v", dataType, val), func(b *testing.B) {
				benchmarkGotaMin(b, val, dataType)
			})
		}
		for _, val := range vals {
			b.Run(fmt.Sprintf("tada/%s=%v", dataType, val), func(b *testing.B) {
				benchmarkTadaMin(b, val, dataType)
			})
		}
	}
}

func benchmarkMin(b *testing.B, numVals int, t arrow.DataType) {
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(b, 0)

	s := newTestSeries(numVals, t, pool, numVals/2)
	defer s.Release()

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		s.Min()
	}
}
func benchmarkGotaMin(b *testing.B, numVals int, tt arrow.DataType) {
	s := newGotaTestSeries(numVals, tt)
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		s.Min()
	}
}
func benchmarkTadaMin(b *testing.B, numVals int, tt arrow.DataType) {
	s := newTadaTestSeries(numVals, tt)
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		s.Min()
	}
}

func BenchmarkMax(b *testing.B) {
	vals := []int{10, 100, 1000}
	dataTypes := []arrow.DataType{
		arrow.PrimitiveTypes.Int32,
		arrow.PrimitiveTypes.Int64,
		arrow.PrimitiveTypes.Float32,
		arrow.PrimitiveTypes.Float64,
	}

	for _, dataType := range dataTypes {
		for _, val := range vals {
			b.Run(fmt.Sprintf("%s=%v", dataType, val), func(b *testing.B) {
				benchmarkMax(b, val, dataType)
			})
		}
		for _, val := range vals {
			b.Run(fmt.Sprintf("gota/%s=%v", dataType, val), func(b *testing.B) {
				benchmarkGotaMax(b, val, dataType)
			})
		}
		for _, val := range vals {
			b.Run(fmt.Sprintf("tada/%s=%v", dataType, val), func(b *testing.B) {
				benchmarkTadaMax(b, val, dataType)
			})
		}
	}
}

func benchmarkMax(b *testing.B, numVals int, t arrow.DataType) {
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(b, 0)

	s := newTestSeries(numVals, t, pool, numVals/2)
	defer s.Release()

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		s.Max()
	}
}
func benchmarkGotaMax(b *testing.B, numVals int, tt arrow.DataType) {
	s := newGotaTestSeries(numVals, tt)
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		s.Max()
	}
}
func benchmarkTadaMax(b *testing.B, numVals int, tt arrow.DataType) {
	s := newTadaTestSeries(numVals, tt)
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		s.Max()
	}
}

func BenchmarkMean(b *testing.B) {
	vals := []int{10, 100, 1000}
	dataTypes := []arrow.DataType{
		arrow.PrimitiveTypes.Int32,
		arrow.PrimitiveTypes.Int64,
		arrow.PrimitiveTypes.Float32,
		arrow.PrimitiveTypes.Float64,
	}

	for _, dataType := range dataTypes {
		for _, val := range vals {
			b.Run(fmt.Sprintf("%s=%v", dataType, val), func(b *testing.B) {
				benchmarkMean(b, val, dataType)
			})
		}
		for _, val := range vals {
			b.Run(fmt.Sprintf("gota/%s=%v", dataType, val), func(b *testing.B) {
				benchmarkGotaMean(b, val, dataType)
			})
		}
		for _, val := range vals {
			b.Run(fmt.Sprintf("tada/%s=%v", dataType, val), func(b *testing.B) {
				benchmarkTadaMean(b, val, dataType)
			})
		}
	}
}

func benchmarkMean(b *testing.B, numVals int, t arrow.DataType) {
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(b, 0)

	s := newTestSeries(numVals, t, pool, numVals/2)
	defer s.Release()

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		s.Mean()
	}
}
func benchmarkGotaMean(b *testing.B, numVals int, tt arrow.DataType) {
	s := newGotaTestSeries(numVals, tt)
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		s.Mean()
	}
}
func benchmarkTadaMean(b *testing.B, numVals int, tt arrow.DataType) {
	s := newTadaTestSeries(numVals, tt)
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		s.Mean()
	}
}

func BenchmarkMedian(b *testing.B) {
	vals := []int{10, 100, 1000}
	dataTypes := []arrow.DataType{
		arrow.PrimitiveTypes.Int32,
		arrow.PrimitiveTypes.Int64,
		arrow.PrimitiveTypes.Float32,
		arrow.PrimitiveTypes.Float64,
	}

	for _, dataType := range dataTypes {
		for _, val := range vals {
			b.Run(fmt.Sprintf("%s=%v", dataType, val), func(b *testing.B) {
				benchmarkMedian(b, val, dataType)
			})
		}
		for _, val := range vals {
			b.Run(fmt.Sprintf("gota/%s=%v", dataType, val), func(b *testing.B) {
				benchmarkGotaMedian(b, val, dataType)
			})
		}
		for _, val := range vals {
			b.Run(fmt.Sprintf("tada/%s=%v", dataType, val), func(b *testing.B) {
				benchmarkTadaMedian(b, val, dataType)
			})
		}
	}
}

func benchmarkMedian(b *testing.B, numVals int, t arrow.DataType) {
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(b, 0)

	s := newTestSeries(numVals, t, pool, numVals/2)
	defer s.Release()

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		s.Median()
	}
}
func benchmarkGotaMedian(b *testing.B, numVals int, tt arrow.DataType) {
	s := newGotaTestSeries(numVals, tt)
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		s.Median()
	}
}
func benchmarkTadaMedian(b *testing.B, numVals int, tt arrow.DataType) {
	s := newTadaTestSeries(numVals, tt)
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		s.Median()
	}
}

func BenchmarkSquare(b *testing.B) {
	vals := []int{10, 100, 1000}
	dataTypes := []arrow.DataType{
		arrow.PrimitiveTypes.Int32,
		arrow.PrimitiveTypes.Int64,
		arrow.PrimitiveTypes.Float32,
		arrow.PrimitiveTypes.Float64,
	}

	for _, dataType := range dataTypes {
		for _, val := range vals {
			b.Run(fmt.Sprintf("%s=%v", dataType, val), func(b *testing.B) {
				benchmarkSquare(b, val, dataType)
			})
		}
	}
}

func benchmarkSquare(b *testing.B, numVals int, t arrow.DataType) {
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(b, 0)

	s := newTestSeries(numVals, t, pool, numVals/2)
	defer s.Release()

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		s3 := s.Square()
		s3.Release()
	}
}

func BenchmarkSqrt(b *testing.B) {
	vals := []int{10, 100, 1000}
	dataTypes := []arrow.DataType{
		arrow.PrimitiveTypes.Int32,
		arrow.PrimitiveTypes.Int64,
		arrow.PrimitiveTypes.Float32,
		arrow.PrimitiveTypes.Float64,
	}

	for _, dataType := range dataTypes {
		for _, val := range vals {
			b.Run(fmt.Sprintf("%s=%v", dataType, val), func(b *testing.B) {
				benchmarkSqrt(b, val, dataType)
			})
		}
	}
}

func benchmarkSqrt(b *testing.B, numVals int, t arrow.DataType) {
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(b, 0)

	s := newTestSeries(numVals, t, pool, numVals/2)
	defer s.Release()

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		s3 := s.Sqrt()
		s3.Release()
	}
}
func BenchmarkSub(b *testing.B) {
	vals := []int{10, 100, 1000}
	dataTypes := []arrow.DataType{
		arrow.PrimitiveTypes.Int32,
		arrow.PrimitiveTypes.Int64,
		arrow.PrimitiveTypes.Float32,
		arrow.PrimitiveTypes.Float64,
	}

	for _, dataType := range dataTypes {
		for _, val := range vals {
			b.Run(fmt.Sprintf("%s=%v", dataType, val), func(b *testing.B) {
				benchmarkSub(b, val, dataType)
			})
		}
	}
}

func benchmarkSub(b *testing.B, numVals int, t arrow.DataType) {
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(b, 0)

	s := newTestSeries(numVals, t, pool, numVals/2)
	defer s.Release()
	s2 := newTestSeries(numVals, t, pool, numVals/2)
	defer s2.Release()

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		s3 := s.Subtract(s2)
		s3.Release()
	}
}

////////////
// HELPERS
////////////

func newTestSeries(numVals int, t arrow.DataType, pool memory.Allocator, numNAs int) series.Series {
	field := arrow.Field{Name: "testing", Type: t}
	var s series.Series

	vNAs := make([]bool, numVals)
	for j := 0; j < len(vNAs); j++ {
		if j < numNAs {
			vNAs[j] = false
			continue
		}
		vNAs[j] = true
	}
	rand.Shuffle(len(vNAs), func(i, j int) {
		vNAs[i], vNAs[j] = vNAs[j], vNAs[i]
	})

	switch t {
	case arrow.PrimitiveTypes.Int32:
		v := make([]int32, numVals)
		for j := 0; j < numVals; j++ {
			v[j] = rand.Int31()
		}
		s = series.FromInt32(pool, field, v, vNAs)
	case arrow.PrimitiveTypes.Int64:
		v := make([]int64, numVals)
		for j := 0; j < numVals; j++ {
			v[j] = rand.Int63()
		}
		s = series.FromInt64(pool, field, v, vNAs)
	case arrow.PrimitiveTypes.Float32:
		v := make([]float32, numVals)
		for j := 0; j < numVals; j++ {
			v[j] = rand.Float32()
		}
		s = series.FromFloat32(pool, field, v, vNAs)
	case arrow.PrimitiveTypes.Float64:
		v := make([]float64, numVals)
		for j := 0; j < numVals; j++ {
			v[j] = rand.Float64()
		}
		s = series.FromFloat64(pool, field, v, vNAs)
	default:
		panic("unknown type")
	}

	return s
}

func newTadaTestSeries(numVals int, t arrow.DataType) *tada.Series {
	var vals interface{}

	switch t {
	case arrow.PrimitiveTypes.Int32:
		v := make([]int32, numVals)
		for j := 0; j < numVals; j++ {
			v[j] = rand.Int31()
		}
		vals = v
	case arrow.PrimitiveTypes.Int64:
		v := make([]int64, numVals)
		for j := 0; j < numVals; j++ {
			v[j] = rand.Int63()
		}
		vals = v
	case arrow.PrimitiveTypes.Float32:
		v := make([]float32, numVals)
		for j := 0; j < numVals; j++ {
			v[j] = rand.Float32()
		}
		vals = v
	case arrow.PrimitiveTypes.Float64:
		v := make([]float64, numVals)
		for j := 0; j < numVals; j++ {
			v[j] = rand.Float64()
		}
		vals = v
	}
	return tada.NewSeries(vals)
}

func newGotaTestSeries(numVals int, tt arrow.DataType) gotaseries.Series {
	var vals interface{}
	var t gotaseries.Type

	switch tt {
	case arrow.PrimitiveTypes.Int32:
		v := make([]int32, numVals)
		for j := 0; j < numVals; j++ {
			v[j] = rand.Int31()
		}
		vals = v

		t = gotaseries.Int
	case arrow.PrimitiveTypes.Int64:
		v := make([]int64, numVals)
		for j := 0; j < numVals; j++ {
			v[j] = rand.Int63()
		}
		vals = v

		t = gotaseries.Int
	case arrow.PrimitiveTypes.Float32:
		v := make([]float32, numVals)
		for j := 0; j < numVals; j++ {
			v[j] = rand.Float32()
		}
		vals = v

		t = gotaseries.Float
	case arrow.PrimitiveTypes.Float64:
		v := make([]float64, numVals)
		for j := 0; j < numVals; j++ {
			v[j] = rand.Float64()
		}
		vals = v

		t = gotaseries.Float
	}
	return gotaseries.New(vals, t, "testing")
}
