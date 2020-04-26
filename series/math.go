package series

import (
	gomath "math"

	"github.com/apache/arrow/go/arrow/array"
	"github.com/apache/arrow/go/arrow/math"
	"gonum.org/v1/gonum/floats"
)

// NOTE: ALL EXPERIMENTAL

// Implemented in Vec32.s
// INT
// func sse2i32Add(a, b [4]int32) [4]int32
// func sse2i64Add(a, b [2]int64) [2]int64
//
// func goi32Add(a, b [4]int32) [4]int32 {
// 	return [4]int32{
// 		a[0] + b[0],
// 		a[1] + b[1],
// 		a[2] + b[2],
// 		a[3] + b[3],
// 	}
// }
//
// // FLOAT
// func sse2f32Add(a, b [4]float32) [4]float32
// func sse2f64Add(a, b [2]float64) [2]float64
//
// // Implemented in Vec32.s
// // FLOAT
// func avxf32Add(a, b [8]float32) [8]float32
// func avxf64Add(a, b [4]float64) [4]float64
//
// func goVec32Add(a, b [4]float32) [4]float32 {
// 	return [4]float32{
// 		a[0] + b[0],
// 		a[1] + b[1],
// 		a[2] + b[2],
// 		a[3] + b[3],
// 	}
// }

//////////////
// Extras
//////////////

// ADD
// TODO: replace with faster functions
func int32Add(a1, a2 *array.Int32) []int32 {
	res := make([]int32, a1.Len())
	vals1 := a1.Int32Values()
	vals2 := a2.Int32Values()
	for i := 0; i < a1.Len(); i++ {
		res[i] = vals1[i] + vals2[i]
	}
	return res
}
func int64Add(a1, a2 *array.Int64) []int64 {
	res := make([]int64, a1.Len())
	vals1 := a1.Int64Values()
	vals2 := a2.Int64Values()
	for i := 0; i < a1.Len(); i++ {
		res[i] = vals1[i] + vals2[i]
	}
	return res
}
func float32Add(a1, a2 *array.Float32) []float32 {
	res := make([]float32, a1.Len())
	vals1 := a1.Float32Values()
	vals2 := a2.Float32Values()
	for i := 0; i < a1.Len(); i++ {
		res[i] = vals1[i] + vals2[i]
	}
	return res
}
func float64Add(a1, a2 *array.Float64) []float64 {
	res := make([]float64, a1.Len())
	vals1 := a1.Float64Values()
	vals2 := a2.Float64Values()
	for i := 0; i < a1.Len(); i++ {
		res[i] = vals1[i] + vals2[i]
	}
	return res
}

// SUM
func int32Sum(a *array.Int32) int32 {
	var sum int32
	for _, val := range a.Int32Values() {
		sum += val
	}
	return sum
}
func int64Sum(a *array.Int64) int64 {
	return math.Int64.Sum(a)
}
func float32Sum(a *array.Float32) float32 {
	var sum float32
	for _, val := range a.Float32Values() {
		sum += val
	}
	return sum
}
func float64Sum(a *array.Float64) float64 {
	// return math.Float64.Sum(a)
	return floats.Sum(a.Float64Values())
}

// func uint64Sum(a *array.Uint64) uint64 {
// 	return math.Uint64.Sum(a)
// }

// MAGNITUDE
func int32Magnitude(a *array.Int32) float64 {
	var sum float64
	for _, val := range a.Int32Values() {
		sum += float64(val) * float64(val)
	}
	return gomath.Sqrt(sum)
}
func int64Magnitude(a *array.Int64) float64 {
	var sum float64
	for _, val := range a.Int64Values() {
		sum += float64(val) * float64(val)
	}
	return gomath.Sqrt(sum)
}
func float32Magnitude(a *array.Float32) float64 {
	var sum float64
	for _, val := range a.Float32Values() {
		sum += float64(val) * float64(val)
	}
	return gomath.Sqrt(sum)
}
func float64Magnitude(a *array.Float64) float64 {
	// return floats.Norm(a.Float64Values(), 2)
	var sum float64
	for _, val := range a.Float64Values() {
		sum += val * val
	}
	return gomath.Sqrt(sum)
}

// UNIQUE
func int32Unique(a *array.Int32) []int32 {
	set := make(map[int32]struct{})
	result := make([]int32, 0, a.Len())
	for _, val := range a.Int32Values() {
		if _, ok := set[val]; !ok {
			set[val] = struct{}{}
			result = append(result, val)
		}
	}
	return result
}
func int64Unique(a *array.Int64) []int64 {
	set := make(map[int64]struct{})
	result := make([]int64, 0, a.Len())
	for _, val := range a.Int64Values() {
		if _, ok := set[val]; !ok {
			set[val] = struct{}{}
			result = append(result, val)
		}
	}
	return result
}
func float32Unique(a *array.Float32) []float32 {
	set := make(map[float32]struct{})
	result := make([]float32, 0, a.Len())
	for _, val := range a.Float32Values() {
		if _, ok := set[val]; !ok {
			set[val] = struct{}{}
			result = append(result, val)
		}
	}
	return result
}
func float64Unique(a *array.Float64) []float64 {
	set := make(map[float64]struct{})
	result := make([]float64, 0, a.Len())
	for _, val := range a.Float64Values() {
		if _, ok := set[val]; !ok {
			set[val] = struct{}{}
			result = append(result, val)
		}
	}
	return result
}

// DROPINDICES
func int32DropIndices(a *array.Int32, indices []int) []int32 {
	result := make([]int32, a.Len()-len(indices))
	var rc int
	var ic int
	vals := a.Int32Values()
	for i, v := range vals {
		if ic < len(indices) && i == indices[ic] {
			ic++
			continue
		}
		result[rc] = v
		rc++
	}
	return result
}
func int64DropIndices(a *array.Int64, indices []int) []int64 {
	result := make([]int64, a.Len()-len(indices))
	var rc int
	var ic int
	vals := a.Int64Values()
	for i, v := range vals {
		if ic < len(indices) && i == indices[ic] {
			ic++
			continue
		}
		result[rc] = v
		rc++
	}
	return result
}
func float32DropIndices(a *array.Float32, indices []int) []float32 {
	result := make([]float32, a.Len()-len(indices))
	var rc int
	var ic int
	vals := a.Float32Values()
	for i, v := range vals {
		if ic < len(indices) && i == indices[ic] {
			ic++
			continue
		}
		result[rc] = v
		rc++
	}
	return result
}
func float64DropIndices(a *array.Float64, indices []int) []float64 {
	result := make([]float64, a.Len()-len(indices))
	var rc int
	var ic int
	vals := a.Float64Values()
	for i, v := range vals {
		if ic < len(indices) && i == indices[ic] {
			ic++
			continue
		}
		result[rc] = v
		rc++
	}
	return result
}

// SELECTINDICES
func int32SelectIndices(a *array.Int32, indices []int) []int32 {
	result := make([]int32, len(indices))
	vals := a.Int32Values()
	for i, j := range indices {
		result[i] = vals[j]
	}
	return result
}
func int64SelectIndices(a *array.Int64, indices []int) []int64 {
	result := make([]int64, len(indices))
	vals := a.Int64Values()
	for i, j := range indices {
		result[i] = vals[j]
	}
	return result
}
func float32SelectIndices(a *array.Float32, indices []int) []float32 {
	result := make([]float32, len(indices))
	vals := a.Float32Values()
	for i, j := range indices {
		result[i] = vals[j]
	}
	return result
}
func float64SelectIndices(a *array.Float64, indices []int) []float64 {
	result := make([]float64, len(indices))
	vals := a.Float64Values()
	for i, j := range indices {
		result[i] = vals[j]
	}
	return result
}

// SUBTRACT
func int32Subtract(a1, a2 *array.Int32) []int32 {
	res := make([]int32, a1.Len())
	vals1 := a1.Int32Values()
	vals2 := a2.Int32Values()
	for i := 0; i < a1.Len(); i++ {
		res[i] = vals1[i] - vals2[i]
	}
	return res
}
func int64Subtract(a1, a2 *array.Int64) []int64 {
	res := make([]int64, a1.Len())
	vals1 := a1.Int64Values()
	vals2 := a2.Int64Values()
	for i := 0; i < a1.Len(); i++ {
		res[i] = vals1[i] - vals2[i]
	}
	return res
}
func float32Subtract(a1, a2 *array.Float32) []float32 {
	res := make([]float32, a1.Len())
	vals1 := a1.Float32Values()
	vals2 := a2.Float32Values()
	for i := 0; i < a1.Len(); i++ {
		res[i] = vals1[i] - vals2[i]
	}
	return res
}

// DOT
func int32Dot(a1, a2 *array.Int32) int32 {
	var res int32
	vals1 := a1.Int32Values()
	vals2 := a2.Int32Values()
	for i := 0; i < a1.Len(); i++ {
		res += vals1[i] * vals2[i]
	}
	return res
}
func int64Dot(a1, a2 *array.Int64) int64 {
	var res int64
	vals1 := a1.Int64Values()
	vals2 := a2.Int64Values()
	for i := 0; i < a1.Len(); i++ {
		res += vals1[i] * vals2[i]
	}
	return res
}
func float32Dot(a1, a2 *array.Float32) float32 {
	var res float32
	vals1 := a1.Float32Values()
	vals2 := a2.Float32Values()
	for i := 0; i < a1.Len(); i++ {
		res += vals1[i] * vals2[i]
	}
	return res
}

// SQUARE
func int32Square(a1 *array.Int32) []int64 {
	res := make([]int64, a1.Len())
	for i, val := range a1.Int32Values() {
		res[i] = int64(val) * int64(val)
	}
	return res
}
func int64Square(a1 *array.Int64) []int64 {
	res := make([]int64, a1.Len())
	for i, val := range a1.Int64Values() {
		res[i] = int64(val) * int64(val)
	}
	return res
}
func float32Square(a1 *array.Float32) []float64 {
	res := make([]float64, a1.Len())
	for i, val := range a1.Float32Values() {
		res[i] = float64(val) * float64(val)
	}
	return res
}
func float64Square(a1 *array.Float64) []float64 {
	res := make([]float64, a1.Len())
	for i, val := range a1.Float64Values() {
		res[i] = float64(val) * float64(val)
	}
	return res
}

// SQRT
func int32Sqrt(a1 *array.Int32) []float64 {
	res := make([]float64, a1.Len())
	for i, val := range a1.Int32Values() {
		res[i] = gomath.Sqrt(float64(val))
	}
	return res
}
func int64Sqrt(a1 *array.Int64) []float64 {
	res := make([]float64, a1.Len())
	for i, val := range a1.Int64Values() {
		res[i] = gomath.Sqrt(float64(val))
	}
	return res
}
func float32Sqrt(a1 *array.Float32) []float64 {
	res := make([]float64, a1.Len())
	for i, val := range a1.Float32Values() {
		res[i] = gomath.Sqrt(float64(val))
	}
	return res
}
func float64Sqrt(a1 *array.Float64) []float64 {
	res := make([]float64, a1.Len())
	for i, val := range a1.Float64Values() {
		res[i] = gomath.Sqrt(float64(val))
	}
	return res
}

// ABS
func int32Abs(a1 *array.Int32) []int32 {
	res := make([]int32, a1.Len())
	for i, val := range a1.Int32Values() {
		res[i] = int32(gomath.Abs(float64(val)))
	}
	return res
}
func int64Abs(a1 *array.Int64) []int64 {
	res := make([]int64, a1.Len())
	for i, val := range a1.Int64Values() {
		res[i] = int64(gomath.Abs(float64(val)))
	}
	return res
}
func float32Abs(a1 *array.Float32) []float32 {
	res := make([]float32, a1.Len())
	for i, val := range a1.Float32Values() {
		res[i] = float32(gomath.Abs(float64(val)))
	}
	return res
}
func float64Abs(a1 *array.Float64) []float64 {
	res := make([]float64, a1.Len())
	for i, val := range a1.Float64Values() {
		res[i] = float64(gomath.Abs(float64(val)))
	}
	return res
}

// MIN
func int32Min(a1 *array.Int32) float64 {
	var min float64
	for i, val := range a1.Int32Values() {
		fval := float64(val)
		if min <= fval && i != 0 {
			continue
		}
		min = fval
	}
	return min
}
func int64Min(a1 *array.Int64) float64 {
	var min float64
	for i, val := range a1.Int64Values() {
		fval := float64(val)
		if min <= fval && i != 0 {
			continue
		}
		min = fval
	}
	return min
}
func float32Min(a1 *array.Float32) float64 {
	var min float64
	for i, val := range a1.Float32Values() {
		fval := float64(val)
		if min <= fval && i != 0 {
			continue
		}
		min = fval
	}
	return min
}
func float64Min(a1 *array.Float64) float64 {
	var min float64
	for i, val := range a1.Float64Values() {
		fval := float64(val)
		if min <= fval && i != 0 {
			continue
		}
		min = fval
	}
	return min
}

// MAX
func int32Max(a1 *array.Int32) float64 {
	var max float64
	for i, val := range a1.Int32Values() {
		fval := float64(val)
		if max >= fval && i != 0 {
			continue
		}
		max = fval
	}
	return max
}
func int64Max(a1 *array.Int64) float64 {
	var max float64
	for i, val := range a1.Int64Values() {
		fval := float64(val)
		if max >= fval && i != 0 {
			continue
		}
		max = fval
	}
	return max
}
func float32Max(a1 *array.Float32) float64 {
	var max float64
	for i, val := range a1.Float32Values() {
		fval := float64(val)
		if max >= fval && i != 0 {
			continue
		}
		max = fval
	}
	return max
}
func float64Max(a1 *array.Float64) float64 {
	var max float64
	for i, val := range a1.Float64Values() {
		fval := float64(val)
		if max >= fval && i != 0 {
			continue
		}
		max = fval
	}
	return max
}

// MEAN
func int32Mean(a1 *array.Int32) float64 {
	var total float64
	for _, val := range a1.Int32Values() {
		total += float64(val)
	}
	return total / float64(a1.Len())
}
func int64Mean(a1 *array.Int64) float64 {
	var total float64
	for _, val := range a1.Int64Values() {
		total += float64(val)
	}
	return total / float64(a1.Len())
}
func float32Mean(a1 *array.Float32) float64 {
	var total float64
	for _, val := range a1.Float32Values() {
		total += float64(val)
	}
	return total / float64(a1.Len())
}
func float64Mean(a1 *array.Float64) float64 {
	var total float64
	for _, val := range a1.Float64Values() {
		total += float64(val)
	}
	return total / float64(a1.Len())
}
