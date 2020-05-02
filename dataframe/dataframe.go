package dataframe

import (
	"sort"
	"strconv"

	"github.com/apache/arrow/go/arrow"
	"github.com/apache/arrow/go/arrow/array"
	"github.com/apache/arrow/go/arrow/memory"
	"github.com/poopoothegorilla/fastframe/series"
	"gonum.org/v1/gonum/mat"
)

// DataFrame ...
type DataFrame struct {
	pool   memory.Allocator
	series []series.Series
}

// NewFromRecords ...
func NewFromRecords(pool memory.Allocator, records []array.Record) DataFrame {
	for _, record := range records {
		record.Retain()
		defer record.Release()
	}

	if len(records) <= 0 {
		panic("dataframe: new_from_records: no records")
	}

	schema := records[0].Schema()
	ss := make([]series.Series, len(schema.Fields()))

	for i, field := range schema.Fields() {
		switch field.Type {
		case arrow.PrimitiveTypes.Int32:
			var vals []int32
			for _, record := range records {
				if !schema.Equal(record.Schema()) {
					panic("dataframe: new_from_records: record schemas do not match")
				}
				var newVals []int32
				switch c := record.Column(i).(type) {
				case *array.Int32:
					newVals = c.Int32Values()
				case series.Series:
					newVals = c.Interface.(*array.Int32).Int32Values()
				}
				vals = append(vals, newVals...)
				s := series.FromInt32(pool, field, vals, nil)
				ss[i] = s
			}
		case arrow.PrimitiveTypes.Int64:
			var vals []int64
			for _, record := range records {
				if !schema.Equal(record.Schema()) {
					panic("dataframe: new_from_records: record schemas do not match")
				}
				var newVals []int64
				switch c := record.Column(i).(type) {
				case *array.Int64:
					newVals = c.Int64Values()
				case series.Series:
					newVals = c.Interface.(*array.Int64).Int64Values()
				}
				vals = append(vals, newVals...)
				s := series.FromInt64(pool, field, vals, nil)
				ss[i] = s
			}
		case arrow.PrimitiveTypes.Float32:
			var vals []float32
			for _, record := range records {
				if !schema.Equal(record.Schema()) {
					panic("dataframe: new_from_records: record schemas do not match")
				}
				var newVals []float32
				switch c := record.Column(i).(type) {
				case *array.Float32:
					newVals = c.Float32Values()
				case series.Series:
					newVals = c.Interface.(*array.Float32).Float32Values()
				}
				vals = append(vals, newVals...)
				s := series.FromFloat32(pool, field, vals, nil)
				ss[i] = s
			}
		case arrow.PrimitiveTypes.Float64:
			var vals []float64
			for _, record := range records {
				if !schema.Equal(record.Schema()) {
					panic("dataframe: new_from_records: record schemas do not match")
				}
				var newVals []float64
				switch c := record.Column(i).(type) {
				case *array.Float64:
					newVals = c.Float64Values()
				case series.Series:
					newVals = c.Interface.(*array.Float64).Float64Values()
				}
				vals = append(vals, newVals...)
				s := series.FromFloat64(pool, field, vals, nil)
				ss[i] = s
			}
		default:
			panic("dataframe: new_from_records: unsupported type")
		}
	}

	return DataFrame{
		pool:   pool,
		series: ss,
	}
}

// NewFromSeries ...
func NewFromSeries(pool memory.Allocator, series []series.Series) DataFrame {
	for _, s := range series {
		s.Retain()
	}

	return DataFrame{
		pool:   pool,
		series: series,
	}
}

//////////////
// NOTE: for gonum Matrix interface
//////////////

// At ...
func (df DataFrame) At(i, j int) float64 {
	df.Retain()
	defer df.Release()

	return df.series[j].At(i, 0)
}

// T ...
func (df DataFrame) T() mat.Matrix {
	df.Retain()
	defer df.Release()

	numRows, _ := df.Dims()
	rows := make([]series.Series, numRows)
	for i := 0; i < numRows; i++ {
		rows[i] = df.Row(i)
	}

	return NewFromSeries(df.pool, rows)
}

// Dims ...
func (df DataFrame) Dims() (r, c int) {
	df.Retain()
	defer df.Release()

	if len(df.series) == 0 {
		return 0, 0
	}

	return df.series[0].Len(), len(df.series)
}

//////////////
// NOTE: for arrow Table interface
//////////////

// Schema ...
func (df DataFrame) Schema() *arrow.Schema {
	fields := make([]arrow.Field, len(df.series))
	for i, col := range df.series {
		fields[i] = col.Field()
	}

	return arrow.NewSchema(fields, nil)
}

// NumRows ...
func (df DataFrame) NumRows() int64 {
	r, _ := df.Dims()
	return int64(r)
}

// NumCols ...
func (df DataFrame) NumCols() int64 {
	_, c := df.Dims()
	return int64(c)
}

// Column ...
func (df DataFrame) Column(i int) *array.Column {
	return df.series[i].Column()
}

// Release ...
func (df DataFrame) Release() {
	for _, c := range df.series {
		c.Release()
	}
}

// Retain ...
func (df DataFrame) Retain() {
	for _, c := range df.series {
		c.Retain()
	}
}

//////////////
// NOTE: regular API
//////////////

// Series ...
func (df DataFrame) Series(i int) series.Series {
	df.Retain()
	defer df.Release()

	return df.series[i]
}

// HasSeries ...
func (df DataFrame) HasSeries(name string) bool {
	df.Retain()
	defer df.Release()

	for _, series := range df.series {
		if series.Name() == name {
			return true
		}
	}

	return false
}

// SeriesByName ...
func (df DataFrame) SeriesByName(name string) series.Series {
	df.Retain()
	defer df.Release()

	for _, series := range df.series {
		if series.Name() == name {
			return series
		}
	}

	panic("dataframe: series_by_name: no series contain name")
	return series.Series{}
}

// ApplyToSeries ...
func (df DataFrame) ApplyToSeries(fn func(series.Series) series.Series) DataFrame {
	df.Retain()
	defer df.Release()

	ss := make([]series.Series, len(df.series))
	for i, s := range df.series {
		ss[i] = fn(s)
	}

	return NewFromSeries(df.pool, ss)
}

// ApplyToRecords ...
func (df DataFrame) ApplyToRecords(fn func(array.Record) array.Record) DataFrame {
	df.Retain()
	defer df.Release()

	records := make([]array.Record, int(df.NumRows()))

	rdr := array.NewTableReader(df, -1)
	defer rdr.Release()

	var i int
	for rdr.Next() {
		record := rdr.Record()
		records[i] = fn(record)
		i++
	}

	return NewFromRecords(df.pool, records)
}

// Head ...
func (df DataFrame) Head(n int) DataFrame {
	df.Retain()
	defer df.Release()

	ss := make([]series.Series, len(df.series))
	for i, s := range df.series {
		ss[i] = s.Head(n)
	}

	return NewFromSeries(df.pool, ss)
}

// Row ...
// NOTE: THIS MIGHT NOT BE A GOODE API OR IDEA
func (df DataFrame) Row(i int) series.Series {
	df.Retain()
	defer df.Release()

	_, cs := df.Dims()
	result := make([]float64, cs)
	for j, col := range df.series {
		result[j] = col.AtVec(i)
	}
	f := arrow.Field{Name: strconv.Itoa(i), Type: arrow.PrimitiveTypes.Float64}

	return series.FromFloat64(df.pool, f, result, nil)
}

// Abs ...
func (df DataFrame) Abs() DataFrame {
	df.Retain()
	defer df.Release()

	newSeries := make([]series.Series, len(df.series))
	for i, col := range df.series {
		newSeries[i] = col.Abs()
	}

	return NewFromSeries(df.pool, newSeries)
}

// Add ...
func (df DataFrame) Add(df2 DataFrame) DataFrame {
	df.Retain()
	defer df.Release()
	df2.Retain()
	defer df2.Release()

	nRows, nCols := df.Dims()
	nRows2, nCols2 := df.Dims()
	if nRows != nRows2 {
		panic("dataframe: add: number of rows not equal")
	}
	if nCols != nCols2 {
		panic("dataframe: add: number of cols not equal")
	}

	var s series.Series
	ss := make([]series.Series, nCols)
	for i, col := range df.series {
		s = col.Add(df2.Series(i))
		defer s.Release()
		ss[i] = s
	}

	return NewFromSeries(df.pool, ss)
}

// AppendRecords ...
// NOTE: CURRENTLY INCLUSIVE ONLY
// TODO: SOME MEMORY ISSUES WITH ARROW
func (df DataFrame) AppendRecords(records []array.Record) DataFrame {
	for _, record := range records {
		record.Retain()
		defer record.Release()
	}
	df.Retain()
	defer df.Release()

	var allRecords []array.Record

	rdr := array.NewTableReader(df, -1)
	defer rdr.Release()

	var record array.Record
	for rdr.Next() {
		record = rdr.Record()
		allRecords = append(allRecords, record)
	}

	allRecords = append(allRecords, records...)

	return NewFromRecords(df.pool, allRecords)
}

// AppendSeries ...
func (df DataFrame) AppendSeries(ss []series.Series) DataFrame {
	df.Retain()
	defer df.Release()
	numRows, _ := df.Dims()

	for _, s := range ss {
		s.Retain()
		defer s.Release()

		if s.Len() != numRows {
			panic("dataframe: append_series: length of series does not match dataframe size")
		}
		if df.HasSeries(s.Name()) {
			panic("dataframe: append_series: series already exists with that header")
		}
	}

	newseries := make([]series.Series, 0, len(df.series)+len(ss))
	newseries = append(newseries, df.series...)
	newseries = append(newseries, ss...)

	return NewFromSeries(df.pool, newseries)
}

// SelectColumnsByNames ...
func (df DataFrame) SelectColumnsByNames(names []string) DataFrame {
	df.Retain()
	defer df.Release()

	var ss []series.Series
	for _, s := range df.series {
		for _, n := range names {
			if n == s.Name() {
				ss = append(ss, s)
			}
		}
	}

	return NewFromSeries(df.pool, ss)
}

// DropColumnsByIndices ...
func (df DataFrame) DropColumnsByIndices(indices []int) DataFrame {
	df.Retain()
	defer df.Release()

	sort.Ints(indices)

	ss := make([]series.Series, len(df.series)-len(indices))
	var rc int
	var ic int
	for i, s := range df.series {
		if ic < len(indices) && i == indices[ic] {
			ic++
			continue
		}

		ss[rc] = s
		rc++
	}

	return NewFromSeries(df.pool, ss)
}

// DropColumnsByNames ...
// TODO: SHOULD THESE BY NAMES BE REPLACED BY A GENERIC METHOD TO DROP?
func (df DataFrame) DropColumnsByNames(names []string) DataFrame {
	df.Retain()
	defer df.Release()

	var ss []series.Series
	for _, s := range df.series {
		var remove bool
		for _, n := range names {
			if n == s.Name() {
				remove = true
				break
			}
		}
		if remove {
			continue
		}

		ss = append(ss, s)
	}

	return NewFromSeries(df.pool, ss)
}

// DropRowsByIndices ...
// TODO: THIS IS AWFUL PERFORMANCE WISE
// TODO: ONLY ALLOW SORTED INDICES?
func (df DataFrame) DropRowsByIndices(indices []int) DataFrame {
	df.Retain()
	defer df.Release()

	// if !sort.IntsAreSorted(indices) {
	// 	sort.Ints(indices)
	// }

	ss := make([]series.Series, len(df.series))
	for i, s := range df.series {
		s2 := s.DropIndices(indices)
		defer s2.Release()
		ss[i] = s2
	}

	return NewFromSeries(df.pool, ss)
}

// SelectRowsByIndices ...
func (df DataFrame) SelectRowsByIndices(indices []int) DataFrame {
	df.Retain()
	defer df.Release()

	if !sort.IntsAreSorted(indices) {
		sort.Ints(indices)
	}

	ss := make([]series.Series, len(df.series))
	for i, s := range df.series {
		s2 := s.SelectIndices(indices)
		defer s2.Release()
		ss[i] = s2
	}

	return NewFromSeries(df.pool, ss)
}

// DropNARowsBySeriesIndices ...
func (df DataFrame) DropNARowsBySeriesIndices(seriesIndices []int) DataFrame {
	df.Retain()
	defer df.Release()

	rowIndices := map[int]struct{}{}
	for _, i := range seriesIndices {
		s := df.series[i]
		for _, j := range s.NAIndices() {
			rowIndices[j] = struct{}{}
		}
	}
	res := make([]int, len(rowIndices))
	var i int
	for k := range rowIndices {
		res[i] = k
		i++
	}
	sort.Ints(res)

	return df.DropRowsByIndices(res)
}

// FillNA ...

// CrossJoin ...
// func CrossJoin(df DataFrame, a string, df2 DataFrame, b string) DataFrame {
//
// }

// LeftJoin ...
// func LeftJoin(dfl DataFrame, nl string, dfr DataFrame, nr string) DataFrame {
// 	vl := dfl.SeriesByName(nl).Values()
// 	dfr.SeriesByName(nr).FindIndices(vl)
// }

// Max ...
func (df DataFrame) Max() float64 {
	df.Retain()
	defer df.Release()

	var max float64
	for i, col := range df.series {
		fval := col.Max()
		if max >= fval && i != 0 {
			continue
		}
		max = fval
	}
	return max
}

// Min ...
func (df DataFrame) Min() float64 {
	df.Retain()
	defer df.Release()

	var min float64
	for i, col := range df.series {
		fval := col.Min()
		if min <= fval && i != 0 {
			continue
		}
		min = fval
	}
	return min
}

// Mean ...
func (df DataFrame) Mean() float64 {
	df.Retain()
	defer df.Release()

	var total float64
	for _, col := range df.series {
		total += col.Mean()
	}
	return total / float64(len(df.series))
}

// Median ...

// Square ...
func (df DataFrame) Square() DataFrame {
	df.Retain()
	defer df.Release()

	ss := make([]series.Series, len(df.series))
	for i, col := range df.series {
		s := col.Square()
		defer s.Release()
		ss[i] = s
	}

	return NewFromSeries(df.pool, ss)
}

// Sqrt ...
func (df DataFrame) Sqrt() DataFrame {
	df.Retain()
	defer df.Release()

	ss := make([]series.Series, len(df.series))
	for i, col := range df.series {
		s := col.Sqrt()
		defer s.Release()
		ss[i] = s
	}

	return NewFromSeries(df.pool, ss)
}

// STD ...
// Sum ...
func (df DataFrame) Sum() float64 {
	df.Retain()
	defer df.Release()

	var sum float64
	for _, col := range df.series {
		sum += col.Sum()
	}
	return sum
}

// Subtract ...
func (df DataFrame) Subtract(df2 DataFrame) DataFrame {
	df.Retain()
	defer df.Release()
	df2.Retain()
	defer df2.Release()

	nRows, nCols := df.Dims()
	nRows2, nCols2 := df.Dims()
	if nRows != nRows2 {
		panic("dataframe: subtract: number of rows not equal")
	}
	if nCols != nCols2 {
		panic("dataframe: subtract: number of cols not equal")
	}

	var s series.Series
	ss := make([]series.Series, nCols)
	for i, col := range df.series {
		s = col.Subtract(df2.Series(i))
		defer s.Release()
		ss[i] = s
	}

	return NewFromSeries(df.pool, ss)
}

// Map ...
// Where ...
// Headers ...
func (df DataFrame) Headers() []string {
	df.Retain()
	defer df.Release()

	result := make([]string, len(df.series))
	for i, s := range df.series {
		result[i] = s.Name()
	}

	return result
}

// SetSeries ...
func (df DataFrame) SetSeries(s series.Series) DataFrame {
	df.Retain()
	defer df.Release()

	ss := df.series
	var replaced bool
	for i, series := range df.series {
		if series.Name() == s.Name() {
			ss[i] = s
			replaced = true
			break
		}
	}
	if !replaced {
		ss = append(ss, s)
	}

	return NewFromSeries(df.pool, ss)
}

// Value ...
func (df DataFrame) Value(rowi, coli int) interface{} {
	df.Retain()
	defer df.Release()

	return df.series[coli].Value(rowi)
}

// Dot ...
//
// A, B = DataFrame.Row(i), DataFrame.Row(j)
// AxBx + AyBy + AzBz ...
func (df DataFrame) Dot(rowi, rowj int) float64 {
	df.Retain()
	defer df.Release()

	var res float64
	for _, c := range df.series {
		res += c.AtVec(rowi) * c.AtVec(rowj)
	}

	return res
}
