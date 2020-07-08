package dataframe

import (
	"encoding/csv"
	"fmt"
	"io"
	"math"
	"sort"
	"strconv"

	"github.com/apache/arrow/go/arrow"
	"github.com/apache/arrow/go/arrow/array"
	"github.com/apache/arrow/go/arrow/bitutil"
	"github.com/apache/arrow/go/arrow/memory"
	"github.com/poopoothegorilla/fastframe/series"
	"gonum.org/v1/gonum/mat"
)

// DataFrame ...
type DataFrame struct {
	pool   memory.Allocator
	series []series.Series
	schema *arrow.Schema
	reader io.Reader
}

// NewFromRecords creates a DataFrame from Arrow records.
//
// TODO(poopoothegorilla): optimizations are needed here
func NewFromRecords(pool memory.Allocator, records []array.Record) DataFrame {
	if len(records) <= 0 {
		panic("dataframe: new_from_records: no records")
	}

	var numRows int64
	var schema *arrow.Schema
	for _, record := range records {
		record.Retain()
		defer record.Release()
		numRows += record.NumRows()
		if schema == nil {
			schema = records[0].Schema()
		}
		// TODO(poopoothegorilla): should records be validated for similarity?
		// NOTE(poopoothegorilla): schema Equal in arrow pkg uses reflect which has
		// a performance impact
	}

	ss := make([]series.Series, len(schema.Fields()))
	nulls := make([]bool, int(numRows))
	for i, field := range schema.Fields() {
		var rowi int
		switch field.Type {
		case arrow.PrimitiveTypes.Int32:
			vals := make([]int32, int(numRows))
			for _, record := range records {
				var nullMask []byte
				var newVals []int32
				switch c := record.Column(i).(type) {
				case *array.Int32:
					newVals = c.Int32Values()
					nullMask = c.NullBitmapBytes()
				case series.Series:
					newVals = c.Interface.(*array.Int32).Int32Values()
					nullMask = c.NullBitmapBytes()
				}
				for j, newVal := range newVals {
					nulls[rowi] = bitutil.BitIsSet(nullMask, j)
					vals[rowi] = newVal
					rowi++
				}
			}
			ss[i] = series.FromInt32(pool, field, vals, nulls)
		case arrow.PrimitiveTypes.Int64:
			vals := make([]int64, int(numRows))
			for _, record := range records {
				var nullMask []byte
				var newVals []int64
				switch c := record.Column(i).(type) {
				case *array.Int64:
					newVals = c.Int64Values()
					nullMask = c.NullBitmapBytes()
				case series.Series:
					newVals = c.Interface.(*array.Int64).Int64Values()
					nullMask = c.NullBitmapBytes()
				}
				for j, newVal := range newVals {
					nulls[rowi] = bitutil.BitIsSet(nullMask, j)
					vals[rowi] = newVal
					rowi++
				}
			}
			ss[i] = series.FromInt64(pool, field, vals, nulls)
		case arrow.PrimitiveTypes.Float32:
			vals := make([]float32, int(numRows))
			for _, record := range records {
				var nullMask []byte
				var newVals []float32
				switch c := record.Column(i).(type) {
				case *array.Float32:
					newVals = c.Float32Values()
					nullMask = c.NullBitmapBytes()
				case series.Series:
					newVals = c.Interface.(*array.Float32).Float32Values()
					nullMask = c.NullBitmapBytes()
				}
				for j, newVal := range newVals {
					nulls[rowi] = bitutil.BitIsSet(nullMask, j)
					vals[rowi] = newVal
					rowi++
				}
			}
			ss[i] = series.FromFloat32(pool, field, vals, nulls)
		case arrow.PrimitiveTypes.Float64:
			vals := make([]float64, int(numRows))
			for _, record := range records {
				var nullMask []byte
				var newVals []float64
				switch c := record.Column(i).(type) {
				case *array.Float64:
					newVals = c.Float64Values()
					nullMask = c.NullBitmapBytes()
				case series.Series:
					newVals = c.Interface.(*array.Float64).Float64Values()
					nullMask = c.NullBitmapBytes()
				}
				for j, newVal := range newVals {
					nulls[rowi] = bitutil.BitIsSet(nullMask, j)
					vals[rowi] = newVal
					rowi++
				}
			}
			ss[i] = series.FromFloat64(pool, field, vals, nulls)
		default:
			panic("dataframe: new_from_records: unsupported type")
		}
	}

	return DataFrame{
		pool:   pool,
		series: ss,
	}
}

// NewFromSeries creates a DataFrame from Series.
func NewFromSeries(pool memory.Allocator, series []series.Series) DataFrame {
	for _, s := range series {
		s.Retain()
	}

	return DataFrame{
		pool:   pool,
		series: series,
	}
}

// NewFromCSV creates a DataFrame from a CSV reader.
//
// TODO(poopoothegorilla): change the batchSize and type List to Optional
// Option params
func NewFromCSV(pool memory.Allocator, r *csv.Reader, batchSize int, tList map[string]arrow.DataType) DataFrame {
	// TODO(poopoothegorilla): add batching
	rows, err := r.ReadAll()
	if err != nil {
		panic(fmt.Sprintf("dataframe: new_from_csv: %s", err))
	}

	var (
		sb *array.StringBuilder
		// i32b *array.Int32Builder
		// i64b *array.Int64Builder
		// f32b *array.Float32Builder
		// f64b *array.Float64Builder
	)

	// TODO(poopoothegorilla): what about csvs without headers
	headers := rows[0]
	ss := make([]series.Series, len(headers))

	for i, header := range headers {
		field := arrow.Field{Name: header, Nullable: true}

		switch tList[header] {
		// TODO(poopoothegorilla: add more types)
		// case arrow.PrimitiveTypes.Int32:
		// case arrow.PrimitiveTypes.Int64:
		// case arrow.PrimitiveTypes.Float32:
		// case arrow.PrimitiveTypes.Float64:
		case arrow.BinaryTypes.String:
			// String is default
			fallthrough
		default:
			// STRING
			field.Type = arrow.BinaryTypes.String
			if sb == nil {
				sb = array.NewStringBuilder(pool)
				defer sb.Release()
			}
			vals := make([]string, len(rows)-1)
			for j, row := range rows {
				if j == 0 {
					continue
				}
				vals[j-1] = string(row[i])
			}
			sb.AppendValues(vals, nil)
			ss[i] = series.FromArrow(pool, field, sb.NewArray())
		}
	}

	return DataFrame{
		pool:   pool,
		series: ss,
	}
}

//////////////
// NOTE: for gonum Matrix interface
//////////////

// At returns the float64 value at row i and Series j.
func (df DataFrame) At(i, j int) float64 {
	df.Retain()
	defer df.Release()

	return df.series[j].At(i, 0)
}

// T returns a transpose of the DataFrame values as a gonum Matrix.
func (df DataFrame) T() mat.Matrix {
	df.Retain()
	defer df.Release()

	numRows, _ := df.Dims()
	rows := make([]series.Series, numRows)
	for i := 0; i < numRows; i++ {
		rows[i] = df.RowToSeries(i)
	}

	return NewFromSeries(df.pool, rows)
}

// Dims returns dimensions of the DataFrame as rows and columns.
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

func (df DataFrame) newSchema() *arrow.Schema {
	fields := make([]arrow.Field, len(df.series))
	for i, col := range df.series {
		fields[i] = col.Field()
	}

	return arrow.NewSchema(fields, nil)
}

// Schema returns the Arrow schema of the DataFrame.
func (df DataFrame) Schema() *arrow.Schema {
	if df.schema == nil {
		df.schema = df.newSchema()
	}

	return df.schema
}

// NumRows returns the number of rows as an int64.
func (df DataFrame) NumRows() int64 {
	r, _ := df.Dims()
	return int64(r)
}

// NumCols returns the number of columns as an int64.
func (df DataFrame) NumCols() int64 {
	_, c := df.Dims()
	return int64(c)
}

// Column returns the Series at position i as an Arrow column.
func (df DataFrame) Column(i int) *array.Column {
	return df.series[i].Column()
}

// Release releases a referece count from all Series.
func (df DataFrame) Release() {
	for _, c := range df.series {
		c.Release()
	}
}

// Retain adds a reference count to each Series.
func (df DataFrame) Retain() {
	for _, c := range df.series {
		c.Retain()
	}
}

//////////////
// NOTE: regular API
//////////////

// Cast ...
func (df DataFrame) Cast(cList map[string]arrow.DataType) DataFrame {
	df.Retain()
	defer df.Release()

	ss := make([]series.Series, len(df.series))
	var found int
	for i, s := range df.series {
		t, ok := cList[s.Name()]
		if !ok {
			ss[i] = s
			continue
		}
		ss[i] = s.Cast(t)
		found++
	}
	if found != len(cList) {
		panic(fmt.Sprintf("dataframe: cast: not all series exist %+x", cList))
	}

	return NewFromSeries(df.pool, ss)
}

// Series returns the Series in the i position.
func (df DataFrame) Series(i int) series.Series {
	return df.series[i]
}

// HasSeries returns a truthy value if the DataFrame has a Series with a given
// name.
func (df DataFrame) HasSeries(name string) bool {
	df.Retain()
	defer df.Release()

	for _, s := range df.series {
		if s.Name() == name {
			return true
		}
	}

	return false
}

// SeriesByName returns the Series with the given name. If no Series exists with
// that name a panic is triggered.
func (df DataFrame) SeriesByName(name string) series.Series {
	df.Retain()
	defer df.Release()

	for _, s := range df.series {
		if s.Name() == name {
			return s
		}
	}

	panic(fmt.Sprintf("dataframe: series_by_name: no series contain name %q", name))
	return series.Series{}
}

// ApplyToSeries applies a function to each Series in the DataFrame.
func (df DataFrame) ApplyToSeries(fn func(series.Series) series.Series) DataFrame {
	df.Retain()
	defer df.Release()

	ss := make([]series.Series, len(df.series))
	for i, s := range df.series {
		ss[i] = fn(s)
	}

	return NewFromSeries(df.pool, ss)
}

// ApplyToRecords applies a function to each Arrow record in the DataFrame.
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

// Head returns a DataFrame with only n records.
func (df DataFrame) Head(n int) DataFrame {
	df.Retain()
	defer df.Release()

	ss := make([]series.Series, len(df.series))
	for i, s := range df.series {
		ss[i] = s.Head(n)
	}

	return NewFromSeries(df.pool, ss)
}

// Record returns the Arrow record in the i position.
func (df DataFrame) Record(i int) array.Record {
	df.Retain()

	_, cs := df.Dims()
	cols := make([]array.Interface, cs)
	for j, s := range df.series {
		slice := array.NewSlice(s, int64(i), int64(i+1))
		defer slice.Release()
		cols[j] = slice
	}

	df.Release()
	return array.NewRecord(df.Schema(), cols, -1)
}

// EmptyRecord returns a empty Arrow record with the DataFrame's schema.
func (df DataFrame) EmptyRecord(n int) array.Record {
	df.Retain()

	cols := make([]array.Interface, int(df.NumCols()))
	for i, s := range df.series {
		empty := s.Empty(n)
		cols[i] = empty
		defer empty.Release()
	}

	df.Release()
	return array.NewRecord(df.Schema(), cols, -1)
}

// MergeRecords merges multiple Arrow records into a new Arrow record.
func MergeRecords(records ...array.Record) array.Record {
	var numCols int64
	for _, record := range records {
		record.Retain()
		defer record.Release()
		numCols += record.NumCols()
	}

	fields := make([]arrow.Field, 0, int(numCols))
	cols := make([]array.Interface, 0, int(numCols))
	for _, record := range records {
		fields = append(fields, record.Schema().Fields()...)
		cols = append(cols, record.Columns()...)
	}

	schema := arrow.NewSchema(fields, nil)

	return array.NewRecord(schema, cols, -1)
}

// RowToSeries creates a float64 Series from a values in row i from the
// DataFrame.
//
// NOTE: THIS MIGHT NOT BE A GOODE API OR IDEA
func (df DataFrame) RowToSeries(i int) series.Series {
	df.Retain()
	defer df.Release()

	_, cs := df.Dims()
	result := make([]float64, cs)
	for j, s := range df.series {
		result[j] = s.AtVec(i)
	}
	f := arrow.Field{Name: strconv.Itoa(i), Type: arrow.PrimitiveTypes.Float64}

	return series.FromFloat64(df.pool, f, result, nil)
}

// Abs calculates the absolute value on each value in the DataFrame.
func (df DataFrame) Abs() DataFrame {
	df.Retain()
	defer df.Release()

	newSeries := make([]series.Series, len(df.series))
	for i, col := range df.series {
		newSeries[i] = col.Abs()
	}

	return NewFromSeries(df.pool, newSeries)
}

// Add adds two equal length DataFrames and returns the resulting DataFrame.
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

// AppendRecords appends records to the DataFrame.
//
// NOTE: CURRENTLY INCLUSIVE ONLY
// TODO(poopoothegorilla): SOME MEMORY ISSUES WITH ARROW
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

// AppendSeries appends Series to the DataFrame.
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

// SelectColumnsByNames returns a DataFrame with Series which match the given
// names.
//
// TODO(poopoothegorilla): change columns to series
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

// DropColumnsByIndices returns a DataFrame without Series in each indices
// position.
//
// TODO(poopoothegorilla): change columns to series
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

// LeftJoinEM ...
// TODO(poopoothegorilla): use Early materialization and compare vs naive
func LeftJoinEM(leftDF DataFrame, leftName string, rightDF DataFrame, rightName string) DataFrame {
	var resultRecords []array.Record

	newRightDF := rightDF.DropColumnsByNames([]string{rightName})
	defer newRightDF.Release()
	newNColsR := int(newRightDF.NumCols())
	newNColsL := int(leftDF.NumCols())
	fields := make([]arrow.Field, 0, newNColsR+newNColsL)
	for _, s := range leftDF.series {
		fields = append(fields, s.Field())
	}
	for _, s := range newRightDF.series {
		fields = append(fields, s.Field())
	}

	schema := arrow.NewSchema(fields, nil)
	rb := array.NewRecordBuilder(leftDF.pool, schema)
	defer rb.Release()

	leftSeries := leftDF.SeriesByName(leftName)
	rightSeries := rightDF.SeriesByName(rightName)
	leftVals := leftSeries.Values()
	for li := 0; li < leftSeries.Len(); li++ {
		var rightIndices []int
		switch lf := leftVals.(type) {
		case []int32:
			rightIndices = rightSeries.FindIndices(lf[li])
		case []int64:
			rightIndices = rightSeries.FindIndices(lf[li])
		case []float32:
			rightIndices = rightSeries.FindIndices(lf[li])
		case []float64:
			rightIndices = rightSeries.FindIndices(lf[li])
		default:
			panic("dataframe: left_join: unknown type")
		}

		if len(rightIndices) <= 0 {
			emptyRecord := newRightDF.EmptyRecord(1)
			defer emptyRecord.Release()
			leftRecord := leftDF.Record(li)
			defer leftRecord.Release()

			record := MergeRecords(leftRecord, emptyRecord)
			defer record.Release()
			resultRecords = append(resultRecords, record)
			continue
		}

		for _, j := range rightIndices {
			rightRecord := newRightDF.Record(j)
			defer rightRecord.Release()
			leftRecord := leftDF.Record(li)
			defer leftRecord.Release()

			record := MergeRecords(leftRecord, rightRecord)
			defer record.Release()
			resultRecords = append(resultRecords, record)
		}
	}

	return NewFromRecords(leftDF.pool, resultRecords)
}

// LeftJoin ...
func LeftJoin(leftDF DataFrame, leftName string, rightDF DataFrame, rightName string) DataFrame {
	// TODO(poopoothegorilla): add check for series name overlaps
	fields := make([]arrow.Field, len(leftDF.series)+len(rightDF.series)-1)
	fieldIndices := make([]int, 0, len(fields))
	for i, s := range leftDF.series {
		fields[i] = s.Field()
		fieldIndices = append(fieldIndices, i)
	}
	midIndice := len(leftDF.series)
	var j int
	for i, s := range rightDF.series {
		// skip series used in join
		if s.Name() == rightName {
			continue
		}
		fields[midIndice+j] = s.Field()
		fieldIndices = append(fieldIndices, i)
		j++
	}
	schema := arrow.NewSchema(fields, nil)
	rb := array.NewRecordBuilder(leftDF.pool, schema)
	defer rb.Release()

	leftSeries := leftDF.SeriesByName(leftName)
	for li := 0; li < leftSeries.Len(); li++ {
		leftVals := leftSeries.Values()
		var rightIndices []int
		switch lf := leftVals.(type) {
		case []int32:
			rightIndices = rightDF.SeriesByName(rightName).FindIndices(lf[li])
		case []int64:
			rightIndices = rightDF.SeriesByName(rightName).FindIndices(lf[li])
		case []float32:
			rightIndices = rightDF.SeriesByName(rightName).FindIndices(lf[li])
		case []float64:
			rightIndices = rightDF.SeriesByName(rightName).FindIndices(lf[li])
		default:
			panic("dataframe: left_join: unknown type")
		}

		var ri int
		for {
			for fi, b := range rb.Fields() {
				if fi < midIndice {
					switch fb := b.(type) {
					case *array.Int32Builder:
						v := leftDF.Series(fieldIndices[fi]).Int32(li)
						fb.AppendValues([]int32{v}, nil)
					case *array.Int64Builder:
						v := leftDF.Series(fieldIndices[fi]).Int64(li)
						fb.AppendValues([]int64{v}, nil)
					case *array.Float32Builder:
						v := leftDF.Series(fieldIndices[fi]).Float32(li)
						fb.AppendValues([]float32{v}, nil)
					case *array.Float64Builder:
						v := leftDF.Series(fieldIndices[fi]).Float64(li)
						fb.AppendValues([]float64{v}, nil)
					default:
						panic("dataframe: left_join: unknown type")
					}
					continue
				}
				if len(rightIndices) > 0 {
					rightIndice := rightIndices[ri]
					switch fb := b.(type) {
					case *array.Int32Builder:
						v := rightDF.Series(fieldIndices[fi]).Int32(rightIndice)
						fb.AppendValues([]int32{v}, nil)
					case *array.Int64Builder:
						v := rightDF.Series(fieldIndices[fi]).Int64(rightIndice)
						fb.AppendValues([]int64{v}, nil)
					case *array.Float32Builder:
						v := rightDF.Series(fieldIndices[fi]).Float32(rightIndice)
						fb.AppendValues([]float32{v}, nil)
					case *array.Float64Builder:
						v := rightDF.Series(fieldIndices[fi]).Float64(rightIndice)
						fb.AppendValues([]float64{v}, nil)
					default:
						panic("dataframe: left_join: unknown type")
					}
					continue
				}

				b.AppendNull()
			}

			ri++
			if ri >= len(rightIndices) {
				break
			}
		}
	}
	rec := rb.NewRecord()
	defer rec.Release()

	return NewFromRecords(leftDF.pool, []array.Record{rec})
}

// RightJoin ...
func RightJoin(leftDF DataFrame, leftName string, rightDF DataFrame, rightName string) DataFrame {
	// TODO(poopoothegorilla): add check for series name overlaps
	fields := make([]arrow.Field, len(leftDF.series)+len(rightDF.series)-1)
	fieldIndices := make([]int, 0, len(fields))
	for i, s := range rightDF.series {
		fields[i] = s.Field()
		fieldIndices = append(fieldIndices, i)
	}
	midIndice := len(rightDF.series)
	var j int
	for i, s := range rightDF.series {
		// skip series used in join
		if s.Name() == rightName {
			continue
		}
		fields[midIndice+j] = s.Field()
		fieldIndices = append(fieldIndices, i)
		j++
	}
	schema := arrow.NewSchema(fields, nil)
	rb := array.NewRecordBuilder(leftDF.pool, schema)
	defer rb.Release()

	rightSeries := rightDF.SeriesByName(rightName)
	for ri := 0; ri < rightSeries.Len(); ri++ {
		rightVals := rightSeries.Values()
		var leftIndices []int
		switch rf := rightVals.(type) {
		case []int32:
			leftIndices = leftDF.SeriesByName(leftName).FindIndices(rf[ri])
		case []int64:
			leftIndices = leftDF.SeriesByName(leftName).FindIndices(rf[ri])
		case []float32:
			leftIndices = leftDF.SeriesByName(leftName).FindIndices(rf[ri])
		case []float64:
			leftIndices = leftDF.SeriesByName(leftName).FindIndices(rf[ri])
		default:
			panic("dataframe: right_join: unknown type")
		}

		var li int
		for {
			for fi, b := range rb.Fields() {
				if fi < midIndice {
					switch fb := b.(type) {
					case *array.Int32Builder:
						v := rightDF.Series(fieldIndices[fi]).Int32(ri)
						fb.AppendValues([]int32{v}, nil)
					case *array.Int64Builder:
						v := rightDF.Series(fieldIndices[fi]).Int64(ri)
						fb.AppendValues([]int64{v}, nil)
					case *array.Float32Builder:
						v := rightDF.Series(fieldIndices[fi]).Float32(ri)
						fb.AppendValues([]float32{v}, nil)
					case *array.Float64Builder:
						v := rightDF.Series(fieldIndices[fi]).Float64(ri)
						fb.AppendValues([]float64{v}, nil)
					default:
						panic("dataframe: right_join: unknown type")
					}
					continue
				}
				if len(leftIndices) > 0 {
					leftIndice := leftIndices[li]
					switch fb := b.(type) {
					case *array.Int32Builder:
						v := leftDF.Series(fieldIndices[fi]).Int32(leftIndice)
						fb.AppendValues([]int32{v}, nil)
					case *array.Int64Builder:
						v := leftDF.Series(fieldIndices[fi]).Int64(leftIndice)
						fb.AppendValues([]int64{v}, nil)
					case *array.Float32Builder:
						v := leftDF.Series(fieldIndices[fi]).Float32(leftIndice)
						fb.AppendValues([]float32{v}, nil)
					case *array.Float64Builder:
						v := leftDF.Series(fieldIndices[fi]).Float64(leftIndice)
						fb.AppendValues([]float64{v}, nil)
					default:
						panic("dataframe: right_join: unknown type")
					}
					continue
				}

				b.AppendNull()
			}

			li++
			if li >= len(leftIndices) {
				break
			}
		}
	}
	rec := rb.NewRecord()
	defer rec.Release()

	return NewFromRecords(rightDF.pool, []array.Record{rec})
}

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

	var res float64
	for _, c := range df.series {
		res += c.AtVec(rowi) * c.AtVec(rowj)
	}

	df.Release()
	return res
}

// RowNorm ...
//
// SQRT( Ax^2 + Ay^2 + Az^2 )
func (df DataFrame) RowNorm(i int) float64 {
	df.Retain()

	var res float64
	for _, c := range df.series {
		res += math.Pow(c.AtVec(i), 2)
	}

	df.Release()
	return math.Sqrt(res)
}

// Pivot ...
func (df DataFrame) Pivot(idx, cols, vals string) DataFrame {
	df.Retain()
	defer df.Release()

	idxSeries := df.SeriesByName(idx)
	uniqueIdxSeries := idxSeries.Unique()
	colsSeries := df.SeriesByName(cols)
	uniqueColsSeries := colsSeries.Unique()
	defer uniqueColsSeries.Release()
	valsSeries := df.SeriesByName(vals)
	valsSeriesType := valsSeries.DataType()

	colNames := uniqueColsSeries.StringValues()
	ss := make([]series.Series, len(colNames)+1)
	ss[0] = uniqueIdxSeries
	for i, colName := range colNames {
		field := arrow.Field{
			Name:     colName,
			Type:     valsSeriesType,
			Nullable: true,
		}
		// Find Indices where colname exists in cols series
		is := colsSeries.FindIndices(colName)
		idxVals := make([]interface{}, len(is))
		valVals := make([]interface{}, len(is))
		for ii, j := range is {
			// Find idx values at prev Indices
			idxVals[ii] = idxSeries.Value(j)
			// Find val values at prev Indices
			valVals[ii] = valsSeries.Value(j)
		}
		data := make([]interface{}, uniqueIdxSeries.Len())
		// Find Indices where idx values exist in idx series
		for ii, v := range idxVals {
			j := uniqueIdxSeries.FindIndices(v)
			if len(j) > 1 {
				panic("dataframe: pivot: index column is not unique")
			}
			if len(j) <= 0 || j == nil {
				panic("dataframe: pivot: value not found in index")
			}
			// Set value to proper location
			data[j[0]] = valVals[ii]
		}

		ss[i+1] = series.FromInterface(df.pool, field, data, nil)
	}

	return DataFrame{
		pool:   df.pool,
		series: ss,
	}
}

// CosineSimilarity ...
func (df DataFrame) CosineSimilarity() DataFrame {
	df.Retain()
	defer df.Release()

	nr := int(df.NumRows())
	ss := make([]series.Series, nr)
	for i := 0; i < nr; i++ {
		field := arrow.Field{
			Name:     strconv.Itoa(i),
			Type:     arrow.PrimitiveTypes.Float64,
			Nullable: true,
		}
		vals := make([]float64, nr)
		for j := 0; j < nr; j++ {
			vals[j] = df.Dot(i, j) / (df.RowNorm(i) * df.RowNorm(j))
		}
		ss[i] = series.FromFloat64(df.pool, field, vals, nil)
	}

	return NewFromSeries(df.pool, ss)
}
