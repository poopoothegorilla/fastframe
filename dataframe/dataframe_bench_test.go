package dataframe_test

import (
	"fmt"
	"log"
	"math/rand"
	"strconv"
	"testing"

	"github.com/apache/arrow/go/arrow"
	"github.com/apache/arrow/go/arrow/array"
	"github.com/apache/arrow/go/arrow/memory"
	gotadataframe "github.com/go-gota/gota/dataframe"
	gotaseries "github.com/go-gota/gota/series"
	"github.com/poopoothegorilla/fastframe/dataframe"
	"github.com/poopoothegorilla/fastframe/series"
	"github.com/ptiger10/tada"
)

func BenchmarkNewFromInterfaces(b *testing.B) {
	colVals := []int{10, 100, 1000}
	rowVals := []int{2}

	for _, colVal := range colVals {
		for _, rowVal := range rowVals {
			b.Run(fmt.Sprintf("size=%vcolsx%vrows", colVal, rowVal), func(b *testing.B) {
				benchmarkNewFromInterfaces(b, colVal, rowVal)
			})
		}
	}
}

func benchmarkNewFromInterfaces(b *testing.B, cols, nrows int) {
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(b, 0)
	t := arrow.PrimitiveTypes.Float32

	fields := make([]arrow.Field, cols)
	for i := 0; i < cols; i++ {
		name := strconv.Itoa(i)
		fields[i] = arrow.Field{Name: name, Type: t}
	}
	rows := make([]dataframe.Row, nrows)
	for i := 0; i < nrows; i++ {
		rows[i] = newDataframeRow(fields)
	}

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		df := dataframe.NewFromRows(pool, fields, rows)
		df.Release()
	}
}

func BenchmarkNewFromRecords(b *testing.B) {
	colVals := []int{10, 100, 1000}
	rowVals := []int{2}

	for _, colVal := range colVals {
		for _, rowVal := range rowVals {
			b.Run(fmt.Sprintf("size=%vcolsx%vrows", colVal, rowVal), func(b *testing.B) {
				benchmarkNewFromRecords(b, colVal, rowVal)
			})
		}
	}
}

func benchmarkNewFromRecords(b *testing.B, cols, rows int) {
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(b, 0)
	recordSize := 1
	t := arrow.PrimitiveTypes.Float32

	fields := make([]arrow.Field, cols)
	for i := 0; i < cols; i++ {
		name := strconv.Itoa(i)
		fields[i] = arrow.Field{Name: name, Type: t}
	}
	records := make([]array.Record, rows)
	for i := 0; i < rows; i++ {
		record := newArrowRecord(pool, recordSize, fields)
		defer record.Release()
		records[i] = record
	}

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		df := dataframe.NewFromRecords(pool, records)
		df.Release()
	}
}

func BenchmarkSeries(b *testing.B) {
	vals := []int{10, 100, 1000}

	for _, val := range vals {
		b.Run(fmt.Sprintf("size=%v", val), func(b *testing.B) {
			benchmarkSeries(b, val)
		})
	}
}

func benchmarkSeries(b *testing.B, cols int) {
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(b, 0)
	rows := 2

	columns := make([]series.Series, cols)
	for i := 0; i < cols; i++ {
		name := strconv.Itoa(i)
		ss := newTestSeries(rows, arrow.PrimitiveTypes.Float64, pool, rows/2)
		defer ss.Release()
		ss = ss.Rename(name)

		columns[i] = ss
	}

	df := dataframe.NewFromSeries(pool, columns)
	defer df.Release()
	in := cols / 2

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		_ = df.Series(in)
	}
}

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
		name := strconv.Itoa(i)
		ss := newTestSeries(rows, arrow.PrimitiveTypes.Float64, pool, rows/2)
		defer ss.Release()
		ss = ss.Rename(name)

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

func BenchmarkLeftJoin(b *testing.B) {
	colVals := []int{10, 100, 1000}
	rowVals := []int{1, 10, 100}

	for _, colVal := range colVals {
		for _, rowVal := range rowVals {
			b.Run(fmt.Sprintf("size=%vcolsx%vrows", colVal, rowVal), func(b *testing.B) {
				benchmarkLeftJoin(b, colVal, rowVal)
			})
		}
		for _, rowVal := range rowVals {
			b.Run(fmt.Sprintf("EM/size=%vcolsx%vrows", colVal, rowVal), func(b *testing.B) {
				benchmarkLeftJoinEM(b, colVal, rowVal)
			})
		}
		for _, rowVal := range rowVals {
			b.Run(fmt.Sprintf("tada/size=%vcolsx%vrows", colVal, rowVal), func(b *testing.B) {
				benchmarkTadaLeftJoin(b, colVal, rowVal)
			})
		}
		for _, rowVal := range rowVals {
			b.Run(fmt.Sprintf("gota/size=%vcolsx%vrows", colVal, rowVal), func(b *testing.B) {
				benchmarkGotaLeftJoin(b, colVal, rowVal)
			})
		}
	}
}

func benchmarkLeftJoin(b *testing.B, cols, rows int) {
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(b, 0)

	columns := make([]series.Series, cols)
	for i := 0; i < cols; i++ {
		name := strconv.Itoa(i)
		if i == 0 {
			name = "join-column"
		}
		ss := newTestSeries(rows, arrow.PrimitiveTypes.Float64, pool, 0)
		defer ss.Release()
		ss = ss.Rename(name)

		columns[i] = ss
	}
	columns2 := make([]series.Series, cols)
	for i := 0; i < cols; i++ {
		name := strconv.Itoa(i)
		if i == 0 {
			name = "join-column"
		}
		ss := newTestSeries(rows, arrow.PrimitiveTypes.Float64, pool, 0)
		defer ss.Release()
		ss = ss.Rename(name)

		columns2[i] = ss
	}

	df := dataframe.NewFromSeries(pool, columns)
	defer df.Release()
	df2 := dataframe.NewFromSeries(pool, columns2)
	defer df2.Release()

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		df3 := dataframe.LeftJoin(df, "join-column", df2, "join-column")
		df3.Release()
	}
}

func benchmarkLeftJoinEM(b *testing.B, cols, rows int) {
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(b, 0)

	columns := make([]series.Series, cols)
	for i := 0; i < cols; i++ {
		name := strconv.Itoa(i)
		if i == 0 {
			name = "join-column"
		}
		ss := newTestSeries(rows, arrow.PrimitiveTypes.Float64, pool, 0)
		defer ss.Release()
		ss = ss.Rename(name)

		columns[i] = ss
	}
	columns2 := make([]series.Series, cols)
	for i := 0; i < cols; i++ {
		name := strconv.Itoa(i)
		if i == 0 {
			name = "join-column"
		}
		ss := newTestSeries(rows, arrow.PrimitiveTypes.Float64, pool, 0)
		defer ss.Release()
		ss = ss.Rename(name)

		columns2[i] = ss
	}

	df := dataframe.NewFromSeries(pool, columns)
	defer df.Release()
	df2 := dataframe.NewFromSeries(pool, columns2)
	defer df2.Release()

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		df3 := dataframe.LeftJoinEM(df, "join-column", df2, "join-column")
		df3.Release()
	}
}

func benchmarkTadaLeftJoin(b *testing.B, cols, rows int) {
	columns := make([]*tada.Series, cols)
	for i := 0; i < cols; i++ {
		ss := newTadaTestSeries(rows, arrow.PrimitiveTypes.Float64)
		if i == 0 {
			data := []int{}
			for j := 0; j < rows; j++ {
				data = append(data, j)
			}
			ss = tada.NewSeries(data)
			ss.SetName("join-column")
		}
		columns[i] = ss
	}
	columns2 := make([]*tada.Series, cols)
	for i := 0; i < cols; i++ {
		ss := newTadaTestSeries(rows, arrow.PrimitiveTypes.Float64)
		if i == 0 {
			data := []int{}
			for j := 0; j < rows; j++ {
				data = append(data, j)
			}
			ss = tada.NewSeries(data)
			ss.SetName("join-column")
		}
		columns2[i] = ss
	}

	df, err := tada.ConcatSeries(columns...)
	if err != nil {
		log.Fatal(err)
	}
	df2, err := tada.ConcatSeries(columns2...)
	if err != nil {
		log.Fatal(err)
	}

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		_, err := df.Merge(df2, tada.JoinOptionHow("left"))
		if err != nil {
			log.Fatal(err)
		}
	}
}

func benchmarkGotaLeftJoin(b *testing.B, cols, rows int) {
	columns := make([]gotaseries.Series, cols)
	for i := 0; i < cols; i++ {
		ss := newGotaTestSeries(rows, arrow.PrimitiveTypes.Float64)
		if i == 0 {
			data := []int{}
			for j := 0; j < rows; j++ {
				data = append(data, j)
			}
			ss = gotaseries.New(data, gotaseries.Float, "join-column")
		}
		columns[i] = ss
	}
	columns2 := make([]gotaseries.Series, cols)
	for i := 0; i < cols; i++ {
		ss := newGotaTestSeries(rows, arrow.PrimitiveTypes.Float64)
		if i == 0 {
			data := []int{}
			for j := 0; j < rows; j++ {
				data = append(data, j)
			}
			ss = gotaseries.New(data, gotaseries.Float, "join-column")
		}
		columns2[i] = ss
	}

	df := gotadataframe.New(columns...)
	df2 := gotadataframe.New(columns2...)

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		df.LeftJoin(df2, "join-column")
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
		s := df.RowToSeries(0)
		s2 := df.RowToSeries(1)
		s.Dot(s2)
		s.Release()
		s2.Release()
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
	default:
		panic("unknown type")
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
	default:
		panic("unknown type")
	}
	return gotaseries.New(vals, t, "testing")
}

func newArrowRecord(pool memory.Allocator, numVals int, fields []arrow.Field) array.Record {
	schema := arrow.NewSchema(fields, nil)

	rb := array.NewRecordBuilder(pool, schema)
	defer rb.Release()
	for _, fb := range rb.Fields() {
		switch b := fb.(type) {
		case *array.Int32Builder:
			v := make([]int32, numVals)
			for j := 0; j < numVals; j++ {
				v[j] = rand.Int31()
			}
			b.AppendValues(v, nil)
		case *array.Int64Builder:
			v := make([]int64, numVals)
			for j := 0; j < numVals; j++ {
				v[j] = rand.Int63()
			}
			b.AppendValues(v, nil)
		case *array.Float32Builder:
			v := make([]float32, numVals)
			for j := 0; j < numVals; j++ {
				v[j] = rand.Float32()
			}
			b.AppendValues(v, nil)
		case *array.Float64Builder:
			v := make([]float64, numVals)
			for j := 0; j < numVals; j++ {
				v[j] = rand.Float64()
			}
			b.AppendValues(v, nil)
		default:
			panic("unknown type")
		}
	}

	return rb.NewRecord()
}

func newDataframeRow(fields []arrow.Field) dataframe.Row {
	vals := make([]interface{}, len(fields))
	valid := make([]bool, len(fields))

	for i, fb := range fields {
		switch fb.Type {
		case arrow.PrimitiveTypes.Int32:
			vals[i] = rand.Int31()
		case arrow.PrimitiveTypes.Int64:
			vals[i] = rand.Int63()
		case arrow.PrimitiveTypes.Float32:
			vals[i] = rand.Float32()
		case arrow.PrimitiveTypes.Float64:
			vals[i] = rand.Float64()
		default:
			panic("unknown type")
		}
	}

	return dataframe.Row{
		Vals:  vals,
		Valid: valid,
	}
}
