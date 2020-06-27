package dataframe_test

import (
	"encoding/csv"
	"fmt"
	"strconv"
	"strings"

	"github.com/apache/arrow/go/arrow"
	"github.com/apache/arrow/go/arrow/array"
	"github.com/apache/arrow/go/arrow/memory"
	"github.com/poopoothegorilla/fastframe/dataframe"
	"github.com/poopoothegorilla/fastframe/series"
)

// func generateUsersDF(pool memory.Allocator) dataframe.DataFrame {
// 	schema := arrow.NewSchema([]arrow.Field{
// 		arrow.Field{Name: "id", Type: arrow.PrimitiveTypes.Int64},
// 		arrow.Field{Name: "age", Type: arrow.PrimitiveTypes.Int32},
// 	}, nil)
//
// 	bi64 := array.NewInt64Builder(pool)
// 	defer bi64.Release()
// 	bi32 := array.NewInt32Builder(pool)
// 	defer bi32.Release()
//
// 	bi64.AppendValues([]int64{1000, 21407, 898989, 8888, 101292}, nil)
// 	ids := bi64.NewArray()
// 	defer ids.Release()
//
// 	bi32.AppendValues([]int32{20, 16, 35, 66, 50}, nil)
// 	ages := bi32.NewArray()
// 	defer ages.Release()
//
// 	cols := []array.Interface{ids, ages}
// 	users := []array.Record{array.NewRecord(schema, cols, -1)}
//
// 	return dataframe.NewFromRecords(pool, users)
// }

func generateMovieRatingsDF(pool memory.Allocator) dataframe.DataFrame {
	// TODO(poopoothegorilla): needs string column type
	// f, err := os.Open(`../data/rating.csv`)
	// if err != nil {
	// 	log.Fatal(err)
	// }
	// defer f.Close()

	f := strings.NewReader(`"user_name","user_id","movie_id","rating"
jim,1,2,3.5
jim,1,29,3.5
jim,1,32,3.5
jim,1,47,3.5
jim,1,50,3.5
jim,1,112,3.5
joe,2,3,4
joe,2,29,5
joe,2,62,5
joe,2,70,5
joe,2,47,4
joe,2,110,4
joe,2,242,3
joe,2,260,5
fig,3,62,4
fig,3,50,3.5
fig,3,70,2
fig,3,112,5`)

	r := csv.NewReader(f)

	return dataframe.NewFromCSV(pool, r, -1)
}

// func generateMovieRatingsDF(pool memory.Allocator) dataframe.DataFrame {
// 	schema := arrow.NewSchema([]arrow.Field{
// 		arrow.Field{Name: "user_id", Type: arrow.PrimitiveTypes.Int64},
// 		arrow.Field{Name: "movie_id", Type: arrow.PrimitiveTypes.Int64},
// 		arrow.Field{Name: "rating", Type: arrow.PrimitiveTypes.Float32},
// 	}, nil)
//
// 	bi64 := array.NewInt64Builder(pool)
// 	defer bi64.Release()
// 	bi32 := array.NewFloat32Builder(pool)
// 	defer bi32.Release()
//
// 	bi64.AppendValues([]int64{1000, 21407, 898989, 8888, 101292}, nil)
// 	userIDs := bi64.NewArray()
// 	defer userIDs.Release()
//
// 	bi64.AppendValues([]int64{1000, 21407, 898989, 8888, 101292}, nil)
// 	movieIDs := bi64.NewArray()
// 	defer movieIDs.Release()
//
// 	bi32.AppendValues([]float32{0.2, 0.16, 0.35, 0.66, 0.50}, nil)
// 	ratings := bi32.NewArray()
// 	defer ratings.Release()
//
// 	cols := []array.Interface{userIDs, movieIDs, ratings}
// 	movieRatings := []array.Record{array.NewRecord(schema, cols, -1)}
//
// 	return dataframe.NewFromRecords(pool, movieRatings)
// }

func Example() {
	pool := memory.NewGoAllocator()

	movieRatingsDF := generateMovieRatingsDF(pool)
	defer movieRatingsDF.Release()

	movieRatingsDF.Cast("user_id", arrow.PrimitiveTypes.Int32)
	movieRatingsDF.Cast("movie_id", arrow.PrimitiveTypes.Int64)
	movieRatingsDF.Cast("rating", arrow.PrimitiveTypes.Float64)

	// TODO(poopoothegorilla): the column output format is not the same if
	// chunksize is larger than total rows.
	fmt.Println("MOVIE RATINGS:")
	table := array.NewTableReader(movieRatingsDF, 5)
	n := 0
	for table.Next() {
		rec := table.Record()
		for i, col := range rec.Columns() {
			fmt.Printf("rec[%d][%q]: %v\n", n, rec.ColumnName(i), col)
		}
		n++
	}
	table.Release()

	// CREATE pivot table
	pivot := movieRatingsDF.Pivot("user_id", "movie_id", "rating")
	fmt.Println("PIVOT:")
	table = array.NewTableReader(pivot, 3)
	n = 0
	for table.Next() {
		rec := table.Record()
		for i, col := range rec.Columns() {
			fmt.Printf("rec[%d][%q]: %v\n", n, rec.ColumnName(i), col)
		}
		n++
	}
	table.Release()

	nr := int(pivot.NumRows())
	ss := make([]series.Series, nr)
	for i := 0; i < nr; i++ {
		field := arrow.Field{
			Name:     strconv.Itoa(i),
			Type:     arrow.PrimitiveTypes.Float64,
			Nullable: true,
		}
		vals := make([]float64, nr)
		for j := 0; j < nr; j++ {
			vals[j] = pivot.Dot(i, j)
		}
		ss[i] = series.FromFloat64(pool, field, vals, nil)
	}
	matrix := dataframe.NewFromSeries(pool, ss)

	fmt.Println("Matrix:")
	table = array.NewTableReader(matrix, 2)
	n = 0
	for table.Next() {
		rec := table.Record()
		for i, col := range rec.Columns() {
			fmt.Printf("rec[%d][%q]: %v\n", n, rec.ColumnName(i), col)
		}
		n++
	}
	table.Release()

	// Output:
	// USERS:
	// rec[0]["id"]: [1000 21407]
	// rec[0]["age"]: [20 16]
	// rec[1]["id"]: [898989 8888]
	// rec[1]["age"]: [35 66]
	// rec[2]["id"]: [101292]
	// rec[2]["age"]: [50]
	// MOVIE RATINGS:
	// rec[0]["user_id"]: [1000 21407]
	// rec[0]["movie_id"]: [1000 21407]
	// rec[0]["rating"]: [20 16]
	// rec[1]["user_id"]: [898989 8888]
	// rec[1]["movie_id"]: [898989 8888]
	// rec[1]["rating"]: [35 66]
	// rec[2]["user_id"]: [101292]
	// rec[2]["movie_id"]: [101292]
	// rec[2]["rating"]: [50]
}
