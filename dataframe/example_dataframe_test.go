package dataframe_test

import (
	"fmt"

	"github.com/apache/arrow/go/arrow"
	"github.com/apache/arrow/go/arrow/array"
	"github.com/apache/arrow/go/arrow/memory"
	"github.com/poopoothegorilla/fastframe/dataframe"
)

func generateUsersDF(pool memory.Allocator) dataframe.DataFrame {
	schema := arrow.NewSchema([]arrow.Field{
		arrow.Field{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		arrow.Field{Name: "age", Type: arrow.PrimitiveTypes.Int32},
	}, nil)

	bi64 := array.NewInt64Builder(pool)
	defer bi64.Release()
	bi32 := array.NewInt32Builder(pool)
	defer bi32.Release()

	bi64.AppendValues([]int64{1000, 21407, 898989, 8888, 101292}, nil)
	ids := bi64.NewArray()
	defer ids.Release()

	bi32.AppendValues([]int32{20, 16, 35, 66, 50}, nil)
	ages := bi32.NewArray()
	defer ages.Release()

	cols := []array.Interface{ids, ages}
	users := []array.Record{array.NewRecord(schema, cols, -1)}

	return dataframe.NewFromRecords(pool, users)
}

func generateMovieRatingsDF(pool memory.Allocator) dataframe.DataFrame {
	schema := arrow.NewSchema([]arrow.Field{
		arrow.Field{Name: "user_id", Type: arrow.PrimitiveTypes.Int64},
		arrow.Field{Name: "movie_id", Type: arrow.PrimitiveTypes.Int64},
		arrow.Field{Name: "rating", Type: arrow.PrimitiveTypes.Int32},
	}, nil)

	bi64 := array.NewInt64Builder(pool)
	defer bi64.Release()
	bi32 := array.NewInt32Builder(pool)
	defer bi32.Release()

	bi64.AppendValues([]int64{1000, 21407, 898989, 8888, 101292}, nil)
	userIDs := bi64.NewArray()
	defer userIDs.Release()

	bi64.AppendValues([]int64{1000, 21407, 898989, 8888, 101292}, nil)
	movieIDs := bi64.NewArray()
	defer movieIDs.Release()

	bi32.AppendValues([]int32{20, 16, 35, 66, 50}, nil)
	ratings := bi32.NewArray()
	defer ratings.Release()

	cols := []array.Interface{userIDs, movieIDs, ratings}
	movieRatings := []array.Record{array.NewRecord(schema, cols, -1)}

	return dataframe.NewFromRecords(pool, movieRatings)
}

func Example() {
	pool := memory.NewGoAllocator()

	usersDF := generateUsersDF(pool)
	defer usersDF.Release()

	// TODO(poopoothegorilla): the column output format is not the same if
	// chunksize is larger than total rows.
	fmt.Println("USERS:")
	table := array.NewTableReader(usersDF, 2)
	n := 0
	for table.Next() {
		rec := table.Record()
		for i, col := range rec.Columns() {
			fmt.Printf("rec[%d][%q]: %v\n", n, rec.ColumnName(i), col)
		}
		n++
	}
	table.Release()

	movieRatingsDF := generateMovieRatingsDF(pool)
	defer movieRatingsDF.Release()

	// TODO(poopoothegorilla): the column output format is not the same if
	// chunksize is larger than total rows.
	fmt.Println("MOVIE RATINGS:")
	table = array.NewTableReader(movieRatingsDF, 2)
	n = 0
	for table.Next() {
		rec := table.Record()
		for i, col := range rec.Columns() {
			fmt.Printf("rec[%d][%q]: %v\n", n, rec.ColumnName(i), col)
		}
		n++
	}
	table.Release()

	// CREATE pivot table

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
