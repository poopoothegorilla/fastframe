# FastFrame
WARNING: DO NOT USE IN PRODUCTION EVERYTHING IS EXPERIMENTAL

FastFrame is a DataFrame project which utilizes Apache Arrow
(https://github.com/apache/arrow/tree/master/go) for the underlying data
storage.

## Benchmarks

Some of the benchmarks show where columnar / arrow based computation are beneficial. 

https://github.com/poopoothegorilla/fastframe/wiki/Benchmarks:-Series

### DataFrame

Currently, the code for the joins could be improved.
https://github.com/poopoothegorilla/fastframe/wiki/Benchmarks:-Datafame

## Example

This is an example of calculating cosine similarity

see dataframe/example... files

```go
func generateMovieRatingsDF(pool memory.Allocator) dataframe.DataFrame {
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

	return dataframe.NewFromCSV(pool, csv.NewReader(f), -1, nil)
}

func Example() {
	pool := memory.NewGoAllocator()

	rawMovieRatingsDF := generateMovieRatingsDF(pool)
	castList := map[string]arrow.DataType{
		"user_id":  arrow.PrimitiveTypes.Int32,
		"movie_id": arrow.PrimitiveTypes.Int64,
		"rating":   arrow.PrimitiveTypes.Float64,
	}
	movieRatingsDF := rawMovieRatingsDF.Cast(castList)
	rawMovieRatingsDF.Release()

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

	matrix := pivot.CosineSimilarity()
	pivot.Release()

	fmt.Println("MATRIX:")
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
	matrix.Release()
	// Output:
	// MOVIE RATINGS:
	// rec[0]["user_name"]: ["jim" "jim" "jim" "jim" "jim"]
	// rec[0]["user_id"]: [1 1 1 1 1]
	// rec[0]["movie_id"]: [2 29 32 47 50]
	// rec[0]["rating"]: [3.5 3.5 3.5 3.5 3.5]
	// rec[1]["user_name"]: ["jim" "joe" "joe" "joe" "joe"]
	// rec[1]["user_id"]: [1 2 2 2 2]
	// rec[1]["movie_id"]: [112 3 29 62 70]
	// rec[1]["rating"]: [3.5 4 5 5 5]
	// rec[2]["user_name"]: ["joe" "joe" "joe" "joe" "fig"]
	// rec[2]["user_id"]: [2 2 2 2 3]
	// rec[2]["movie_id"]: [47 110 242 260 62]
	// rec[2]["rating"]: [4 4 3 5 4]
	// rec[3]["user_name"]: ["fig" "fig" "fig"]
	// rec[3]["user_id"]: [3 3 3]
	// rec[3]["movie_id"]: [50 70 112]
	// rec[3]["rating"]: [3.5 2 5]
	// PIVOT:
	// rec[0]["user_id"]: [1 2 3]
	// rec[0]["2"]: [3.5 0 0]
	// rec[0]["29"]: [3.5 5 0]
	// rec[0]["32"]: [3.5 0 0]
	// rec[0]["47"]: [3.5 4 0]
	// rec[0]["50"]: [3.5 0 3.5]
	// rec[0]["112"]: [3.5 0 5]
	// rec[0]["3"]: [0 4 0]
	// rec[0]["62"]: [0 5 4]
	// rec[0]["70"]: [0 5 2]
	// rec[0]["110"]: [0 4 0]
	// rec[0]["242"]: [0 3 0]
	// rec[0]["260"]: [0 5 0]
	// MATRIX:
	// rec[0]["0"]: [1 0.30588186723552135]
	// rec[0]["1"]: [0.30588186723552135 1]
	// rec[0]["2"]: [0.46616560472322743 0.3485753093366648]
	// rec[1]["0"]: [0.46616560472322743]
	// rec[1]["1"]: [0.3485753093366648]
	// rec[1]["2"]: [1]
}
```
