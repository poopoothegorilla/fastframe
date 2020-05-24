# FastFrame
TODO(poopoothegorilla): add automated testing tag

TODO(poopoothegorilla): add coverage tag

TODO(poopoothegorilla): add automated build tag

TODO(poopoothegorilla): add go report tag

TODO(poopoothegorilla): add godoc tag

TODO(poopoothegorilla): add cii tag?

TODO(poopoothegorilla): add stability tag?

TODO(poopoothegorilla): add short description of package

## Example

## Installation

## Feature List

### Readers

- [ ] Arrow
- [ ] Parquet
- [ ] CSV
- [ ] JSON

### Writers

- [ ] Arrow
- [ ] Parquet
- [ ] CSV
- [ ] JSON

### DataFrame

- [x] NewFromRecords
- [x] NewFromSeries
- [ ] NewFromMatrix
- [ ] NewFromStructs

- [x] ApplyToSeries(fn func(series.Series) series.Series) DataFrame
- [x] ApplyToRecords(fn func(array.Record) array.Record) DataFrame
- [x] Head(n int) DataFrame
- [x] Abs() DataFrame
- [x] Add(df2 DataFrame) DataFrame
- [x] AppendRecords(records []array.Record) DataFrame
- [x] AppendSeries(ss []series.Series) DataFrame
- [x] SelectColumnsByNames(names []string) DataFrame
- [x] DropColumnsByIndices(indices []int) DataFrame
- [x] DropColumnsByNames(names []string) DataFrame
- [x] DropRowsByIndices(indices []int) DataFrame
- [x] SelectRowsByIndices(indices []int) DataFrame
- [x] DropNARowsBySeriesIndices(seriesIndices []int) DataFrame
- [x] Square() DataFrame
- [x] Sqrt() DataFrame
- [x] Substract(df2 DataFrame) DataFrame
- [x] SetSeries(s series.Series) DataFrame
- [ ] Unique(names []string) DataFrame
- [ ] RenameColumn
- [ ] FillNA
- [ ] Replace
- [x] LeftJoin(DataFrame, string, DataFrame, string) DataFrame
- [x] RightJoin(DataFrame, string, DataFrame, string) DataFrame
- [ ] Inner Join
- [ ] Cross Join
- [ ] Outer Join
- [ ] Map
- [ ] Where

- [x] Headers() []string
- [x] Value(rowi, coli int) interface{}

- [x] Max() float64
- [x] Min() float64
- [x] Mean() float64
- [x] Sum() float64
- [x] Dot(rowi, rowj int) float64
- [ ] Median
- [ ] STD

- [x] Series(i int) series.Series
- [x] HasSeries(name string) bool
- [x] SeriesByName(name string) series.Series
- [x] Row(i int) series.Series

- [x] Implement Gonum Mat Interface
- [x] At(i, _ int) float64
- [x] T() mat.Matrix
- [x] Dims() (r,c int)

- [x] Implement Arrow Table Interface
- [x] Schema() *arrow.Schema
- [x] NumRows() int64
- [x] NumCols() int64
- [x] Column(i int) *array.Column
- [x] Release()
- [x] Retain()


### Series

- [x] NewFromArrow
- [x] NewFromInt32
- [x] NewFromInt64
- [x] NewFromFloat32
- [x] NewFromFloat64

- [x] Column() *array.Column
- [x] Value(i int) interface{}
- [x] Values() interface{}
- [x] Field() arrow.Field
- [x] Name() string

- [x] Unique() Series
- [x] Truncate(i, j int64) Series
- [x] Subtract(b Series) Series
- [x] Add(b Series) Series
- [x] Append(b Series) Series
- [x] SortValues() Series
- [x] Rename() Series
- [x] SelectIndices(indices []int) Series
- [x] DropIndices(indices []int) Series
- [x] Head(n int) Series
- [x] DropNA() Series
- [x] Map(fn func(interface{}) interface{}) Series
- [x] Where(cs ...Condition) Series
- [x] Abs() Series
- [x] Square() Series
- [x] Sqrt() Series

- [x] Dot(b Series) float64
- [x] Sum() float64
- [x] STD() float64
- [x] Magnitude() float64
- [x] Min() float64
- [x] Max() float64
- [x] Mean() float64
- [x] Median() float64

- [x] IsNA() []bool
- [x] FindIndices(interface{}) []int
- [x] NAIndices() []int

- [x] Implement Gonum Mat Interface
- [x] Dims() (r,c int)
- [x] At(i, _ int) float64
- [x] T() mat.Matrix
- [x] Implement Gonum Vector Interface
- [x] AtVec(i int) float64
