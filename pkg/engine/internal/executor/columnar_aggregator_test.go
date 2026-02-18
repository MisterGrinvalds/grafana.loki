package executor

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/cespare/xxhash/v2"
	"github.com/stretchr/testify/require"

	"github.com/grafana/loki/v3/pkg/engine/internal/semconv"
	"github.com/grafana/loki/v3/pkg/engine/internal/types"
	"github.com/grafana/loki/v3/pkg/util/arrowtest"
)

var (
	columnarGroupBy = []arrow.Field{
		semconv.FieldFromIdent(semconv.NewIdentifier("env", types.ColumnTypeLabel, types.Loki.String), true),
		semconv.FieldFromIdent(semconv.NewIdentifier("service", types.ColumnTypeLabel, types.Loki.String), true),
	}
)

func makeTimestampArray(mem memory.Allocator, values []int64) *array.Timestamp {
	builder := array.NewTimestampBuilder(mem, &arrow.TimestampType{Unit: arrow.Nanosecond})
	defer builder.Release()
	for _, v := range values {
		builder.Append(arrow.Timestamp(v))
	}
	return builder.NewTimestampArray()
}

func makeFloat64Array(mem memory.Allocator, values []float64) *array.Float64 {
	builder := array.NewFloat64Builder(mem)
	defer builder.Release()
	for _, v := range values {
		builder.Append(v)
	}
	return builder.NewFloat64Array()
}

func makeStringArray(mem memory.Allocator, values []string) *array.String {
	builder := array.NewStringBuilder(mem)
	defer builder.Release()
	for _, v := range values {
		builder.Append(v)
	}
	return builder.NewStringArray()
}

func TestColumnarAggregator(t *testing.T) {
	colTs := semconv.ColumnIdentTimestamp.FQN()
	colVal := semconv.ColumnIdentValue.FQN()
	colEnv := semconv.NewIdentifier("env", types.ColumnTypeLabel, types.Loki.String).FQN()
	colSvc := semconv.NewIdentifier("service", types.ColumnTypeLabel, types.Loki.String).FQN()

	mem := memory.NewGoAllocator()

	ts1 := int64(1704103200000000000) // 2024-01-01 10:00:00 UTC
	ts2 := int64(1704103260000000000) // 2024-01-01 10:01:00 UTC

	t.Run("basic SUM aggregation", func(t *testing.T) {
		agg := newColumnarAggregator(10, columnarAggregatorOpts{
			operation:     aggregationOperationSum,
			groupByLabels: columnarGroupBy,
		})

		timestamps := makeTimestampArray(mem, []int64{
			ts1, ts1, ts1,
			ts2, ts2, ts2,
			ts1, ts2,
		})
		defer timestamps.Release()

		values := makeFloat64Array(mem, []float64{
			10, 20, 30,
			15, 25, 35,
			5, 10,
		})
		defer values.Release()

		envCol := makeStringArray(mem, []string{
			"prod", "prod", "dev",
			"prod", "prod", "dev",
			"prod", "prod",
		})
		defer envCol.Release()

		svcCol := makeStringArray(mem, []string{
			"app1", "app2", "app1",
			"app1", "app2", "app2",
			"app1", "app1",
		})
		defer svcCol.Release()

		err := agg.AddBatch(timestamps, values, []*array.String{envCol, svcCol})
		require.NoError(t, err)

		record, err := agg.BuildRecord()
		require.NoError(t, err)

		expect := arrowtest.Rows{
			{colTs: arrow.Timestamp(ts1).ToTime(arrow.Nanosecond), colVal: float64(15), colEnv: "prod", colSvc: "app1"},
			{colTs: arrow.Timestamp(ts1).ToTime(arrow.Nanosecond), colVal: float64(20), colEnv: "prod", colSvc: "app2"},
			{colTs: arrow.Timestamp(ts1).ToTime(arrow.Nanosecond), colVal: float64(30), colEnv: "dev", colSvc: "app1"},

			{colTs: arrow.Timestamp(ts2).ToTime(arrow.Nanosecond), colVal: float64(25), colEnv: "prod", colSvc: "app1"},
			{colTs: arrow.Timestamp(ts2).ToTime(arrow.Nanosecond), colVal: float64(25), colEnv: "prod", colSvc: "app2"},
			{colTs: arrow.Timestamp(ts2).ToTime(arrow.Nanosecond), colVal: float64(35), colEnv: "dev", colSvc: "app2"},
		}

		rows, err := arrowtest.RecordRows(record)
		require.NoError(t, err, "should be able to convert record back to rows")
		require.Equal(t, len(expect), len(rows), "number of rows should match")
		require.ElementsMatch(t, expect, rows)
	})

	t.Run("basic AVG aggregation", func(t *testing.T) {
		agg := newColumnarAggregator(10, columnarAggregatorOpts{
			operation:     aggregationOperationAvg,
			groupByLabels: columnarGroupBy,
		})

		timestamps := makeTimestampArray(mem, []int64{ts1, ts1})
		defer timestamps.Release()

		values := makeFloat64Array(mem, []float64{10, 30})
		defer values.Release()

		envCol := makeStringArray(mem, []string{"prod", "prod"})
		defer envCol.Release()

		svcCol := makeStringArray(mem, []string{"app1", "app1"})
		defer svcCol.Release()

		err := agg.AddBatch(timestamps, values, []*array.String{envCol, svcCol})
		require.NoError(t, err)

		record, err := agg.BuildRecord()
		require.NoError(t, err)

		expect := arrowtest.Rows{
			{colTs: arrow.Timestamp(ts1).ToTime(arrow.Nanosecond), colVal: float64(20), colEnv: "prod", colSvc: "app1"},
		}

		rows, err := arrowtest.RecordRows(record)
		require.NoError(t, err)
		require.Equal(t, len(expect), len(rows))
		require.ElementsMatch(t, expect, rows)
	})

	t.Run("basic COUNT aggregation", func(t *testing.T) {
		agg := newColumnarAggregator(10, columnarAggregatorOpts{
			operation:     aggregationOperationCount,
			groupByLabels: columnarGroupBy,
		})

		timestamps := makeTimestampArray(mem, []int64{ts1, ts1, ts1})
		defer timestamps.Release()

		values := makeFloat64Array(mem, []float64{10, 20, 30})
		defer values.Release()

		envCol := makeStringArray(mem, []string{"prod", "prod", "prod"})
		defer envCol.Release()

		svcCol := makeStringArray(mem, []string{"app1", "app1", "app2"})
		defer svcCol.Release()

		err := agg.AddBatch(timestamps, values, []*array.String{envCol, svcCol})
		require.NoError(t, err)

		record, err := agg.BuildRecord()
		require.NoError(t, err)

		expect := arrowtest.Rows{
			{colTs: arrow.Timestamp(ts1).ToTime(arrow.Nanosecond), colVal: float64(2), colEnv: "prod", colSvc: "app1"},
			{colTs: arrow.Timestamp(ts1).ToTime(arrow.Nanosecond), colVal: float64(1), colEnv: "prod", colSvc: "app2"},
		}

		rows, err := arrowtest.RecordRows(record)
		require.NoError(t, err)
		require.Equal(t, len(expect), len(rows))
		require.ElementsMatch(t, expect, rows)
	})

	t.Run("basic MAX aggregation", func(t *testing.T) {
		agg := newColumnarAggregator(10, columnarAggregatorOpts{
			operation:     aggregationOperationMax,
			groupByLabels: columnarGroupBy,
		})

		timestamps := makeTimestampArray(mem, []int64{ts1, ts1, ts1})
		defer timestamps.Release()

		values := makeFloat64Array(mem, []float64{10, 30, 20})
		defer values.Release()

		envCol := makeStringArray(mem, []string{"prod", "prod", "prod"})
		defer envCol.Release()

		svcCol := makeStringArray(mem, []string{"app1", "app1", "app1"})
		defer svcCol.Release()

		err := agg.AddBatch(timestamps, values, []*array.String{envCol, svcCol})
		require.NoError(t, err)

		record, err := agg.BuildRecord()
		require.NoError(t, err)

		expect := arrowtest.Rows{
			{colTs: arrow.Timestamp(ts1).ToTime(arrow.Nanosecond), colVal: float64(30), colEnv: "prod", colSvc: "app1"},
		}

		rows, err := arrowtest.RecordRows(record)
		require.NoError(t, err)
		require.Equal(t, len(expect), len(rows))
		require.ElementsMatch(t, expect, rows)
	})

	t.Run("basic MIN aggregation", func(t *testing.T) {
		agg := newColumnarAggregator(10, columnarAggregatorOpts{
			operation:     aggregationOperationMin,
			groupByLabels: columnarGroupBy,
		})

		timestamps := makeTimestampArray(mem, []int64{ts1, ts1, ts1})
		defer timestamps.Release()

		values := makeFloat64Array(mem, []float64{30, 10, 20})
		defer values.Release()

		envCol := makeStringArray(mem, []string{"prod", "prod", "prod"})
		defer envCol.Release()

		svcCol := makeStringArray(mem, []string{"app1", "app1", "app1"})
		defer svcCol.Release()

		err := agg.AddBatch(timestamps, values, []*array.String{envCol, svcCol})
		require.NoError(t, err)

		record, err := agg.BuildRecord()
		require.NoError(t, err)

		expect := arrowtest.Rows{
			{colTs: arrow.Timestamp(ts1).ToTime(arrow.Nanosecond), colVal: float64(10), colEnv: "prod", colSvc: "app1"},
		}

		rows, err := arrowtest.RecordRows(record)
		require.NoError(t, err)
		require.Equal(t, len(expect), len(rows))
		require.ElementsMatch(t, expect, rows)
	})

	t.Run("SUM aggregation with empty groupBy", func(t *testing.T) {
		agg := newColumnarAggregator(1, columnarAggregatorOpts{
			operation: aggregationOperationSum,
		})

		timestamps := makeTimestampArray(mem, []int64{ts1, ts1, ts1, ts2, ts2})
		defer timestamps.Release()

		values := makeFloat64Array(mem, []float64{10, 20, 30, 15, 25})
		defer values.Release()

		err := agg.AddBatch(timestamps, values, []*array.String{})
		require.NoError(t, err)

		record, err := agg.BuildRecord()
		require.NoError(t, err)

		expect := arrowtest.Rows{
			{colTs: arrow.Timestamp(ts1).ToTime(arrow.Nanosecond), colVal: float64(60)},
			{colTs: arrow.Timestamp(ts2).ToTime(arrow.Nanosecond), colVal: float64(40)},
		}

		rows, err := arrowtest.RecordRows(record)
		require.NoError(t, err)
		require.Equal(t, len(expect), len(rows))
		require.ElementsMatch(t, expect, rows)
	})

	t.Run("series limit enforcement", func(t *testing.T) {
		agg := newColumnarAggregator(10, columnarAggregatorOpts{
			operation:     aggregationOperationSum,
			groupByLabels: columnarGroupBy,
			maxSeries:     2,
		})

		timestamps1 := makeTimestampArray(mem, []int64{ts1, ts1})
		defer timestamps1.Release()

		values1 := makeFloat64Array(mem, []float64{10, 20})
		defer values1.Release()

		envCol1 := makeStringArray(mem, []string{"prod", "prod"})
		defer envCol1.Release()

		svcCol1 := makeStringArray(mem, []string{"app1", "app2"})
		defer svcCol1.Release()

		err := agg.AddBatch(timestamps1, values1, []*array.String{envCol1, svcCol1})
		require.NoError(t, err)

		timestamps2 := makeTimestampArray(mem, []int64{ts2})
		defer timestamps2.Release()

		values2 := makeFloat64Array(mem, []float64{30})
		defer values2.Release()

		envCol2 := makeStringArray(mem, []string{"dev"})
		defer envCol2.Release()

		svcCol2 := makeStringArray(mem, []string{"app1"})
		defer svcCol2.Release()

		err = agg.AddBatch(timestamps2, values2, []*array.String{envCol2, svcCol2})
		require.Error(t, err)
		require.True(t, errors.Is(err, ErrSeriesLimitExceeded))

		// After reset with no series limit, adding new series should succeed.
		agg2 := newColumnarAggregator(10, columnarAggregatorOpts{
			operation:     aggregationOperationSum,
			groupByLabels: columnarGroupBy,
		})

		timestamps3 := makeTimestampArray(mem, []int64{ts2})
		defer timestamps3.Release()

		values3 := makeFloat64Array(mem, []float64{15})
		defer values3.Release()

		envCol3 := makeStringArray(mem, []string{"prod"})
		defer envCol3.Release()

		svcCol3 := makeStringArray(mem, []string{"app1"})
		defer svcCol3.Release()

		err = agg2.AddBatch(timestamps3, values3, []*array.String{envCol3, svcCol3})
		require.NoError(t, err)
	})

	t.Run("multiple batches", func(t *testing.T) {
		agg := newColumnarAggregator(10, columnarAggregatorOpts{
			operation:     aggregationOperationSum,
			groupByLabels: columnarGroupBy,
		})

		timestamps1 := makeTimestampArray(mem, []int64{ts1, ts1})
		defer timestamps1.Release()

		values1 := makeFloat64Array(mem, []float64{10, 20})
		defer values1.Release()

		envCol1 := makeStringArray(mem, []string{"prod", "prod"})
		defer envCol1.Release()

		svcCol1 := makeStringArray(mem, []string{"app1", "app2"})
		defer svcCol1.Release()

		err := agg.AddBatch(timestamps1, values1, []*array.String{envCol1, svcCol1})
		require.NoError(t, err)

		timestamps2 := makeTimestampArray(mem, []int64{ts1, ts1})
		defer timestamps2.Release()

		values2 := makeFloat64Array(mem, []float64{5, 10})
		defer values2.Release()

		envCol2 := makeStringArray(mem, []string{"prod", "prod"})
		defer envCol2.Release()

		svcCol2 := makeStringArray(mem, []string{"app1", "app2"})
		defer svcCol2.Release()

		err = agg.AddBatch(timestamps2, values2, []*array.String{envCol2, svcCol2})
		require.NoError(t, err)

		record, err := agg.BuildRecord()
		require.NoError(t, err)

		expect := arrowtest.Rows{
			{colTs: arrow.Timestamp(ts1).ToTime(arrow.Nanosecond), colVal: float64(15), colEnv: "prod", colSvc: "app1"},
			{colTs: arrow.Timestamp(ts1).ToTime(arrow.Nanosecond), colVal: float64(30), colEnv: "prod", colSvc: "app2"},
		}

		rows, err := arrowtest.RecordRows(record)
		require.NoError(t, err)
		require.Equal(t, len(expect), len(rows))
		require.ElementsMatch(t, expect, rows)
	})

	t.Run("with window function", func(t *testing.T) {
		windowStart := arrow.Timestamp(ts1).ToTime(arrow.Nanosecond)

		w0End := windowStart.UnixNano()
		w0Start := windowStart.Add(-60 * 1e9).UnixNano()

		matchFunc := columnarWindowMatchFunc(func(inputTs, outTs []int64, validMask []bool, numRows int) {
			for i := range numRows {
				if !validMask[i] {
					continue
				}
				ts := inputTs[i]
				if ts > w0Start && ts <= w0End {
					outTs[i] = w0End
				} else {
					validMask[i] = false
				}
			}
		})

		agg := newColumnarAggregator(10, columnarAggregatorOpts{
			operation:     aggregationOperationSum,
			groupByLabels: columnarGroupBy,
			matchWindows:  matchFunc,
		})

		tsInWindow := windowStart.Add(-15 * 1e9).UnixNano()

		timestamps := makeTimestampArray(mem, []int64{tsInWindow})
		defer timestamps.Release()

		values := makeFloat64Array(mem, []float64{100})
		defer values.Release()

		envCol := makeStringArray(mem, []string{"prod"})
		defer envCol.Release()

		svcCol := makeStringArray(mem, []string{"app1"})
		defer svcCol.Release()

		err := agg.AddBatch(timestamps, values, []*array.String{envCol, svcCol})
		require.NoError(t, err)

		record, err := agg.BuildRecord()
		require.NoError(t, err)

		rows, err := arrowtest.RecordRows(record)
		require.NoError(t, err)

		require.Equal(t, 1, len(rows))
		require.Equal(t, float64(100), rows[0][colVal])
		require.Equal(t, "prod", rows[0][colEnv])
		require.Equal(t, "app1", rows[0][colSvc])
		require.Equal(t, time.Unix(0, w0End).UTC(), rows[0][colTs])
	})

	t.Run("with window function filtering out-of-range", func(t *testing.T) {
		matchFunc := columnarWindowMatchFunc(func(_, _ []int64, validMask []bool, numRows int) {
			for i := range numRows {
				validMask[i] = false
			}
		})

		agg := newColumnarAggregator(10, columnarAggregatorOpts{
			operation:     aggregationOperationSum,
			groupByLabels: columnarGroupBy,
			matchWindows:  matchFunc,
		})

		timestamps := makeTimestampArray(mem, []int64{ts1, ts2})
		defer timestamps.Release()

		values := makeFloat64Array(mem, []float64{100, 200})
		defer values.Release()

		envCol := makeStringArray(mem, []string{"prod", "prod"})
		defer envCol.Release()

		svcCol := makeStringArray(mem, []string{"app1", "app1"})
		defer svcCol.Release()

		err := agg.AddBatch(timestamps, values, []*array.String{envCol, svcCol})
		require.NoError(t, err)

		record, err := agg.BuildRecord()
		require.NoError(t, err)

		require.Equal(t, int64(0), record.NumRows(), "all timestamps should be filtered out")
	})

	t.Run("composite key correctness", func(t *testing.T) {
		windowStart := arrow.Timestamp(ts1).ToTime(arrow.Nanosecond)

		w0Start := windowStart.Add(-120 * 1e9).UnixNano()
		w0End := windowStart.Add(-60 * 1e9).UnixNano()
		w1Start := windowStart.Add(-60 * 1e9).UnixNano()
		w1End := windowStart.UnixNano()

		matchFunc := columnarWindowMatchFunc(func(inputTs, outTs []int64, validMask []bool, numRows int) {
			for i := range numRows {
				if !validMask[i] {
					continue
				}
				ts := inputTs[i]
				if ts > w0Start && ts <= w0End {
					outTs[i] = w0End
				} else if ts > w1Start && ts <= w1End {
					outTs[i] = w1End
				} else {
					validMask[i] = false
				}
			}
		})

		agg := newColumnarAggregator(10, columnarAggregatorOpts{
			operation:     aggregationOperationSum,
			groupByLabels: columnarGroupBy,
			matchWindows:  matchFunc,
		})

		tsW0 := windowStart.Add(-90 * 1e9).UnixNano() // falls in window 0
		tsW1 := windowStart.Add(-30 * 1e9).UnixNano() // falls in window 1

		timestamps := makeTimestampArray(mem, []int64{tsW0, tsW1})
		defer timestamps.Release()

		values := makeFloat64Array(mem, []float64{10, 20})
		defer values.Release()

		envCol := makeStringArray(mem, []string{"prod", "prod"})
		defer envCol.Release()

		svcCol := makeStringArray(mem, []string{"app1", "app1"})
		defer svcCol.Release()

		err := agg.AddBatch(timestamps, values, []*array.String{envCol, svcCol})
		require.NoError(t, err)

		record, err := agg.BuildRecord()
		require.NoError(t, err)

		rows, err := arrowtest.RecordRows(record)
		require.NoError(t, err)

		require.Equal(t, 2, len(rows), "same series in different windows gets different groups")

		for _, row := range rows {
			require.Equal(t, "prod", row[colEnv])
			require.Equal(t, "app1", row[colSvc])
		}

		valSet := map[float64]bool{}
		for _, row := range rows {
			valSet[row[colVal].(float64)] = true
		}
		require.True(t, valSet[10], "window 0 should have value 10")
		require.True(t, valSet[20], "window 1 should have value 20")
	})
}

func TestGroupKey(t *testing.T) {
	h := xxhash.Sum64String("test-series")

	k1 := groupKey{tsNano: 100, seriesHash: h}
	k2 := groupKey{tsNano: 200, seriesHash: h}
	k3 := groupKey{tsNano: 100, seriesHash: h}
	k4 := groupKey{tsNano: 100, seriesHash: h + 1}

	require.NotEqual(t, k1, k2, "different timestamps should produce different keys")
	require.Equal(t, k1, k3, "same inputs should produce same keys")
	require.NotEqual(t, k1, k4, "different series hashes should produce different keys")
}

type benchLabelConfig struct {
	name        string
	cardinality int
}

func benchFields(labels []benchLabelConfig) []arrow.Field {
	fields := make([]arrow.Field, len(labels))
	for i, l := range labels {
		fields[i] = semconv.FieldFromIdent(
			semconv.NewIdentifier(l.name, types.ColumnTypeLabel, types.Loki.String), true,
		)
	}
	return fields
}

// benchBatch holds pre-built Arrow arrays for one batch in a benchmark scenario.
type benchBatch struct {
	ts   *array.Timestamp
	vals *array.Float64
	cols []*array.String
}

func (bb benchBatch) Release() {
	bb.ts.Release()
	bb.vals.Release()
	for _, c := range bb.cols {
		c.Release()
	}
}

// benchRawData holds plain Go slices for the full dataset, used by the
// row-based aggregator which does not operate on Arrow arrays.
type benchRawData struct {
	timestamps []int64
	values     []float64
	labels     [][]string // labels[colIdx][rowIdx]
	rowLabels  [][]string // rowLabels[rowIdx][colIdx] — transposed for row-based Add()
}

func buildBenchBatches(mem memory.Allocator, totalRows, batchSize, numDistinctTs int, labels []benchLabelConfig) ([]benchBatch, benchRawData) {
	const baseTs = int64(1704103200000000000)

	timestamps := make([]int64, totalRows)
	values := make([]float64, totalRows)
	for i := range totalRows {
		timestamps[i] = baseTs + int64(i%numDistinctTs)*1_000_000_000
		values[i] = float64(i)
	}

	labelCols := make([][]string, len(labels))
	for c, l := range labels {
		col := make([]string, totalRows)
		for i := range totalRows {
			col[i] = fmt.Sprintf("%s-%d", l.name, i%l.cardinality)
		}
		labelCols[c] = col
	}

	rowLabels := make([][]string, totalRows)
	for row := range totalRows {
		vals := make([]string, len(labels))
		for c := range labels {
			vals[c] = labelCols[c][row]
		}
		rowLabels[row] = vals
	}

	numBatches := (totalRows + batchSize - 1) / batchSize
	batches := make([]benchBatch, numBatches)
	for bi := range numBatches {
		start := bi * batchSize
		end := start + batchSize
		if end > totalRows {
			end = totalRows
		}
		arrCols := make([]*array.String, len(labels))
		for c := range labels {
			arrCols[c] = makeStringArray(mem, labelCols[c][start:end])
		}
		batches[bi] = benchBatch{
			ts:   makeTimestampArray(mem, timestamps[start:end]),
			vals: makeFloat64Array(mem, values[start:end]),
			cols: arrCols,
		}
	}

	raw := benchRawData{
		timestamps: timestamps,
		values:     values,
		labels:     labelCols,
		rowLabels:  rowLabels,
	}
	return batches, raw
}

// BenchmarkAggregatorAddBatch measures AddBatch throughput for
// both the columnar and row-based aggregators using identical test data.
func BenchmarkAggregatorAddBatch(b *testing.B) {
	mem := memory.NewGoAllocator()

	type benchCase struct {
		name      string
		totalRows int
		batchSize int
		labels    []benchLabelConfig
	}

	cardLow := []benchLabelConfig{
		{"level", 5},
	}
	cardMedium := []benchLabelConfig{
		{"level", 5},
		{"namespace", 30},
	}
	cardHigh := []benchLabelConfig{
		{"level", 5},
		{"namespace", 30},
		{"service", 20},
	}

	rowCounts := []struct {
		name string
		n    int
	}{
		{"rows=10k", 10_000},
		{"rows=100k", 100_000},
		{"rows=1M", 1_000_000},
	}
	batchSizes := []int{100, 1_000, 8_000}
	cards := []struct {
		name   string
		labels []benchLabelConfig
	}{
		{"cardinality=low", cardLow},
		{"cardinality=medium", cardMedium},
		{"cardinality=high_card", cardHigh},
	}

	var cases []benchCase
	for _, rc := range rowCounts {
		for _, bs := range batchSizes {
			for _, cd := range cards {
				cases = append(cases, benchCase{
					name:      fmt.Sprintf("%s/batch=%d/%s", rc.name, bs, cd.name),
					totalRows: rc.n,
					batchSize: bs,
					labels:    cd.labels,
				})
			}
		}
	}

	const numDistinctTs = 100

	b.Run("aggregator=columnar", func(b *testing.B) {
		for _, tc := range cases {
			b.Run(tc.name, func(b *testing.B) {
				fields := benchFields(tc.labels)
				batches, _ := buildBenchBatches(mem, tc.totalRows, tc.batchSize, numDistinctTs, tc.labels)
				defer func() {
					for _, batch := range batches {
						batch.Release()
					}
				}()

				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					agg := newColumnarAggregator(100, columnarAggregatorOpts{
						operation:     aggregationOperationSum,
						groupByLabels: fields,
					})
					for _, batch := range batches {
						_ = agg.AddBatch(batch.ts, batch.vals, batch.cols)
					}
				}
				b.ReportMetric(float64(tc.totalRows)*float64(b.N)/b.Elapsed().Seconds(), "rows/s")
			})
		}
	})

	b.Run("aggregator=row_based", func(b *testing.B) {
		for _, tc := range cases {
			b.Run(tc.name, func(b *testing.B) {
				fields := benchFields(tc.labels)
				_, raw := buildBenchBatches(mem, tc.totalRows, tc.batchSize, numDistinctTs, tc.labels)

				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					agg := newAggregator(100, aggregationOperationSum)
					for row := range tc.totalRows {
						ts := arrow.Timestamp(raw.timestamps[row]).ToTime(arrow.Nanosecond)
						_ = agg.Add(ts, raw.values[row], fields, raw.rowLabels[row])
					}
				}
				b.ReportMetric(float64(tc.totalRows)*float64(b.N)/b.Elapsed().Seconds(), "rows/s")
			})
		}
	})
}
