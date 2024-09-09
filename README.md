# SPS TimeSeries

Substreams helper to easily manage Timeseries aggregations within the Substream and processing those aggregations to a Subgraph.

## Features

- Time-series data storage and management (total/day/hour)
- Flexible metric aggregation
- Support for custom mutations on metrics and groupings

## Usage

### TimeSeriesStore

The `TimeSeriesStore` struct allows you to manage time-series data:

```rust
let mut time_series_store = TimeSeriesStore::new(clock, output);
// Update multiple entities
let entities = vec!["MessageStats", "MessageStatsByDestination", "MessageStatsBySender", "MessageStatsByRecipient"];
time_series_store.update(entities);
// Add metrics for an entity
time_series_store.add_metrics(
"MessageStats",
HashMap::from([
("count", BigInt::one()),
("totalBurnAmountUSD", amount.clone()),
]),
vec![]
);
```

### DeltaProcessor

The `DeltaProcessor` struct processes delta changes and applies custom mutations:

```rust
let mut processor = DeltaProcessor::new(&mut table);
// Add custom metric mutation
processor.add_metric_mutation("totalBurnAmountUSD".to_string(), move |value: BigInt| {
let conversion = value.to_decimal(decimals);
conversion
});
// Add custom group mutation
processor.add_group_mutation("destinationDomain".to_string(), move |value: String| {
BigInt::from_str(&value).unwrap()
});
// Process deltas
processor.process_deltas(
store_aggregates_count.clone().into_iter()
.operation_not_eq(Operation::Delete)
.key_first_segment_eq("MessageStats".to_string()),
vec![]
);
```
