use substreams::store::{StoreAdd, StoreAddBigInt, StoreDelete, DeltaBigInt};
use substreams_entity_change::tables::{Tables, ToValue};
use substreams::pb::substreams::Clock;
use substreams::scalar::{BigInt, BigDecimal};
use std::collections::HashMap;
use std::any::Any;

pub struct TimeSeriesStore {
    clock: Clock,
    store: StoreAddBigInt,
}

impl TimeSeriesStore {
    pub fn new(clock: Clock, store: StoreAddBigInt) -> Self {
        Self { clock, store }
    }

    pub fn update(&mut self, entities: Vec<&str>) {
        let current_timestamp = self.clock.timestamp.as_ref().unwrap().seconds;
        let day_timestamp = (current_timestamp / 86400) * 86400;
        let hour_timestamp = (current_timestamp / 3600) * 3600;
        let prev_day_timestamp = day_timestamp - 86400;
        let prev_hour_timestamp = hour_timestamp - 3600;

        for entity in entities {
            self.store.delete_prefix(0, &format!("{}:day:{prev_day_timestamp}:", entity));
            self.store.delete_prefix(0, &format!("{}:hour:{prev_hour_timestamp}:", entity));
        }
    }

    pub fn add_metrics(&mut self, entity: &str, metrics: HashMap<&str, BigInt>, groups: Vec<String>) {
        self.update(vec![entity]);

        let current_timestamp = self.clock.timestamp.as_ref().unwrap().seconds;
        let day_timestamp = (current_timestamp / 86400) * 86400;
        let hour_timestamp = (current_timestamp / 3600) * 3600;
        let prev_day_timestamp = day_timestamp - 86400;
        let prev_hour_timestamp = hour_timestamp - 3600;

        self.store.delete_prefix(0, &format!("{}:day:{prev_day_timestamp}:", entity));
        self.store.delete_prefix(0, &format!("{}:hour:{prev_hour_timestamp}:", entity));

        let group_suffix = if groups.is_empty() {
            String::new()
        } else {
            format!(":{}", groups.join(":"))
        };

        for (metric, value) in metrics.iter() {
            let keys = vec![
                format!("{}:total:{}{}", entity, metric, group_suffix),
                format!("{}:day:{day_timestamp}:{}{}", entity, metric, group_suffix),
                format!("{}:hour:{hour_timestamp}:{}{}", entity, metric, group_suffix),
            ];

            self.store.add_many(0, &keys, value);
        }
    }
}

pub struct DeltaProcessor<'a> {
    tables: &'a mut Tables,
    metric_mutations: HashMap<String, Box<dyn Fn(BigInt) -> Box<dyn Any>>>,
    group_mutations: HashMap<String, Box<dyn Fn(String) -> Box<dyn Any>>>,
}

impl<'a> DeltaProcessor<'a> {
    pub fn new(tables: &'a mut Tables) -> Self {
        Self {
            tables,
            metric_mutations: HashMap::new(),
            group_mutations: HashMap::new(),
        }
    }

    pub fn add_metric_mutation<F, T>(&mut self, metric: String, mutation: F)
    where
        F: Fn(BigInt) -> T + 'static,
        T: 'static + ToValue,
    {
        self.metric_mutations.insert(metric, Box::new(move |v| Box::new(mutation(v)) as Box<dyn Any>));
    }

    pub fn add_group_mutation<F, T>(&mut self, group: String, mutation: F)
    where
        F: Fn(String) -> T + 'static,
        T: 'static + ToValue,
    {
        self.group_mutations.insert(group, Box::new(move |v| Box::new(mutation(v)) as Box<dyn Any>));
    }

    pub fn process_deltas(&mut self, deltas: impl Iterator<Item = DeltaBigInt>, group_fields: Vec<String>) {
        for delta in deltas {
            let key_segments: Vec<&str> = delta.get_key().split(':').collect();
            
            if key_segments.len() < 3 {
                continue;
            }

            let entity = key_segments[0];
            let interval = key_segments[1];
            let (timestamp, metric, groupings) = match interval {
                "total" => (
                    "0".to_string(),
                    key_segments[2],
                    key_segments.get(3..).unwrap_or(&[]).join("-")
                ),
                "day" | "hour" => {
                    if key_segments.len() < 4 {
                        continue;
                    }
                    (
                        key_segments[2].to_string(),
                        key_segments[3],
                        key_segments.get(4..).unwrap_or(&[]).join("-")
                    )
                },
                _ => continue,
            };

            let id = if groupings.is_empty() {
                format!("{}-{}", timestamp, interval)
            } else {
                format!("{}-{}-{}", timestamp, interval, groupings)
            };

            let row = self.tables.update_row(entity, &id);
            row.set("interval", interval);
            row.set_bigint("timestamp", &timestamp);

            if let Some(mutation) = self.metric_mutations.get(metric) {
                let value = mutation(delta.new_value.clone());
                if let Some(int) = value.downcast_ref::<BigInt>() {
                    row.set(metric, int);
                } else if let Some(string) = value.downcast_ref::<String>() {
                    row.set(metric, string);
                } else if let Some(bool_val) = value.downcast_ref::<bool>() {
                    row.set(metric, bool_val);
                } else if let Some(decimal) = value.downcast_ref::<BigDecimal>() {
                    row.set(metric, decimal);
                }
            } else {
                row.set(metric, &delta.new_value);
            }

            if !groupings.is_empty() {
                let group_values: Vec<&str> = groupings.split('-').collect();
                for (i, group_value) in group_values.iter().enumerate() {
                    let group_field = if i < group_fields.len() {
                        group_fields[i].clone()
                    } else {
                        format!("group{}", i + 1)
                    };

                    if let Some(mutation) = self.group_mutations.get(&group_field) {
                        let value = mutation(group_value.to_string());
                        if let Some(string) = value.downcast_ref::<String>() {
                            row.set(&group_field, string);
                        } else if let Some(int) = value.downcast_ref::<BigInt>() {
                            row.set(&group_field, int);
                        } else if let Some(bool_val) = value.downcast_ref::<bool>() {
                            row.set(&group_field, bool_val);
                        }
                    } else {
                        row.set(&group_field, *group_value);
                    }
                }
            }
        }
    }
}

