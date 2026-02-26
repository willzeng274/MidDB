use std::collections::HashMap;

#[derive(Debug, Clone)]
pub struct TableStatistics {
    pub row_count: u64,
    pub column_stats: HashMap<String, ColumnStatistics>,
}

#[derive(Debug, Clone)]
pub struct ColumnStatistics {
    pub distinct_count: u64,
    pub null_count: u64,
    pub min_value: Option<Vec<u8>>,
    pub max_value: Option<Vec<u8>>,
}

impl TableStatistics {
    pub fn new(row_count: u64) -> Self {
        TableStatistics {
            row_count,
            column_stats: HashMap::new(),
        }
    }

    pub fn selectivity_for_eq(&self, column: &str) -> f64 {
        if let Some(stats) = self.column_stats.get(column) {
            if stats.distinct_count > 0 {
                return 1.0 / stats.distinct_count as f64;
            }
        }
        0.1
    }

    pub fn selectivity_for_range(&self, _column: &str) -> f64 {
        0.33
    }
}

#[derive(Debug, Default)]
pub struct StatisticsCollector {
    pub stats: HashMap<String, TableStatistics>,
}

impl StatisticsCollector {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn set_table_stats(&mut self, table: String, stats: TableStatistics) {
        self.stats.insert(table, stats);
    }

    pub fn get_table_stats(&self, table: &str) -> Option<&TableStatistics> {
        self.stats.get(table)
    }

    pub fn estimated_row_count(&self, table: &str) -> u64 {
        self.stats.get(table).map_or(1000, |s| s.row_count)
    }
}
