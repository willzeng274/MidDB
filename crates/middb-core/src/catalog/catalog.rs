use super::schema::TableSchema;
use std::collections::HashMap;

#[derive(Debug)]
pub enum CatalogError {
    TableNotFound(String),
    TableAlreadyExists(String),
    ColumnNotFound { table: String, column: String },
}

impl std::fmt::Display for CatalogError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CatalogError::TableNotFound(name) => write!(f, "table not found: {}", name),
            CatalogError::TableAlreadyExists(name) => write!(f, "table already exists: {}", name),
            CatalogError::ColumnNotFound { table, column } => {
                write!(f, "column '{}' not found in table '{}'", column, table)
            }
        }
    }
}

impl std::error::Error for CatalogError {}

pub type CatalogResult<T> = Result<T, CatalogError>;

pub struct Catalog {
    tables: HashMap<String, TableSchema>,
}

impl Catalog {
    pub fn new() -> Self {
        Catalog {
            tables: HashMap::new(),
        }
    }

    pub fn register_table(&mut self, schema: TableSchema) -> CatalogResult<()> {
        if self.tables.contains_key(&schema.name) {
            return Err(CatalogError::TableAlreadyExists(schema.name.clone()));
        }
        self.tables.insert(schema.name.clone(), schema);
        Ok(())
    }

    pub fn get_table(&self, name: &str) -> Option<&TableSchema> {
        self.tables.get(name)
    }

    pub fn get_table_mut(&mut self, name: &str) -> Option<&mut TableSchema> {
        self.tables.get_mut(name)
    }

    pub fn drop_table(&mut self, name: &str) -> CatalogResult<TableSchema> {
        self.tables
            .remove(name)
            .ok_or_else(|| CatalogError::TableNotFound(name.to_string()))
    }

    pub fn list_tables(&self) -> Vec<&str> {
        self.tables.keys().map(|s| s.as_str()).collect()
    }

    pub fn table_exists(&self, name: &str) -> bool {
        self.tables.contains_key(name)
    }

    pub fn table_count(&self) -> usize {
        self.tables.len()
    }
}

impl Default for Catalog {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::schema::{DataType, TableSchemaBuilder};

    #[test]
    fn test_register_and_get_table() {
        let mut catalog = Catalog::new();
        let schema = TableSchemaBuilder::new("users")
            .column("id", DataType::Int64, false)
            .column("name", DataType::String, false)
            .build();

        catalog.register_table(schema).unwrap();

        let retrieved = catalog.get_table("users").unwrap();
        assert_eq!(retrieved.name, "users");
        assert_eq!(retrieved.column_count(), 2);
    }

    #[test]
    fn test_duplicate_table_error() {
        let mut catalog = Catalog::new();
        let schema1 = TableSchemaBuilder::new("products").build();
        let schema2 = TableSchemaBuilder::new("products").build();

        catalog.register_table(schema1).unwrap();
        let result = catalog.register_table(schema2);

        assert!(matches!(result, Err(CatalogError::TableAlreadyExists(_))));
    }

    #[test]
    fn test_drop_table() {
        let mut catalog = Catalog::new();
        let schema = TableSchemaBuilder::new("temp").build();

        catalog.register_table(schema).unwrap();
        assert!(catalog.table_exists("temp"));

        catalog.drop_table("temp").unwrap();
        assert!(!catalog.table_exists("temp"));
    }

    #[test]
    fn test_list_tables() {
        let mut catalog = Catalog::new();

        catalog
            .register_table(TableSchemaBuilder::new("a").build())
            .unwrap();
        catalog
            .register_table(TableSchemaBuilder::new("b").build())
            .unwrap();

        let mut tables = catalog.list_tables();
        tables.sort();
        assert_eq!(tables, vec!["a", "b"]);
    }

    #[test]
    fn test_drop_nonexistent_table() {
        let mut catalog = Catalog::new();
        let result = catalog.drop_table("nonexistent");
        assert!(matches!(result, Err(CatalogError::TableNotFound(_))));
    }
}
