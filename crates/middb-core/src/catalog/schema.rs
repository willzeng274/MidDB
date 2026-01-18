use std::fmt;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DataType {
    Int64,
    String,
    Bytes,
    Bool,
}

impl DataType {
    pub fn is_compatible(&self, other: &DataType) -> bool {
        self == other
    }
}

impl fmt::Display for DataType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DataType::Int64 => write!(f, "INT64"),
            DataType::String => write!(f, "STRING"),
            DataType::Bytes => write!(f, "BYTES"),
            DataType::Bool => write!(f, "BOOL"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Column {
    pub name: String,
    pub data_type: DataType,
    pub nullable: bool,
    pub position: usize,
}

impl Column {
    pub fn new(name: impl Into<String>, data_type: DataType) -> Self {
        Column {
            name: name.into(),
            data_type,
            nullable: true,
            position: 0,
        }
    }

    pub fn non_null(name: impl Into<String>, data_type: DataType) -> Self {
        Column {
            name: name.into(),
            data_type,
            nullable: false,
            position: 0,
        }
    }

    pub fn with_position(mut self, position: usize) -> Self {
        self.position = position;
        self
    }

    pub fn set_nullable(mut self, nullable: bool) -> Self {
        self.nullable = nullable;
        self
    }
}

#[derive(Debug, Clone)]
pub struct TableSchema {
    pub name: String,
    pub columns: Vec<Column>,
}

impl TableSchema {
    pub fn new(name: impl Into<String>, columns: Vec<Column>) -> Self {
        let columns = columns
            .into_iter()
            .enumerate()
            .map(|(i, mut col)| {
                col.position = i;
                col
            })
            .collect();
        TableSchema {
            name: name.into(),
            columns,
        }
    }

    pub fn empty(name: impl Into<String>) -> Self {
        TableSchema {
            name: name.into(),
            columns: Vec::new(),
        }
    }

    pub fn add_column(&mut self, mut column: Column) {
        column.position = self.columns.len();
        self.columns.push(column);
    }

    pub fn column_count(&self) -> usize {
        self.columns.len()
    }

    pub fn get_column(&self, name: &str) -> Option<&Column> {
        self.columns.iter().find(|c| c.name == name)
    }

    pub fn get_column_index(&self, name: &str) -> Option<usize> {
        self.columns.iter().position(|c| c.name == name)
    }

    pub fn column_names(&self) -> Vec<&str> {
        self.columns.iter().map(|c| c.name.as_str()).collect()
    }
}

pub struct TableSchemaBuilder {
    name: String,
    columns: Vec<Column>,
}

impl TableSchemaBuilder {
    pub fn new(name: impl Into<String>) -> Self {
        TableSchemaBuilder {
            name: name.into(),
            columns: Vec::new(),
        }
    }

    pub fn column(mut self, name: impl Into<String>, data_type: DataType, nullable: bool) -> Self {
        let col = if nullable {
            Column::new(name, data_type)
        } else {
            Column::non_null(name, data_type)
        };
        self.columns.push(col);
        self
    }

    pub fn build(self) -> TableSchema {
        TableSchema::new(self.name, self.columns)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_column_creation() {
        let col = Column::non_null("id", DataType::Int64);
        assert_eq!(col.name, "id");
        assert_eq!(col.data_type, DataType::Int64);
        assert!(!col.nullable);
    }

    #[test]
    fn test_schema_builder() {
        let schema = TableSchemaBuilder::new("users")
            .column("id", DataType::Int64, false)
            .column("name", DataType::String, false)
            .column("email", DataType::String, true)
            .build();

        assert_eq!(schema.name, "users");
        assert_eq!(schema.column_count(), 3);

        let id_col = schema.get_column("id").unwrap();
        assert_eq!(id_col.position, 0);
        assert!(!id_col.nullable);

        let email_col = schema.get_column("email").unwrap();
        assert_eq!(email_col.position, 2);
        assert!(email_col.nullable);
    }

    #[test]
    fn test_column_lookup() {
        let schema = TableSchemaBuilder::new("products")
            .column("sku", DataType::String, false)
            .column("price", DataType::Int64, false)
            .build();

        assert_eq!(schema.get_column_index("price"), Some(1));
        assert_eq!(schema.get_column_index("unknown"), None);
    }

    #[test]
    fn test_data_type_display() {
        assert_eq!(format!("{}", DataType::Int64), "INT64");
        assert_eq!(format!("{}", DataType::String), "STRING");
    }
}
