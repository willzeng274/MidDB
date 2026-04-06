use std::cmp::Ordering;
use std::fmt;

#[derive(Debug, Clone, PartialEq)]
pub enum Expr {
    Literal(Value),
    Column(String),
    BinaryOp {
        op: BinaryOperator,
        left: Box<Expr>,
        right: Box<Expr>,
    },
    Aggregate {
        func: AggregateFunc,
        arg: Box<Expr>,
    },
    IsNull(Box<Expr>),
    IsNotNull(Box<Expr>),
    UnaryNot(Box<Expr>),
    InList {
        expr: Box<Expr>,
        list: Vec<Expr>,
        negated: bool,
    },
    Between {
        expr: Box<Expr>,
        low: Box<Expr>,
        high: Box<Expr>,
        negated: bool,
    },
    Wildcard,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    Int(i64),
    Float(f64),
    String(String),
    Bool(bool),
    Bytes(Vec<u8>),
    Null,
}

impl Value {
    pub fn as_int(&self) -> Option<i64> {
        match self {
            Value::Int(i) => Some(*i),
            _ => None,
        }
    }

    pub fn as_float(&self) -> Option<f64> {
        match self {
            Value::Float(f) => Some(*f),
            _ => None,
        }
    }

    pub fn as_string(&self) -> Option<&str> {
        match self {
            Value::String(s) => Some(s),
            _ => None,
        }
    }

    pub fn as_bool(&self) -> Option<bool> {
        match self {
            Value::Bool(b) => Some(*b),
            _ => None,
        }
    }

    pub fn as_bytes(&self) -> Option<&[u8]> {
        match self {
            Value::Bytes(b) => Some(b),
            _ => None,
        }
    }

    pub fn compare(&self, other: &Value) -> Option<Ordering> {
        match (self, other) {
            (Value::Int(a), Value::Int(b)) => Some(a.cmp(b)),
            (Value::Float(a), Value::Float(b)) => a.partial_cmp(b),
            (Value::Int(a), Value::Float(b)) => (*a as f64).partial_cmp(b),
            (Value::Float(a), Value::Int(b)) => a.partial_cmp(&(*b as f64)),
            (Value::String(a), Value::String(b)) => Some(a.cmp(b)),
            (Value::Bool(a), Value::Bool(b)) => Some(a.cmp(b)),
            (Value::Bytes(a), Value::Bytes(b)) => Some(a.cmp(b)),
            (Value::Null, Value::Null) => Some(Ordering::Equal),
            _ => None,
        }
    }

    pub fn to_sort_key(&self) -> Vec<u8> {
        match self {
            Value::Int(i) => i.to_be_bytes().to_vec(),
            Value::Float(f) => f.to_be_bytes().to_vec(),
            Value::String(s) => s.as_bytes().to_vec(),
            Value::Bool(b) => vec![*b as u8],
            Value::Bytes(b) => b.clone(),
            Value::Null => vec![],
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BinaryOperator {
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
    And,
    Or,
    Add,
    Sub,
    Mul,
    Div,
    Mod,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AggregateFunc {
    Count,
    Sum,
    Avg,
    Min,
    Max,
}

impl fmt::Display for Expr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Expr::Literal(v) => write!(f, "{v:?}"),
            Expr::Column(name) => write!(f, "{name}"),
            Expr::BinaryOp { op, left, right } => {
                write!(f, "({left} {op:?} {right})")
            }
            Expr::Aggregate { func, arg } => write!(f, "{func:?}({arg})"),
            Expr::IsNull(e) => write!(f, "({e} IS NULL)"),
            Expr::IsNotNull(e) => write!(f, "({e} IS NOT NULL)"),
            Expr::UnaryNot(e) => write!(f, "(NOT {e})"),
            Expr::Wildcard => write!(f, "*"),
            Expr::InList { expr, list, negated } => {
                write!(f, "({} {}IN ({:?}))", expr, if *negated { "NOT " } else { "" }, list)
            }
            Expr::Between { expr, low, high, negated } => {
                write!(f, "({} {}BETWEEN {} AND {})", expr, if *negated { "NOT " } else { "" }, low, high)
            }
        }
    }
}
