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
}

#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    Int(i64),
    String(String),
    Bool(bool),
    Null,
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
}

impl fmt::Display for Expr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Expr::Literal(v) => write!(f, "{:?}", v),
            Expr::Column(name) => write!(f, "{}", name),
            Expr::BinaryOp { op, left, right } => {
                write!(f, "({} {:?} {})", left, op, right)
            }
        }
    }
}
