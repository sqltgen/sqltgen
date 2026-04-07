use std::collections::HashMap;

use crate::ir::SqlType;

pub type ParamMapping = HashMap<usize, (String, SqlType, bool)>;
pub type UserFunctions = HashMap<String, Vec<(Vec<SqlType>, SqlType)>>;
