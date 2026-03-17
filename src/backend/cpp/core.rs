use crate::ir::SqlType;

pub(super) fn cpp_type(sql_type: &SqlType, nullable: bool) -> String {
    let base = match sql_type {
        SqlType::Boolean => "bool".to_string(),
        SqlType::SmallInt => "std::int16_t".to_string(),
        SqlType::Integer => "std::int32_t".to_string(),
        SqlType::BigInt => "std::int64_t".to_string(),
        SqlType::Real => "float".to_string(),
        SqlType::Double => "double".to_string(),

        // or double?? precision?
        SqlType::Decimal => "std::string".to_string(),
        SqlType::Text | SqlType::Char(_) | SqlType::VarChar(_) => "std::string".to_string(),
        SqlType::Bytes => "std::vector<std::uint8_t>".to_string(),
        
        // chrono or string??
        SqlType::Date => "std::chrono::year_month_day".to_string(),
        SqlType::Time => "std::chrono::seconds".to_string(),
        SqlType::Timestamp | SqlType::TimestampTz => "std::chrono::system_clock::time_point".to_string(),
        SqlType::Interval => "std::chrono::microseconds".to_string(),
        
        // or std::array<std::uint8_t, 16>??
        SqlType::Uuid => "std::string".to_string(),
        
        // or json lib? nlohmann::json?
        SqlType::Json | SqlType::Jsonb => "std::string".to_string(),
        
        SqlType::Array(inner) => format!("std::vector<{}>", cpp_type(inner, false)),
        
        // other??
        SqlType::Custom(_) => "std::string".to_string(),
    };

    if nullable {
        format!("std::optional<{base}>")
    } else {
        base
    }
}
