use std::fmt;
use std::str::FromStr;

#[derive(Debug, Clone, PartialEq, Eq, sqlx::Type)]
#[sqlx(type_name = "genre", rename_all = "snake_case")]
pub enum Genre {
    Fiction,
    NonFiction,
    Science,
    History,
    Biography,
}

impl fmt::Display for Genre {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Fiction => "fiction",
            Self::NonFiction => "non_fiction",
            Self::Science => "science",
            Self::History => "history",
            Self::Biography => "biography",
        })
    }
}

impl FromStr for Genre {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "fiction" => Ok(Self::Fiction),
            "non_fiction" => Ok(Self::NonFiction),
            "science" => Ok(Self::Science),
            "history" => Ok(Self::History),
            "biography" => Ok(Self::Biography),
            _ => Err(format!("unknown Genre: {}", s)),
        }
    }
}
