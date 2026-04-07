/// Convert snake_case to PascalCase: `get_user` → `GetUser`.
pub fn to_pascal_case(s: &str) -> String {
    s.split('_')
        .map(|w| {
            let mut c = w.chars();
            match c.next() {
                None => String::new(),
                Some(f) => f.to_uppercase().collect::<String>() + c.as_str(),
            }
        })
        .collect()
}

/// Convert snake_case/PascalCase to camelCase: `get_user` → `getUser`.
pub fn to_camel_case(s: &str) -> String {
    let pascal = to_pascal_case(s);
    let mut c = pascal.chars();
    match c.next() {
        None => String::new(),
        Some(f) => f.to_lowercase().collect::<String>() + c.as_str(),
    }
}

/// Convert a string to SCREAMING_SNAKE_CASE: `active` → `ACTIVE`, `my_status` → `MY_STATUS`.
pub fn to_screaming_snake_case(s: &str) -> String {
    to_snake_case(&to_pascal_case(s)).to_uppercase()
}

/// Convert PascalCase/camelCase to snake_case: `GetUserById` → `get_user_by_id`.
pub fn to_snake_case(s: &str) -> String {
    let mut out = String::new();
    for (i, c) in s.chars().enumerate() {
        if c.is_uppercase() {
            if i > 0 {
                out.push('_');
            }
            out.extend(c.to_lowercase());
        } else {
            out.push(c);
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_to_pascal_case_snake_input() {
        assert_eq!(to_pascal_case("get_user_by_id"), "GetUserById");
    }

    #[test]
    fn test_to_pascal_case_single_word() {
        assert_eq!(to_pascal_case("user"), "User");
    }

    #[test]
    fn test_to_camel_case_snake_input() {
        assert_eq!(to_camel_case("get_user"), "getUser");
    }

    #[test]
    fn test_to_camel_case_single_word() {
        assert_eq!(to_camel_case("user"), "user");
    }

    #[test]
    fn test_to_snake_case_pascal_input() {
        assert_eq!(to_snake_case("GetUserById"), "get_user_by_id");
    }

    #[test]
    fn test_to_snake_case_already_lowercase() {
        assert_eq!(to_snake_case("user"), "user");
    }
}
