use std::sync::LazyLock;

use indexmap::IndexMap;

use super::Literal;
use crate::{DucklakeError, DucklakeResult};

/* -------------------------------------------- LIST ------------------------------------------- */

impl Literal for Vec<String> {
    fn parse(s: &str) -> DucklakeResult<Self> {
        let inner = s
            .strip_prefix('[')
            .and_then(|s| s.strip_suffix(']'))
            .ok_or_else(|| {
                DucklakeError::Parsing(format!("list literal is not enclosed in brackets: {s}"))
            })?;
        let result = parse_comma_separated(inner)
            .iter()
            .map(|item| unescape_str(item))
            .collect();
        Ok(result)
    }

    fn format(&self) -> String {
        let elements = self
            .iter()
            .map(|s| escape_str(s))
            .collect::<Vec<_>>()
            .join(", ");
        format!("[{}]", elements)
    }
}

/* ------------------------------------------- STRUCT ------------------------------------------ */

impl Literal for IndexMap<String, String> {
    fn parse(s: &str) -> DucklakeResult<Self> {
        // Split a struct literal of the form `{'key': value, 'key2': value2, ...}`
        // into pairs of `(unescaped key, unescaped value)` strings.
        let inner = s
            .strip_prefix('{')
            .and_then(|s| s.strip_suffix('}'))
            .ok_or_else(|| {
                DucklakeError::Parsing(format!("struct literal is not enclosed in braces: {s}"))
            })?;
        Ok(parse_struct_entries(inner))
    }

    fn format(&self) -> String {
        let elements = self
            .iter()
            .map(|(k, v)| format!("{}: {}", escape_str(k), escape_str(v)))
            .collect::<Vec<_>>()
            .join(", ");
        format!("{{{}}}", elements)
    }
}

/// Parse struct entries in the format `'key': value, 'key2': value2, ...`.
/// Returns pairs of (unescaped key, unescaped value) strings.
fn parse_struct_entries(s: &str) -> IndexMap<String, String> {
    if s.is_empty() {
        return IndexMap::new();
    }
    let elements = parse_comma_separated(s);
    elements
        .into_iter()
        .filter_map(|entry| {
            split_on_colon(&entry).map(|(k, v)| (unescape_str(k.trim()), unescape_str(v.trim())))
        })
        .collect()
}

/// Split a string on `:`, respecting quotes.
/// Returns (key, value) or None if no `:` found outside quotes.
fn split_on_colon(s: &str) -> Option<(String, String)> {
    // Pattern for a single token: unquoted chars OR a quoted string with escapes
    // - `[^':]` matches any char except quotes and colons
    // - `'(?:[^'\\]|\\.)*'` matches a quoted string (handles \' and \\ escapes)
    const TOKEN: &str = r"(?:[^':]|'(?:[^'\\]|\\.)*')*";
    static RE: LazyLock<regex::Regex> =
        LazyLock::new(|| regex::Regex::new(&format!("^({TOKEN}): ?({TOKEN})$")).unwrap());
    RE.captures(s)
        .map(|caps| (caps[1].to_string(), caps[2].to_string()))
}

/* -------------------------------------------- MAP -------------------------------------------- */

impl Literal for Vec<(String, String)> {
    fn parse(s: &str) -> DucklakeResult<Self> {
        // Split a map literal of the form `{key1=value1, key2=value2, ...}` into
        // pairs of unescaped `(key, value)` strings.
        let inner = s
            .strip_prefix('{')
            .and_then(|s| s.strip_suffix('}'))
            .ok_or_else(|| {
                DucklakeError::Parsing(format!("map literal is not enclosed in braces: {s}"))
            })?;
        Ok(parse_map_entries(inner))
    }

    fn format(&self) -> String {
        let elements = self
            .iter()
            .map(|(k, v)| format!("{}={}", escape_str(k), escape_str(v)))
            .collect::<Vec<_>>()
            .join(", ");
        format!("{{{}}}", elements)
    }
}

/// Parse map entries in the format `key=value, key=value, ...`.
/// Returns pairs of unescaped key-value strings.
fn parse_map_entries(s: &str) -> Vec<(String, String)> {
    if s.is_empty() {
        return Vec::new();
    }
    let elements = parse_comma_separated(s);
    elements
        .into_iter()
        .filter_map(|entry| {
            split_on_equals(&entry).map(|(k, v)| (unescape_str(k.trim()), unescape_str(v.trim())))
        })
        .collect()
}

/// Split a string on `=`, respecting quotes.
/// Returns (key, value) or None if no `=` found outside quotes.
fn split_on_equals(s: &str) -> Option<(String, String)> {
    // Pattern for a single token: unquoted chars OR a quoted string with escapes
    // - `[^'=]` matches any char except quotes and equals
    // - `'(?:[^'\\]|\\.)*'` matches a quoted string (handles \' and \\ escapes)
    const TOKEN: &str = r"(?:[^'=]|'(?:[^'\\]|\\.)*')*";
    static RE: LazyLock<regex::Regex> =
        LazyLock::new(|| regex::Regex::new(&format!("^({TOKEN})=({TOKEN})$")).unwrap());
    RE.captures(s)
        .map(|caps| (caps[1].to_string(), caps[2].to_string()))
}

/* --------------------------------------------------------------------------------------------- */
/*                                             UTILS                                             */
/* --------------------------------------------------------------------------------------------- */

/* ------------------------------------------ PARSING ------------------------------------------ */

/// Unescape a string that may have been escaped by the corresponding
/// formatter. If wrapped in single quotes, removes quotes and unescapes
/// `\'` and `\\`.
fn unescape_str(s: &str) -> String {
    static RE: LazyLock<regex::Regex> = LazyLock::new(|| regex::Regex::new(r"\\([\\'])").unwrap());

    if let Some(inner) = s.strip_prefix('\'').and_then(|s| s.strip_suffix('\'')) {
        RE.replace_all(inner, "$1").into_owned()
    } else {
        s.to_string()
    }
}

/// Parse comma-separated values, respecting quotes and nested brackets.
/// Returns the raw elements without unescaping.
fn parse_comma_separated(s: &str) -> Vec<String> {
    if s.is_empty() {
        return Vec::new();
    }
    let mut elements = Vec::new();
    let mut current = String::new();
    let mut in_quotes = false;
    let mut bracket_depth = 0;
    let mut brace_depth = 0;
    let mut chars = s.chars().peekable();

    while let Some(c) = chars.next() {
        match c {
            '\\' if in_quotes => {
                current.push(c);
                if let Some(&next) = chars.peek()
                    && (next == '\\' || next == '\'')
                {
                    current.push(chars.next().unwrap());
                }
            }
            '\'' if !in_quotes => {
                in_quotes = true;
                current.push(c);
            }
            '\'' if in_quotes => {
                in_quotes = false;
                current.push(c);
            }
            '[' if !in_quotes => {
                bracket_depth += 1;
                current.push(c);
            }
            ']' if !in_quotes => {
                bracket_depth -= 1;
                current.push(c);
            }
            '{' if !in_quotes => {
                brace_depth += 1;
                current.push(c);
            }
            '}' if !in_quotes => {
                brace_depth -= 1;
                current.push(c);
            }
            ',' if !in_quotes && bracket_depth == 0 && brace_depth == 0 => {
                elements.push(current.trim().to_string());
                current = String::new();
            }
            _ => {
                current.push(c);
            }
        }
    }
    elements.push(current.trim().to_string());
    elements
}

/* ----------------------------------------- FORMATTING ---------------------------------------- */

/// Always quote a string with single quotes and escape internal quotes/backslashes.
fn quote_str(s: &str) -> String {
    format!("'{}'", s.replace('\\', "\\\\").replace('\'', "\\'"))
}

/// Escape a string only if it contains special characters.
fn escape_str(s: &str) -> String {
    if s.contains('\\')
        || s.contains('\'')
        || s.contains(',')
        || s.contains('=')
        || s.contains(':')
    {
        quote_str(s)
    } else {
        s.to_string()
    }
}

/* --------------------------------------------------------------------------------------------- */
/*                                             TESTS                                             */
/* --------------------------------------------------------------------------------------------- */

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests {
    use rstest::rstest;

    use super::*;

    /* -------------------------------------------- LIST ------------------------------------------ */

    #[rstest]
    #[case("[]", vec![])]
    #[case("[a]", vec!["a"])]
    #[case("[a, b, c]", vec!["a", "b", "c"])]
    #[case("['hello, world']", vec!["hello, world"])]
    #[case("['it\\'s', 'fine']", vec!["it's", "fine"])]
    #[case("['a\\\\b']", vec!["a\\b"])]
    fn test_list_parse(#[case] input: &str, #[case] expected: Vec<&str>) {
        let parsed = <Vec<String> as Literal>::parse(input).unwrap();
        let expected: Vec<String> = expected.into_iter().map(String::from).collect();
        assert_eq!(parsed, expected);
    }

    #[rstest]
    #[case(vec![], "[]")]
    #[case(vec!["a"], "[a]")]
    #[case(vec!["a", "b", "c"], "[a, b, c]")]
    #[case(vec!["hello, world"], "['hello, world']")]
    #[case(vec!["it's"], "['it\\'s']")]
    #[case(vec!["a\\b"], "['a\\\\b']")]
    fn test_list_format(#[case] input: Vec<&str>, #[case] expected: &str) {
        let value: Vec<String> = input.into_iter().map(String::from).collect();
        assert_eq!(value.format(), expected);
    }

    #[rstest]
    #[case("[]")]
    #[case("[a]")]
    #[case("[a, b, c]")]
    #[case("['hello, world']")]
    #[case("['it\\'s', fine]")]
    fn test_list_roundtrip(#[case] input: &str) {
        let parsed = <Vec<String> as Literal>::parse(input).unwrap();
        assert_eq!(parsed.format(), input);
    }

    #[test]
    fn test_list_parse_missing_brackets() {
        assert!(<Vec<String> as Literal>::parse("a, b").is_err());
    }

    /* ------------------------------------------- STRUCT ----------------------------------------- */

    #[rstest]
    #[case("{}", vec![])]
    #[case("{a: 1}", vec![("a", "1")])]
    #[case("{a: 1, b: 2}", vec![("a", "1"), ("b", "2")])]
    #[case("{'key with: colon': value}", vec![("key with: colon", "value")])]
    #[case("{key: 'value, with comma'}", vec![("key", "value, with comma")])]
    fn test_struct_parse(#[case] input: &str, #[case] expected: Vec<(&str, &str)>) {
        let parsed = <IndexMap<String, String> as Literal>::parse(input).unwrap();
        let expected: IndexMap<String, String> = expected
            .into_iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        assert_eq!(parsed, expected);
    }

    #[rstest]
    #[case(vec![], "{}")]
    #[case(vec![("a", "1")], "{a: 1}")]
    #[case(vec![("a", "1"), ("b", "2")], "{a: 1, b: 2}")]
    #[case(vec![("key with: colon", "value")], "{'key with: colon': value}")]
    #[case(vec![("key", "value, with comma")], "{key: 'value, with comma'}")]
    fn test_struct_format(#[case] input: Vec<(&str, &str)>, #[case] expected: &str) {
        let value: IndexMap<String, String> = input
            .into_iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        assert_eq!(value.format(), expected);
    }

    #[rstest]
    #[case("{}")]
    #[case("{a: 1}")]
    #[case("{a: 1, b: 2}")]
    #[case("{'key with: colon': value}")]
    #[case("{key: 'value, with comma'}")]
    fn test_struct_roundtrip(#[case] input: &str) {
        let parsed = <IndexMap<String, String> as Literal>::parse(input).unwrap();
        assert_eq!(parsed.format(), input);
    }

    #[test]
    fn test_struct_parse_missing_braces() {
        assert!(<IndexMap<String, String> as Literal>::parse("a: 1").is_err());
    }

    /* -------------------------------------------- MAP ------------------------------------------- */

    #[rstest]
    #[case("{}", vec![])]
    #[case("{a=1}", vec![("a", "1")])]
    #[case("{a=1, b=2}", vec![("a", "1"), ("b", "2")])]
    #[case("{'key=with=equals'=value}", vec![("key=with=equals", "value")])]
    #[case("{key='value, with comma'}", vec![("key", "value, with comma")])]
    fn test_map_parse(#[case] input: &str, #[case] expected: Vec<(&str, &str)>) {
        let parsed = <Vec<(String, String)> as Literal>::parse(input).unwrap();
        let expected: Vec<(String, String)> = expected
            .into_iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        assert_eq!(parsed, expected);
    }

    #[rstest]
    #[case(vec![], "{}")]
    #[case(vec![("a", "1")], "{a=1}")]
    #[case(vec![("a", "1"), ("b", "2")], "{a=1, b=2}")]
    #[case(vec![("key=with=equals", "value")], "{'key=with=equals'=value}")]
    #[case(vec![("key", "value, with comma")], "{key='value, with comma'}")]
    fn test_map_format(#[case] input: Vec<(&str, &str)>, #[case] expected: &str) {
        let value: Vec<(String, String)> = input
            .into_iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        assert_eq!(value.format(), expected);
    }

    #[rstest]
    #[case("{}")]
    #[case("{a=1}")]
    #[case("{a=1, b=2}")]
    #[case("{'key=with=equals'=value}")]
    #[case("{key='value, with comma'}")]
    fn test_map_roundtrip(#[case] input: &str) {
        let parsed = <Vec<(String, String)> as Literal>::parse(input).unwrap();
        assert_eq!(parsed.format(), input);
    }

    #[test]
    fn test_map_parse_missing_braces() {
        assert!(<Vec<(String, String)> as Literal>::parse("a=1").is_err());
    }
}
