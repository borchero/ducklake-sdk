use std::sync::LazyLock;

use regex::Regex;

static UNQUOTED_IDENTIFIER: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r#"([a-zA-Z_][a-zA-Z0-9_]*)"#).unwrap());
static QUOTED_IDENTIFIER: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r#""((?:[^"]|"")+)""#).unwrap());

static DOT_SEPARATED_UNQUOTED_IDENTIFIERS: LazyLock<Regex> = LazyLock::new(|| {
    let identifier = UNQUOTED_IDENTIFIER.as_str();
    Regex::new(&format!(r#"^{}(\.{}+)*$"#, identifier, identifier)).unwrap()
});
static DOT_SEPARATED_QUOTED_IDENTIFIERS: LazyLock<Regex> = LazyLock::new(|| {
    let identifier = QUOTED_IDENTIFIER.as_str();
    Regex::new(&format!(r#"^{}(\.{}+)*$"#, identifier, identifier)).unwrap()
});

/// Parse an identifier into its dot-separated components.
pub fn parse_identifier(s: &str) -> Option<Vec<String>> {
    if DOT_SEPARATED_UNQUOTED_IDENTIFIERS.is_match(s) {
        Some(s.split('.').map(str::to_string).collect())
    } else if DOT_SEPARATED_QUOTED_IDENTIFIERS.is_match(s) {
        Some(
            QUOTED_IDENTIFIER
                .captures_iter(s)
                .map(|caps| unquote(&caps[1]))
                .collect(),
        )
    } else {
        None
    }
}

/// Format an identifier from its components, quoting each component as necessary.
pub fn format_identifier<S: AsRef<str>>(components: &[S]) -> String {
    components
        .iter()
        .map(|s| format!("\"{}\"", quote(s.as_ref())))
        .collect::<Vec<_>>()
        .join(".")
}

/* ------------------------------------------- UTILS ------------------------------------------- */

fn quote(s: &str) -> String {
    s.replace("\"", "\"\"")
}

fn unquote(s: &str) -> String {
    s.replace("\"\"", "\"")
}

/* --------------------------------------------------------------------------------------------- */
/*                                             TESTS                                             */
/* --------------------------------------------------------------------------------------------- */

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests {
    use rstest::rstest;

    use super::*;

    #[rstest]
    #[case("a", vec!["a"])]
    #[case("abc", vec!["abc"])]
    #[case("_a", vec!["_a"])]
    #[case("a1", vec!["a1"])]
    #[case("a.b", vec!["a", "b"])]
    #[case("a.b.c", vec!["a", "b", "c"])]
    #[case("\"a\"", vec!["a"])]
    #[case("\"hello world\"", vec!["hello world"])]
    #[case("\"a\".\"b\"", vec!["a", "b"])]
    #[case("\"a\"\"b\"", vec!["a\"b"])]
    #[case("\"a\"\"b\".\"c\"\"d\"", vec!["a\"b", "c\"d"])]
    fn test_parse_identifier_valid(#[case] input: &str, #[case] expected: Vec<&str>) {
        let parsed = parse_identifier(input).unwrap();
        let expected: Vec<String> = expected.into_iter().map(String::from).collect();
        assert_eq!(parsed, expected);
    }

    #[rstest]
    #[case("")]
    #[case("1abc")]
    #[case("a.")]
    #[case(".a")]
    #[case("a..b")]
    #[case("\"unterminated")]
    #[case("a.\"b\"")]
    #[case("\"a\".b")]
    fn test_parse_identifier_invalid(#[case] input: &str) {
        assert!(parse_identifier(input).is_none());
    }

    #[rstest]
    #[case(vec!["a"], "\"a\"")]
    #[case(vec!["a", "b"], "\"a\".\"b\"")]
    #[case(vec!["hello world"], "\"hello world\"")]
    #[case(vec!["a\"b"], "\"a\"\"b\"")]
    #[case(vec!["a\"b", "c\"d"], "\"a\"\"b\".\"c\"\"d\"")]
    fn test_format_identifier(#[case] components: Vec<&str>, #[case] expected: &str) {
        assert_eq!(format_identifier(&components), expected);
    }

    #[rstest]
    #[case(vec!["a"])]
    #[case(vec!["a", "b"])]
    #[case(vec!["hello world"])]
    #[case(vec!["a\"b"])]
    #[case(vec!["a\"b", "c\"d"])]
    fn test_format_parse_roundtrip(#[case] components: Vec<&str>) {
        let formatted = format_identifier(&components);
        let parsed = parse_identifier(&formatted).unwrap();
        let expected: Vec<String> = components.into_iter().map(String::from).collect();
        assert_eq!(parsed, expected);
    }
}
