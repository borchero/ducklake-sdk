use std::sync::LazyLock;

use regex::Regex;

static UNQUOTED_IDENTIFIER: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r#"([a-zA-Z_][a-zA-Z0-9_]*)"#).unwrap());
static QUOTED_IDENTIFIER: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r#""((?:[^"]|"")+)""#).unwrap());

static IDENTIFIER: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(&format!(
        r#"(?:{}|{})"#,
        UNQUOTED_IDENTIFIER.as_str(),
        QUOTED_IDENTIFIER.as_str()
    ))
    .unwrap()
});

static DOT_SEPARATED_UNQUOTED_IDENTIFIERS: LazyLock<Regex> = LazyLock::new(|| {
    let identifier = UNQUOTED_IDENTIFIER.as_str();
    Regex::new(&format!(r#"^{}(\.{}+)*$"#, identifier, identifier)).unwrap()
});
static DOT_SEPARATED_QUOTED_IDENTIFIERS: LazyLock<Regex> = LazyLock::new(|| {
    let identifier = QUOTED_IDENTIFIER.as_str();
    Regex::new(&format!(r#"^{}(\.{}+)*$"#, identifier, identifier)).unwrap()
});
static COMMA_SEPARATED_IDENTIFIERS: LazyLock<Regex> = LazyLock::new(|| {
    let identifier = IDENTIFIER.as_str();
    Regex::new(&format!(r#"^{identifier}(,{identifier})*$"#)).unwrap()
});

/// Parse an identifier into its dot-separated components.
pub(crate) fn parse_identifier(s: &str) -> Option<Vec<String>> {
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
pub(crate) fn format_identifier<S: AsRef<str>>(components: &[S]) -> String {
    components
        .iter()
        .map(|s| format!("\"{}\"", quote(s.as_ref())))
        .collect::<Vec<_>>()
        .join(".")
}

/// Parse a comma-separated list of identifiers.
pub(crate) fn parse_identifier_list(s: &str) -> Option<Vec<String>> {
    if s.is_empty() {
        return Some(Vec::new());
    }
    if !COMMA_SEPARATED_IDENTIFIERS.is_match(s) {
        return None;
    }
    IDENTIFIER
        .find_iter(s)
        .map(|identifier| {
            parse_identifier(identifier.as_str())
                .and_then(|components| components.into_iter().next())
        })
        .collect()
}

/// Format a comma-separated list of identifiers, quoting each identifier as necessary.
pub(crate) fn format_identifier_list<S: AsRef<str>>(identifiers: &[S]) -> String {
    identifiers
        .iter()
        .map(|identifier| format_identifier(std::slice::from_ref(identifier)))
        .collect::<Vec<_>>()
        .join(",")
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

    #[rstest]
    #[case(vec![], "")]
    #[case(vec!["a", "b"], r#""a","b""#)]
    #[case(vec!["last, first"], r#""last, first""#)]
    #[case(vec!["a b", "c\"d"], r#""a b","c""d""#)]
    fn test_format_parse_identifier_list_roundtrip(
        #[case] identifiers: Vec<&str>,
        #[case] expected: &str,
    ) {
        // Arrange
        let expected_identifiers = identifiers
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>();

        // Act
        let formatted = format_identifier_list(&identifiers);
        let parsed = parse_identifier_list(&formatted).unwrap();

        // Assert
        assert_eq!(formatted, expected);
        assert_eq!(parsed, expected_identifiers);
    }

    #[rstest]
    #[case("a,b", vec!["a", "b"])]
    #[case(r#""a",b"#, vec!["a", "b"])]
    fn test_parse_identifier_list_valid(#[case] input: &str, #[case] expected: Vec<&str>) {
        // Arrange
        let expected = expected.into_iter().map(String::from).collect::<Vec<_>>();

        // Act
        let parsed = parse_identifier_list(input).unwrap();

        // Assert
        assert_eq!(parsed, expected);
    }

    #[rstest]
    #[case(r#""unterminated"#)]
    #[case("a,")]
    #[case("a.b")]
    fn test_parse_identifier_list_invalid(#[case] input: &str) {
        // Act
        let parsed = parse_identifier_list(input);

        // Assert
        assert!(parsed.is_none());
    }
}
