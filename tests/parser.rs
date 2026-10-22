use datafusion_kql::ast::{BinaryOp, BindingValue, ExprKind, Literal, Operator, Source};
use datafusion_kql::{Error, ParserOptions, parse, parse_with_options};

#[test]
fn precedence_and_spans() {
    let input = "Events | where a + 2 * 3 > 6 and b == 1 or c != 0";
    let query = parse(input).unwrap();
    let Operator::Where(predicate) = &query.pipeline.operators[0].kind else {
        panic!("expected a filter");
    };
    let ExprKind::Binary {
        left,
        op: BinaryOp::Or,
        ..
    } = &predicate.kind
    else {
        panic!("or should have the lowest precedence");
    };
    assert!(matches!(
        left.kind,
        ExprKind::Binary {
            op: BinaryOp::And,
            ..
        }
    ));
    assert_eq!(
        &input[predicate.span.start..predicate.span.end],
        "a + 2 * 3 > 6 and b == 1 or c != 0"
    );
}

#[test]
fn comments_escaped_identifiers_and_verbatim_strings() {
    let query = parse("// header\n['Events.v1'] | project ['path name'] = @'C:\\logs\\app', s = 'it\\'s fine' // end").unwrap();
    assert!(matches!(&query.pipeline.source, Source::Table { name, .. } if name == &["Events.v1"]));
    let Operator::Project(items) = &query.pipeline.operators[0].kind else {
        panic!("expected project");
    };
    assert_eq!(items[0].name.as_deref(), Some("path name"));
    assert_eq!(
        items[0].expr.kind,
        ExprKind::Literal(Literal::String("C:\\logs\\app".into()))
    );
    assert_eq!(
        items[1].expr.kind,
        ExprKind::Literal(Literal::String("it's fine".into()))
    );
}

#[test]
fn parses_bindings_and_nested_pipelines() {
    let query =
        parse("let n = 2; let t = Events | take 3; t | join (Events | where x > n) on x;").unwrap();
    assert_eq!(query.bindings.len(), 2);
    assert!(matches!(query.bindings[0].value, BindingValue::Scalar(_)));
    assert!(matches!(query.bindings[1].value, BindingValue::Tabular(_)));
    assert!(matches!(
        query.pipeline.operators[0].kind,
        Operator::Join { .. }
    ));
}

#[test]
fn typed_literals_and_decimal_timespans() {
    let query =
        parse("print t=datetime(2025-01-02T03:04:05.1234567Z), d=1.25h, n=long(null)").unwrap();
    let Source::Print(items) = query.pipeline.source else {
        panic!("expected print");
    };
    assert!(matches!(
        items[0].expr.kind,
        ExprKind::Literal(Literal::Typed { .. })
    ));
    assert_eq!(
        items[1].expr.kind,
        ExprKind::Literal(Literal::Timespan("1.25h".into()))
    );
}

#[test]
fn sort_defaults_are_kql_not_sql() {
    let query = parse("T | sort by a, b asc, c desc nulls first").unwrap();
    let Operator::Sort(keys) = &query.pipeline.operators[0].kind else {
        panic!("expected sort");
    };
    assert!(!keys[0].ascending);
    assert!(!keys[0].nulls_first);
    assert!(keys[1].ascending);
    assert!(keys[1].nulls_first);
    assert!(!keys[2].ascending);
    assert!(keys[2].nulls_first);
}

#[test]
fn between_in_and_negated_string_operators() {
    parse("T | where a between (1..10) and b !in~ ('x', 'y') and s !contains_cs 'z'").unwrap();
    parse("T | where a !between (1 .. 10) or s matches regex @'\\d+'").unwrap();
}

#[test]
fn qualified_join_and_bare_right_table() {
    parse("L | join kind=leftouter R on $left.a == $right.b, $left.c == $right.d").unwrap();
}

#[test]
fn query_size_and_nesting_are_bounded() {
    let options = ParserOptions {
        max_query_bytes: 4,
        ..Default::default()
    };
    assert!(parse_with_options("print x=1", &options).is_err());
    let nested = format!("print x={}1{}", "(".repeat(200), ")".repeat(200));
    assert!(
        parse(&nested)
            .unwrap_err()
            .to_string()
            .contains("nesting limit")
    );
    let chain = format!("print x=1{}", "+1".repeat(200));
    assert!(
        parse(&chain)
            .unwrap_err()
            .to_string()
            .contains("depth limit")
    );
    let unary = format!("print x={}true", "!".repeat(200));
    assert!(parse(&unary).is_err());
}

#[test]
fn pipelines_are_bounded() {
    let query = format!("T{}", " | take 1".repeat(200));
    assert!(
        parse(&query)
            .unwrap_err()
            .to_string()
            .contains("operator limit")
    );
}

#[test]
fn invalid_syntax_reports_source_location() {
    let input = "T\n| where x >";
    let error = parse(input).unwrap_err();
    assert!(matches!(error, Error::Syntax { .. }));
    let diagnostic = error.diagnostic(input);
    assert!(diagnostic.contains("2:12"), "{diagnostic}");
    assert!(diagnostic.contains('^'));
}

#[test]
fn malformed_queries_are_errors_not_panics() {
    for query in [
        "",
        "T |",
        "print x='unfinished",
        "print x='\\q'",
        "print x='\\uZZZZ'",
        "print x=1e",
        "T | take -1",
        "T | top 1.5 by x",
        "let x=1 T",
        "datatable(x:long)[1,2,3,4] | where",
        "datatable(a:long,b:string)[1]",
        "T | join (R) on $left.x == x",
        "print d=datetime(2025-01-01",
        "T | where a in (1,)",
        "T; T",
        "T | where x between (1,2)",
    ] {
        assert!(parse(query).is_err(), "{query}");
    }
}

#[test]
fn unsupported_operators_and_types_are_explicit() {
    assert!(matches!(
        parse("T | unsupported-operator x"),
        Err(Error::Unsupported { .. })
    ));
    assert!(matches!(
        parse("datatable(x:unsupported_type)[]"),
        Err(Error::Unsupported { .. })
    ));
}

#[test]
fn unicode_diagnostics_use_character_columns() {
    let input = "print café = 1, x = ?";
    let error = parse(input).unwrap_err();
    assert!(error.diagnostic(input).contains("1:21"));
}

#[test]
fn generated_malformed_inputs_do_not_panic() {
    let fragments = [
        "'",
        "\"",
        "['",
        "[]",
        "(",
        ")",
        "|",
        "let ",
        "where ",
        "in ",
        "!",
        "1e+",
        "1..2",
        "datetime(",
        "x",
        "λ",
        " ",
        ",",
        ";",
        "\\",
        "@'",
    ];
    let mut state = 0x5a17_u64;
    for _ in 0..2_000 {
        let mut input = String::new();
        for _ in 0..32 {
            state = state.wrapping_mul(6364136223846793005).wrapping_add(1);
            input.push_str(fragments[(state as usize) % fragments.len()]);
        }
        let _ = parse(&input);
    }
}
