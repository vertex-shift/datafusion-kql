use datafusion::arrow::array::Array;
use datafusion::arrow::util::display::array_value_to_string;
use datafusion::prelude::*;
use datafusion_kql::SessionContextKqlExt;

async fn check(query: &str, expected: &[&[&str]]) {
    let ctx = SessionContext::new_with_config(
        SessionConfig::new()
            .with_batch_size(2)
            .with_target_partitions(4),
    );
    let frame = ctx
        .kql(query)
        .await
        .unwrap_or_else(|e| panic!("{}\n{query}", e.diagnostic(query)));
    let batches = frame
        .collect()
        .await
        .unwrap_or_else(|e| panic!("{e}\n{query}"));
    let actual = batches
        .iter()
        .flat_map(|batch| {
            (0..batch.num_rows()).map(|row| {
                batch
                    .columns()
                    .iter()
                    .map(|array| {
                        if array.logical_nulls().is_some_and(|n| n.is_null(row)) {
                            "<null>".into()
                        } else {
                            array_value_to_string(array, row).unwrap()
                        }
                    })
                    .collect::<Vec<_>>()
            })
        })
        .collect::<Vec<_>>();
    let expected = expected
        .iter()
        .map(|row| row.iter().map(|s| s.to_string()).collect::<Vec<_>>())
        .collect::<Vec<_>>();
    assert_eq!(actual, expected, "{query}");
}

#[tokio::test]
async fn has_any_and_all_use_case_insensitive_term_boundaries() {
    check("datatable(s:string)['North America','SOUTH DAKOTA','terror','error_disk','small'] | project s,a=s has_any('north','error'),b=s has_all('north','america')",
        &[&["North America","true","true"],&["SOUTH DAKOTA","false","false"],&["terror","false","false"],&["error_disk","true","false"],&["small","false","false"]]).await;
}

#[tokio::test]
async fn dynamic_lists_nested_lists_and_lexical_values_are_flattened() {
    check("let terms=dynamic(['north',['south','error']]); print a='South East' has_any(terms),b='north south' has_all(terms),c='North South Error' has_all(terms)",
        &[&["true","false","true"]]).await;
    check("print a='error' has_any('ERROR','error','Error'),b='error' has_all('ERROR','error','Error')", &[&["true","true"]]).await;
}

#[tokio::test]
async fn empty_lists_and_empty_terms_have_distinct_boolean_semantics() {
    check("print a='text' has_any(dynamic([])),b='text' has_all(dynamic([])),c='text' has_any(''),d='text' has_all('')",
        &[&["false","true","false","false"]]).await;
}

#[tokio::test]
async fn prefix_suffix_operators_support_case_and_negation() {
    check("print a='North America' hasprefix 'ame',b='North America' !hasprefix 'mer',c='North America' hasprefix_cs 'Ame',d='North America' !hasprefix_cs 'ame',e='North America' hassuffix 'ICA',f='North America' hassuffix_cs 'ica',g='North America' !hassuffix_cs 'ICA'",
        &[&["true","true","true","true","true","true","true"]]).await;
    check("print a='alphabet' hasprefix 'pha',b='alphabet' hassuffix 'pha',c='foo_hello' hasprefix 'hell',d='foo_hello' hassuffix 'ello'",
        &[&["false","false","true","true"]]).await;
}

#[tokio::test]
async fn term_sets_support_unicode_short_terms_and_literal_regex_characters() {
    check("print a='Österreich 中文 x' has_all('österreich','中文','x'),b='item a.b value' has_any('a.b'),c='item axb value' has_any('a.b')",
        &[&["true","true","false"]]).await;
}

#[tokio::test]
async fn table_sources_are_lazy_and_use_only_the_first_column() {
    check("let T=datatable(term:string,ignored:long)['north',1,'south',2]; datatable(s:string)['North East','South West','other'] | where s has_any(T) | project s | sort by s asc",
        &[&["North East"],&["South West"]]).await;
    check("print a='north south' has_all((datatable(term:string)['north','south'])),b='north' has_all((datatable(term:string)['north','south']))",
        &[&["true","false"]]).await;
}

#[tokio::test]
async fn tabular_functions_and_empty_sources_keep_query_context() {
    check("let T=(){datatable(term:string)['north','south']}; print a='north' has_any(T()),b='text' has_all((datatable(term:string)[]))",
        &[&["true","true"]]).await;
    check(
        "let T=datatable(term:string)['north']; let f=(s:string){s has_any(T)}; print a=f('NORTH')",
        &[&["true"]],
    )
    .await;
}

#[tokio::test]
async fn scalar_functions_can_expand_constant_term_lists_and_dynamic_sources() {
    check(
        "let f=(s:string){s has_any('error','warn')}; print a=f('WARN disk')",
        &[&["true"]],
    )
    .await;
    check(
        "print d=dynamic('North America') | project a=d has_any('north'),b=d hasprefix 'ame'",
        &[&["true", "true"]],
    )
    .await;
}

#[tokio::test]
async fn source_terms_are_checked_instead_of_silently_truncated() {
    let ctx = SessionContext::new();
    let terms = (0..257)
        .map(|i| format!("'term{i}'"))
        .collect::<Vec<_>>()
        .join(",");
    let query = format!("print a='term0' has_all({terms})");
    assert!(ctx.kql(&query).await.is_err());
    let frame=ctx.kql("print a='word' has_all((range x from 1 to 257 step 1 | project term=strcat('term',x)))").await.unwrap();
    assert!(frame.collect().await.is_err());
}

#[tokio::test]
async fn row_dependent_rhs_and_nonstring_sources_are_rejected() {
    let ctx = SessionContext::new();
    for query in [
        "print s='text' | project a='text' has_any(s)",
        "print a=1 has_any('one')",
        "print a='text' has_any()",
        "print a='text' has_all((print a=1 | project-away *))",
    ] {
        assert!(ctx.kql(query).await.is_err(), "{query}");
    }
}
