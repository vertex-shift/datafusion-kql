use std::sync::Arc;

use datafusion::arrow::array::{Array, Int64Array, StringArray};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::arrow::util::display::array_value_to_string;
use datafusion::common::TableReference;
use datafusion::prelude::*;
use datafusion_kql::SessionContextKqlExt;

async fn check(query: &str, expected: &[&[&str]]) {
    let ctx = SessionContext::new();
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
                    .map(|column| {
                        if column
                            .logical_nulls()
                            .is_some_and(|nulls| nulls.is_null(row))
                        {
                            "<null>".to_owned()
                        } else {
                            array_value_to_string(column, row).unwrap()
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
async fn dynamic_literals_preserve_distinct_primitive_types() {
    check(
        "print a=gettype(dynamic(1)), b=gettype(dynamic(1.5)), c=gettype(dynamic(true)), d=gettype(dynamic('1')), e=gettype(dynamic([])), f=gettype(dynamic({})), g=gettype(dynamic(null)), h=gettype(dynamic(datetime(2025-01-01))), i=gettype(dynamic(1h))",
        &[&["long","double","bool","string","array","dictionary","null","datetime","timespan"]],
    ).await;
}

#[tokio::test]
async fn native_type_reporting_uses_logical_dictionary_types_even_for_nulls() {
    use datafusion::arrow::array::{DictionaryArray, Int8Array, Int32Array};
    use datafusion::arrow::datatypes::Int8Type;
    let ctx = SessionContext::new();
    let dictionary = DictionaryArray::<Int8Type>::try_new(
        Int8Array::from(vec![Some(0), Some(1), None]),
        Arc::new(Int32Array::from(vec![Some(10), None])),
    )
    .unwrap();
    ctx.register_batch(
        TableReference::bare("T"),
        RecordBatch::try_from_iter(vec![("x", Arc::new(dictionary) as _)]).unwrap(),
    )
    .unwrap();
    let batches = ctx
        .kql("T | project TypeName=gettype(x)")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 3);
    for batch in batches {
        for row in 0..batch.num_rows() {
            assert_eq!(array_value_to_string(batch.column(0), row).unwrap(), "int");
        }
    }
    let batches = ctx
        .kql("T | getschema")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_eq!(
        array_value_to_string(batches[0].column(2), 0).unwrap(),
        "System.Int32"
    );
    assert_eq!(
        array_value_to_string(batches[0].column(3), 0).unwrap(),
        "int"
    );
}

#[tokio::test]
async fn objects_arrays_and_postfix_access_compose() {
    check(
        "let d=dynamic({'a':{'b':[1,'hello',null]}, 'spaced key':true}); print a=tolong(d.a.b[0]), b=tostring(d['a']['b'][1]), c=tobool(d['spaced key']), n=gettype(d.a.b[2]), m=gettype(d.missing), last=tostring((d.a.b)[-2])",
        &[&["1","hello","true","null","null","hello"]],
    ).await;
}

#[tokio::test]
async fn property_access_after_calls_and_escaped_column_names() {
    check(
        "print ['a.b']=parse_json('{\"x\":42}') | project value=tolong(['a.b'].x), direct=tostring(parse_json('{\"x\":\"hi\"}').x)",
        &[&["42","hi"]],
    ).await;
}

#[tokio::test]
async fn parse_json_keeps_invalid_input_as_a_dynamic_string() {
    check(
        "print a=gettype(parse_json('bad JSON')), b=tostring(parse_json('bad JSON')), c=gettype(parse_json('null')), d=gettype(parse_json('42')), e=tostring(parse_json('\"hello\"')), f=tostring(parse_json('{\"x\":1}'))",
        &[&["string","bad JSON","null","long","hello","{\"x\":1}"]],
    ).await;
}

#[tokio::test]
async fn dynamic_conversions_use_native_child_casts() {
    check(
        "print a=tolong(dynamic('42')), b=toint(dynamic(42)), c=toreal(dynamic(1)), d=tobool(dynamic(1)), e=tostring(dynamic(null)), f=tolong(dynamic('bad')), g=todatetime(dynamic(datetime(2025-01-01)))",
        &[&["42","42","1.0","true","","<null>","2025-01-01T00:00:00"]],
    ).await;
}

#[tokio::test]
async fn dynamic_equality_and_nulls_match_kql() {
    check(
        "print a=dynamic(1)==1, b=dynamic('x')=='x', c=dynamic(null)==1, d=dynamic(null)!=1, e=dynamic(null)==dynamic(null), f=dynamic({'x':1,'y':2})==dynamic({'y':2,'x':1}), g=isnull(dynamic(null))",
        &[&["true","true","false","true","<null>","true","true"]],
    ).await;
}

#[tokio::test]
async fn dynamic_datatable_is_a_real_union_not_a_string_column() {
    use datafusion::arrow::datatypes::DataType;
    let ctx = SessionContext::new();
    let frame = ctx
        .kql("datatable(d:dynamic)[dynamic(1),dynamic('text'),dynamic({'x':2}),dynamic(null)]")
        .await
        .unwrap();
    assert!(matches!(
        frame.schema().field(0).data_type(),
        DataType::Union(_, _)
    ));
    assert_eq!(frame.collect().await.unwrap()[0].num_rows(), 4);
    check(
        "datatable(d:dynamic)[dynamic(1),dynamic('text'),dynamic({'x':2}),dynamic(null)] | project t=gettype(d), s=tostring(d)",
        &[&["long","1"],&["string","text"],&["dictionary","{\"x\":2}"],&["null",""]],
    ).await;
}

#[tokio::test]
async fn batch_json_processing_handles_mixed_rows_without_changing_registry() {
    let ctx = SessionContext::new();
    let batch = RecordBatch::try_from_iter(vec![
        (
            "raw",
            Arc::new(StringArray::from(vec![
                "{\"x\":1}",
                "{\"x\":\"2\"}",
                "{}",
                "bad",
            ])) as _,
        ),
        ("index", Arc::new(Int64Array::from(vec![0, 1, 2, 3])) as _),
    ])
    .unwrap();
    ctx.register_batch(TableReference::bare("T"), batch)
        .unwrap();
    let registry = ctx.state().scalar_functions().len();
    let batches = ctx
        .kql("T | extend d=parse_json(raw) | project x=tolong(d.x), index")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let values = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(
        values.iter().collect::<Vec<_>>(),
        vec![Some(1), Some(2), None, None]
    );
    assert_eq!(ctx.state().scalar_functions().len(), registry);
}

#[tokio::test]
async fn bags_pack_merge_query_and_remove_keys() {
    check(
        "let b=bag_pack('a',1,'b',dynamic([2,3])); print a=bag_has_key(b,'a'), n=bag_has_key(b,'absent'), keys=array_length(bag_keys(b)), merged=tostring(bag_merge(b,dynamic({'a':99,'c':4}))), set=tostring(bag_set_key(b,'c',true)), removed=tostring(bag_remove_keys(b,dynamic(['b'])))",
        &[&["true","false","2","{\"a\":1,\"b\":[2,3],\"c\":4}","{\"a\":1,\"b\":[2,3],\"c\":true}","{\"a\":1}"]],
    ).await;
}

#[tokio::test]
async fn bag_remove_keys_jsonpaths_remove_nested_and_root_properties() {
    check(
        r#"datatable(input:dynamic)[
            dynamic({'key1':123,'key2':{'prop1':'abc','prop2':'xyz'},'key3':[100,200]})]
        | project result=tostring(bag_remove_keys(input,dynamic(['$.key2.prop1','key3'])))"#,
        &[&[r#"{"key1":123,"key2":{"prop2":"xyz"}}"#]],
    )
    .await;
}

#[tokio::test]
async fn bag_remove_keys_jsonpaths_preserve_parents_and_ignore_missing_properties() {
    check(
        r#"datatable(b:dynamic,keys:dynamic)[
            dynamic({'a':{'b':1}}),dynamic(['$.a.b','$.missing.b']),
            dynamic({'a':1}),dynamic(['$.a.b']),
            dynamic({'a':null}),dynamic(['$.a.b']),
            dynamic({'a':[1]}),dynamic(['$.a.b']),
            dynamic({'a':{'b':1},'keep':2}),dynamic(['a','$.a.b','a']),
            dynamic({'a':{'b':1},'keep':2}),dynamic(['$.a.b','a']),
            dynamic({'a.b':1,'a':{'b':2}}),dynamic(['a.b']),
            dynamic({'a.b':{'spaced key':1,'keep':2}}),dynamic(["$['a.b']['spaced key']"]),
            dynamic({'a':1}),dynamic([])]
        | project result=tostring(bag_remove_keys(b,keys))"#,
        &[
            &[r#"{"a":{}}"#],
            &[r#"{"a":1}"#],
            &[r#"{"a":null}"#],
            &[r#"{"a":[1]}"#],
            &[r#"{"keep":2}"#],
            &[r#"{"keep":2}"#],
            &[r#"{"a":{"b":2}}"#],
            &[r#"{"a.b":{"keep":2}}"#],
            &[r#"{"a":1}"#],
        ],
    )
    .await;
    check(
        "datatable(b:dynamic,keys:dynamic)[dynamic(null),dynamic(['a']),\
         dynamic([]),dynamic(['a']),dynamic({}),dynamic(null),dynamic({}),dynamic(1)] \
         | project result=gettype(bag_remove_keys(b,keys))",
        &[&["null"], &["null"], &["null"], &["null"]],
    )
    .await;
}

#[tokio::test]
async fn bag_has_key_jsonpaths_understand_quoted_property_names_and_nulls() {
    check(
        r#"let b=dynamic({'a.b':{'spaced key':null},'a':{'b':1}});
        print quoted=bag_has_key(b,"$['a.b']['spaced key']"),
              mixed=bag_has_key(b,"$.a['b']"),
              literal=bag_has_key(b,'a.b'),
              missing=bag_has_key(b,"$['a.b']['missing']")"#,
        &[&["true", "true", "true", "false"]],
    )
    .await;
}

#[tokio::test]
async fn bag_has_key_jsonpaths_support_row_varying_paths_and_missing_parents() {
    check(
        r#"datatable(b:dynamic,path:string)[
            dynamic({'a':{'b':null}}),'$.a.b',
            dynamic({'a':{'b':[]}}),'$.a.b',
            dynamic({'a':1}),'$.a.b',
            dynamic({'a':null}),'$.a.b',
            dynamic(null),'$.a.b',
            dynamic([]),'$.a.b',
            dynamic({'':null}),'',
            dynamic({'$literal':1}),'$literal']
        | project result=bag_has_key(b,path)"#,
        &[
            &["true"],
            &["true"],
            &["false"],
            &["false"],
            &["false"],
            &["false"],
            &["true"],
            &["true"],
        ],
    )
    .await;
}

#[tokio::test]
async fn bag_property_jsonpaths_fail_explicitly_for_invalid_paths() {
    let ctx = SessionContext::new();
    for path in ["$.a[0]", "$.a[-1]", "$..a", "$.*", "$.a[", "$['bad'"] {
        let literal = serde_json::to_string(path).unwrap();
        for query in [
            format!("print b=bag_remove_keys(dynamic({{}}),pack_array({literal}))"),
            format!("print found=bag_has_key(dynamic({{}}),{literal})"),
        ] {
            let error = match ctx.kql(&query).await {
                Ok(frame) => frame.collect().await.unwrap_err().to_string(),
                Err(error) => error.to_string(),
            };
            assert!(
                error.contains("JSONPath") || error.contains("array indexing"),
                "{query}: {error}"
            );
        }
    }
    for query in [
        "print b=bag_remove_keys(dynamic({}),dynamic(['$']))",
        "print b=bag_remove_keys(dynamic({}),dynamic([1]))",
        "datatable(path:string)['$.a','$.a[0]'] | project b=bag_remove_keys(dynamic({}),pack_array(path))",
        "datatable(path:string)['$.a','$.a[0]'] | project found=bag_has_key(dynamic({}),path)",
    ] {
        match ctx.kql(query).await {
            Ok(frame) => assert!(frame.collect().await.is_err(), "{query}"),
            Err(_) => {}
        }
    }
}

#[tokio::test]
async fn bag_property_jsonpaths_enforce_the_exact_path_depth_boundary() {
    let path = format!("${}", ".a".repeat(64));
    let parent = format!("${}", ".a".repeat(63));
    check(
        &format!(
            "let path='{path}'; let b=bag_set_key(dynamic({{}}),path,1); \
             print original=bag_has_key(b,path),\
               removed=bag_has_key(bag_remove_keys(b,pack_array(path)),path),\
               parent=bag_has_key(bag_remove_keys(b,pack_array(path)),'{parent}')"
        ),
        &[&["true", "false", "true"]],
    )
    .await;
    let ctx = SessionContext::new();
    for query in [
        format!("print b=bag_remove_keys(dynamic({{}}),dynamic(['{path}.a']))"),
        format!("print found=bag_has_key(dynamic({{}}),'{path}.a')"),
    ] {
        let error = match ctx.kql(&query).await {
            Ok(frame) => frame.collect().await.unwrap_err().to_string(),
            Err(error) => error.to_string(),
        };
        assert!(error.contains("JSONPath nesting limit"), "{error}");
    }
}

#[tokio::test]
async fn bag_set_key_updates_nested_properties_and_creates_missing_parents() {
    check(
        r#"datatable(input:dynamic)[
            dynamic({'key1':123,'key2':{'prop1':123,'prop2':'xyz'}}),
            dynamic({'key1':123})]
        | project result=tostring(bag_set_key(input,'$.key2.prop1','abc'))"#,
        &[
            &[r#"{"key1":123,"key2":{"prop1":"abc","prop2":"xyz"}}"#],
            &[r#"{"key1":123,"key2":{"prop1":"abc"}}"#],
        ],
    )
    .await;
    check(
        r#"datatable(b:dynamic,k:string,v:long)[
            dynamic({}),'$.a.b.c',1,
            dynamic({'a':{'other':2}}),'$.a.b',3,
            dynamic({}),'a.b',4,
            dynamic({}),"$['a.b']['spaced key']",5,
            dynamic({}),'',6]
        | project result=tostring(bag_set_key(b,k,v))"#,
        &[
            &[r#"{"a":{"b":{"c":1}}}"#],
            &[r#"{"a":{"b":3,"other":2}}"#],
            &[r#"{"a.b":4}"#],
            &[r#"{"a.b":{"spaced key":5}}"#],
            &[r#"{"":6}"#],
        ],
    )
    .await;
}

#[tokio::test]
async fn bag_set_key_preserves_null_values_and_returns_null_for_non_bag_inputs() {
    check(
        r#"datatable(b:dynamic)[dynamic({}),dynamic({'a':{'keep':true}})]
        | project result=tostring(bag_set_key(b,'$.a.b',dynamic(null)))"#,
        &[
            &[r#"{"a":{"b":null}}"#],
            &[r#"{"a":{"b":null,"keep":true}}"#],
        ],
    )
    .await;
    check(
        "datatable(b:dynamic)[dynamic(null),dynamic([]),dynamic(1),dynamic('text')] | project result=gettype(bag_set_key(b,'$.a.b',1))",
        &[&["null"], &["null"], &["null"], &["null"]],
    )
    .await;
}

#[tokio::test]
async fn bag_set_key_non_bag_parents_fail_explicitly_until_reference_behavior_is_verified() {
    let ctx = SessionContext::new();
    for bag in ["{'a':1}", "{'a':null}", "{'a':[1,2]}", "{'a':1h}"] {
        let query = format!("print b=bag_set_key(dynamic({bag}),'$.a.b',1)");
        let error = match ctx.kql(&query).await {
            Ok(frame) => frame.collect().await.unwrap_err().to_string(),
            Err(error) => error.to_string(),
        };
        assert!(error.contains("parent must be a property bag"), "{error}");
    }
}

#[tokio::test]
async fn bag_set_key_rejects_unsupported_or_malformed_paths() {
    let ctx = SessionContext::new();
    for path in ["$", "$.a[0]", "$.a[-1]", "$..a", "$.*", "$.a[", "$a"] {
        let query = format!("print result=bag_set_key(dynamic({{}}),'{path}',1)");
        let error = match ctx.kql(&query).await {
            Ok(frame) => frame.collect().await.unwrap_err().to_string(),
            Err(error) => error.to_string(),
        };
        assert!(
            error.contains("JSONPath") || error.contains("array indexing"),
            "{error}"
        );
    }
}

#[tokio::test]
async fn bag_set_key_expands_lexical_functions_without_mutating_the_input() {
    check(
        "let path='$.a.b'; let update=(b:dynamic,v:long){bag_set_key(b,path,v)}; \
         print original=dynamic({'a':{'b':1}}) \
         | extend updated=update(original,2) \
         | project a=tolong(original.a.b),b=tolong(updated.a.b)",
        &[&["1", "2"]],
    )
    .await;
}

#[tokio::test]
async fn array_slicing_and_search_use_zero_based_indices() {
    check(
        "let a=dynamic([1,2,1,4]); print length=array_length(a), slice=tostring(array_slice(a,-3,-1)), index=array_index_of(a,1,1,-1,1), found=set_has_element(a,4), missing=array_index_of(a,9), reversed=tostring(array_reverse(a)), empty=tostring(array_slice(a,3,1))",
        &[&["4","[2,1,4]","2","true","-1","[4,1,2,1]","[]"]],
    ).await;
}

#[tokio::test]
async fn array_pack_zip_range_and_set_operations() {
    check(
        "let a=range(1,3,1); print a=tostring(a), packed=tostring(pack_array(1,'a',true)), zip=tostring(zip(a,dynamic(['a']))), repeat=tostring(repeat('x',2)), concat=tostring(array_concat(a,dynamic([4]))), union=tostring(set_union(a,dynamic([2,4]))), intersection=tostring(set_intersect(a,dynamic([2,4]))), difference=tostring(set_difference(a,dynamic([2,4])))",
        &[&["[1,2,3]","[1,\"a\",true]","[[1,\"a\"],[2,null],[3,null]]","[\"x\",\"x\"]","[1,2,3,4]","[1,2,3,4]","[2]","[1,3]"]],
    ).await;
}

#[tokio::test]
async fn array_functions_keep_empty_and_null_distinct() {
    check(
        "print a=array_length(dynamic([])), b=array_length(dynamic(null)), c=array_length(dynamic({})), d=array_sum(dynamic([1,2.5])), e=array_sum(dynamic([1,'x'])), f=tostring(pack_array()), g=tostring(bag_pack())",
        &[&["0","<null>","<null>","3.5","<null>","[]","{}"]],
    ).await;
}

#[tokio::test]
async fn split_and_strcat_array_are_kql_shaped() {
    check(
        "print a=tostring(split('a__b','_')), b=tostring(split('a_b','_',1)), c=strcat_array(dynamic([1,'two',null]),'|')",
        &[&["[\"a\",\"\",\"b\"]","[\"b\"]","1|two|"]],
    ).await;
}

#[tokio::test]
async fn collection_builders_are_bounded() {
    let ctx = SessionContext::new();
    for query in ["print a=repeat(1,1048577)", "print a=range(1,1048577,1)"] {
        let result = match ctx.kql(query).await {
            Ok(frame) => frame.collect().await.map(|_| ()).map_err(|e| e.to_string()),
            Err(error) => Err(error.to_string()),
        };
        assert!(result.is_err(), "{query}");
    }
}

#[tokio::test]
async fn mv_expand_uses_native_unnest_and_retains_other_columns() {
    use datafusion::physical_plan::displayable;
    let ctx = SessionContext::new();
    let query = "datatable(id:long,a:dynamic)[1,dynamic([10,20]),2,dynamic([30])] | mv-expand with_itemindex=i a to typeof(long)";
    let frame = ctx.kql(query).await.unwrap();
    let physical = frame.clone().create_physical_plan().await.unwrap();
    let display = displayable(physical.as_ref()).indent(false).to_string();
    assert!(display.contains("UnnestExec"), "{display}");
    assert_eq!(
        frame
            .collect()
            .await
            .unwrap()
            .iter()
            .map(RecordBatch::num_rows)
            .sum::<usize>(),
        3
    );
    check(
        "datatable(id:long,a:dynamic)[1,dynamic([10,20]),2,dynamic([30])] | mv-expand with_itemindex=i a to typeof(long) | sort by id asc,i asc",
        &[&["1","10","0"],&["1","20","1"],&["2","30","0"]],
    ).await;
}

#[tokio::test]
async fn mv_expand_null_empty_and_cast_only_elements() {
    check(
        "datatable(id:long,a:dynamic)[1,dynamic(null),2,dynamic([]),3,dynamic([1,'2',true,null,3.5])] | mv-expand with_itemindex=i a to typeof(long) | sort by id asc,i asc",
        &[&["1","<null>","0"],&["3","1","0"],&["3","<null>","1"],&["3","<null>","2"],&["3","<null>","3"],&["3","<null>","4"]],
    ).await;
}

#[tokio::test]
async fn mv_expand_multiple_columns_zip_and_pad_rather_than_cross_join() {
    check(
        "print a=dynamic([1,2,3]),b=dynamic(['x']) | mv-expand with_itemindex=i a to typeof(long),b | project a,b=tostring(b),i",
        &[&["1","x","0"],&["2","","1"],&["3","","2"]],
    ).await;
    check(
        "print a=dynamic([]),b=dynamic([1,2]) | mv-expand a,b to typeof(long) | project a=gettype(a),b",
        &[&["null","1"],&["null","2"]],
    ).await;
}

#[tokio::test]
async fn consecutive_mv_expand_operators_form_a_cartesian_product() {
    check(
        "print a=dynamic([1,2]),b=dynamic(['x','y']) | mv-expand a to typeof(long) | mv-expand b | project a,b=tostring(b) | sort by a asc,b asc",
        &[&["1","x"],&["1","y"],&["2","x"],&["2","y"]],
    ).await;
}

#[tokio::test]
async fn mv_expand_bags_support_both_expansion_modes() {
    check(
        "print b=dynamic({'a':1,'b':2}) | mv-expand kind=array with_itemindex=i b | project key=tostring(b[0]),value=tolong(b[1]),i | sort by key asc",
        &[&["a","1","0"],&["b","2","1"]],
    ).await;
    check(
        "print b=dynamic({'a':1,'b':2}) | mv-expand b | project s=tostring(b) | sort by s asc",
        &[&["{\"a\":1}"], &["{\"b\":2}"]],
    )
    .await;
}

#[tokio::test]
async fn mv_expand_limits_apply_per_input_row() {
    check(
        "datatable(id:long,a:dynamic)[1,dynamic([1,2,3]),2,dynamic([4,5,6])] | mv-expand a to typeof(long) limit 1 | sort by id asc",
        &[&["1","1"],&["2","4"]],
    ).await;
    check(
        "print a=dynamic(null) | mv-expand a limit 0 | count",
        &[&["0"]],
    )
    .await;
    check("print a=range(1,200,1) | mvexpand a | count", &[&["128"]]).await;
}

#[tokio::test]
async fn mv_expand_can_add_named_expressions() {
    check(
        "print x=3 | mv-expand value=range(1,x,1) to typeof(long) | sort by value asc",
        &[&["3", "1"], &["3", "2"], &["3", "3"]],
    )
    .await;
}

#[tokio::test]
async fn dynamic_membership_flattens_arrays_and_coalesce_preserves_types() {
    check(
        "print a=1 in (dynamic([1,2])),b='ABC' in~ (dynamic(['abc'])),c=dynamic(null) in (dynamic([null])),d=dynamic(3) !in (dynamic([1,2])),e=gettype(coalesce(dynamic(null),dynamic(42))),f=tostring(coalesce(dynamic(null),dynamic({'a':1})))",
        &[&["true","true","<null>","true","long","{\"a\":1}"]],
    ).await;
}
