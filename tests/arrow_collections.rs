use std::sync::Arc;

use datafusion::arrow::array::{
    Array, ArrayRef, BinaryArray, Decimal128Array, DurationNanosecondArray, FixedSizeBinaryBuilder,
    FixedSizeListArray, Int8Array, Int64Array, LargeListArray, LargeListViewArray, ListArray,
    ListBuilder, ListViewArray, MapBuilder, StringBuilder, StructArray, TimestampMicrosecondArray,
    UInt64Array,
};
use datafusion::arrow::buffer::{NullBuffer, ScalarBuffer};
use datafusion::arrow::datatypes::{DataType, Field, Int8Type, Int64Type};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::arrow::util::display::array_value_to_string;
use datafusion::common::TableReference;
use datafusion::prelude::*;
use datafusion_kql::SessionContextKqlExt;

fn context(array: ArrayRef) -> SessionContext {
    let ctx = SessionContext::new_with_config(SessionConfig::new().with_batch_size(2));
    let id = Arc::new(Int64Array::from_iter_values(
        (0..array.len()).map(|i| i as i64),
    )) as ArrayRef;
    ctx.register_batch(
        TableReference::bare("T"),
        RecordBatch::try_from_iter(vec![("id", id), ("v", array)]).unwrap(),
    )
    .unwrap();
    ctx
}

async fn check(ctx: &SessionContext, query: &str, expected: &[&[&str]]) {
    let frame = ctx
        .kql(query)
        .await
        .unwrap_or_else(|error| panic!("{}\n{query}", error.diagnostic(query)));
    let batches = frame
        .collect()
        .await
        .unwrap_or_else(|error| panic!("{error}\n{query}"));
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

fn list() -> ArrayRef {
    Arc::new(ListArray::from_iter_primitive::<Int64Type, _, _>([
        Some(vec![Some(1), Some(2), None]),
        Some(vec![]),
        None,
        Some(vec![Some(4)]),
    ]))
}

#[tokio::test]
async fn native_lists_expand_with_dynamic_tags_and_cast_only_types() {
    let ctx = context(list());
    check(
        &ctx,
        "T | mv-expand with_itemindex=i v | project id,i,value=tostring(v),kind=gettype(v)",
        &[
            &["0", "0", "1", "long"],
            &["0", "1", "2", "long"],
            &["0", "2", "", "null"],
            &["2", "0", "", "null"],
            &["3", "0", "4", "long"],
        ],
    )
    .await;
    check(
        &ctx,
        "T | mv-expand v to typeof(long) | project id,v",
        &[
            &["0", "1"],
            &["0", "2"],
            &["0", "<null>"],
            &["2", "<null>"],
            &["3", "4"],
        ],
    )
    .await;
    check(
        &ctx,
        "T | mv-expand v to typeof(string) | summarize N=countif(isnull(v) or v=='')",
        &[&["5"]],
    )
    .await;
}

#[tokio::test]
async fn typed_native_expansion_uses_arrow_buffers_and_matches_the_general_path() {
    let ctx = context(list());
    let frame = ctx.kql("T | mv-expand v to typeof(long)").await.unwrap();
    let physical = frame.create_physical_plan().await.unwrap();
    let display = datafusion::physical_plan::displayable(physical.as_ref())
        .indent(false)
        .to_string();
    assert!(display.contains("__kql_native_expand_list"), "{display}");
    assert!(
        !display.contains("dynamic_expand") && !display.contains("mv_expand_array"),
        "{display}"
    );
    let fast = frame.collect().await.unwrap();
    let general = ctx
        .kql("T | mv-expand with_itemindex=i v to typeof(long) | project-away i")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let values = |batches: Vec<RecordBatch>| {
        batches
            .iter()
            .flat_map(|batch| {
                (0..batch.num_rows()).map(|row| {
                    batch
                        .columns()
                        .iter()
                        .map(|column| array_value_to_string(column, row).unwrap())
                        .collect::<Vec<_>>()
                })
            })
            .collect::<Vec<_>>()
    };
    assert_eq!(values(fast), values(general));
    check(
        &ctx,
        "T | mv-expand v to typeof(long) limit 1",
        &[&["0", "1"], &["2", "<null>"], &["3", "4"]],
    )
    .await;
    check(&ctx, "T | mv-expand v to typeof(long) limit 0", &[]).await;
    check(
        &ctx,
        "T | mv-expand renamed=v to typeof(long) | project id,original=tostring(v),renamed",
        &[
            &["0", "[1,2,null]", "1"],
            &["0", "[1,2,null]", "2"],
            &["0", "[1,2,null]", "<null>"],
            &["2", "", "<null>"],
            &["3", "[4]", "4"],
        ],
    )
    .await;
}

#[tokio::test]
async fn native_and_explicit_dynamic_distinct_counts_have_identical_identity() {
    check(&context(list()),"T | summarize Native=dcount(v),Dynamic=dcount(todynamic(v)),Exact=count_distinct(v),s=hll(v),d=hll(todynamic(v)) | project Native,Dynamic,Exact,Merged=dcount_hll(hll_merge(s,d))",
        &[&["3","3","3","3"]]).await;
}

#[tokio::test]
async fn native_lists_work_in_array_functions_pack_and_dynamic_conversion() {
    let ctx = context(list());
    check(&ctx,"T | extend d=todynamic(v) | project id,kind=gettype(d),value=tostring(v),n=array_length(v),reversed=tostring(array_reverse(v))",
        &[&["0","array","[1,2,null]","3","[null,2,1]"],&["1","array","[]","0","[]"],&["2","null","","<null>",""],&["3","array","[4]","1","[4]"]]).await;
    check(
        &ctx,
        "T | extend p=pack_all(),a=pack_array(v) | project id,p=tostring(p.v),a=tostring(a[0])",
        &[
            &["0", "[1,2,null]", "[1,2,null]"],
            &["1", "[]", "[]"],
            &["2", "", ""],
            &["3", "[4]", "[4]"],
        ],
    )
    .await;
    check(
        &ctx,
        "T | sort by id asc | summarize a=make_list(v) | project n=array_length(a),first=tostring(a[0])",
        &[&["3", "[1,2,null]"]],
    )
    .await;
}

#[tokio::test]
async fn native_and_dynamic_expansion_zip_with_null_padding_without_cartesian_products() {
    let ctx = context(list());
    check(&ctx,"T | extend other=iff(id==2,dynamic([10,20]),dynamic([9])) | mv-expand with_itemindex=i v to typeof(long),other to typeof(long) | project id,i,v,other",
        &[&["0","0","1","9"],&["0","1","2","<null>"],&["0","2","<null>","<null>"],&["1","0","<null>","9"],&["2","0","<null>","10"],&["2","1","<null>","20"],&["3","0","4","9"]]).await;
    check(
        &ctx,
        "T | mv-expand v limit 1 | project id,value=tostring(v)",
        &[&["0", "1"], &["2", ""], &["3", "4"]],
    )
    .await;
}

#[tokio::test]
async fn native_lists_work_in_grouped_subqueries_and_ordered_row_offsets() {
    let ctx = context(list());
    check(
        &ctx,
        "T | mv-apply item=v to typeof(long) on(summarize Total=sum(item)) | project id,Total",
        &[&["0", "3"], &["1", "0"], &["2", "0"], &["3", "4"]],
    )
    .await;
    check(&ctx,"let X=materialize(T | extend d=todynamic(v)); X | serialize | project p=tostring(prev(d)),n=tostring(next(d))",
        &[&["","[]"],&["[1,2,null]",""],&["[]","[4]"],&["",""]]).await;
}

#[tokio::test]
async fn large_fixed_and_view_list_layouts_and_slices_keep_their_offsets() {
    let large: ArrayRef = Arc::new(LargeListArray::from_iter_primitive::<Int64Type, _, _>([
        Some(vec![Some(90)]),
        Some(vec![Some(1), Some(2)]),
        None,
        Some(vec![]),
    ]));
    let fixed: ArrayRef = Arc::new(FixedSizeListArray::from_iter_primitive::<Int64Type, _, _>(
        [
            Some(vec![Some(90), Some(91)]),
            Some(vec![Some(1), Some(2)]),
            None,
            Some(vec![Some(3), None]),
        ],
        2,
    ));
    check(
        &context(large.slice(1, 3)),
        "T | project id,value=tostring(v),n=array_length(v)",
        &[
            &["0", "[1,2]", "2"],
            &["1", "", "<null>"],
            &["2", "[]", "0"],
        ],
    )
    .await;
    check(
        &context(fixed.slice(1, 3)),
        "T | project id,value=tostring(v),n=array_length(v)",
        &[
            &["0", "[1,2]", "2"],
            &["1", "", "<null>"],
            &["2", "[3,null]", "2"],
        ],
    )
    .await;
    let field = Arc::new(Field::new("item", DataType::Int64, true));
    let values = Arc::new(Int64Array::from(vec![10, 20, 30, 40])) as ArrayRef;
    let view: ArrayRef = Arc::new(
        ListViewArray::try_new(
            Arc::clone(&field),
            ScalarBuffer::from(vec![2, 0, 1]),
            ScalarBuffer::from(vec![2, 1, 0]),
            Arc::clone(&values),
            None,
        )
        .unwrap(),
    );
    let large_view: ArrayRef = Arc::new(
        LargeListViewArray::try_new(
            field,
            ScalarBuffer::from(vec![2_i64, 0, 1]),
            ScalarBuffer::from(vec![2_i64, 1, 0]),
            values,
            None,
        )
        .unwrap(),
    );
    for array in [view, large_view] {
        check(
            &context(Arc::clone(&array)),
            "T | project value=tostring(v),first=tolong(v[0])",
            &[&["[30,40]", "30"], &["[10]", "10"], &["[]", "<null>"]],
        )
        .await;
        check(
            &context(array.slice(1, 2)),
            "T | mv-expand v to typeof(long)",
            &[&["0", "10"]],
        )
        .await;
    }
}

#[tokio::test]
async fn native_struct_nulls_override_children_and_rich_scalar_types_survive() {
    let mut guids = FixedSizeBinaryBuilder::new(16);
    guids.append_value([7; 16]).unwrap();
    guids.append_value([8; 16]).unwrap();
    let fields = [
        (
            "when",
            Arc::new(TimestampMicrosecondArray::from(vec![
                1_735_689_600_000_001_i64,
                0,
            ])) as ArrayRef,
        ),
        (
            "span",
            Arc::new(DurationNanosecondArray::from(vec![1, 2])) as ArrayRef,
        ),
        ("id", Arc::new(guids.finish()) as ArrayRef),
        (
            "amount",
            Arc::new(
                Decimal128Array::from(vec![125_i128, 250])
                    .with_precision_and_scale(10, 2)
                    .unwrap(),
            ) as ArrayRef,
        ),
    ];
    let arrays = fields
        .iter()
        .map(|(_, array)| Arc::clone(array))
        .collect::<Vec<_>>();
    let fields = fields
        .iter()
        .map(|(name, array)| Arc::new(Field::new(*name, array.data_type().clone(), true)))
        .collect::<Vec<_>>();
    let array = Arc::new(
        StructArray::try_new(
            fields.into(),
            arrays,
            Some(NullBuffer::from(vec![true, false])),
        )
        .unwrap(),
    ) as ArrayRef;
    check(&context(array),"T | extend d=parse_json(v) | project kind=gettype(d),t=gettype(d.when),span=gettype(d.span),id=gettype(d.id),amount=gettype(d.amount),value=tostring(todecimal(d.amount)),ns=tolong(d.span)",
        &[&["dictionary","datetime","timespan","guid","decimal","1.25","1"],&["null","null","null","null","null","","<null>"]]).await;
}

#[tokio::test]
async fn nested_dictionary_values_are_not_stringified_or_confused_with_dictionary_indices() {
    use datafusion::arrow::array::DictionaryArray;
    let dictionary = DictionaryArray::<Int8Type>::try_new(
        Int8Array::from(vec![Some(1), Some(0), None]),
        Arc::new(Int64Array::from(vec![10, 20])),
    )
    .unwrap();
    let array = ListArray::try_new(
        Arc::new(Field::new("item", dictionary.data_type().clone(), true)),
        datafusion::arrow::buffer::OffsetBuffer::new(ScalarBuffer::from(vec![0, 3])),
        Arc::new(dictionary),
        None,
    )
    .unwrap();
    check(
        &context(Arc::new(array)),
        "T | mv-expand v | project kind=gettype(v),n=tolong(v)",
        &[&["long", "20"], &["long", "10"], &["null", "<null>"]],
    )
    .await;
}

#[tokio::test]
async fn native_maps_have_string_keys_and_support_bag_functions_and_expansion() {
    let mut maps = MapBuilder::new(None, StringBuilder::new(), Int64Array::builder(0));
    maps.keys().append_value("b");
    maps.values().append_value(2);
    maps.keys().append_value("a");
    maps.values().append_value(1);
    maps.append(true).unwrap();
    maps.append(true).unwrap();
    maps.append(false).unwrap();
    let ctx = context(Arc::new(maps.finish()));
    check(
        &ctx,
        "T | project id,kind=gettype(v),text=tostring(v),a=tolong(v['a'])",
        &[
            &["0", "dictionary", "{\"a\":1,\"b\":2}", "1"],
            &["1", "dictionary", "{}", "<null>"],
            &["2", "null", "", "<null>"],
        ],
    )
    .await;
    check(&ctx,"T | mv-expand kind=array v | project id,key=tostring(v[0]),value=tolong(v[1]) | sort by id asc,key asc",
        &[&["0","a","1"],&["0","b","2"],&["2","","<null>"]]).await;
}

async fn failure(array: ArrayRef, needle: &str) {
    let ctx = context(array);
    let error = match ctx.kql("T | project v=todynamic(v)").await {
        Ok(frame) => frame.collect().await.unwrap_err().to_string(),
        Err(error) => error.to_string(),
    };
    assert!(error.contains(needle), "{error}");
}

#[tokio::test]
async fn ambiguous_or_lossy_native_values_fail_instead_of_becoming_success_shaped_json() {
    let mut maps = MapBuilder::new(None, StringBuilder::new(), Int64Array::builder(0));
    maps.keys().append_value("a");
    maps.values().append_value(1);
    maps.keys().append_value("a");
    maps.values().append_value(2);
    maps.append(true).unwrap();
    failure(Arc::new(maps.finish()), "duplicate keys").await;
    let values = UInt64Array::from(vec![u64::MAX]);
    let structure = StructArray::from(vec![(
        Arc::new(Field::new("x", DataType::UInt64, true)),
        Arc::new(values) as ArrayRef,
    )]);
    failure(Arc::new(structure), "long range").await;
    let structure = StructArray::from(vec![(
        Arc::new(Field::new("x", DataType::Binary, true)),
        Arc::new(BinaryArray::from(vec![Some(b"x".as_slice())])) as ArrayRef,
    )]);
    failure(Arc::new(structure), "cannot preserve").await;
    let values = TimestampMicrosecondArray::from(vec![i64::MAX]);
    let structure = StructArray::from(vec![(
        Arc::new(Field::new("x", values.data_type().clone(), true)),
        Arc::new(values) as ArrayRef,
    )]);
    failure(Arc::new(structure), "nanosecond range").await;
}

#[tokio::test]
async fn native_collection_limits_are_checked_before_constructing_unbounded_nodes() {
    let mut list = ListBuilder::new(Int8Array::builder(0));
    for _ in 0..1_048_577 {
        list.values().append_value(1);
    }
    list.append(true);
    failure(Arc::new(list.finish()), "1048576").await;
    let mut array: ArrayRef = Arc::new(Int64Array::from(vec![1]));
    for _ in 0..65 {
        array = Arc::new(StructArray::from(vec![(
            Arc::new(Field::new("x", array.data_type().clone(), true)),
            array,
        )]));
    }
    failure(array, "nesting limit").await;
}

#[tokio::test]
async fn native_schema_depth_limit_accepts_its_exact_boundary() {
    for depth in [16, 32, 64] {
        let mut array: ArrayRef = Arc::new(Int64Array::from(vec![1]));
        for _ in 0..depth {
            array = Arc::new(StructArray::from(vec![(
                Arc::new(Field::new("x", array.data_type().clone(), true)),
                array,
            )]));
        }
        let ctx = context(array);
        let frame = ctx
            .kql("T | extend d=todynamic(v) | project kind=gettype(d)")
            .await
            .unwrap();
        let batches = frame.collect().await.unwrap();
        assert_eq!(
            array_value_to_string(batches[0].column(0), 0).unwrap(),
            "dictionary"
        );
    }
}

#[tokio::test]
async fn native_parameter_schemas_obey_the_same_early_depth_guard() {
    use datafusion::common::ScalarValue;
    use datafusion_kql::QueryParameters;
    let mut array: ArrayRef = Arc::new(Int64Array::from(vec![1]));
    for _ in 0..65 {
        array = Arc::new(StructArray::from(vec![(
            Arc::new(Field::new("x", array.data_type().clone(), true)),
            array,
        )]));
    }
    let parameter = ScalarValue::try_from_array(&array, 0).unwrap();
    let error = SessionContext::new()
        .kql_with_parameters(
            "declare query_parameters(p:dynamic); print kind=gettype(p)",
            &QueryParameters::from([("p".into(), parameter)]),
        )
        .await
        .unwrap_err()
        .to_string();
    assert!(error.contains("nesting limit"), "{error}");
}
