use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use datafusion::arrow::array::{Array, ArrayRef, Float64Array, ListArray};
use datafusion::arrow::datatypes::{DataType, Float64Type, Int64Type};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::arrow::util::display::array_value_to_string;
use datafusion::common::{ScalarValue, TableReference};
use datafusion::logical_expr::{ColumnarValue, Volatility, create_udf};
use datafusion::prelude::*;
use datafusion_kql::{QueryParameters, SessionContextKqlExt};

fn context() -> SessionContext {
    SessionContext::new_with_config(
        SessionConfig::new()
            .with_target_partitions(4)
            .with_batch_size(2),
    )
}

async fn rows(ctx: &SessionContext, query: &str) -> Vec<Vec<String>> {
    ctx.kql(query)
        .await
        .unwrap_or_else(|error| panic!("{}\n{query}", error.diagnostic(query)))
        .collect()
        .await
        .unwrap_or_else(|error| panic!("{error}\n{query}"))
        .iter()
        .flat_map(|batch| {
            (0..batch.num_rows()).map(|row| {
                batch
                    .columns()
                    .iter()
                    .map(|array| {
                        if array
                            .logical_nulls()
                            .is_some_and(|nulls| nulls.is_null(row))
                        {
                            "<null>".to_owned()
                        } else {
                            array_value_to_string(array, row).unwrap()
                        }
                    })
                    .collect()
            })
        })
        .collect()
}

async fn check(query: &str, expected: &[&[&str]]) {
    assert_eq!(
        rows(&context(), query).await,
        expected
            .iter()
            .map(|row| row
                .iter()
                .map(|value| value.to_string())
                .collect::<Vec<_>>())
            .collect::<Vec<_>>(),
        "{query}"
    );
}

#[tokio::test]
async fn comparison_functions_return_native_real_scalars_in_all_expression_positions() {
    let ctx = context();
    let frame=ctx.kql("print dot=series_dot_product(dynamic([1,2,4]),dynamic([4,2,1])),cosine=series_cosine_similarity(dynamic([1,2,4]),dynamic([4,2,1])),pearson=series_pearson_correlation(dynamic([1,2,3]),dynamic([2,4,6]))").await.unwrap();
    assert!(
        frame
            .schema()
            .fields()
            .iter()
            .all(|field| field.data_type() == &DataType::Float64)
    );
    check("range x from 1 to 3 step 1 | extend a=pack_array(x,2*x,4*x),b=pack_array(4*x,2*x,x) | where series_dot_product(a,b)>12 | summarize Total=sum(series_dot_product(a,b)),N=count()",
        &[&["156.0","2"]]).await;
    check("print n=array_length(pack_array(series_dot_product(dynamic([1,2]),dynamic([3,4])))),positive=series_cosine_similarity(dynamic([1,2]),dynamic([1,2]))>0.999999999",
        &[&["1","true"]]).await;
}

#[tokio::test]
async fn cosine_norms_cover_full_vectors_while_dot_truncates_and_pearson_requires_equal_lengths() {
    check("print a=dynamic([1,2,3]),b=dynamic([4,5]) | project Dot=series_dot_product(a,b),Cosine=abs(series_cosine_similarity(a,b)-0.5843487097907776)<1E-12,Pearson=isnull(series_pearson_correlation(a,b))",
        &[&["14.0","true","true"]]).await;
}

#[tokio::test]
async fn missing_nonnumeric_boolean_and_nested_elements_follow_the_captured_service_profile() {
    check("print a=dynamic([1,null,3]),b=dynamic([4,5,6]) | project Dot=series_dot_product(a,b),Cosine=abs(series_cosine_similarity(a,b)-0.7928249671720918)<1E-12,Pearson=isnull(series_pearson_correlation(a,b)),Text=series_dot_product(dynamic([1,'2',3]),b),Nested=series_dot_product(dynamic([1,[2],3]),b),Bool=series_dot_product(dynamic([1,true,3]),b),BoolPearson=abs(series_pearson_correlation(dynamic([1,true,3]),b)-0.8660254037844387)<1E-12",
        &[&["22.0","true","true","22.0","22.0","27.0","true"]]).await;
}

#[tokio::test]
async fn scalar_dot_broadcasts_do_not_treat_dynamic_scalars_as_arrays() {
    check("print a=dynamic([1,2,3]) | project Forward=series_dot_product(a,10),Reverse=series_dot_product(10,a),Dynamic=isnull(series_dot_product(a,dynamic(10))),Missing=isnull(series_dot_product(a,real(null))),Decimal=series_dot_product(a,decimal(1.5))",
        &[&["60.0","60.0","true","true","9.0"]]).await;
    check("datatable(k:long)[1,2,3] | project k,Dot=series_dot_product(dynamic([1,2,4]),k) | sort by k asc",
        &[&["1","7.0"],&["2","14.0"],&["3","21.0"]]).await;
}

#[tokio::test]
async fn optional_runtime_magnitudes_and_ieee_results_are_not_clamped_or_hidden() {
    check("print a=dynamic([1,2,3]),b=dynamic([4,5]) | project One=abs(series_cosine_similarity(a,b,1.0)-2.186432666440485)<1E-12,Both=series_cosine_similarity(a,b,1.0,1.0),Negative=series_cosine_similarity(a,b,-1.0,1.0),Zero=series_cosine_similarity(a,b,0.0,1.0)==real(inf),Null=abs(series_cosine_similarity(a,b,real(null))-0.5843487097907776)<1E-12,NaN=isnan(series_cosine_similarity(a,b,real(nan))),Infinity=series_cosine_similarity(a,b,real(inf))",
        &[&["true","14.0","-14.0","true","true","true","0.0"]]).await;
    check("datatable(m:real)[1.0,2.0,real(null)] | project Score=series_cosine_similarity(dynamic([3,4]),dynamic([3,4]),m,5.0)",
        &[&["5.0"],&["2.5"],&["1.0"]]).await;
}

#[tokio::test]
async fn empty_zero_constant_and_nonfinite_vectors_preserve_null_nan_and_infinity_distinctions() {
    check("print a=dynamic([0,0,0]),b=dynamic([1,2,3]) | project Dot=series_dot_product(a,b),Cosine=isnan(series_cosine_similarity(a,b)),Pearson=isnan(series_pearson_correlation(a,b)),EmptyDot=isnull(series_dot_product(dynamic([]),b)),EmptyCosine=isnull(series_cosine_similarity(dynamic([]),b)),EmptyPearson=isnull(series_pearson_correlation(dynamic([]),dynamic([]))),Constant=isnan(series_pearson_correlation(dynamic([7,7,7]),b)),Single=isnan(series_pearson_correlation(dynamic([7]),dynamic([9])))",
        &[&["0.0","true","true","true","true","true","true","true"]]).await;
    check("print a=pack_array(real(nan),1.0,2.0),b=dynamic([3,4,5]),i=pack_array(real(inf),1.0,2.0) | project DotNaN=isnan(series_dot_product(a,b)),CosNaN=isnan(series_cosine_similarity(a,b)),PearsonNaN=isnull(series_pearson_correlation(a,b)),DotInf=series_dot_product(i,b)==real(inf),CosInf=isnan(series_cosine_similarity(i,b)),PearsonInf=isnull(series_pearson_correlation(i,b))",
        &[&["true","true","true","true","true","true"]]).await;
}

#[tokio::test]
async fn rich_numeric_elements_use_service_boolean_decimal_and_temporal_coercions() {
    check("print a=pack_array(decimal(1.5),decimal(2.5),3),b=dynamic([3,4,5]),t=pack_array(1s,2s,-1s),one=dynamic([1,1,1]) | project Decimal=series_dot_product(a,b),Cosine=abs(series_cosine_similarity(a,b)-0.9972820205796494)<1E-12,Pearson=abs(series_pearson_correlation(a,b)-0.9819805060619665)<1E-12,Timespan=series_dot_product(t,one),Datetime=series_dot_product(pack_array(datetime(1970-01-01),datetime(1969-12-31),datetime(2025-01-01)),one)==1.881423936E18",
        &[&["29.5","true","true","20000000.0","true"]]).await;
}

#[tokio::test]
async fn native_arrow_numeric_lists_bypass_dynamic_conversion_and_keep_null_masks() {
    let ctx = context();
    let a = ListArray::from_iter_primitive::<Float64Type, _, _>([
        Some(vec![Some(1.0), Some(2.0), Some(3.0)]),
        Some(vec![Some(1.0), None, Some(3.0)]),
        Some(vec![]),
        None,
    ]);
    let b = ListArray::from_iter_primitive::<Int64Type, _, _>([
        Some(vec![Some(4), Some(5), Some(6)]),
        Some(vec![Some(4), Some(5), Some(6)]),
        Some(vec![Some(4)]),
        Some(vec![Some(4)]),
    ]);
    ctx.register_batch(
        TableReference::bare("Vectors"),
        RecordBatch::try_from_iter(vec![
            ("A", Arc::new(a) as ArrayRef),
            ("B", Arc::new(b) as ArrayRef),
        ])
        .unwrap(),
    )
    .unwrap();
    let frame=ctx.kql("Vectors | project Dot=series_dot_product(A,B),Cosine=series_cosine_similarity(A,B),Pearson=series_pearson_correlation(A,B)").await.unwrap();
    let physical = frame.clone().create_physical_plan().await.unwrap();
    let text = datafusion::physical_plan::displayable(physical.as_ref())
        .indent(false)
        .to_string();
    assert!(text.contains("__kql_series_dot_product_arrow"), "{text}");
    assert!(
        text.contains("__kql_series_cosine_similarity_arrow"),
        "{text}"
    );
    assert!(
        text.contains("__kql_series_pearson_correlation_arrow"),
        "{text}"
    );
    assert!(!text.contains("into_dynamic"), "{text}");
    let result=rows(&ctx,"Vectors | project Dot=series_dot_product(A,B),PNull=isnull(series_pearson_correlation(A,B))").await;
    assert_eq!(
        result,
        vec![
            vec!["32.0", "false"],
            vec!["22.0", "true"],
            vec!["<null>", "true"],
            vec!["<null>", "true"]
        ]
    );
}

#[tokio::test]
async fn native_special_values_and_narrow_large_offsets_keep_scalar_type_fidelity() {
    let ctx = context();
    let a = ListArray::from_iter_primitive::<Float64Type, _, _>([
        Some(vec![Some(1E308), Some(-1E308)]),
        Some(vec![Some(1E-300), Some(2E-300)]),
        Some(vec![Some(f64::NAN), Some(1.0)]),
    ]);
    ctx.register_batch(
        TableReference::bare("Extremes"),
        RecordBatch::try_from_iter(vec![("A", Arc::new(a) as ArrayRef)]).unwrap(),
    )
    .unwrap();
    let batches=ctx.kql("Extremes | project Cosine=series_cosine_similarity(A,A),Pearson=series_pearson_correlation(A,A)").await.unwrap().collect().await.unwrap();
    let points = batches
        .iter()
        .flat_map(|batch| {
            (0..batch.num_rows()).map(|row| {
                let c = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .unwrap();
                let p = batch
                    .column(1)
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .unwrap();
                (c.value(row), (!p.is_null(row)).then(|| p.value(row)))
            })
        })
        .collect::<Vec<_>>();
    for (cosine, pearson) in &points[..2] {
        assert!((cosine - 1.0).abs() < 1E-12);
        assert!((pearson.unwrap() - 1.0).abs() < 1E-12);
    }
    assert!(points[2].0.is_nan());
    assert!(points[2].1.is_none());
    check("print a=dynamic([1000000000000000.0,1000000000000001.0,1000000000000002.0]),b=dynamic([6,4,2]) | project Good=abs(series_pearson_correlation(a,b)+1.0)<1E-12",
        &[&["true"]]).await;
}

#[tokio::test]
async fn query_parameters_and_lexical_functions_remain_lazy_and_expression_local() {
    let ctx = context();
    let calls = Arc::new(AtomicUsize::new(0));
    let observed = Arc::clone(&calls);
    ctx.register_udf(create_udf(
        "observe_vector",
        vec![DataType::Int64],
        DataType::Int64,
        Volatility::Volatile,
        Arc::new(move |args| {
            observed.fetch_add(
                match &args[0] {
                    ColumnarValue::Array(values) => values.len(),
                    ColumnarValue::Scalar(_) => 1,
                },
                Ordering::SeqCst,
            );
            Ok(args[0].clone())
        }),
    ));
    let frame=ctx.kql("let dot=(a:dynamic,b:dynamic){series_dot_product(a,b)}; range n from 1 to 25 step 1 | project D=dot(pack_array(observe_vector(n),2,3),dynamic([1,2,3]))").await.unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    frame.collect().await.unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 25);
    let parameters = QueryParameters::from([("magnitude".into(), ScalarValue::Float64(Some(5.0)))]);
    let batches=ctx.kql_with_parameters("declare query_parameters(magnitude:real); print score=series_cosine_similarity(dynamic([3,4]),dynamic([3,4]),magnitude,magnitude)",&parameters).await.unwrap().collect().await.unwrap();
    assert_eq!(
        array_value_to_string(batches[0].column(0), 0).unwrap(),
        "1.0"
    );
    for name in [
        "series_dot_product",
        "series_cosine_similarity",
        "series_pearson_correlation",
    ] {
        ctx.register_udf(create_udf(
            name,
            vec![DataType::Int64],
            DataType::Int64,
            Volatility::Immutable,
            Arc::new(|_| Ok(ColumnarValue::Scalar(ScalarValue::Int64(Some(777))))),
        ));
    }
    let registered = ctx.state().scalar_functions().len();
    ctx.kql("print series_dot_product(dynamic([1,2]),dynamic([3,4]))")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    for name in [
        "series_dot_product",
        "series_cosine_similarity",
        "series_pearson_correlation",
    ] {
        let batch = ctx
            .sql(&format!("select {name}(1)"))
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        assert_eq!(array_value_to_string(batch[0].column(0), 0).unwrap(), "777");
    }
    assert_eq!(ctx.state().scalar_functions().len(), registered);
}

#[tokio::test]
async fn native_comparisons_compose_with_series_generation_groups_and_snapshots() {
    check("range x from 1 to 5 step 1 | extend y=2*x | make-series X=avg(x),Y=avg(y) on x from 1 to 6 step 1 | project Pearson=abs(series_pearson_correlation(X,Y)-1.0)<1E-12,Cosine=abs(series_cosine_similarity(X,Y)-1.0)<1E-12,Dot=series_dot_product(X,Y)",
        &[&["true","true","110.0"]]).await;
    check("datatable(g:string,a:dynamic)['a',dynamic([1,2,3]),'b',dynamic([4,5,6])] | partition hint.strategy=native by g (project g,Dot=series_dot_product(a,2)) | sort by g asc",
        &[&["a","12.0"],&["b","30.0"]]).await;
    check("print arrays=dynamic([[1,2],[3,4]]) | mv-apply a=arrays on (project Dot=series_dot_product(a,2)) | project Dot",
        &[&["6.0"],&["14.0"]]).await;
    let frame=context().kql("let T=materialize(print Dot=series_dot_product(dynamic([1,2,3]),dynamic([4,5,6]))); union T,T | project Dot").await.unwrap();
    for _ in 0..2 {
        let batches = frame.clone().collect().await.unwrap();
        assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 2);
    }
    check(
        "datatable(a:dynamic)[] | project score=series_cosine_similarity(a,dynamic([1,2]))",
        &[],
    )
    .await;
}

#[tokio::test]
async fn invalid_argument_shapes_and_scalar_magnitudes_fail_at_planning() {
    for query in [
        "print series_dot_product()",
        "print series_dot_product(1,2)",
        "print series_dot_product(dynamic([1]),'2')",
        "print series_dot_product(dynamic([1]),true)",
        "print series_pearson_correlation(dynamic([1]),2)",
        "print series_cosine_similarity(dynamic([1]),2)",
        "print series_cosine_similarity(dynamic([1]),dynamic([1]),'magnitude')",
        "print series_cosine_similarity(dynamic([1]),dynamic([1]),dynamic(1))",
        "print series_cosine_similarity(dynamic([1]),dynamic([1]),1,1,1)",
    ] {
        assert!(context().kql(query).await.is_err(), "{query}");
    }
}

#[tokio::test]
async fn native_api_checks_both_full_vector_lengths_before_truncation_or_cast_allocation() {
    for (size, succeeds) in [(1_048_576, true), (1_048_577, false)] {
        let ctx = context();
        let values = ListArray::from_iter_primitive::<Int64Type, _, _>([Some(
            std::iter::repeat_n(Some(1), size),
        )]);
        ctx.register_batch(
            TableReference::bare("Boundary"),
            RecordBatch::try_from_iter(vec![("A", Arc::new(values) as ArrayRef)]).unwrap(),
        )
        .unwrap();
        let frame=ctx.kql("Boundary | project D=series_dot_product(A,2),C=series_cosine_similarity(A,A),Short=series_dot_product(A,dynamic([1]))").await.unwrap();
        if succeeds {
            let batches = frame.collect().await.unwrap();
            assert_eq!(
                array_value_to_string(batches[0].column(0), 0).unwrap(),
                "2097152.0"
            );
            let cosine = batches[0]
                .column(1)
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap()
                .value(0);
            assert!((cosine - 1.0).abs() < 1E-12);
            assert_eq!(
                array_value_to_string(batches[0].column(2), 0).unwrap(),
                "1.0"
            );
        } else {
            assert!(
                frame
                    .collect()
                    .await
                    .unwrap_err()
                    .to_string()
                    .contains("1048576")
            );
        }
    }
}

#[tokio::test]
async fn sliced_fixed_large_and_view_lists_keep_their_own_offsets() {
    use datafusion::arrow::array::{
        FixedSizeListArray, Int64Array, LargeListArray, LargeListViewArray, ListViewArray,
    };
    use datafusion::arrow::buffer::ScalarBuffer;
    use datafusion::arrow::datatypes::Field;
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
    for (array, expected) in [
        (large.slice(1, 3), vec!["3.0", "<null>", "<null>"]),
        (fixed.slice(1, 3), vec!["3.0", "<null>", "3.0"]),
        (view, vec!["70.0", "10.0", "<null>"]),
        (large_view, vec!["70.0", "10.0", "<null>"]),
    ] {
        let ctx = context();
        ctx.register_batch(
            TableReference::bare("Layouts"),
            RecordBatch::try_from_iter(vec![("A", array)]).unwrap(),
        )
        .unwrap();
        let result=rows(&ctx,"Layouts | project Native=series_dot_product(A,1),General=series_dot_product(todynamic(A),1)").await;
        assert_eq!(
            result,
            expected
                .into_iter()
                .map(|value| vec![value.to_string(), value.to_string()])
                .collect::<Vec<_>>()
        );
    }
}

#[tokio::test]
async fn dictionary_boolean_decimal_temporal_and_unsigned_native_inputs_preserve_coercion() {
    use datafusion::arrow::array::{
        BooleanArray, Decimal128Array, DictionaryArray, DurationNanosecondArray, Int8Array,
        ListArray, UInt64Array,
    };
    use datafusion::arrow::buffer::OffsetBuffer;
    use datafusion::arrow::datatypes::{Field, Int8Type};
    let children: Vec<ArrayRef> = vec![
        Arc::new(BooleanArray::from(vec![
            Some(true),
            Some(false),
            Some(true),
        ])),
        Arc::new(
            Decimal128Array::from(vec![Some(15), Some(25), Some(30)])
                .with_precision_and_scale(8, 1)
                .unwrap(),
        ),
        Arc::new(DurationNanosecondArray::from(vec![
            Some(1_000_000_000),
            Some(2_000_000_000),
            Some(-1_000_000_000),
        ])),
        Arc::new(UInt64Array::from(vec![Some(1), Some(2), Some(3)])),
    ];
    for (values, expected) in children
        .into_iter()
        .zip(["2.0", "7.0", "20000000.0", "6.0"])
    {
        let list = ListArray::try_new(
            Arc::new(Field::new("item", values.data_type().clone(), true)),
            OffsetBuffer::new(vec![0_i32, 3].into()),
            values,
            None,
        )
        .unwrap();
        let ctx = context();
        ctx.register_batch(
            TableReference::bare("Typed"),
            RecordBatch::try_from_iter(vec![("A", Arc::new(list) as ArrayRef)]).unwrap(),
        )
        .unwrap();
        assert_eq!(rows(&ctx,"Typed | project D=series_dot_product(A,1),C=abs(series_cosine_similarity(A,A)-1.0)<1E-12").await,
                vec![vec![expected.to_string(),"true".to_string()]]);
    }
    let vectors = Arc::new(ListArray::from_iter_primitive::<Float64Type, _, _>([
        Some(vec![Some(1.0), Some(2.0)]),
        Some(vec![Some(3.0), Some(4.0)]),
    ])) as ArrayRef;
    let dictionary = DictionaryArray::<Int8Type>::try_new(
        Int8Array::from(vec![Some(1), None, Some(0)]),
        vectors,
    )
    .unwrap();
    let ctx = context();
    ctx.register_batch(
        TableReference::bare("DictionaryVectors"),
        RecordBatch::try_from_iter(vec![("A", Arc::new(dictionary) as ArrayRef)]).unwrap(),
    )
    .unwrap();
    assert_eq!(
        rows(
            &ctx,
            "DictionaryVectors | project D=series_dot_product(A,1)"
        )
        .await,
        vec![vec!["7.0"], vec!["<null>"], vec!["3.0"]]
    );
    let values = Arc::new(UInt64Array::from(vec![u64::MAX])) as ArrayRef;
    let list = ListArray::try_new(
        Arc::new(Field::new("item", DataType::UInt64, false)),
        OffsetBuffer::new(vec![0_i32, 1].into()),
        values,
        None,
    )
    .unwrap();
    ctx.register_batch(
        TableReference::bare("BadUnsigned"),
        RecordBatch::try_from_iter(vec![("A", Arc::new(list) as ArrayRef)]).unwrap(),
    )
    .unwrap();
    assert!(
        ctx.kql("BadUnsigned | project series_dot_product(A,1)")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap_err()
            .to_string()
            .contains("long range")
    );
}
