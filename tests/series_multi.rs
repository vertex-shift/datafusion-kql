use std::sync::Arc;

use datafusion::arrow::array::{Array, Float64Array, ListArray};
use datafusion::arrow::datatypes::{DataType, Float64Type};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::arrow::util::display::array_value_to_string;
use datafusion::common::{ScalarValue, TableReference};
use datafusion::logical_expr::{ColumnarValue, Volatility, create_udf};
use datafusion::prelude::*;
use datafusion_kql::{QueryParameters, SessionContextKqlExt};

fn context() -> SessionContext {
    SessionContext::new_with_config(SessionConfig::new().with_batch_size(2))
}

async fn rows(ctx: &SessionContext, query: &str) -> Vec<Vec<String>> {
    let frame = ctx
        .kql(query)
        .await
        .unwrap_or_else(|error| panic!("{}\n{query}", error.diagnostic(query)));
    frame
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

#[tokio::test]
async fn statistics_and_regression_have_native_source_defined_tuple_types() {
    let ctx = context();
    let query = "print x=dynamic([1,3,5]) | project series_fit_line(x)";
    let frame = ctx.kql(query).await.unwrap();
    assert_eq!(
        frame
            .schema()
            .fields()
            .iter()
            .map(|field| field.name().as_str())
            .collect::<Vec<_>>(),
        [
            "series_fit_line_x_rsquare",
            "series_fit_line_x_slope",
            "series_fit_line_x_variance",
            "series_fit_line_x_rvariance",
            "series_fit_line_x_interception",
            "series_fit_line_x_line_fit"
        ]
    );
    assert!(
        frame.schema().fields()[..5]
            .iter()
            .all(|field| field.data_type() == &DataType::Float64)
    );
    let values = rows(&ctx,"print x=dynamic([1,3,5]) | project (R,S,V,RV,B,F)=series_fit_line(x) | project R,S,V,RV,B,F=tostring(F)").await;
    assert_eq!(
        values,
        vec![vec!["1.0", "2.0", "4.0", "0.0", "1.0", "[1.0,3.0,5.0]"]]
    );
    let frame = ctx
        .kql("print x=dynamic([1,3,5]) | project series_stats(x)")
        .await
        .unwrap();
    assert_eq!(
        frame
            .schema()
            .fields()
            .iter()
            .map(|field| field.name().as_str())
            .collect::<Vec<_>>(),
        [
            "series_stats_x_min",
            "series_stats_x_min_idx",
            "series_stats_x_max",
            "series_stats_x_max_idx",
            "series_stats_x_avg",
            "series_stats_x_stdev",
            "series_stats_x_variance"
        ]
    );
    assert_eq!(frame.schema().field(1).data_type(), &DataType::Int64);
    assert_eq!(frame.schema().field(3).data_type(), &DataType::Int64);
}

#[tokio::test]
async fn partial_output_names_follow_project_and_extend_shapes() {
    let ctx = context();
    let projected = ctx
        .kql("print x=dynamic([1,3,5]) | project (R,S)=series_fit_line(x)")
        .await
        .unwrap();
    assert_eq!(projected.schema().fields().len(), 6);
    assert_eq!(projected.schema().field(0).name(), "R");
    assert_eq!(
        projected.schema().field(2).name(),
        "series_fit_line_x_variance"
    );
    let extended = ctx
        .kql("print x=dynamic([1,3,5]) | extend (R,S)=series_fit_line(x)")
        .await
        .unwrap();
    assert_eq!(
        extended
            .schema()
            .fields()
            .iter()
            .map(|field| field.name().as_str())
            .collect::<Vec<_>>(),
        ["x", "R", "S"]
    );
    let aliased = ctx
        .kql("print x=dynamic([1,3,5]) | project fitted=series_fit_line(x)")
        .await
        .unwrap();
    assert_eq!(aliased.schema().field(0).name(), "fitted_rsquare");
    assert_eq!(aliased.schema().field(5).name(), "fitted_line_fit");
    assert_eq!(
        rows(
            &ctx,
            "print x=dynamic([1,3,5]),R=99 | serialize (R,S)=series_fit_line(x) | project R,S"
        )
        .await,
        vec![vec!["1.0", "2.0"]]
    );
}

#[tokio::test]
async fn fft_and_ifft_match_known_complex_transforms_and_normalization() {
    let ctx = context();
    let result = rows(&ctx,"print x=dynamic([0,1,0,0]) | extend (r,i)=series_fft(x) | extend (back,zero)=series_ifft(r,i) | project r=tostring(r),i=tostring(i),back=tostring(back),zero=tostring(zero)").await;
    let values = result[0]
        .iter()
        .map(|value| serde_json::from_str::<Vec<f64>>(value).unwrap())
        .collect::<Vec<_>>();
    assert_eq!(values[0], [1.0, 0.0, -1.0, 0.0]);
    assert_eq!(values[1], [0.0, -1.0, 0.0, 1.0]);
    assert_eq!(values[2], [0.0, 1.0, 0.0, 0.0]);
    assert_eq!(values[3], [0.0; 4]);
    let result = rows(&ctx,"print (r,i)=series_fft(dynamic([1,2,3]),dynamic([3,2,1])) | extend (back,imag)=series_ifft(r,i) | project back=tostring(back),imag=tostring(imag)").await;
    for (actual, expected) in result[0].iter().zip([[1.0, 2.0, 3.0], [3.0, 2.0, 1.0]]) {
        for (actual, expected) in serde_json::from_str::<Vec<f64>>(actual)
            .unwrap()
            .into_iter()
            .zip(expected)
        {
            assert!((actual - expected).abs() < 1E-12);
        }
    }
}

#[tokio::test]
async fn fourier_plan_reuse_handles_varying_array_sizes_and_empty_inputs() {
    let ctx = context();
    let result = rows(&ctx,"datatable(a:dynamic)[dynamic([1,2,3]),dynamic([4]),dynamic([]),dynamic([5,6,7,8,9])] | extend (r,i)=series_fft(a) | extend (b,z)=series_ifft(r,i) | project a=tostring(a),b=tostring(b),z=tostring(z)").await;
    for row in result {
        let original: Vec<f64> = serde_json::from_str(&row[0]).unwrap();
        let restored: Vec<f64> = serde_json::from_str(&row[1]).unwrap();
        let imaginary: Vec<f64> = serde_json::from_str(&row[2]).unwrap();
        assert_eq!(original.len(), restored.len());
        for (actual, expected) in restored.into_iter().zip(original) {
            assert!((actual - expected).abs() < 1E-12);
        }
        assert!(imaginary.iter().all(|value| value.abs() < 1E-12));
    }
    assert_eq!(
        rows(
            &ctx,
            "print (r,i)=series_fft(dynamic(null)) | project r=isnull(r),i=isnull(i)"
        )
        .await,
        vec![vec!["true", "true"]]
    );
}

#[tokio::test]
async fn tuple_statistics_share_existing_null_and_ignore_nonfinite_semantics() {
    let ctx = context();
    assert_eq!(rows(&ctx,"print x=dynamic([1,null,3]) | extend (minimum,index,maximum,maxindex,mean,sd,variance)=series_stats(x,true) | project minimum,index,maximum,maxindex,mean,sd,variance").await,
        vec![vec!["1.0","0","3.0","2","2.0","1.4142135623730951","2.0"]]);
    assert_eq!(rows(&ctx,"print x=dynamic([1,null,3]) | extend (a,b)=series_stats(x), (c,d)=series_fit_line(x), (r,i)=series_fft(x) | project a=isnull(a),b=isnull(b),c=isnull(c),d=isnull(d),r=isnull(r),i=isnull(i)").await,
        vec![vec!["true","true","true","true","true","true"]]);
    assert_eq!(rows(&ctx,"print x=dynamic([]) | project (minimum,index)=series_stats(x) | project minimum=isnull(minimum),index=isnull(index)").await,
        vec![vec!["true","true"]]);
}

#[tokio::test]
async fn native_arrow_inputs_and_typed_request_parameters_are_supported() {
    let ctx = context();
    let values = ListArray::from_iter_primitive::<Float64Type, _, _>([
        Some(vec![Some(1.0), Some(0.0), Some(0.0), Some(0.0)]),
        Some(vec![Some(2.0)]),
    ]);
    let batch = RecordBatch::try_from_iter(vec![("values", Arc::new(values) as _)]).unwrap();
    ctx.register_batch(TableReference::bare("Signals"), batch)
        .unwrap();
    let result = rows(
        &ctx,
        "Signals | project (r,i)=series_fft(values) | project r=tostring(r),i=tostring(i)",
    )
    .await;
    assert_eq!(
        serde_json::from_str::<Vec<f64>>(&result[0][0]).unwrap(),
        [1.0; 4]
    );
    assert_eq!(
        serde_json::from_str::<Vec<f64>>(&result[1][0]).unwrap(),
        [2.0]
    );
    let batch = ctx
        .kql("print v=dynamic([1,3,5])")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let parameter = ScalarValue::try_from_array(batch[0].column(0), 0).unwrap();
    let batch = ctx
        .kql_with_parameters(
            "declare query_parameters(v:dynamic); print (R,S)=series_fit_line(v)",
            &QueryParameters::from([("v".into(), parameter)]),
        )
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let slope = batch[0]
        .column(1)
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    assert_eq!(slope.value(0), 2.0);
}

#[tokio::test]
async fn lexical_expansion_and_quoted_output_names_preserve_tuple_bindings() {
    let ctx = context();
    let result = rows(&ctx,"let f=(a:dynamic){series_fft(a)}; print a=dynamic([1,0,0,0]) | project (['when'],['TRUE'])=f(a) | project a=tostring(['when']),b=tostring(['TRUE'])").await;
    assert_eq!(
        serde_json::from_str::<Vec<f64>>(&result[0][0]).unwrap(),
        [1.0; 4]
    );
    assert_eq!(
        serde_json::from_str::<Vec<f64>>(&result[0][1]).unwrap(),
        [0.0; 4]
    );
    assert_eq!(
        rows(&ctx, "print (1+2),x=(3+4)").await,
        vec![vec!["3", "7"]]
    );
}

#[tokio::test]
async fn series_functions_remain_expression_local_and_do_not_replace_host_udfs() {
    let ctx = context();
    ctx.register_udf(create_udf(
        "series_fft",
        vec![DataType::Int64],
        DataType::Int64,
        Volatility::Immutable,
        Arc::new(|_| Ok(ColumnarValue::Scalar(ScalarValue::Int64(Some(777))))),
    ));
    let registry = ctx.state().scalar_functions().len();
    let frame = ctx
        .kql("print (r,i)=series_fft(dynamic([1,0]))")
        .await
        .unwrap();
    assert_eq!(frame.schema().fields().len(), 2);
    frame.collect().await.unwrap();
    let result = ctx
        .sql("select series_fft(1)")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_eq!(
        array_value_to_string(result[0].column(0), 0).unwrap(),
        "777"
    );
    assert_eq!(ctx.state().scalar_functions().len(), registry);
}

#[tokio::test]
async fn tuple_projection_rejects_bad_shapes_duplicate_names_and_scalar_nesting() {
    let ctx = context();
    for query in [
        "print (a,b)=1",
        "print (a,b,c)=series_fft(dynamic([1,2]))",
        "print (a,a)=series_fft(dynamic([1,2]))",
        "print (a,b)=series_fft()",
        "print (a,b)=series_fft(1)",
        "print x=tostring(series_fft(dynamic([1,2])))",
        "print (a,b)=series_stats(dynamic([1,2]),'yes')",
        "print (a,b)=series_stats(dynamic([1,2]),bool(null))",
        "datatable(a:dynamic,b:bool)[dynamic([1,2]),true] | extend (x,y)=series_stats(a,b)",
        "print (a,b)=series_fit_line(dynamic([1]),dynamic([2]))",
    ] {
        assert!(ctx.kql(query).await.is_err(), "{query}");
    }
    let mut invalid = datafusion_kql::parse("print (a,b)=series_fft(dynamic([1,2]))").unwrap();
    let datafusion_kql::ast::Source::Print(items) = &mut invalid.pipeline.source else {
        unreachable!()
    };
    let datafusion_kql::ast::ExprKind::ColumnNames { names, .. } = &mut items[0].expr.kind else {
        unreachable!()
    };
    names.clear();
    assert!(
        ctx.kql_ast(&invalid).await.is_err(),
        "public AST input must not bypass column-list validation"
    );
    let error = match ctx
        .kql("print (a,b)=series_fft(dynamic([1,2]),dynamic([1]))")
        .await
    {
        Ok(frame) => frame.collect().await.unwrap_err().to_string(),
        Err(error) => error.to_string(),
    };
    assert!(error.contains("same length"), "{error}");
}

#[tokio::test]
async fn empty_relations_and_reexecution_keep_typed_tuple_schemas() {
    let ctx = context();
    let frame = ctx
        .kql("datatable(x:dynamic)[] | project series_stats(x)")
        .await
        .unwrap();
    assert_eq!(frame.schema().field(1).data_type(), &DataType::Int64);
    assert_eq!(
        frame
            .collect()
            .await
            .unwrap()
            .iter()
            .map(RecordBatch::num_rows)
            .sum::<usize>(),
        0
    );
    let frame = ctx.kql("range x from 1 to 10 step 1 | project signal=pack_array(x,0,0,0) | extend (r,i)=series_fft(signal) | extend (back,z)=series_ifft(r,i) | summarize Bad=countif(abs(toreal(back[0])-x)>0.00000001)").await;
    assert!(
        frame.is_err(),
        "x was projected away and must not be captured accidentally"
    );
    let frame = ctx.kql("range x from 1 to 10 step 1 | extend signal=pack_array(x,0,0,0) | extend (r,i)=series_fft(signal) | extend (back,z)=series_ifft(r,i) | summarize Bad=countif(abs(toreal(back[0])-x)>0.00000001)").await.unwrap();
    for _ in 0..2 {
        let result = frame.clone().collect().await.unwrap();
        assert_eq!(array_value_to_string(result[0].column(0), 0).unwrap(), "0");
    }
}

#[tokio::test]
async fn multiple_field_accesses_share_one_native_transform() {
    let frame = context().kql("range x from 1 to 20 step 1 | extend signal=pack_array(x,0,0,0) | project (r,i)=series_fft(signal)").await.unwrap();
    let plan = frame.create_physical_plan().await.unwrap();
    let text = datafusion::physical_plan::displayable(plan.as_ref())
        .indent(false)
        .to_string();
    assert_eq!(
        text.matches("__kql_series_fft_columns").count(),
        1,
        "{text}"
    );
}

#[test]
fn parser_bounds_column_lists_without_confusing_parenthesized_expressions() {
    use datafusion_kql::ast::{ExprKind, Source};
    let names = (0..1024)
        .map(|index| format!("v{index}"))
        .collect::<Vec<_>>()
        .join(",");
    let query = format!("print ({names})=series_fft(dynamic([0]))");
    let parsed = datafusion_kql::parse(&query).unwrap();
    let Source::Print(items) = parsed.pipeline.source else {
        panic!("expected print source");
    };
    let ExprKind::ColumnNames { names: parsed, .. } = &items[0].expr.kind else {
        panic!("expected a column-name list");
    };
    assert_eq!(parsed.len(), 1024);
    let error = datafusion_kql::parse(&format!(
        "print ({names},overflow)=series_fft(dynamic([0]))"
    ))
    .unwrap_err();
    assert!(error.to_string().contains("1024"), "{error}");
    for query in [
        "print ()=series_fft(dynamic([0]))",
        "print (a,)=series_fft(dynamic([0]))",
    ] {
        assert!(datafusion_kql::parse(query).is_err(), "{query}");
    }
    datafusion_kql::parse("print (1+2),value=(3+4)").unwrap();
}

#[tokio::test]
async fn tuple_evaluation_preserves_one_volatile_input_call_per_source_row() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    let ctx = context();
    let calls = Arc::new(AtomicUsize::new(0));
    let observed = Arc::clone(&calls);
    ctx.register_udf(create_udf(
        "observe_sample",
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
    let frame = ctx.kql("range x from 1 to 25 step 1 | project (r,i)=series_fft(pack_array(observe_sample(x),0,0,0))").await.unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    let batches = frame.collect().await.unwrap();
    assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 25);
    assert_eq!(calls.load(Ordering::SeqCst), 25);
}

#[tokio::test]
async fn tuple_helpers_do_not_leak_or_capture_existing_names() {
    let ctx = context();
    let frame = ctx.kql("print signal=dynamic([1,0]),['__kql_temporary_0_tuple_0.real']=9 | project *, (r,i)=series_fft(signal)").await.unwrap();
    assert_eq!(frame.schema().fields().len(), 4);
    assert_eq!(
        frame.schema().field(1).name(),
        "__kql_temporary_0_tuple_0.real"
    );
    let result = frame.collect().await.unwrap();
    assert_eq!(array_value_to_string(result[0].column(1), 0).unwrap(), "9");
    assert!(
        ctx.kql("print x=dynamic([1,0]) | project (r,i)=series_fft(x),copy=r")
            .await
            .is_err(),
        "sibling outputs do not become input bindings"
    );
    assert!(
        ctx.kql("print x=dynamic([1,0]) | extend (r,i)=series_fft(x),copy=r")
            .await
            .is_err()
    );
}

#[tokio::test]
async fn tuples_preserve_serialized_order_and_group_local_execution() {
    let ctx = context();
    assert_eq!(rows(&ctx,"range x from 1 to 4 step 1 | serialize n=row_number() | extend (r,i)=series_fft(pack_array(x,0)) | extend p=prev(x,1,0) | project n,p,r=toreal(r[0])").await,
        vec![vec!["1","0","1.0"],vec!["2","1","2.0"],vec!["3","2","3.0"],vec!["4","3","4.0"]]);
    assert_eq!(rows(&ctx,"datatable(g:long,v:dynamic)[1,dynamic([1,2]),2,dynamic([3,4])] | partition hint.strategy=native by g (extend (r,i)=series_fft(v) | project Total=toreal(r[0])) | sort by Total asc").await,
        vec![vec!["3.0"],vec!["7.0"]]);
    assert_eq!(rows(&ctx,"print v=dynamic([[1,0],[2,0]]) | mv-apply v on (extend (r,i)=series_fft(v) | project Total=toreal(r[0])) | sort by Total asc").await,
        vec![vec!["1.0"],vec!["2.0"]]);
    assert_eq!(rows(&ctx,"let T=materialize(print (r,i)=series_fft(dynamic([1,0]))); union T,T | project Total=toreal(r[0])").await,
        vec![vec!["1.0"],vec!["1.0"]]);
    assert_eq!(rows(&ctx,"range x from 1 to 3 step 1 | serialize | project (r,i)=series_fft(pack_array(prev(x,1,0),0)) | project v=toreal(r[0])").await,
        vec![vec!["0.0"],vec!["1.0"],vec!["2.0"]]);
}

#[tokio::test]
async fn distinct_after_named_projection_does_not_retain_hidden_ordinals() {
    let ctx = context();
    assert_eq!(
        rows(
            &ctx,
            "range x from 1 to 10 step 1 | serialize | extend key=x%2 | distinct key | count"
        )
        .await,
        vec![vec!["2"]]
    );
    assert!(
        ctx.kql("range x from 1 to 10 step 1 | serialize | distinct x | extend p=prev(x)")
            .await
            .is_err()
    );
}

#[tokio::test]
async fn full_api_accepts_the_maximum_zero_signal_and_rejects_one_more_point() {
    for (length, succeeds) in [(1_048_576, true), (1_048_577, false)] {
        let ctx = context();
        let signal = ListArray::from_iter_primitive::<Float64Type, _, _>([Some(
            std::iter::repeat_n(Some(0.0), length),
        )]);
        let batch = RecordBatch::try_from_iter(vec![("signal", Arc::new(signal) as _)]).unwrap();
        ctx.register_batch(TableReference::bare("BoundarySignal"), batch)
            .unwrap();
        let frame = ctx.kql("BoundarySignal | project (r,i)=series_fft(signal) | project Re=array_length(r),Im=array_length(i),Zero=toreal(r[1048575])==0.0 and toreal(i[1048575])==0.0").await.unwrap();
        if succeeds {
            let result = frame.collect().await.unwrap();
            for index in 0..2 {
                assert_eq!(
                    array_value_to_string(result[0].column(index), 0).unwrap(),
                    "1048576"
                );
            }
            assert_eq!(
                array_value_to_string(result[0].column(2), 0).unwrap(),
                "true"
            );
        } else {
            let error = frame.collect().await.unwrap_err().to_string();
            assert!(
                error.contains("1048576") || error.contains("1,048,576"),
                "{error}"
            );
        }
    }
}
