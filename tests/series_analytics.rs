use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, Float64Array, ListArray};
use datafusion::arrow::datatypes::{DataType, Float64Type};
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

async fn check(query: &str, expected: &[&[&str]]) {
    let expected = expected
        .iter()
        .map(|row| {
            row.iter()
                .map(|value| value.to_string())
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    assert_eq!(rows(&context(), query).await, expected, "{query}");
}

const SIGNAL: &str = "dynamic([1,3,5,7,20,19,18,17])";

#[tokio::test]
async fn segmented_fits_return_the_source_defined_native_schema() {
    let ctx = context();
    let frame = ctx
        .kql(&format!("print x={SIGNAL} | project series_fit_2lines(x)"))
        .await
        .unwrap();
    let fields = frame.schema().fields();
    assert_eq!(
        fields
            .iter()
            .map(|field| field.name().as_str())
            .collect::<Vec<_>>(),
        [
            "series_fit_2lines_x_rsquare",
            "series_fit_2lines_x_split_idx",
            "series_fit_2lines_x_variance",
            "series_fit_2lines_x_rvariance",
            "series_fit_2lines_x_line_fit",
            "series_fit_2lines_x_right_rsquare",
            "series_fit_2lines_x_right_slope",
            "series_fit_2lines_x_right_interception",
            "series_fit_2lines_x_right_variance",
            "series_fit_2lines_x_right_rvariance",
            "series_fit_2lines_x_left_rsquare",
            "series_fit_2lines_x_left_slope",
            "series_fit_2lines_x_left_interception",
            "series_fit_2lines_x_left_variance",
            "series_fit_2lines_x_left_rvariance",
        ]
    );
    for (index, field) in fields.iter().enumerate() {
        match index {
            1 => assert_eq!(field.data_type(), &DataType::Int64),
            4 => assert!(matches!(field.data_type(), DataType::Union(_, _))),
            _ => assert_eq!(field.data_type(), &DataType::Float64),
        }
    }
    check(&format!("print x={SIGNAL} | extend (R,Split)=series_fit_2lines(x) | project R=R>0.99999999,Split"),
        &[&["true", "4"]]).await;
    let frame = ctx
        .kql(&format!(
            "print x={SIGNAL} | project (R,Split)=series_fit_2lines(x)"
        ))
        .await
        .unwrap();
    assert_eq!(frame.schema().fields().len(), 15);
    assert_eq!(
        frame.schema().field(2).name(),
        "series_fit_2lines_x_variance"
    );
}

#[tokio::test]
async fn dynamic_and_native_segmented_outputs_are_correlated_and_share_one_call() {
    let query = format!(
        "print x={SIGNAL} | extend f=series_fit_2lines_dynamic(x) | extend (R,Split,V,RV,F,RR,RS,RB,RV0,RRV,LR,LS,LB,LV,LRV)=series_fit_2lines(x) | project same=Split==tolong(f.split_idx) and abs(R-toreal(f.rsquare))<0.00000001 and abs(V-toreal(f.variance))<0.00000001,left=abs(LS-2.0)<0.00000001 and abs(LB-1.0)<0.00000001,right=abs(RS+1.0)<0.00000001 and abs(RB-20.0)<0.00000001,aliases=abs(toreal(f.left.slope)-LS)<0.00000001 and abs(toreal(f.right.slope)-RS)<0.00000001,fit=abs(toreal(F[7])-17.0)<0.00000001"
    );
    check(&query, &[&["true", "true", "true", "true", "true"]]).await;
    let ctx = context();
    let plan = ctx.kql("range x from 1 to 20 step 1 | project (R,Split,V,RV,F)=series_fit_2lines(pack_array(x,3,5,7,20,19,18,17))")
        .await.unwrap().create_physical_plan().await.unwrap();
    let text = datafusion::physical_plan::displayable(plan.as_ref())
        .indent(false)
        .to_string();
    assert_eq!(
        text.matches("__kql_series_fit_2lines_columns").count(),
        1,
        "{text}"
    );
}

#[tokio::test]
async fn small_missing_and_nonnumeric_regression_inputs_produce_typed_nulls() {
    for signal in [
        "dynamic([])",
        "dynamic([1])",
        "dynamic([1,2,3])",
        "dynamic([1,null,3,4])",
        "dynamic([1,'2',3,4])",
        "dynamic(null)",
    ] {
        check(&format!(
            "print x={signal} | extend f=series_fit_2lines_dynamic(x) | extend (R,Split,V,RV,F)=series_fit_2lines(x) | project bag=isnull(f),R=isnull(R),Split=isnull(Split),V=isnull(V),RV=isnull(RV),F=isnull(F)"
        ), &[&["true", "true", "true", "true", "true", "true"]]).await;
    }
    check("print x=dynamic([7,7,7,7,7,7]) | extend (R,Split,V,RV)=series_fit_2lines(x) | project R,Split,V,RV",
        &[&["1.0", "2", "0.0", "0.0"]]).await;
}

#[tokio::test]
async fn short_segment_cases_and_breakpoints_preserve_local_coordinates() {
    check("print f=series_fit_2lines_dynamic(dynamic([1,3,10,9])) | project Split=tolong(f.split_idx),left=abs(toreal(f.left_slope)-2.0)<0.00000001,right=abs(toreal(f.right_slope)+1.0)<0.00000001,origin=abs(toreal(f.right_interception)-10.0)<0.00000001,points=array_length(f.line_fit)",
        &[&["2", "true", "true", "true", "4"]]).await;
}

#[tokio::test]
async fn tukey_scores_use_linear_quartiles_and_have_signed_thresholds() {
    let result = rows(
        &context(),
        "print s=series_outliers(dynamic([-20,1,2,3,4,5,6,40]),'tukey') | project s=tostring(s)",
    )
    .await;
    let actual: Vec<f64> = serde_json::from_str(&result[0][0]).unwrap();
    let expected = [
        -21.75 / 3.5,
        -0.75 / 3.5,
        0.0,
        0.0,
        0.0,
        0.0,
        0.75 / 3.5,
        34.75 / 3.5,
    ];
    assert_eq!(actual.len(), expected.len());
    for (actual, expected) in actual.iter().zip(expected) {
        assert!((*actual - expected).abs() < 1E-12);
    }
    assert!(actual[0] < -1.5 && actual[7] > 1.5);
}

#[tokio::test]
async fn custom_tukey_normalization_is_symmetric_and_reduces_to_standard_quartiles() {
    check("print x=dynamic([-20,1,2,3,4,5,6,40]) | extend t=series_outliers(x,'tukey'),c=series_outliers(x,'ctukey',real(null),25,75),d=series_outliers(x),explicit=series_outliers(x,'ctukey',real(null),10,90) | project standard=abs(toreal(t[0])-toreal(c[0]))<0.00000001,defaults=abs(toreal(d[7])-toreal(explicit[7]))<0.00000001,symmetric=abs(toreal(series_outliers(dynamic([-40,-6,-5,-4,-3,-2,-1,20]))[0])+toreal(d[7]))<0.00000001",
        &[&["true", "true", "true"]]).await;
}

#[tokio::test]
async fn ignored_values_and_null_positions_keep_the_original_score_array_shape() {
    check("print s=series_outliers(dynamic([null,-999,1,2,3,4,5,100]),'tukey',-999) | project points=array_length(s),nullScore=toreal(s[0]),ignored=toreal(s[1]),high=toreal(s[7])>1.5",
        &[&["8", "0.0", "0.0", "true"]]).await;
    check("print a=tostring(series_outliers(dynamic([]))),b=tostring(series_outliers(dynamic([null,null]))),c=tostring(series_outliers(dynamic([7,7,7,7]))),d=tostring(series_outliers(dynamic([1,1,1]),'tukey',1))",
        &[&["[]", "[0.0,0.0]", "[0.0,0.0,0.0,0.0]", "[0.0,0.0,0.0]"]]).await;
    check(
        "print s=series_outliers(dynamic([1,'2',3,4]),'tukey') | project missing=isnull(s)",
        &[&["true"]],
    )
    .await;
}

#[tokio::test]
async fn runtime_outlier_configuration_and_query_parameters_are_supported() {
    let query = "datatable(method:string,low:real,high:real)['tukey',10.0,90.0,'ctukey',25.0,75.0] | project s=series_outliers(dynamic([-20,1,2,3,4,5,6,40]),method,real(null),low,high) | project lo=toreal(s[0]),hi=toreal(s[7])";
    let actual = rows(&context(), query).await;
    for (standard, custom) in actual[0].iter().zip(&actual[1]) {
        assert!((standard.parse::<f64>().unwrap() - custom.parse::<f64>().unwrap()).abs() < 1E-12);
    }
    let parameters = QueryParameters::from([
        ("method".into(), ScalarValue::Utf8(Some("ctukey".into()))),
        ("low".into(), ScalarValue::Float64(Some(25.0))),
        ("high".into(), ScalarValue::Float64(Some(75.0))),
    ]);
    let batch = context().kql_with_parameters(
        "declare query_parameters(method:string,low:real,high:real); print s=series_outliers(dynamic([-20,1,2,3,4,5,6,40]),method,real(null),low,high) | project lo=toreal(s[0])",
        &parameters,
    ).await.unwrap().collect().await.unwrap();
    assert!(
        (batch[0]
            .column(0)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .value(0)
            + 21.75 / 3.5)
            .abs()
            < 1E-12
    );
}

#[tokio::test]
async fn analytics_compose_with_make_series_native_arrow_and_grouped_templates() {
    check("range x from 0 to 7 step 1 | extend v=iff(x<4,1.0+2.0*x,24.0-x) | make-series V=avg(v) on x from 0 to 8 step 1 | extend (R,Split)=series_fit_2lines(V) | project R=R>0.99999999,Split",
        &[&["true", "4"]]).await;
    let ctx = context();
    let signals = ListArray::from_iter_primitive::<Float64Type, _, _>([
        Some(vec![
            Some(1.0),
            Some(3.0),
            Some(5.0),
            Some(7.0),
            Some(20.0),
            Some(19.0),
            Some(18.0),
            Some(17.0),
        ]),
        Some(vec![Some(1.0), Some(3.0), Some(10.0), Some(9.0)]),
    ]);
    let batch =
        RecordBatch::try_from_iter(vec![("signal", Arc::new(signals) as ArrayRef)]).unwrap();
    ctx.register_batch(TableReference::bare("Signals"), batch)
        .unwrap();
    assert_eq!(rows(&ctx, "Signals | project (R,Split)=series_fit_2lines(signal) | project Split | sort by Split asc").await,
        vec![vec!["2"],vec!["4"]]);
    check(&format!(    "print signals=pack_array({SIGNAL},{SIGNAL}) | mv-apply s=signals on (extend (R,Split)=series_fit_2lines(s) | project Split) | project Split"),
        &[&["4"],&["4"]]).await;
}

#[tokio::test]
async fn lexical_functions_materialized_snapshots_and_reexecution_do_not_lose_outputs() {
    check(&format!("let f=(s:dynamic){{series_fit_2lines(s)}}; print x={SIGNAL} | extend (R,Split)=f(x) | project Split"),
        &[&["4"]]).await;
    let ctx = context();
    let frame = ctx.kql(&format!(
        "let T=materialize(print s={SIGNAL} | extend (R,Split)=series_fit_2lines(s)); union T,T | project Split"
    )).await.unwrap();
    for _ in 0..2 {
        let actual = frame.clone().collect().await.unwrap();
        assert_eq!(actual.iter().map(RecordBatch::num_rows).sum::<usize>(), 2);
        for batch in actual {
            for row in 0..batch.num_rows() {
                assert_eq!(array_value_to_string(batch.column(0), row).unwrap(), "4");
            }
        }
    }
}

#[tokio::test]
async fn analytics_are_expression_local_and_preserve_registered_host_functions() {
    let ctx = context();
    for name in ["series_outliers", "series_fit_2lines"] {
        ctx.register_udf(create_udf(
            name,
            vec![DataType::Int64],
            DataType::Int64,
            Volatility::Immutable,
            Arc::new(|_| Ok(ColumnarValue::Scalar(ScalarValue::Int64(Some(777))))),
        ));
    }
    let registered = ctx.state().scalar_functions().len();
    let result = ctx
        .kql(&format!(
            "print x={SIGNAL} | project (R,Split)=series_fit_2lines(x),Scores=series_outliers(x)"
        ))
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_eq!(result[0].num_columns(), 16);
    for name in ["series_outliers", "series_fit_2lines"] {
        let result = ctx
            .sql(&format!("select {name}(1)"))
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        assert_eq!(
            array_value_to_string(result[0].column(0), 0).unwrap(),
            "777"
        );
    }
    assert_eq!(ctx.state().scalar_functions().len(), registered);
}

#[tokio::test]
async fn normalized_line_fits_avoid_unnecessary_sum_overflow() {
    let ctx = context();
    let batch = RecordBatch::try_from_iter(vec![(
        "signal",
        Arc::new(ListArray::from_iter_primitive::<Float64Type, _, _>([Some(
            vec![Some(1E308); 8],
        )])) as ArrayRef,
    )])
    .unwrap();
    ctx.register_batch(TableReference::bare("HugeConstant"), batch)
        .unwrap();
    check("print fit=series_fit_line_dynamic(dynamic([1,3,5])) | project R=toreal(fit.rsquare),Slope=toreal(fit.slope)",
        &[&["1.0","2.0"]]).await;
    let result = rows(&ctx, "HugeConstant | extend f=series_fit_line_dynamic(signal),g=series_fit_2lines_dynamic(signal) | project same=toreal(f.interception)==1E308,R=toreal(f.rsquare),V=toreal(f.variance),Split=tolong(g.split_idx)").await;
    assert_eq!(result, vec![vec!["true", "1.0", "0.0", "2"]]);
}

#[tokio::test]
async fn invalid_analytics_configuration_is_rejected_explicitly() {
    let ctx = context();
    for query in [
        "print s=series_outliers()",
        "print s=series_outliers(dynamic([1,2,3]),'ctukey',real(null),10,90,1)",
        "print (R,Split)=series_fit_2lines()",
        "print (R,Split)=series_fit_2lines(1)",
        "print (a,b,c,d,e,f,g,h,i,j,k,l,m,n,o,p)=series_fit_2lines(dynamic([1,2,3,4]))",
        "print s=array_length(series_fit_2lines(dynamic([1,2,3,4])))",
    ] {
        assert!(ctx.kql(query).await.is_err(), "{query}");
    }
    for query in [
        "print s=series_outliers(dynamic([1,2,3,4]),'unknown')",
        "print s=series_outliers(dynamic([1,2,3,4]),'ctukey',real(null),1,90)",
        "print s=series_outliers(dynamic([1,2,3,4]),'ctukey',real(null),10,99)",
        "print s=series_outliers(dynamic([1,2,3,4]),'ctukey',real(null),90,10)",
        "print s=series_outliers(dynamic([1,2,3,4]),'ctukey',real(null),50,50)",
        "print s=series_outliers(dynamic([1,2,3,4]),'ctukey','missing')",
        "print s=series_outliers(dynamic([1,2,3,4]),'ctukey',real(null),real(nan),90)",
        "print s=series_outliers(dynamic([0,0,0,0,0,0,0,0,0,100]),'tukey')",
    ] {
        let error = match ctx.kql(query).await {
            Ok(frame) => frame.collect().await.unwrap_err().to_string(),
            Err(error) => error.to_string(),
        };
        assert!(
            error.contains("outlier") || error.contains("ctukey"),
            "{query}\n{error}"
        );
    }
}

#[tokio::test]
async fn typed_segmented_fields_share_a_volatile_input_evaluation() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    let ctx = context();
    let calls = Arc::new(AtomicUsize::new(0));
    let observed = Arc::clone(&calls);
    ctx.register_udf(create_udf(
        "observe_trend",
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
    let frame = ctx.kql("range x from 1 to 25 step 1 | project (R,Split,V,RV,F)=series_fit_2lines(pack_array(observe_trend(x),3,5,7,20,19,18,17))")
        .await.unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    let result = frame.collect().await.unwrap();
    assert_eq!(result.iter().map(RecordBatch::num_rows).sum::<usize>(), 25);
    assert_eq!(calls.load(Ordering::SeqCst), 25);
}

#[tokio::test]
async fn native_arrow_api_accepts_the_exact_analytics_point_limit() {
    for (size, succeeds) in [(1_048_576, true), (1_048_577, false)] {
        let ctx = context();
        let signals = ListArray::from_iter_primitive::<Float64Type, _, _>([Some(
            std::iter::repeat_n(Some(7.0), size),
        )]);
        ctx.register_batch(
            TableReference::bare("Boundary"),
            RecordBatch::try_from_iter(vec![("signal", Arc::new(signals) as ArrayRef)]).unwrap(),
        )
        .unwrap();
        let frame = ctx.kql("Boundary | extend (R,Split,V,RV,F)=series_fit_2lines(signal) | project Scores=series_outliers(signal),F,Split | project Scores=array_length(Scores),Fit=array_length(F),Split")
            .await.unwrap();
        if succeeds {
            let result = frame.collect().await.unwrap();
            for (index, expected) in [(0, "1048576"), (1, "1048576"), (2, "2")] {
                assert_eq!(
                    array_value_to_string(result[0].column(index), 0).unwrap(),
                    expected
                );
            }
        } else {
            let error = frame.collect().await.unwrap_err().to_string();
            assert!(error.contains("1048576"), "{error}");
        }
    }
}
