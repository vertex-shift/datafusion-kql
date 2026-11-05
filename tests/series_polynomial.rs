use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

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
async fn polynomial_tuple_types_names_and_prefixes_match_the_live_reference() {
    let ctx = context();
    let frame = ctx
        .kql("print y=dynamic([1,4,9,16]) | project series_fit_poly(y)")
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
            "series_fit_poly_y_rsquare",
            "series_fit_poly_y_coefficients",
            "series_fit_poly_y_variance",
            "series_fit_poly_y_rvariance",
            "series_fit_poly_y_poly_fit"
        ]
    );
    for (index, field) in frame.schema().fields().iter().enumerate() {
        match index {
            1 | 4 => assert!(matches!(field.data_type(), DataType::Union(_, _))),
            _ => assert_eq!(field.data_type(), &DataType::Float64),
        }
    }
    let projected = ctx
        .kql("print y=dynamic([1,4,9,16]) | project (R,C)=series_fit_poly(y)")
        .await
        .unwrap();
    assert_eq!(projected.schema().fields().len(), 5);
    assert_eq!(projected.schema().field(0).name(), "R");
    assert_eq!(
        projected.schema().field(2).name(),
        "series_fit_poly_y_variance"
    );
    let extended = ctx
        .kql("print y=dynamic([1,4,9,16]) | extend (R,C)=series_fit_poly(y)")
        .await
        .unwrap();
    assert_eq!(extended.schema().fields().len(), 3);
    let prefix = ctx
        .kql("print fit=series_fit_poly(dynamic([1,4,9,16]))")
        .await
        .unwrap();
    assert_eq!(prefix.schema().field(1).name(), "fit_coefficients");
}

#[tokio::test]
async fn omitted_x_uses_one_based_indices_and_omitted_degree_fits_a_line() {
    check("print (R,C,V,RV,F)=series_fit_poly(dynamic([1,4,9,16])) | project Slope=abs(toreal(C[0])-5.0)<1E-10,Intercept=abs(toreal(C[1])+5.0)<1E-10,R=abs(R-0.9689922480620156)<1E-10,V=abs(V-43.0)<1E-10,RV=abs(RV-4.0/3.0)<1E-10,First=abs(toreal(F[0]))<1E-10",
        &[&["true","true","true","true","true","true"]]).await;
}

#[tokio::test]
async fn explicit_irregular_x_and_quadratic_degree_retain_all_correlated_outputs() {
    check("print (R,C,V,RV,F)=series_fit_poly(dynamic([3,7,43,73]),dynamic([0,1,4,5]),2) | project R=abs(R-0.9959487927402366)<1E-10,C=abs(toreal(C[0])-3.25)<1E-10 and abs(toreal(C[1])+2.7794117647059013)<1E-10 and abs(toreal(C[2])-4.3235294117647225)<1E-10,V=abs(V-1089.0)<1E-10,RV=abs(RV-4.411764705882351)<1E-10,Points=array_length(F)",
        &[&["true","true","true","true","4"]]).await;
    check("print (R,C,V,RV,F)=series_fit_poly(dynamic([1,4,9,16]),dynamic([1,2,3,4]),2) | project R=R>0.999999999,C=abs(toreal(C[0])-1.0)<1E-10 and abs(toreal(C[1]))<1E-10 and abs(toreal(C[2]))<1E-10,F=abs(toreal(F[3])-16.0)<1E-10",
        &[&["true","true","true"]]).await;
}

#[tokio::test]
async fn null_coordinates_do_not_mean_omitted_and_bad_shapes_produce_typed_nulls() {
    for arguments in [
        "dynamic([])",
        "dynamic(null)",
        "dynamic([1,null,3])",
        "dynamic([1,'2',3])",
        "dynamic([1,2,3]),dynamic(null)",
        "dynamic([1,2,3]),dynamic([1,2])",
        "dynamic([1,2,3]),dynamic([1,null,3])",
        "dynamic([1,2,3]),dynamic([1,'2',3])",
    ] {
        check(&format!("print (R,C,V,RV,F)=series_fit_poly({arguments}) | project R=isnull(R),C=isnull(C),V=isnull(V),RV=isnull(RV),F=isnull(F)"),
            &[&["true","true","true","true","true"]]).await;
    }
}

#[tokio::test]
async fn zero_negative_and_null_degree_match_captured_service_coercions() {
    check("print y=dynamic([1,2,3,4]),x=dynamic([1,2,3,4]) | extend (R,C,V,RV,F)=series_fit_poly(y,x,0) | project R,Coefficients=array_length(C),Mean=abs(toreal(C[0])-2.5)<1E-10,V=abs(V-RV)<1E-10",
        &[&["0.0","1","true","true"]]).await;
    check("print y=dynamic([1,2,3,4]),x=dynamic([1,2,3,4]) | extend (R,C)=series_fit_poly(y,x,long(null)) | project R,Points=array_length(C),Mean=abs(toreal(C[0])-2.5)<1E-10",
        &[&["0.0","1","true"]]).await;
    check("print y=dynamic([1,2,3,4]),x=dynamic([1,2,3,4]) | extend (R,C)=series_fit_poly(y,x,-9) | project R=R>0.9999999,Points=array_length(C),Slope=abs(toreal(C[0])-1.0)<1E-10",
        &[&["true","2","true"]]).await;
}

#[tokio::test]
async fn underdetermined_and_rank_deficient_fits_have_original_power_basis_minimum_norm() {
    check("print (R,C,V,RV,F)=series_fit_poly(dynamic([1,2]),dynamic([1,2]),2) | project R=R>0.9999999,Leading=abs(toreal(C[0])-3.0/14.0)<1E-10,Linear=abs(toreal(C[1])-5.0/14.0)<1E-10,Constant=abs(toreal(C[2])-3.0/7.0)<1E-10,Last=abs(toreal(F[1])-2.0)<1E-10",
        &[&["true","true","true","true","true"]]).await;
    check("print (R,C,V,RV,F)=series_fit_poly(dynamic([1,2,3,4]),dynamic([1,1,1,1])) | project R,Slope=abs(toreal(C[0])-1.25)<1E-10,Constant=abs(toreal(C[1])-1.25)<1E-10,V=abs(V-RV)<1E-10,Mean=abs(toreal(F[3])-2.5)<1E-10",
        &[&["0.0","true","true","true","true"]]).await;
    check("print (R,C,V,RV,F)=series_fit_poly(dynamic([7])) | project R,V,RV,Points=array_length(F),Coefficients=array_length(C),Fit=abs(toreal(F[0])-7.0)<1E-10",
        &[&["0.0","0.0","0.0","1","2","true"]]).await;
}

#[tokio::test]
async fn native_arrow_rows_with_different_lengths_and_nulls_share_the_batch_function() {
    let ctx = context();
    let y = ListArray::from_iter_primitive::<Float64Type, _, _>([
        Some(vec![Some(1.0), Some(4.0), Some(9.0), Some(16.0)]),
        Some(vec![Some(7.0)]),
        None,
        Some(vec![]),
        Some(vec![Some(7.0); 8]),
        Some(vec![Some(f64::NAN), Some(1.0)]),
    ]);
    ctx.register_batch(
        TableReference::bare("Signals"),
        RecordBatch::try_from_iter(vec![("Y", Arc::new(y) as ArrayRef)]).unwrap(),
    )
    .unwrap();
    let result = rows(&ctx,"Signals | project (R,C,V,RV,F)=series_fit_poly(Y) | project Length=array_length(F),RNull=isnull(R),V").await;
    assert_eq!(result.len(), 6);
    assert_eq!(result[0][0], "4");
    assert_eq!(result[1], ["1", "false", "0.0"]);
    assert_eq!(result[2], ["<null>", "true", "<null>"]);
    assert_eq!(result[3], ["<null>", "true", "<null>"]);
    assert_eq!(result[4], ["8", "false", "0.0"]);
    assert_eq!(result[5], ["<null>", "true", "<null>"]);
}

#[tokio::test]
async fn native_coordinate_arrays_serialized_projections_and_empty_batches_are_supported() {
    let ctx = context();
    let y = ListArray::from_iter_primitive::<Float64Type, _, _>([
        Some(vec![Some(3.0), Some(7.0), Some(43.0), Some(73.0)]),
        Some(vec![Some(1.0), Some(4.0), Some(9.0), Some(16.0)]),
    ]);
    let x = ListArray::from_iter_primitive::<Float64Type, _, _>([
        Some(vec![Some(0.0), Some(1.0), Some(4.0), Some(5.0)]),
        Some(vec![Some(1.0), Some(2.0), Some(3.0), Some(4.0)]),
    ]);
    ctx.register_batch(
        TableReference::bare("Coordinates"),
        RecordBatch::try_from_iter(vec![
            ("Y", Arc::new(y) as ArrayRef),
            ("X", Arc::new(x) as ArrayRef),
        ])
        .unwrap(),
    )
    .unwrap();
    assert_eq!(
        rows(&ctx, "Coordinates | serialize (R,C)=series_fit_poly(Y,X,2) | project Leading=round(toreal(C[0]),2),Row=row_number()").await,
        vec![vec!["3.25", "1"], vec!["1.0", "2"]]
    );
    let frame = ctx
        .kql("Coordinates | where false | project series_fit_poly(Y,X,2)")
        .await
        .unwrap();
    assert_eq!(frame.schema().fields().len(), 5);
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
}

#[tokio::test]
async fn lexical_bindings_and_query_parameters_supply_constant_degrees() {
    check("let degree=1+1; let fit=(y:dynamic,x:dynamic){series_fit_poly(y,x,degree)}; print y=dynamic([1,4,9]),x=dynamic([1,2,3]) | extend (R,C)=fit(y,x) | project R=R>0.999999,C=abs(toreal(C[0])-1.0)<1E-10",
        &[&["true","true"]]).await;
    let params = QueryParameters::from([("degree".into(), ScalarValue::Int64(Some(2)))]);
    let batches = context().kql_with_parameters(
        "declare query_parameters(degree:long); print (R,C)=series_fit_poly(dynamic([1,4,9]),dynamic([1,2,3]),degree)",
        &params).await.unwrap().collect().await.unwrap();
    assert_eq!(batches[0].num_columns(), 5);
    assert!(
        batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .value(0)
            > 0.999999
    );
}

#[tokio::test]
async fn make_series_grouped_templates_and_materialized_snapshots_preserve_polynomial_results() {
    check("range x from 1 to 5 step 1 | extend y=x*x | make-series Y=avg(y) on x from 1 to 6 step 1 | extend (R,C,V,RV,F)=series_fit_poly(Y,x,2) | project R=R>0.999999,C=abs(toreal(C[0])-1.0)<1E-10,Last=abs(toreal(F[4])-25.0)<1E-10",
        &[&["true","true","true"]]).await;
    check("datatable(g:string,y:dynamic)['a',dynamic([1,3,5]),'b',dynamic([2,4,6])] | partition hint.strategy=native by g (extend (R,C)=series_fit_poly(y) | project g,Good=abs(toreal(C[0])-2.0)<1E-10) | sort by g asc",
        &[&["a","true"],&["b","true"]]).await;
    check("print signals=dynamic([[1,4,9],[4,9,16]]) | mv-apply y=signals on (extend (R,C)=series_fit_poly(y,dynamic([1,2,3]),2) | project Good=abs(toreal(C[0])-1.0)<1E-10) | project Good",
        &[&["true"],&["true"]]).await;
    let frame = context().kql("let T=materialize(print (R,C)=series_fit_poly(dynamic([1,4,9]),dynamic([1,2,3]),2)); union T,T | project Good=R>0.999999").await.unwrap();
    for _ in 0..2 {
        let batches = frame.clone().collect().await.unwrap();
        assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 2);
    }
}

#[tokio::test]
async fn polynomial_plans_stay_lazy_and_evaluate_volatile_input_once_per_row() {
    let ctx = context();
    let calls = Arc::new(AtomicUsize::new(0));
    let observed = Arc::clone(&calls);
    ctx.register_udf(create_udf(
        "observe_poly",
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
    let frame = ctx.kql("range n from 1 to 25 step 1 | project (R,C,V,RV,F)=series_fit_poly(pack_array(observe_poly(n),4,9,16),dynamic([1,2,3,4]),2)").await.unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    let plan = frame.clone().create_physical_plan().await.unwrap();
    let text = datafusion::physical_plan::displayable(plan.as_ref())
        .indent(false)
        .to_string();
    assert_eq!(
        text.matches("__kql_series_fit_poly_columns").count(),
        1,
        "{text}"
    );
    let batches = frame.collect().await.unwrap();
    assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 25);
    assert_eq!(calls.load(Ordering::SeqCst), 25);
}

#[tokio::test]
async fn host_functions_remain_registered_and_invalid_configuration_fails_explicitly() {
    let ctx = context();
    ctx.register_udf(create_udf(
        "series_fit_poly",
        vec![DataType::Int64],
        DataType::Int64,
        Volatility::Immutable,
        Arc::new(|_| Ok(ColumnarValue::Scalar(ScalarValue::Int64(Some(777))))),
    ));
    let registered = ctx.state().scalar_functions().len();
    ctx.kql("print series_fit_poly(dynamic([1,2,3]))")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let native = ctx
        .sql("select series_fit_poly(1)")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_eq!(
        array_value_to_string(native[0].column(0), 0).unwrap(),
        "777"
    );
    assert_eq!(ctx.state().scalar_functions().len(), registered);
    for query in [
        "print series_fit_poly()",
        "print series_fit_poly(1)",
        "print series_fit_poly(dynamic([1,2,3]),2)",
        "print series_fit_poly(dynamic([1,2,3]),dynamic([1,2,3]),1.5)",
        "print series_fit_poly(dynamic([1,2,3]),dynamic([1,2,3]),true)",
        "range degree from 1 to 2 step 1 | project series_fit_poly(dynamic([1,2,3]),dynamic([1,2,3]),degree)",
        "print (a,b,c,d,e,f)=series_fit_poly(dynamic([1,2,3]))",
        "print x=array_length(series_fit_poly(dynamic([1,2,3])))",
    ] {
        assert!(ctx.kql(query).await.is_err(), "{query}");
    }
    let error = match ctx
        .kql("print series_fit_poly(dynamic([1,2,3]),dynamic([1,2,3]),9223372036854775807)")
        .await
    {
        Ok(frame) => frame.collect().await.unwrap_err().to_string(),
        Err(error) => error.to_string(),
    };
    assert!(error.contains("budget"), "{error}");
}

#[tokio::test]
async fn native_arrow_point_limit_is_checked_at_the_exact_api_boundary() {
    for (size, succeeds) in [(1_048_576, true), (1_048_577, false)] {
        let ctx = context();
        let array = ListArray::from_iter_primitive::<Float64Type, _, _>([Some(
            std::iter::repeat_n(Some(7.0), size),
        )]);
        ctx.register_batch(
            TableReference::bare("Boundary"),
            RecordBatch::try_from_iter(vec![("Y", Arc::new(array) as ArrayRef)]).unwrap(),
        )
        .unwrap();
        let frame = ctx.kql("Boundary | extend (R,C,V,RV,F)=series_fit_poly(Y) | project Points=array_length(F),R,V").await.unwrap();
        if succeeds {
            let batches = frame.collect().await.unwrap();
            assert_eq!(
                array_value_to_string(batches[0].column(0), 0).unwrap(),
                "1048576"
            );
            assert_eq!(
                array_value_to_string(batches[0].column(1), 0).unwrap(),
                "0.0"
            );
            assert_eq!(
                array_value_to_string(batches[0].column(2), 0).unwrap(),
                "0.0"
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
