//! A native KQL frontend for Apache DataFusion.
//!
//! KQL is lowered directly into DataFusion logical plans, not translated to SQL.
//!
//! ```no_run
//! use datafusion::prelude::*;
//! use datafusion_kql::SessionContextKqlExt;
//!
//! # async fn example() -> datafusion_kql::Result<()> {
//! let ctx = SessionContext::new();
//! let frame = ctx.kql("range x from 1 to 100 step 1 | summarize Total=sum(x)").await?;
//! frame.show().await?;
//! # Ok(())
//! # }
//! ```

mod aggregate_functions;
mod aggregation;
pub mod ast;
mod calendar;
mod dynamic;
mod error;
mod expressions;
mod format_time;
mod functions;
mod grouped;
mod json_path;
mod lexer;
mod literals;
mod make_series;
mod materialize;
mod ordering;
mod parser;
mod parsing;
mod planner;
mod ranges;
mod regex_extract;
mod series_functions;
mod tabular;
mod temporal;
mod windows;

pub use error::{Error, Result};
pub use parser::{ParserOptions, parse, parse_with_options};
pub use planner::{logical_plan, logical_plan_with_parameters};

use std::future::Future;

use datafusion::dataframe::DataFrame;
use datafusion::prelude::SessionContext;

/// Typed request values; parameters must be declared by the query before use.
pub type QueryParameters = std::collections::HashMap<String, datafusion::common::ScalarValue>;

/// Add KQL to an existing session without altering its configuration or registry.
pub trait SessionContextKqlExt {
    /// Parse and plan KQL, returning a lazy native DataFusion DataFrame.
    fn kql(&self, query: &str) -> impl Future<Output = Result<DataFrame>> + Send;

    /// Reuse an AST while resolving tables and query-stable time for this call.
    fn kql_ast(&self, query: &ast::Query) -> impl Future<Output = Result<DataFrame>> + Send;

    /// Bind typed values without interpolating user input into KQL source.
    fn kql_with_parameters(
        &self,
        query: &str,
        parameters: &QueryParameters,
    ) -> impl Future<Output = Result<DataFrame>> + Send;

    /// Reuse a parsed query with different typed request parameters.
    fn kql_ast_with_parameters(
        &self,
        query: &ast::Query,
        parameters: &QueryParameters,
    ) -> impl Future<Output = Result<DataFrame>> + Send;
}

impl SessionContextKqlExt for SessionContext {
    async fn kql(&self, query: &str) -> Result<DataFrame> {
        self.kql_ast(&parse(query)?).await
    }

    async fn kql_ast(&self, query: &ast::Query) -> Result<DataFrame> {
        self.kql_ast_with_parameters(query, &QueryParameters::new())
            .await
    }

    async fn kql_with_parameters(
        &self,
        query: &str,
        parameters: &QueryParameters,
    ) -> Result<DataFrame> {
        self.kql_ast_with_parameters(&parse(query)?, parameters)
            .await
    }

    async fn kql_ast_with_parameters(
        &self,
        query: &ast::Query,
        parameters: &QueryParameters,
    ) -> Result<DataFrame> {
        planner::dataframe(self, query, parameters).await
    }
}
