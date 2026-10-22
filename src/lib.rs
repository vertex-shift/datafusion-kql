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

pub mod ast;
mod dynamic;
mod error;
mod expressions;
mod lexer;
mod literals;
mod parser;
mod planner;

pub use error::{Error, Result};
pub use parser::{ParserOptions, parse, parse_with_options};
pub use planner::logical_plan;

use std::future::Future;

use datafusion::dataframe::DataFrame;
use datafusion::prelude::SessionContext;

/// Add KQL to an existing session without altering its configuration or registry.
pub trait SessionContextKqlExt {
    /// Parse and plan KQL, returning a lazy native DataFusion DataFrame.
    fn kql(&self, query: &str) -> impl Future<Output = Result<DataFrame>> + Send;

    /// Reuse an AST while resolving tables and query-stable time for this call.
    fn kql_ast(&self, query: &ast::Query) -> impl Future<Output = Result<DataFrame>> + Send;
}

impl SessionContextKqlExt for SessionContext {
    async fn kql(&self, query: &str) -> Result<DataFrame> {
        self.kql_ast(&parse(query)?).await
    }

    async fn kql_ast(&self, query: &ast::Query) -> Result<DataFrame> {
        planner::dataframe(self, query).await
    }
}
