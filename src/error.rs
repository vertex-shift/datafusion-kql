use crate::ast::Span;

/// Errors retain source locations without retaining or logging the query text.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("KQL syntax error at bytes {span}: {message}")]
    Syntax { message: String, span: Span },
    #[error("unsupported KQL feature at bytes {span}: {message}")]
    Unsupported { message: String, span: Span },
    #[error("KQL planning error: {0}")]
    Plan(String),
    #[error(transparent)]
    DataFusion(#[from] datafusion::error::DataFusionError),
}

impl Error {
    pub(crate) fn syntax(message: impl Into<String>, span: Span) -> Self {
        Self::Syntax {
            message: message.into(),
            span,
        }
    }

    pub(crate) fn unsupported(message: impl Into<String>, span: Span) -> Self {
        Self::Unsupported {
            message: message.into(),
            span,
        }
    }

    /// Render a caret diagnostic. The caller controls whether query text is shown.
    pub fn diagnostic(&self, query: &str) -> String {
        let span = match self {
            Self::Syntax { span, .. } | Self::Unsupported { span, .. } => *span,
            _ => return self.to_string(),
        };
        let offset = span.start.min(query.len());
        let Some(prefix) = query.get(..offset) else {
            return self.to_string();
        };
        let line = prefix.bytes().filter(|&c| c == b'\n').count() + 1;
        let start = prefix.rfind('\n').map_or(0, |n| n + 1);
        let column = query[start..offset].chars().count();
        let text = query[start..].lines().next().unwrap_or("");
        format!(
            "{self}\n --> {line}:{}\n{text}\n{}^",
            column + 1,
            " ".repeat(column)
        )
    }
}

pub type Result<T> = std::result::Result<T, Error>;
