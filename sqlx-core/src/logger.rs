use crate::connection::LogSettings;
use regex::RegexBuilder;
use std::time::Instant;

// Yes these look silly. `tracing` doesn't currently support dynamic levels
// https://github.com/tokio-rs/tracing/issues/372
#[doc(hidden)]
#[macro_export]
macro_rules! private_tracing_dynamic_enabled {
    (target: $target:expr, $level:expr) => {{
        use ::tracing::Level;

        match $level {
            Level::ERROR => ::tracing::enabled!(target: $target, Level::ERROR),
            Level::WARN => ::tracing::enabled!(target: $target, Level::WARN),
            Level::INFO => ::tracing::enabled!(target: $target, Level::INFO),
            Level::DEBUG => ::tracing::enabled!(target: $target, Level::DEBUG),
            Level::TRACE => ::tracing::enabled!(target: $target, Level::TRACE),
        }
    }};
    ($level:expr) => {{
        $crate::private_tracing_dynamic_enabled!(target: module_path!(), $level)
    }};
}

#[doc(hidden)]
#[macro_export]
macro_rules! private_tracing_dynamic_event {
    (target: $target:expr, $level:expr, $($args:tt)*) => {{
        use ::tracing::Level;

        match $level {
            Level::ERROR => ::tracing::event!(target: $target, Level::ERROR, $($args)*),
            Level::WARN => ::tracing::event!(target: $target, Level::WARN, $($args)*),
            Level::INFO => ::tracing::event!(target: $target, Level::INFO, $($args)*),
            Level::DEBUG => ::tracing::event!(target: $target, Level::DEBUG, $($args)*),
            Level::TRACE => ::tracing::event!(target: $target, Level::TRACE, $($args)*),
        }
    }};
}

#[doc(hidden)]
pub fn private_level_filter_to_levels(
    filter: log::LevelFilter,
) -> Option<(tracing::Level, log::Level)> {
    let tracing_level = match filter {
        log::LevelFilter::Error => Some(tracing::Level::ERROR),
        log::LevelFilter::Warn => Some(tracing::Level::WARN),
        log::LevelFilter::Info => Some(tracing::Level::INFO),
        log::LevelFilter::Debug => Some(tracing::Level::DEBUG),
        log::LevelFilter::Trace => Some(tracing::Level::TRACE),
        log::LevelFilter::Off => None,
    };

    tracing_level.zip(filter.to_level())
}

pub use sqlformat;
use tracing::{info_span, Span};

pub struct QueryLogger<'q> {
    sql: &'q str,
    rows_returned: u64,
    rows_affected: u64,
    start: Instant,
    settings: LogSettings,
    span: Span,
}

impl<'q> QueryLogger<'q> {
    pub fn new(sql: &'q str, settings: LogSettings) -> Self {
        let trimmed_query = trim_query(sql);
        Self {
            sql,
            rows_returned: 0,
            rows_affected: 0,
            start: Instant::now(),
            settings,
            span: info_span!(
                target: "sqlx::query-trace",
                "query",
                resource.name = trimmed_query.as_str(),
                "span.type" = "db",
                span.kind = "client",
                service = "sqlx",
                db.system = "postgres",
                db.operation = trimmed_query.as_str(),
                db.row_count = tracing::field::Empty,
            ),
        }
    }

    pub fn increment_rows_returned(&mut self) {
        self.rows_returned += 1;
    }

    pub fn increase_rows_affected(&mut self, n: u64) {
        self.rows_affected += n;
    }

    pub fn finish(&self) {
        self.span.record(
            "db.row_count",
            if self.rows_affected > 0 {
                self.rows_affected
            } else {
                self.rows_returned
            },
        );
        let elapsed = self.start.elapsed();

        let was_slow = elapsed >= self.settings.slow_statements_duration;

        let lvl = if was_slow {
            self.settings.slow_statements_level
        } else {
            self.settings.statements_level
        };

        if let Some((tracing_level, log_level)) = private_level_filter_to_levels(lvl) {
            // The enabled level could be set from either tracing world or log world, so check both
            // to see if logging should be enabled for our level
            let log_is_enabled = log::log_enabled!(target: "sqlx::query", log_level)
                || private_tracing_dynamic_enabled!(target: "sqlx::query", tracing_level);
            if log_is_enabled {
                let mut summary = parse_query_summary(&self.sql);

                let sql = if summary != self.sql {
                    summary.push_str(" …");
                    format!(
                        "\n\n{}\n",
                        sqlformat::format(
                            &self.sql,
                            &sqlformat::QueryParams::None,
                            sqlformat::FormatOptions::default()
                        )
                    )
                } else {
                    String::new()
                };

                if was_slow {
                    private_tracing_dynamic_event!(
                        target: "sqlx::query",
                        tracing_level,
                        summary,
                        db.statement = sql,
                        rows_affected = self.rows_affected,
                        rows_returned = self.rows_returned,
                        // Human-friendly - includes units (usually ms). Also kept for backward compatibility
                        ?elapsed,
                        // Search friendly - numeric
                        elapsed_secs = elapsed.as_secs_f64(),
                        // When logging to JSON, one can trigger alerts from the presence of this field.
                        slow_threshold=?self.settings.slow_statements_duration,
                        // Make sure to use "slow" in the message as that's likely
                        // what people will grep for.
                        "slow statement: execution time exceeded alert threshold"
                    );
                } else {
                    private_tracing_dynamic_event!(
                        target: "sqlx::query",
                        tracing_level,
                        summary,
                        db.statement = sql,
                        rows_affected = self.rows_affected,
                        rows_returned = self.rows_returned,
                        // Human-friendly - includes units (usually ms). Also kept for backward compatibility
                        ?elapsed,
                        // Search friendly - numeric
                        elapsed_secs = elapsed.as_secs_f64(),
                    );
                }
            }
        }
    }
}

impl<'q> Drop for QueryLogger<'q> {
    fn drop(&mut self) {
        self.finish();
    }
}

pub fn parse_query_summary(sql: &str) -> String {
    // For now, just take the first 4 words
    sql.split_whitespace()
        .take(4)
        .collect::<Vec<&str>>()
        .join(" ")
}

pub fn trim_query(sql: &str) -> String {
    // First, trim the string to remove leading and trailing whitespace
    let trimmed_sql = sql.trim();

    // Use a regex to find the minimum indentation (spaces) before any non-space character in all lines

    let indent_regex = RegexBuilder::new(r"^( +)\S")
        .multi_line(true)
        .build()
        .unwrap();
    let mut min_indent = None;

    for cap in indent_regex.captures_iter(trimmed_sql) {
        let indent = cap.get(1).unwrap().as_str().len();
        min_indent = Some(min_indent.map_or(indent, |min: usize| usize::min(min, indent)));
    }

    // Dedent each line based on the minimum indentation found
    if let Some(indent) = min_indent {
        let dedent_regex = RegexBuilder::new(&format!(r"^[ ]{{1,{}}}", indent))
            .multi_line(true)
            .build()
            .unwrap();
        dedent_regex.replace_all(trimmed_sql, "").to_string()
    } else {
        // If no indentation was found, just return the trimmed string
        trimmed_sql.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_trim_query_already_trimmed() {
        let sql = "SELECT * FROM table WHERE column = 'value';";
        assert_eq!(
            trim_query(sql),
            "SELECT * FROM table WHERE column = 'value';"
        );
    }

    #[test]
    fn test_trim_query_simple_dedent() {
        let sql = "
            SELECT * FROM table WHERE column = 'value';
        ";
        assert_eq!(
            trim_query(sql),
            "SELECT * FROM table WHERE column = 'value';"
        );
    }

    #[test]
    fn test_trim_query_complex_dedent() {
        let sql = "
                SELECT id, name
                FROM users
                WHERE age > 18
                ORDER BY name;
        ";
        let expected = "SELECT id, name\nFROM users\nWHERE age > 18\nORDER BY name;";
        assert_eq!(trim_query(sql), expected);
    }

    #[test]
    fn test_trim_query_mixed_indentation() {
        let sql = "
                SELECT id, name
            FROM users
                  WHERE age > 18
                ORDER BY name;
        ";
        let expected = "SELECT id, name\nFROM users\n      WHERE age > 18\n    ORDER BY name;";
        assert_eq!(trim_query(sql), expected);
    }

    #[test]
    fn test_trim_query_empty_string() {
        let sql = "";
        assert_eq!(trim_query(sql), "");
    }

    #[test]
    fn test_trim_query_spaces_only() {
        let sql = "     ";
        assert_eq!(trim_query(sql), "");
    }

    #[test]
    fn test_trim_query_newlines_only() {
        let sql = "\n\n";
        assert_eq!(trim_query(sql), "");
    }
}
