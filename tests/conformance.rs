use std::error::Error;
use std::io::Write;
use std::process::{Command, Stdio};

use datafusion::arrow::datatypes::DataType;
use datafusion::common::ScalarValue;
use datafusion::prelude::*;
use datafusion_kql::SessionContextKqlExt;
use serde_json::{Value, json};
use tempfile::NamedTempFile;

type TestResult<T> = Result<T, Box<dyn Error + Send + Sync>>;
type Rows = Vec<Vec<Option<String>>>;

struct Fixture {
    name: &'static str,
    query: &'static str,
    columns: &'static [&'static str],
    rows: &'static [&'static [&'static str]],
}
// Captured through Azure MCP from help.kusto.windows.net/Samples on 2026-10-25.
// The separately ignored decimal regressions below are not covered by this baseline.
const FIXTURES: &[Fixture] = &[
    Fixture {
        name: "exact-decimal-arithmetic",
        query: r#"
        print total=tostring(decimal(0.1)+decimal(0.2)),
            wide=tostring(decimal(9007199254740993)+1),
            invalid=tostring(isnull(decimal(1)/decimal(0)))
    "#,
        columns: &["total", "wide", "invalid"],
        rows: &[&["0.3", "9007199254740994", "True"]],
    },
    Fixture {
        name: "decimal-json-string-extraction",
        query: r#"
        print value=tostring(extract_json('$.amount',
            '{"amount":"9007199254740993.123456789"}',typeof(decimal)))
    "#,
        columns: &["value"],
        rows: &[&["9007199254740993.123456789"]],
    },
    Fixture {
        name: "rich-nested-scalar-tags",
        query: r#"
        print value=bag_pack('items',pack_array(datetime(2025-01-01),1h,
            guid(74be27de-1e4e-49d9-b579-fe0b331d3642),decimal(1.25)))
        | project dateValue=gettype(value.items[0]),span=gettype(value.items[1]),
            id=gettype(value.items[2]),amount=gettype(value.items[3])
    "#,
        columns: &["dateValue", "span", "id", "amount"],
        rows: &[&["datetime", "timespan", "guid", "decimal"]],
    },
    Fixture {
        name: "term-set-boundaries",
        query: r#"
        print any=tostring("North America" has_any("north","south")),
            all=tostring("North America" has_all("north","america")),
            prefix=tostring("North America" hasprefix "ame"),
            suffix=tostring("North America" hassuffix "ica")
    "#,
        columns: &["any", "all", "prefix", "suffix"],
        rows: &[&["True", "True", "True", "True"]],
    },
    Fixture {
        name: "ipv4-cidr-parsing-and-matching",
        query: r#"
        print ip=tostring(parse_ipv4("192.168.1.1/24")),
            prefix=tostring(ipv4_netmask_suffix("192.168.1.1/24")),
            match=tostring(ipv4_is_in_any_range("192.168.1.6",
                dynamic(["10.0.0.0/8","192.168.1.0/24"])))
    "#,
        columns: &["ip", "prefix", "match"],
        rows: &[&["3232235776", "24", "True"]],
    },
    Fixture {
        name: "canonical-ipv6-and-mapped-ipv4",
        query: r#"
        print ip=parse_ipv6("192.168.255.255/24"),
            same=tostring(ipv6_compare("192.168.1.1","::ffff:c0a8:0101"))
    "#,
        columns: &["ip", "same"],
        rows: &[&["0000:0000:0000:0000:0000:ffff:c0a8:ff00", "0"]],
    },
    Fixture {
        name: "base64-and-url-encoding",
        query: r#"
        print encoded=base64_encode_tostring("Kusto"),
            decoded=base64_decode_tostring("S3VzdG8="),
            url=url_encode_component("hello world"),
            parameter=tostring(parse_urlquery("x=hello+world")["Query Parameters"].x)
    "#,
        columns: &["encoded", "decoded", "url", "parameter"],
        rows: &[&["S3VzdG8=", "Kusto", "hello%20world", "hello+world"]],
    },
    Fixture {
        name: "regex-trimming",
        query: r#"
        print value=trim(@"\s+","  Hello, world!  "),
            start=trim_start("--","----text----"),
            end=trim_end("--","----text----")
    "#,
        columns: &["value", "start", "end"],
        rows: &[&["Hello, world!", "--text----", "----text--"]],
    },
    Fixture {
        name: "empty-numeric-average-nan",
        query: r#"
        datatable(x:long)[] | summarize value=avg(x)
        | project nan=tostring(isnan(value)),is_null=tostring(isnull(value))
    "#,
        columns: &["nan", "is_null"],
        rows: &[&["True", "False"]],
    },
    Fixture {
        name: "grouped-count-and-null-sum",
        query: r#"
        datatable(g:string,x:long)['a',1,'a',long(null),'b',3]
        | summarize N=count(),Total=sum(x) by g
        | project g,N=tostring(N),Total=tostring(Total)
    "#,
        columns: &["g", "N", "Total"],
        rows: &[&["a", "2", "1"], &["b", "1", "3"]],
    },
    Fixture {
        name: "nullable-equality",
        query: r#"
        print a=tostring(long(null)==long(1)),
            b=tostring(long(null)!=long(1)),c=tostring(long(null)==long(null))
    "#,
        columns: &["a", "b", "c"],
        rows: &[&["False", "True", ""]],
    },
    Fixture {
        name: "zipped-expansion",
        query: r#"
        datatable(id:long,a:dynamic,b:dynamic)[1,dynamic([1,2]),dynamic([5])]
        | mv-expand a to typeof(long),b to typeof(long)
        | project id=tostring(id),a=tostring(a),b=tostring(b)
    "#,
        columns: &["id", "a", "b"],
        rows: &[&["1", "1", "5"], &["1", "2", ""]],
    },
    Fixture {
        name: "per-row-top-and-aggregation",
        query: r#"
        datatable(id:long,a:dynamic)[1,dynamic([1,5,3]),2,dynamic([4,8,7])]
        | mv-apply v=a to typeof(long) on(top 2 by v | summarize Total=sum(v))
        | project id=tostring(id),Total=tostring(Total)
    "#,
        columns: &["id", "Total"],
        rows: &[&["1", "8"], &["2", "15"]],
    },
    Fixture {
        name: "partitioned-row-windows",
        query: r#"
        datatable(g:string,x:long)['a',2,'b',4,'a',1,'b',3]
        | partition hint.strategy=native by g(
            sort by x asc | extend N=row_number(),Previous=prev(x,1,0))
        | project g,x=tostring(x),N=tostring(N),Previous=tostring(Previous)
    "#,
        columns: &["g", "x", "N", "Previous"],
        rows: &[
            &["a", "1", "1", "0"],
            &["a", "2", "2", "1"],
            &["b", "3", "1", "0"],
            &["b", "4", "2", "3"],
        ],
    },
    Fixture {
        name: "stateful-scan-promotion",
        query: r#"
        datatable(t:timespan,event:string)[0m,'Start',1m,'A',2m,'Stop',5m,'Start',6m,'Stop']
        | sort by t asc
        | scan with_match_id=id with(
            step s1:event=='Start';
            step s2:event!='Start' and event!='Stop' and t-s1.t<=3m;
            step s3:event=='Stop' and t-s1.t<=3m;)
        | project event,id=tostring(id)
    "#,
        columns: &["event", "id"],
        rows: &[
            &["Start", "0"],
            &["A", "0"],
            &["Stop", "0"],
            &["Start", "1"],
        ],
    },
    Fixture {
        name: "hierarchical-parent-totals",
        query: r#"
        datatable(r:string,c:string,v:long)['P1','a',10,'P1','b',9,'P2','a',5,'P2','b',15]
        | top-nested 1 of r by sum(v),top-nested 1 of c by sum(v)
        | project r,c,parent=tostring(aggregated_r),child=tostring(aggregated_c)
    "#,
        columns: &["r", "c", "parent", "child"],
        rows: &[&["P2", "b", "20", "15"]],
    },
    Fixture {
        name: "schema-declared-bag-unpack",
        query: r#"
        datatable(d:dynamic)[dynamic({'name':'a','n':2}),dynamic({'name':'b','n':3})]
        | evaluate bag_unpack(d):(name:string,n:long)
        | project name,n=tostring(n)
    "#,
        columns: &["name", "n"],
        rows: &[&["a", "2"], &["b", "3"]],
    },
    Fixture {
        name: "schema-declared-pivot",
        query: r#"
        datatable(k:string,g:string,v:long)['A','x',2,'B','x',3,'A','y',4]
        | evaluate pivot(k,sum(v),g):(g:string,A:long,B:long)
        | project g,A=tostring(A),B=tostring(B)
    "#,
        columns: &["g", "A", "B"],
        rows: &[&["x", "2", "3"], &["y", "4", "0"]],
    },
    Fixture {
        name: "column-restricted-search",
        query: r#"
        datatable(message:string,n:long)['ERROR disk',1,'terror',2,'error network',3]
        | search message:'error' and n>1
        | project message,n=tostring(n)
    "#,
        columns: &["message", "n"],
        rows: &[&["error network", "3"]],
    },
    Fixture {
        name: "lexical-scalar-subquery-function",
        query: r#"
        let T=range x from 1 to 5 step 1;
        let f=(n:long){toscalar(T | where x>n | count)};
        print N=tostring(f(2))
    "#,
        columns: &["N"],
        rows: &[&["3"]],
    },
    Fixture {
        name: "staged-native-range",
        query: r#"
        let End=toscalar(range x from 1 to 5 step 1 | count);
        range x from 1 to End step 2 | project x=tostring(x)
    "#,
        columns: &["x"],
        rows: &[&["1"], &["3"], &["5"]],
    },
    Fixture {
        name: "typed-guid-conversion",
        query: r#"
        datatable(s:string)['0123456789abcdef0123456789abcdef','not-a-guid']
        | project id=tostring(toguid(s))
    "#,
        columns: &["id"],
        rows: &[&["01234567-89ab-cdef-0123-456789abcdef"], &[""]],
    },
    Fixture {
        name: "frozen-guid-generation",
        query: r#"
        let id=toscalar(new_guid());
        range x from 1 to 5 step 1 | summarize N=count_distinct(id)
        | project N=tostring(N)
    "#,
        columns: &["N"],
        rows: &[&["1"]],
    },
    Fixture {
        name: "typed-parse-filter",
        query: r#"
        datatable(s:string)['id=3 value=10','id=4 value=20','invalid']
        | parse-where s with 'id=' id:long ' value=' value:long
        | project id=tostring(id),value=tostring(value)
    "#,
        columns: &["id", "value"],
        rows: &[&["3", "10"], &["4", "20"]],
    },
    Fixture {
        name: "series-shape-and-default-bins",
        query: r#"
        datatable(x:long,v:long)[0,2,2,4]
        | make-series Total=sum(v) on x from 0 to 3 step 1
        | project N=tostring(array_length(Total)),firstValue=tostring(tolong(Total[0])),
            missingValue=tostring(tolong(Total[1])),lastValue=tostring(tolong(Total[2]))
    "#,
        columns: &["N", "firstValue", "missingValue", "lastValue"],
        rows: &[&["3", "2", "0", "4"]],
    },
    Fixture {
        name: "encoding-and-decoding-edge-semantics",
        query: r#"
        print Ordinary=url_encode("!'()*~_-. /:&"),
            Component=url_encode_component("!'()*~_-. /:&"),
            BadEscape=url_decode('a%ZZ+b%2'),BadUtf=url_decode('%ff'),
            Bytes=tostring(base64_decode_toarray('////')),
            BadBase64=tostring(isnull(base64_decode_tostring('invalid!'))),
            BadText=tostring(isnull(base64_decode_tostring('////'))),
            Whitespace=base64_decode_tostring(' S3Vz\tdG8=\n')
    "#,
        columns: &[
            "Ordinary",
            "Component",
            "BadEscape",
            "BadUtf",
            "Bytes",
            "BadBase64",
            "BadText",
            "Whitespace",
        ],
        rows: &[&[
            "!'()*~%5f%2d.+%2f%3a%26",
            "!'()*~%5f%2d.%20%2f%3a%26",
            "a%ZZ b%2",
            "%ff",
            "[255,255,255]",
            "False",
            "False",
            "",
        ]],
    },
    Fixture {
        name: "raw-url-component-semantics",
        query: r#"
        print u=parse_url('https://Example.COM/a%20b/../c?a=hello+world&b=%20#x%20y')
        | project scheme=tostring(u.Scheme),host=tostring(u.Host),
            port=tostring(u.Port),path=tostring(u.Path),
            a=tostring(u['Query Parameters'].a),b=tostring(u['Query Parameters'].b),
            fragment=tostring(u.Fragment)
    "#,
        columns: &["scheme", "host", "port", "path", "a", "b", "fragment"],
        rows: &[&[
            "https",
            "Example.COM",
            "",
            "/a%20b/../c",
            "hello+world",
            "%20",
            "x%20y",
        ]],
    },
    Fixture {
        name: "base64-unused-bits-and-padding",
        query: r#"
        print Canonical=base64_decode_tostring('Zg=='),
            UnusedBits=base64_decode_tostring('Zh=='),
            Array=tostring(base64_decode_toarray('Zh==')),
            Missing=base64_decode_tostring('Zg='),Excess=base64_decode_tostring('Zg==='),
            InteriorSpace=base64_decode_tostring('S3Vz dG8='),
            InvalidUtf=base64_decode_tostring('////')
    "#,
        columns: &[
            "Canonical",
            "UnusedBits",
            "Array",
            "Missing",
            "Excess",
            "InteriorSpace",
            "InvalidUtf",
        ],
        rows: &[&["f", "f", "[102]", "", "", "", ""]],
    },
];

fn endpoint(uri: &str) -> TestResult<(String, bool)> {
    let uri = uri.trim_end_matches('/');
    let (scheme, authority) = uri
        .split_once("://")
        .ok_or("KUSTO_CLUSTER_URI needs a URL scheme")?;
    if authority.is_empty()
        || !authority.is_ascii()
        || authority
            .bytes()
            .any(|byte| !byte.is_ascii_alphanumeric() && !b".-:[]".contains(&byte))
    {
        return Err("KUSTO_CLUSTER_URI must contain only a cluster authority, without credentials, path, query, or fragment".into());
    }
    let local = if scheme == "https" {
        false
    } else if scheme == "http" {
        let host = if let Some(rest) = authority.strip_prefix('[') {
            rest.split_once(']').ok_or("invalid loopback IPv6 URL")?.0
        } else {
            authority.split(':').next().ok_or("missing loopback host")?
        };
        if !matches!(host, "localhost" | "127.0.0.1" | "::1") {
            return Err("unencrypted Kusto endpoints are permitted only on loopback".into());
        }
        true
    } else {
        return Err("Kusto conformance requires HTTPS, or HTTP for a loopback emulator".into());
    };
    Ok((format!("{uri}/v2/rest/query"), local))
}

struct Kusto {
    endpoint: String,
    database: String,
    headers: Option<NamedTempFile>,
    local: bool,
}

fn authorization(token: &str) -> TestResult<String> {
    if token.is_empty() || token.contains(['\r', '\n']) {
        return Err("invalid KUSTO_BEARER_TOKEN".into());
    }
    Ok(format!("{}: {} {token}", "Authorization", "Bearer"))
}

fn request(database: &str, query: &str) -> Value {
    let properties =
        json!({"Options":{"results_progressive_enabled":false,"servertimeout":"00:00:45"}});
    json!({"db":database,"csl":query,"properties":properties.to_string()})
}

impl Kusto {
    fn from_env() -> TestResult<Self> {
        let (endpoint, local) = endpoint(&std::env::var("KUSTO_CLUSTER_URI")?)?;
        let database = std::env::var("KUSTO_DATABASE")?;
        if database.trim().is_empty() {
            return Err("KUSTO_DATABASE must not be empty".into());
        }
        let headers = if local {
            None
        } else {
            let token = std::env::var("KUSTO_BEARER_TOKEN")?;
            let mut file = NamedTempFile::new()?;
            writeln!(file, "{}", authorization(&token)?)?;
            file.flush()?;
            Some(file)
        };
        Ok(Self {
            endpoint,
            database,
            headers,
            local,
        })
    }
    fn query(&self, query: &str, columns: &[String]) -> TestResult<Rows> {
        let mut command = Command::new("curl");
        command.arg("--disable").args([
            "--silent",
            "--show-error",
            "--fail-with-body",
            "--connect-timeout",
            "10",
            "--max-time",
            "60",
            "--max-filesize",
            "10485760",
            "--proto",
            if self.local { "=http" } else { "=https" },
            "--header",
            "Content-Type: application/json; charset=utf-8",
            "--header",
            "Accept: application/json",
            "--header",
            "x-ms-readonly: true",
            "--request",
            "POST",
            "--data-binary",
            "@-",
        ]);
        if self.local {
            command.args(["--noproxy", "*"]);
        }
        if let Some(headers) = &self.headers {
            command
                .arg("--header")
                .arg(format!("@{}", headers.path().display()));
        }
        let mut child = command
            .arg(&self.endpoint)
            .env_remove("KUSTO_BEARER_TOKEN")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()?;
        let send: TestResult<()> = if let Some(mut stdin) = child.stdin.take() {
            serde_json::to_writer(&mut stdin, &request(&self.database, query)).map_err(Into::into)
        } else {
            Err("curl request has no stdin".into())
        };
        let output = child.wait_with_output()?;
        send?;
        if !output.status.success() {
            return Err(format!(
                "Kusto query transport failed ({}): {}",
                output.status,
                String::from_utf8_lossy(&output.stderr)
            )
            .into());
        }
        response(&serde_json::from_slice(&output.stdout)?, columns)
    }
}

fn response(value: &Value, expected: &[String]) -> TestResult<Rows> {
    let frames = value
        .as_array()
        .ok_or("expected Kusto v2 response frames")?;
    let header = frames
        .first()
        .filter(|frame| frame.get("FrameType").and_then(Value::as_str) == Some("DataSetHeader"))
        .ok_or("Kusto response has no dataset header")?;
    if header.get("Version").and_then(Value::as_str) != Some("v2.0")
        || header.get("IsProgressive").and_then(Value::as_bool) != Some(false)
    {
        return Err("conformance requires a non-progressive Kusto v2.0 response".into());
    }
    if frames
        .iter()
        .any(|frame| frame.get("IsProgressive").and_then(Value::as_bool) == Some(true))
    {
        return Err("progressive Kusto responses are not supported by this fixture client".into());
    }
    let completion = frames
        .last()
        .filter(|frame| frame.get("FrameType").and_then(Value::as_str) == Some("DataSetCompletion"))
        .ok_or("incomplete Kusto response: no DataSetCompletion")?;
    if completion.get("HasErrors").and_then(Value::as_bool) != Some(false)
        || completion.get("Cancelled").and_then(Value::as_bool) == Some(true)
    {
        return Err("Kusto reported a failed or cancelled query, not a complete result".into());
    }
    let table = frames
        .iter()
        .find(|frame| {
            frame.get("FrameType").and_then(Value::as_str) == Some("DataTable")
                && frame.get("TableKind").and_then(Value::as_str) == Some("PrimaryResult")
        })
        .ok_or("Kusto response has no primary result table")?;
    let columns = table
        .get("Columns")
        .and_then(Value::as_array)
        .ok_or("Kusto primary result has no columns")?;
    let names = columns
        .iter()
        .map(|column| {
            column
                .get("ColumnName")
                .and_then(Value::as_str)
                .map(str::to_owned)
                .ok_or("Kusto column has no name")
        })
        .collect::<Result<Vec<_>, _>>()?;
    if names != expected
        || columns
            .iter()
            .any(|column| column.get("ColumnType").and_then(Value::as_str) != Some("string"))
    {
        return Err(format!(
            "Kusto result schema mismatch: expected string columns {expected:?}, received {columns:?}"
        )
        .into());
    }
    let mut rows = Vec::new();
    for row in table
        .get("Rows")
        .and_then(Value::as_array)
        .ok_or("Kusto primary result has no rows")?
    {
        let cells = row.as_array().ok_or("Kusto row is not an array")?;
        if cells.len() != expected.len() {
            return Err("Kusto row width does not match its schema".into());
        }
        rows.push(
            cells
                .iter()
                .map(|cell| match cell {
                    Value::Null => Ok(None),
                    Value::String(value) => Ok(Some(value.clone())),
                    _ => Err("Kusto returned a non-string cell for a string column"),
                })
                .collect::<Result<Vec<_>, _>>()?,
        );
    }
    rows.sort_unstable();
    Ok(rows)
}

async fn local(ctx: &SessionContext, fixture: &Fixture) -> TestResult<(Vec<String>, Rows)> {
    let frame = ctx.kql(fixture.query).await?;
    let names = frame
        .schema()
        .fields()
        .iter()
        .map(|field| field.name().clone())
        .collect();
    if frame.schema().fields().iter().any(|field| {
        !matches!(
            field.data_type(),
            DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
        )
    }) {
        return Err(format!(
            "fixture '{}' must project strings for representation-independent comparison",
            fixture.name
        )
        .into());
    }
    let mut rows = Vec::new();
    for batch in frame.collect().await? {
        for row in 0..batch.num_rows() {
            rows.push(
                batch
                    .columns()
                    .iter()
                    .map(|column| {
                        let value = ScalarValue::try_from_array(column, row)?;
                        Ok(value.try_as_str().flatten().map(str::to_owned))
                    })
                    .collect::<TestResult<Vec<_>>>()?,
            );
        }
    }
    rows.sort_unstable();
    Ok((names, rows))
}

async fn captured(ctx: &SessionContext, fixture: &Fixture) -> TestResult<()> {
    assert!(
        !fixture.columns.is_empty() && !fixture.rows.is_empty(),
        "empty reference fixture"
    );
    let (columns, actual) = local(ctx, fixture).await?;
    assert_eq!(
        columns.iter().map(String::as_str).collect::<Vec<_>>(),
        fixture.columns,
        "reference schema for '{}'",
        fixture.name
    );
    let mut expected = fixture
        .rows
        .iter()
        .map(|row| {
            assert_eq!(row.len(), fixture.columns.len());
            row.iter()
                .map(|value| Some((*value).to_owned()))
                .collect::<Vec<_>>()
        })
        .collect::<Rows>();
    expected.sort_unstable();
    assert_eq!(
        actual, expected,
        "captured reference mismatch for '{}'",
        fixture.name
    );
    Ok(())
}

#[tokio::test]
async fn captured_reference_fixtures_match_locally() -> TestResult<()> {
    let mut names = std::collections::HashSet::new();
    let ctx = SessionContext::new();
    for fixture in FIXTURES {
        assert!(names.insert(fixture.name), "duplicate reference fixture");
        captured(&ctx, fixture).await?;
    }
    Ok(())
}

#[tokio::test]
#[ignore = "known gap: the current bounded decimal profile differs from Kusto runtime range/precision"]
async fn reference_decimal_range_and_precision_gap() -> TestResult<()> {
    captured(&SessionContext::new(), &Fixture {
        name: "decimal-runtime-range-and-precision",
        query: "print maximum=tostring(decimal(max)),third=tostring(decimal(1)/decimal(3)),overflow=tostring(isnull(decimal(max)+1))",
        columns: &["maximum", "third", "overflow"],
        rows: &[&["1E+38", "0.3333333333333333333333333333333333", "False"]],
    }).await
}

#[tokio::test]
#[ignore = "known difference: exact raw JSON decimal extraction does not reproduce Kusto binary64 rounding"]
async fn reference_decimal_json_number_rounding_gap() -> TestResult<()> {
    captured(&SessionContext::new(), &Fixture {
        name: "decimal-json-number-rounding",
        query: r#"print value=tostring(extract_json('$.amount','{"amount":9007199254740993.123456789}',typeof(decimal)))"#,
        columns: &["value"],
        rows: &[&["9007199254740992"]],
    }).await
}

#[test]
fn reference_endpoint_validation_rejects_credential_leaks_and_nonlocal_plaintext() {
    assert!(endpoint("https://cluster.region.kusto.windows.net").is_ok());
    assert!(endpoint("http://127.0.0.1:8080").is_ok());
    assert!(endpoint("http://[::1]:8080").is_ok());
    for invalid in [
        "http://remote.example",
        "https://user:secret@example",
        "file:///tmp/data",
        "https://example/path",
        "https://example?other=value",
        "https://example\nHeader: value",
    ] {
        assert!(endpoint(invalid).is_err(), "{invalid}");
    }
}

#[test]
fn reference_response_rejects_partial_results_and_schema_mismatches() {
    let header = json!({"FrameType":"DataSetHeader","Version":"v2.0","IsProgressive":false});
    let table = json!({"FrameType":"DataTable","TableKind":"PrimaryResult",
        "Columns":[{"ColumnName":"x","ColumnType":"string"}],"Rows":[["ok"]]});
    let success = json!([header,table,{"FrameType":"DataSetCompletion","HasErrors":false}]);
    assert_eq!(
        response(&success, &["x".into()]).unwrap(),
        vec![vec![Some("ok".into())]]
    );
    assert!(response(&json!([header, table]), &["x".into()]).is_err());
    assert!(
        response(
            &json!([header,table,{"FrameType":"DataSetCompletion","HasErrors":true}]),
            &["x".into()]
        )
        .is_err()
    );
    assert!(response(&success, &["wrong".into()]).is_err());
    assert!(response(&json!([header,table,{"FrameType":"DataSetCompletion","HasErrors":false,"Cancelled":true}]),&["x".into()]).is_err());
}

#[test]
fn reference_request_uses_serialized_properties_and_rejects_header_injection() {
    let value = request("TestDatabase", "print x='synthetic'");
    let properties: Value = serde_json::from_str(value["properties"].as_str().unwrap()).unwrap();
    assert_eq!(properties["Options"]["results_progressive_enabled"], false);
    assert_eq!(value["db"], "TestDatabase");
    assert!(
        authorization("sample-token")
            .unwrap()
            .ends_with("sample-token")
    );
    assert!(authorization("sample-token\nInjected: value").is_err());
}

#[test]
#[ignore = "requires curl; uses only a self-owned loopback HTTP fixture, not Kusto"]
fn reference_transport_local_http() -> TestResult<()> {
    use std::io::{BufRead, BufReader, Read};
    use std::net::TcpListener;
    use std::time::{Duration, Instant};
    let listener = TcpListener::bind(("127.0.0.1", 0))?;
    let address = listener.local_addr()?;
    listener.set_nonblocking(true)?;
    let server = std::thread::spawn(move || -> TestResult<()> {
        let deadline = Instant::now() + Duration::from_secs(10);
        let mut stream = loop {
            match listener.accept() {
                Ok((stream, _)) => break stream,
                Err(error)
                    if error.kind() == std::io::ErrorKind::WouldBlock
                        && Instant::now() < deadline =>
                {
                    std::thread::sleep(Duration::from_millis(10))
                }
                Err(error) => return Err(error.into()),
            }
        };
        stream.set_read_timeout(Some(Duration::from_secs(10)))?;
        stream.set_write_timeout(Some(Duration::from_secs(10)))?;
        let mut reader = BufReader::new(stream.try_clone()?);
        let mut line = String::new();
        reader.read_line(&mut line)?;
        assert!(line.starts_with("POST /v2/rest/query "));
        let mut length = None;
        let mut readonly = false;
        loop {
            line.clear();
            reader.read_line(&mut line)?;
            if line == "\r\n" {
                break;
            }
            if line.is_empty() {
                return Err("incomplete loopback request".into());
            }
            let header = line.to_ascii_lowercase();
            if let Some(value) = header.strip_prefix("content-length:") {
                length = Some(value.trim().parse::<usize>()?);
            }
            if header.trim() == "x-ms-readonly: true" {
                readonly = true;
            }
            assert!(!header.starts_with("authorization:"));
        }
        assert!(readonly);
        let length = length
            .filter(|length| *length <= 65_536)
            .ok_or("loopback request is missing a bounded content length")?;
        let mut body = vec![0; length];
        reader.read_exact(&mut body)?;
        let request: Value = serde_json::from_slice(&body)?;
        assert_eq!(request["db"], "TestDatabase");
        assert_eq!(request["csl"], "print x='synthetic'");
        let properties: Value = serde_json::from_str(
            request["properties"]
                .as_str()
                .ok_or("properties must be serialized")?,
        )?;
        assert_eq!(properties["Options"]["results_progressive_enabled"], false);
        let response=json!([
            {"FrameType":"DataSetHeader","Version":"v2.0","IsProgressive":false},
            {"FrameType":"DataTable","TableKind":"PrimaryResult","Columns":[{"ColumnName":"x","ColumnType":"string"}],"Rows":[["synthetic"]]},
            {"FrameType":"DataSetCompletion","HasErrors":false}
        ]).to_string();
        write!(
            stream,
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{response}",
            response.len()
        )?;
        Ok(())
    });
    let client = Kusto {
        endpoint: format!("http://{address}/v2/rest/query"),
        database: "TestDatabase".into(),
        headers: None,
        local: true,
    };
    let result = client.query("print x='synthetic'", &["x".into()]);
    server
        .join()
        .map_err(|_| "loopback reference transport server panicked")??;
    assert_eq!(result?, vec![vec![Some("synthetic".into())]]);
    Ok(())
}

#[tokio::test]
#[ignore = "requires an explicitly configured Kusto emulator or ADX test database; never runs in default CI"]
async fn live_kusto_conformance() -> TestResult<()> {
    let client = Kusto::from_env()?;
    let ctx = SessionContext::new();
    for fixture in FIXTURES {
        let (columns, expected) = local(&ctx, fixture).await?;
        let actual = client
            .query(fixture.query, &columns)
            .map_err(|error| format!("reference fixture '{}' failed: {error}", fixture.name))?;
        assert_eq!(
            actual, expected,
            "reference mismatch for '{}'",
            fixture.name
        );
        println!("Kusto conformance: {} passed", fixture.name);
    }
    Ok(())
}
