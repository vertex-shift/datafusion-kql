use datafusion::prelude::*;
use datafusion_kql::SessionContextKqlExt;

#[tokio::main]
async fn main() -> datafusion_kql::Result<()> {
    SessionContext::new()
        .kql(
            r#"
        let InternalNetworks=dynamic(["10.0.0.0/8","192.168.0.0/16"]);
        datatable(Event:dynamic) [
            dynamic({"ip":"10.1.2.3","message":"ERROR disk",
                "cost":decimal(1.25),"url":"https://example.test/api?region=west"}),
            dynamic({"ip":"192.168.1.5","message":"WARN request",
                "cost":decimal(2.75),"url":"https://example.test/worker?region=east"}),
            dynamic({"ip":"8.8.8.8","message":"INFO healthy",
                "cost":decimal(3),"url":"https://example.test/health"})
        ]
        | evaluate bag_unpack(Event): (ip:string,message:string,cost:decimal,url:string)
        | where message has_any ("error","warn")
        | extend Internal=ipv4_is_in_any_range(ip,InternalNetworks),
            Region=tostring(parse_url(url)["Query Parameters"].region)
        | project Region,Internal,Cost=tostring(cost),
            Message=base64_decode_tostring(base64_encode_tostring(message))
        | sort by Region asc
    "#,
        )
        .await?
        .show()
        .await?;
    Ok(())
}
