mod hms;
mod iceberg;

use iceberg::spec::table_metadata::TableMetadata;

use std::error::Error;

use thrift::protocol::{TBinaryInputProtocol, TBinaryOutputProtocol};
use thrift::transport::{TBufferedReadTransport, TBufferedWriteTransport};
use thrift::transport::{TIoChannel, TTcpChannel};

use hms::hms_api::ThriftHiveMetastoreSyncClient;

use crate::hms::hms_api::TThriftHiveMetastoreSyncClient;

fn main() -> Result<(), Box<dyn Error>> {
    println!("connect to Hive Metastore on localhost:9083");
    let mut c = TTcpChannel::new();
    c.open("localhost:9083")?;

    let (i_chan, o_chan) = c.split()?;

    let i_prot = TBinaryInputProtocol::new(TBufferedReadTransport::new(i_chan), true);
    let o_prot = TBinaryOutputProtocol::new(TBufferedWriteTransport::new(o_chan), true);

    let mut client = ThriftHiveMetastoreSyncClient::new(i_prot, o_prot);

    let dbs = client.get_all_databases()?;

    println!("{:?}", dbs);

    let table = client.get_table("db1".to_string(), "db1v2table1".to_string())?;
    // println!("{:#?}", table);

    let params = table
        .parameters
        .ok_or("Couldn't find parameters attribute in HMS table")?;
    let metadata_location = params
        .get("metadata_location")
        .ok_or("Couldn't find metadata location for table")?;

    println!("{}", metadata_location);

    // A hack for now
    let metadata_location = metadata_location.strip_prefix("file:").unwrap();

    let metadata = std::fs::read_to_string(metadata_location).unwrap();

    let metadata: TableMetadata = serde_json::from_str(&metadata).unwrap();

    println!("{:#?}", metadata);

    Ok(())
}
