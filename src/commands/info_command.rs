use std::{collections::HashMap, str};

use base64::{engine::general_purpose, Engine};
use tracing::{debug, error};

use super::{CommandError, Result};
use crate::{cluster::node::FeatureSupport, net::Connection, Host};

pub(crate) mod commands {
    pub const CLUSTER_NAME: &str = "cluster-name";
    pub const FEATURES: &str = "features";
    pub const NODE: &str = "node";
    pub const PARTITION_GENERATION: &str = "partition-generation";
    pub const REPLICAS_MASTER: &str = "replicas-master";
    pub const SERVICES: &str = "services";
    pub const SERVICES_ALTERNATE: &str = "services-alternate";
}

#[derive(Default)]
pub(crate) struct Info {
    pub cluster_name: Option<String>,
    pub features: Option<FeatureSupport>,
    pub node: Option<String>,
    pub partition_generation: Option<isize>,
    pub replicas_master: Option<HashMap<String, Vec<u8>>>,
    pub services: Option<Vec<Host>>,
    pub services_alternate: Option<Vec<Host>>,

    pub others: HashMap<String, String>,
}

pub(crate) async fn raw(
    conn: &mut Connection,
    commands: &[&str],
) -> Result<HashMap<String, String>> {
    send(conn, commands, parse_raw).await
}

pub(crate) async fn typed(conn: &mut Connection, commands: &[&str]) -> Result<Info> {
    send(conn, commands, parse_typed).await
}

async fn send<T>(
    conn: &mut Connection,
    commands: &[&str],
    transform: impl Fn(&str) -> Result<T>,
) -> Result<T> {
    conn.buffer().set_info(commands)?;
    conn.flush().await?;

    let size = conn.read_proto_header().await?.size;
    conn.read_buffer(size).await?;

    let buffer = conn.buffer();
    let response = str::from_utf8(buffer.as_ref())?;

    debug!(?response, "response from server for info command");

    (transform)(response)
}

fn parse_raw(response: &str) -> Result<HashMap<String, String>> {
    response
        .lines()
        .map(|tuple| {
            let (key, value) = tuple
                .split_once('\t')
                .ok_or(CommandError::Parse("failed parsing info command"))?;

            Ok((key.to_owned(), value.to_owned()))
        })
        .collect()
}

fn parse_typed(response: &str) -> Result<Info> {
    let mut info = Info::default();

    info.others = response
        .lines()
        .filter_map(|tuple| {
            let (key, value) = match tuple.split_once('\t') {
                Some(kv) => kv,
                None => return Some(Err(CommandError::Parse("failed parsing info command"))),
            };

            match key {
                commands::CLUSTER_NAME => info.cluster_name = Some(value.to_owned()),
                commands::FEATURES => info.features = Some(value.into()),
                commands::NODE => info.node = Some(value.to_owned()),
                commands::PARTITION_GENERATION => match value.parse() {
                    Ok(gen) => info.partition_generation = Some(gen),
                    Err(e) => error!(value, error = ?e, "malformed partition generation"),
                },
                commands::REPLICAS_MASTER => info.replicas_master = Some(parse_replicas(value)),
                commands::SERVICES => info.services = Some(parse_hosts(value)),
                commands::SERVICES_ALTERNATE => info.services_alternate = Some(parse_hosts(value)),
                _ => return Some(Ok((key.to_owned(), value.to_owned()))),
            }

            None
        })
        .collect::<Result<_, _>>()?;

    Ok(info)
}

fn parse_hosts(value: &str) -> Vec<Host> {
    value
        .split(';')
        .filter(|s| !s.is_empty())
        .filter_map(|v| {
            if let Some(host) = v
                .split_once(':')
                .and_then(|(host, port)| Some(Host::new(host, port.parse().ok()?)))
            {
                Some(host)
            } else {
                error!(got = v, "malformed services response, expected HOST:PORT");
                None
            }
        })
        .collect()
}

fn parse_replicas(value: &str) -> HashMap<String, Vec<u8>> {
    value
        .split(';')
        .filter_map(|pair| pair.split_once(':'))
        .filter_map(|(key, value)| {
            let value = general_purpose::STANDARD.decode(value).ok()?;
            Some((key.to_owned(), value))
        })
        .collect()
}
