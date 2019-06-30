use serde_derive::{Deserialize, Serialize};
use std::net::IpAddr;

#[derive(Serialize, Deserialize)]
pub struct Config {
    pub postgres_uri: String,
    pub rpc_listen_port: u16,
    pub rpc_listen_address: IpAddr,
}
