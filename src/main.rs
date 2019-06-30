use jsonrpc_core::{Error, IoHandler, Result};
use jsonrpc_derive::rpc;

use serde_derive::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
pub struct TaskId(u64);

#[derive(Serialize, Deserialize)]
pub struct ProfileId(u64);

#[rpc]
pub trait Rpc {
    #[rpc(name = "create_profile")]
    fn create_profile(&self, base_name: String, name: String, yaml: String) -> Result<ProfileId>;

    #[rpc(name = "create_task")]
    fn create_task(&self, profile: ProfileId, file_name: String, data: Vec<u8>) -> Result<TaskId>;
}

pub struct RpcImpl;
impl Rpc for RpcImpl {
    fn create_task(&self, profile: ProfileId, file_name: String, data: Vec<u8>) -> Result<TaskId> {
        Ok(TaskId(0))
    }

    fn create_profile(&self, base_name: String, name: String, yaml: String) -> Result<ProfileId> {
        Ok(ProfileId(0))
    }
}

mod config;

use config::Config;

fn main() {
    let args = clap::App::new("coordinator")
        .arg(
            clap::Arg::with_name("config")
                .short("c")
                .long("config")
                .max_values(1)
                .takes_value(true)
                .default_value("coordinator.yaml"),
        )
        .get_matches();

    let mut io = IoHandler::new();
    io.extend_with(RpcImpl.to_delegate());

    let config_path = args.value_of("config").unwrap();
    let config: Config =
        serde_yaml::from_reader(std::fs::File::open(config_path).expect("could not open config"))
            .expect("could not parse config");

    let server = jsonrpc_tcp_server::ServerBuilder::new(io)
        .start(&std::net::SocketAddr::from((
            config.rpc_listen_address,
            config.rpc_listen_port,
        )))
        .expect("jsonrpc tcp server failed to start");

    server.wait();
}
