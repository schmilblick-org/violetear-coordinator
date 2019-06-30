use jsonrpc_core::{Error, IoHandler, Result};
use jsonrpc_derive::rpc;

use serde_derive::{Deserialize, Serialize};

use r2d2_postgres::{PostgresConnectionManager, TlsMode};

#[derive(Serialize, Deserialize)]
pub struct TaskId(u64);

#[derive(Serialize, Deserialize)]
pub struct ProfileId(u64);

#[rpc]
pub trait Rpc {
    #[rpc(name = "create_profile")]
    fn create_profile(&self, base_name: String, name: String, yaml: String) -> Result<ProfileId>;

    #[rpc(name = "list_profiles")]
    fn list_profiles(&self, by_base: Option<Vec<String>>) -> Result<Vec<ProfileId>>;

    #[rpc(name = "fetch_profile")]
    fn fetch_profile(&self, id: ProfileId) -> Result<String>;

    #[rpc(name = "create_task")]
    fn create_task(&self, profile: ProfileId, file_name: String, data: Vec<u8>) -> Result<TaskId>;

    #[rpc(name = "list_tasks")]
    fn list_tasks(&self, by_profile: Option<Vec<ProfileId>>) -> Result<Vec<TaskId>>;

    #[rpc(name = "fetch_task")]
    fn fetch_task(&self, id: TaskId) -> Result<String>;
}

pub struct RpcImpl {
    db_pool: r2d2::Pool<PostgresConnectionManager>,
}

impl Rpc for RpcImpl {
    fn create_profile(&self, base_name: String, name: String, json: String) -> Result<ProfileId> {
        Ok(ProfileId(0))
    }

    fn list_profiles(&self, by_base: Option<Vec<String>>) -> Result<Vec<ProfileId>> {
        Ok(vec![])
    }

    fn fetch_profile(&self, id: ProfileId) -> Result<String> {
        Ok(String::new())
    }

    fn create_task(&self, profile: ProfileId, file_name: String, data: Vec<u8>) -> Result<TaskId> {
        Ok(TaskId(0))
    }

    fn list_tasks(&self, by_profile: Option<Vec<ProfileId>>) -> Result<Vec<TaskId>> {
        Ok(vec![])
    }

    fn fetch_task(&self, id: TaskId) -> Result<String> {
        Ok(String::new())
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

    let config_path = args.value_of("config").unwrap();
    let config: Config =
        serde_yaml::from_reader(std::fs::File::open(config_path).expect("could not open config"))
            .expect("could not parse config");

    let db_pool = r2d2::Pool::new(
        PostgresConnectionManager::new(config.postgres_uri, TlsMode::None)
            .expect("could not create PostgresConnectionManager"),
    )
    .expect("could not create r2d2::Pool");

    db_pool
        .get()
        .unwrap()
        .execute(
            "CREATE TABLE IF NOT EXISTS profiles (
                id BIGSERIAL PRIMARY KEY NOT NULL,
                base VARCHAR(255) NOT NULL,
                name VARCHAR(255) UNIQUE NOT NULL,
                json JSONB NOT NULL
            );",
            &[],
        )
        .unwrap();

    db_pool
        .get()
        .unwrap()
        .execute(
            "CREATE TABLE IF NOT EXISTS tasks (
                id BIGSERIAL PRIMARY KEY NOT NULL,
                profile_id BIGSERIAL NOT NULL,
                file_name TEXT NOT NULL,
                data BYTEA,
                hash VARCHAR(255)
            );",
            &[],
        )
        .unwrap();

    let rpc = RpcImpl { db_pool };

    let mut io = IoHandler::new();
    io.extend_with(rpc.to_delegate());

    let server = jsonrpc_tcp_server::ServerBuilder::new(io)
        .start(&std::net::SocketAddr::from((
            config.rpc_listen_address,
            config.rpc_listen_port,
        )))
        .expect("jsonrpc tcp server failed to start");

    server.wait();
}
