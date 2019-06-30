use jsonrpc_core::{Error, IoHandler, Result};
use jsonrpc_derive::rpc;

use serde_derive::{Deserialize, Serialize};

use r2d2_postgres::{PostgresConnectionManager, TlsMode};

use multihash::{decode, encode, Hash};

#[derive(Serialize, Deserialize)]
pub struct TaskId(i64);

#[derive(Serialize, Deserialize)]
pub struct ProfileId(i64);

#[derive(Serialize, Deserialize)]
pub struct Profile {
    id: ProfileId,
    base: String,
    name: String,
    json: String,
}

#[derive(Serialize, Deserialize)]
pub struct Task {
    id: TaskId,
    profile_id: ProfileId,
    file_name: String,
    data: Vec<u8>,
    multihash: Vec<u8>,
}

#[rpc]
pub trait Rpc {
    #[rpc(name = "create_profile")]
    fn create_profile(&self, base_name: String, name: String, json: String) -> Result<ProfileId>;

    #[rpc(name = "list_profiles")]
    fn list_profiles(&self, by_base: Option<String>) -> Result<Vec<ProfileId>>;

    #[rpc(name = "fetch_profile")]
    fn fetch_profile(&self, id: ProfileId) -> Result<Profile>;

    #[rpc(name = "create_task")]
    fn create_task(&self, profile: ProfileId, file_name: String, data: Vec<u8>) -> Result<TaskId>;

    #[rpc(name = "list_tasks")]
    fn list_tasks(&self, by_profile: Option<ProfileId>) -> Result<Vec<TaskId>>;

    #[rpc(name = "fetch_task")]
    fn fetch_task(&self, id: TaskId) -> Result<Task>;
}

pub struct RpcImpl {
    db_pool: r2d2::Pool<PostgresConnectionManager>,
}

impl Rpc for RpcImpl {
    fn create_profile(&self, base_name: String, name: String, json: String) -> Result<ProfileId> {
        let conn = self.db_pool.get().unwrap();

        Ok(ProfileId(
            conn.query(
                "INSERT INTO profiles (base, name, json) VALUES ($1, $2, $3) RETURNING id",
                &[&base_name, &name, &json],
            )
            .unwrap()
            .iter()
            .next()
            .unwrap()
            .get(0),
        ))
    }

    fn list_profiles(&self, by_base: Option<String>) -> Result<Vec<ProfileId>> {
        let conn = self.db_pool.get().unwrap();

        if let Some(by_base) = by_base {
            Ok(conn
                .query("SELECT (id) FROM profiles WHERE base = $1", &[&by_base])
                .unwrap()
                .iter()
                .map(|row| ProfileId(row.get(0)))
                .collect())
        } else {
            Ok(conn
                .query("SELECT (id) FROM profiles", &[])
                .unwrap()
                .iter()
                .map(|row| ProfileId(row.get(0)))
                .collect())
        }
    }

    fn fetch_profile(&self, id: ProfileId) -> Result<Profile> {
        let conn = self.db_pool.get().unwrap();

        let rows = conn
            .query("SELECT * FROM profiles WHERE id = $1", &[&id.0])
            .unwrap();
        let profile_row = rows.iter().next().unwrap();

        Ok(Profile {
            id: ProfileId(profile_row.get("id")),
            base: profile_row.get("base"),
            name: profile_row.get("name"),
            json: profile_row.get("json"),
        })
    }

    fn create_task(&self, profile: ProfileId, file_name: String, data: Vec<u8>) -> Result<TaskId> {
        // Call before requesting conn from pool to not require two conns for one RPC call
        let profile = self.fetch_profile(profile).unwrap();

        let conn = self.db_pool.get().unwrap();

        Ok(TaskId(
            conn.query(
                "INSERT INTO tasks (profile_id, file_name, data, multihash) VALUES ($1, $2, $3, $4) RETURNING id",
                &[&profile.id.0, &file_name, &data, &encode(Hash::SHA2256, &data).unwrap()],
            )
            .unwrap()
            .iter()
            .next()
            .unwrap()
            .get(0),
        ))
    }

    fn list_tasks(&self, by_profile: Option<ProfileId>) -> Result<Vec<TaskId>> {
        let conn = self.db_pool.get().unwrap();

        if let Some(by_profile) = by_profile {
            Ok(conn
                .query(
                    "SELECT (id) FROM tasks WHERE profile_id = $1",
                    &[&by_profile.0],
                )
                .unwrap()
                .iter()
                .map(|row| TaskId(row.get(0)))
                .collect())
        } else {
            Ok(conn
                .query("SELECT (id) FROM tasks", &[])
                .unwrap()
                .iter()
                .map(|row| TaskId(row.get(0)))
                .collect())
        }
    }

    fn fetch_task(&self, id: TaskId) -> Result<Task> {
        let conn = self.db_pool.get().unwrap();

        let rows = conn
            .query("SELECT * FROM tasks WHERE id = $1", &[&id.0])
            .unwrap();
        let task_row = rows.iter().next().unwrap();

        Ok(Task {
            id: TaskId(task_row.get("id")),
            profile_id: ProfileId(task_row.get("profile_id")),
            file_name: task_row.get("file_name"),
            data: task_row.get("data"),
            multihash: task_row.get("multihash"),
        })
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
                multihash BYTEA
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
