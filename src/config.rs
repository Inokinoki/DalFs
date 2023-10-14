use clap::{Args, Parser, Subcommand};
use opendal::Scheme;
use std::{collections::HashMap, str::FromStr};

#[derive(Debug, Parser)]
#[command(author, version, about, long_about = None)]
pub struct App {
    #[command(subcommand)]
    pub command: Commands,
}

#[derive(Debug, Subcommand)]
pub enum Commands {
    Mount(MountArgs),
}

#[derive(Debug, Args)]
pub struct MountArgs {
    pub mount_point: String,
    pub device: Option<String>,

    /// OpenDAL scheme
    #[arg(short, long, value_parser = parse_type)]
    pub r#type: Scheme,

    /// Configuration of the OpenDAL scheme in the format <key1>=<val1>,<key2>=<val2>,..
    #[arg(short, long, value_parser = parse_options)]
    pub options: Option<HashMap<String, String>>,
}

fn parse_options(raw: &str) -> Result<HashMap<String, String>, String> {
    raw.split(',')
        .map(|kv| {
            kv.split_once('=')
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .ok_or(String::from("Invalid key value format".to_string()))
        })
        .collect::<Result<HashMap<String, String>, String>>()
}

fn parse_type(raw: &str) -> Result<Scheme, String> {
    Scheme::from_str(raw).map_err(|_| "Invalid OpenDAL scheme".to_string())
}
