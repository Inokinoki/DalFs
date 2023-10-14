use clap::Parser;
use config::App;
use opendal::Operator;
use tap::Tap;

use std::process::ExitCode;

use log;

mod config;
mod dalfs;
mod inode;

#[tokio::main]
async fn main() -> ExitCode {
    let config = config::App::parse();
    env_logger::init();

    if let Err(e) = run(config) {
        log::error!("{}", e);
    }

    ExitCode::SUCCESS
}

fn run(config: App) -> Result<(), Box<dyn std::error::Error>> {
    match config.command {
        config::Commands::Mount(args) => {
            let mut options = args.options.unwrap_or_default();
            if let Some(root) = args.device {
                options.insert("root".to_string(), root);
            }

            let fs = dalfs::DalFs {
                op: Operator::via_map(args.r#type, options)?
                    .tap(|op| log::debug!("operator: {op:?}")),
                inodes: inode::InodeStore::new(0o550, 1000, 1000), // Temporarilly hardcode
            };

            fuser::mount(fs, args.mount_point, &[])?;
        }
    }

    Ok(())
}
