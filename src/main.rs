extern crate env_logger;
extern crate fuse;
extern crate libc;
extern crate time;

use opendal::Operator;
use opendal::Result;
use opendal::services::Fs;

use std::env;

mod dalfs;
mod inode;

fn main() -> Result<()> {
    let op = init_operator_via_builder()?.blocking();
    println!("operator from builder: {:?}", op);

    let fs = dalfs::DalFs {
        op: op,
        inodes: inode::InodeStore::new(0o550, 1000, 1000),  // Temporarilly hardcode
    };

    env_logger::init();
    let mountpoint = env::args_os().nth(1).unwrap();
    fuse::mount(fs, &mountpoint, &[]).unwrap();

    Ok(())
}

fn init_operator_via_builder() -> Result<Operator> {
    let mut builder = Fs::default();
    builder.root("/tmp");

    let op = Operator::new(builder)?.finish();
    Ok(op)
}

