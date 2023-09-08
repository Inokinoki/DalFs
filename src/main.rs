use opendal::services::Fs;
use futures::executor; // 0.3.1

use std::collections::HashMap;

use opendal::services::S3;
use opendal::Operator;
use opendal::Result;
use opendal::Scheme;

fn main() -> Result<()> {
    let op = init_operator_via_builder()?.blocking();
    println!("operator from builder: {:?}", op);

    op.write("hello.txt", "Hello, World!");

    Ok(())
}

fn init_operator_via_builder() -> Result<Operator> {
    let mut builder = Fs::default();
    builder.root("/tmp");

    let op = Operator::new(builder)?.finish();
    Ok(op)
}

