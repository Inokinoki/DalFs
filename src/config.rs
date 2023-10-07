use opendal::Operator;
use opendal::Result;
use opendal::Scheme;

use std::collections::HashMap;
use std::env;

#[feature(str_split_remainder)]
pub fn get_operator_from_env(scheme: &str) -> Result<Operator> {
    let mut map = HashMap::new();

    let mut args = env::args_os();
    args.next(); args.next(); args.next();  // Ignore the first three args
    for arg_os in args {
        println!("Parsing arg: {:?}", arg_os);
        let arg = arg_os.to_str().unwrap();
        let mut split = arg.split('=');
        map.insert(split.next().unwrap().to_string(), split.next().unwrap_or(&"").to_string());
    }
    println!("Parsed {:?}", map);

    get_operator_with_config(scheme, map)
}

pub fn get_operator_with_config(scheme: &str, map: HashMap<String, String>) -> Result<Operator> {
    match scheme {
        "azblob" => Operator::via_map(Scheme::Azblob, map),
        "azdls" => Operator::via_map(Scheme::Azdls, map),
        // "cacache" => Operator::via_map(Scheme::Cacache, map),
        "cos" => Operator::via_map(Scheme::Cos, map),
        "dashmap" => Operator::via_map(Scheme::Dashmap, map),
        // "etcd" => Operator::via_map(Scheme::Etcd, map),
        // "foundationdb" => Operator::via_map(Scheme::Foundationdb, map),
        "fs" => Operator::via_map(Scheme::Fs, map),
        "ftp" => Operator::via_map(Scheme::Ftp, map),
        "gcs" => Operator::via_map(Scheme::Gcs, map),
        "ghac" => Operator::via_map(Scheme::Ghac, map),
        "hdfs" => Operator::via_map(Scheme::Hdfs, map),
        "http" => Operator::via_map(Scheme::Http, map),
        "ipfs" => Operator::via_map(Scheme::Ipfs, map),
        "ipmfs" => Operator::via_map(Scheme::Ipmfs, map),
        // "memcached" => Operator::via_map(Scheme::Memecached, map),
        "memory" => Operator::via_map(Scheme::Memory, map),
        // "minimoka" => Operator::via_map(Scheme::MiniMoka, map),
        "moka" => Operator::via_map(Scheme::Moka, map),
        "obs" => Operator::via_map(Scheme::Obs, map),
        "onedrive" => Operator::via_map(Scheme::Onedrive, map),
        "gdrive" => Operator::via_map(Scheme::Gdrive, map),
        // "dropbox" => Operator::via_map(Scheme::Dropbox, map),
        "oss" => Operator::via_map(Scheme::Oss, map),
        // "persy" => Operator::via_map(Scheme::Persy, map),
        "redis" => Operator::via_map(Scheme::Redis, map),
        // "postgresql" => Operator::via_map(Scheme::Postgresql, map),
        "rocksdb" => Operator::via_map(Scheme::Rocksdb, map),
        "s3" => Operator::via_map(Scheme::S3, map),
        "sftp" => Operator::via_map(Scheme::Sftp, map),
        "sled" => Operator::via_map(Scheme::Sled, map),
        "supabase" => Operator::via_map(Scheme::Supabase, map),
        "vercelartifacts" => Operator::via_map(Scheme::VercelArtifacts, map),
        "wasabi" => Operator::via_map(Scheme::Wasabi, map),
        "webdav" => Operator::via_map(Scheme::Webdav, map),
        "webhdfs" => Operator::via_map(Scheme::Webhdfs, map),
        // "redb" => Operator::via_map(Scheme::Redb, map),
        // "tikv" => Operator::via_map(Scheme::Tikv, map),
        _ => {
            panic!("Not yet support {}", scheme);
        }
    }
} 

