# DalFs: a Userspace filesystem based on OpenDAL

**Still a WIP!!! Could be buggy and risky, please use a VM to test.** Take your own risk.

Currently, it can provide a Userspace filesystem backing by [OpenDAL](https://github.com/apache/incubator-opendal), thus supports the following backend as data sources:

- Atomicserver
- Azblob
- Azdls
- Cacache
- Cos
- Dashmap
- Etcd
- Foundationdb
- Fs
- Ftp
- Gcs
- Ghac
- Hdfs
- Http
- Ipfs
- Ipmfs
- Memcached
- Memory
- MiniMoka
- Moka
- Obs
- Onedrive
- Gdrive
- Dropbox
- Oss
- Persy
- Redis
- Postgresql
- Rocksdb
- S3
- Sftp
- Sled
- Supabase
- VercelArtifacts
- Wasabi
- Webdav
- Webhdfs
- Redb
- Tikv

The functionnality is very limited right now, because I just use several hours to come up with the idea and implement it.

Here are the main fs functionnalities implemented:

- Read directory
- Read file
- Read attributes (not well implemented)
- Create directory
- Create file
- Write file

or not yet implemented:
- Permission (?)
- Fsync (?)
- ...

## Build

You might need to install `libfuse-dev` in order to use Rust `fuser` crate.

Just run the following command to build it:

```bash
cargo build
```

## Run

To run, you will need to provide a series of 

```bash
cargo run <mount-point> <scheme> ...
```

where the `mount-point` is a path to mount the filesystem; `scheme` is an OpenDAL scheme, all in lowercase (e.g. "ftp", "s3", "fs", etc.).

The remaining parameters are `<key>=<value>` pairs needed by OpenDAL schemes.

Currently `fs` and `s3` backends are tested. For example, the following command will mount a filesystem using the data in your `/tmp` directory to the mount-point.

```bash
cargo run <mount-point> fs root=/tmp
```

<img width="1185" alt="image" src="https://github.com/Inokinoki/DalFs/assets/8311300/c591ffe1-be35-4c79-8ffa-368c66872b9f">

And the following mount a filesystem backed by s3:

```bash
cargo run <mount-point> s3 root=/tmp endpoint=<end-point-url> bucket=<bucket> access_key_id=<access-key-id> secret_access_key=<secret-access-key> region=auto
```

For more details and more backends, please check [OpenDAL scheme doc](https://opendal.apache.org/docs/rust/opendal/enum.Scheme.html).

## Contribution

All kinds of contributions are welcomed. But I will firstly work on the functionnalities.
