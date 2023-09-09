use time::Timespec;
use fuse::{FileType, FileAttr, Filesystem, Request, ReplyData, ReplyEntry, ReplyAttr, ReplyDirectory};

use opendal::EntryMode;
use opendal::BlockingOperator;

use libc::ENOENT;
use libc::EACCES;
use std::ffi::OsStr;
use std::ffi::OsString;
use std::path::Path;

use crate::inode;

const TTL: Timespec = Timespec { sec: 1, nsec: 0 };                     // 1 second

const CREATE_TIME: Timespec = Timespec { sec: 1381237736, nsec: 0 };    // 2013-10-08 08:56

const HELLO_DIR_ATTR: FileAttr = FileAttr {
    ino: 1,
    size: 0,
    blocks: 0,
    atime: CREATE_TIME,
    mtime: CREATE_TIME,
    ctime: CREATE_TIME,
    crtime: CREATE_TIME,
    kind: FileType::Directory,
    perm: 0o755,
    nlink: 2,
    uid: 1000,
    gid: 1000,
    rdev: 0,
    flags: 0,
};

pub struct DalFs {
    pub op: BlockingOperator,
    pub inodes: inode::InodeStore,
}

fn get_basename(path: &Path) -> &OsStr {
    path.file_name().expect("missing filename")
}

pub type LibcError = libc::c_int;

impl DalFs {
    fn cache_readdir<'a>(&'a mut self, ino: u64) -> Box<dyn Iterator<Item=Result<(OsString, FileAttr), LibcError>> + 'a> {
        let iter = self.inodes
            .children(ino)
            .into_iter()
            .map( move |child| {
                Ok((get_basename(&child.path).into(), child.attr.clone()))
            });
        Box::new(iter)
    }
}

impl Filesystem for DalFs {
    fn lookup(&mut self, _req: &Request, parent: u64, name: &OsStr, reply: ReplyEntry) {
        let name_str = name.to_str().unwrap();
        println!("lookup(parent={}, name=\"{}\")", parent, name_str);

        match self.inodes.child(parent, &name).cloned() {
            Some(child_inode) => reply.entry(&TTL, &child_inode.attr, 0),
            None => {
                let parent_inode = self.inodes[parent].clone();
                let child_path = parent_inode.path.join(&name).as_path().display().to_string();
                match self.op.stat(&child_path) {
                    Ok(child_metadata) => {
                        let inode = self.inodes.insert_metadata(&child_path, &child_metadata);
                        reply.entry(&TTL, &inode.attr, 0)
                    }
                    Err(err) => {
                        println!("{}", err);
                        reply.error(ENOENT)
                    }
                }
            }
        }
    }

    fn getattr(&mut self, _req: &Request, ino: u64, reply: ReplyAttr) {
        println!("getattr(ino={})", ino);
        // TODO: Allow to read more attr
        match ino {
            1 => reply.attr(&TTL, &HELLO_DIR_ATTR),
            _ => {
                match self.inodes.get(ino) {
                    Some(_) => {
                        reply.attr(&TTL,  &FileAttr {
                            ino: ino,
                            size: 0,
                            blocks: 0,
                            atime: CREATE_TIME,
                            mtime: CREATE_TIME,
                            ctime: CREATE_TIME,
                            crtime: CREATE_TIME,
                            kind: FileType::Directory,
                            perm: 0o755,
                            nlink: 2,
                            uid: 1000,
                            gid: 1000,
                            rdev: 0,
                            flags: 0,
                        });
                    },
                    None => reply.error(ENOENT),
                };
            },
        }
    }

    fn read(&mut self, _req: &Request, ino: u64, _fh: u64, offset: i64, size: u32, reply: ReplyData) {
        println!("read(ino={}, fh={}, offset={}, size={})", ino, _fh, offset, size);

        match self.inodes.get(ino) {
            Some(inode) => {
                let path = Path::new(&inode.path);
                let result = self.op.read(path.to_str().unwrap());

                match result {
                    Ok(buffer) => {
                        // Return the read data
                        let end_offset = offset + size as i64;
                        match buffer.len() {
                            len if len as i64 > offset + size as i64 => {
                                reply.data(&buffer[(offset as usize)..(end_offset as usize)]);
                            }
                            len if len as i64 > offset => {
                                reply.data(&buffer[(offset as usize)..]);
                            }
                            len => {
                                println!("attempted read beyond buffer for ino {} len={} offset={} size={}", ino, len, offset, size);
                                reply.error(ENOENT);
                            }
                        }
                    },
                    Err(_) => {
                        reply.error(ENOENT);
                    },
                };
            },
            None => {
                // FS will firstly lookup and then read inode, so inode should be there
                reply.error(ENOENT);
            },
        };
    }

    fn readdir(&mut self, _req: &Request, ino: u64, _fh: u64, offset: i64, mut reply: ReplyDirectory) {
        println!("readdir(ino={}, fh={}, offset={})", ino, _fh, offset);
        if offset > 0 {
            // TODO: Support offset
            reply.ok();
            return;
        }

        let parent_ino = match ino {
            1 => 1,
            _ => self.inodes.parent(ino).expect("inode has no parent").attr.ino,
        };

        reply.add(ino, 0, FileType::Directory, ".");
        reply.add(parent_ino, 1, FileType::Directory, "..");

        let dir_visited  = self.inodes.get(ino).map(|n| n.visited).unwrap_or(false);
        match dir_visited {
            // read directory from cache
            true =>  {
                for (i, next) in self.cache_readdir(ino).enumerate().skip(offset as usize) {
                    match next {
                        Ok((filename, attr)) => {
                            reply.add(attr.ino, i as i64 + offset + 2, attr.kind, &filename);
                        }
                        Err(err) => { return reply.error(err); }
                    }
                }
            },
            // read directory from OpenDAL and save to cache
            false => {
                let ref parent_path = self.inodes[ino].path.clone();

                let entries = match self.op.list(parent_path.to_str().unwrap()) {
                    Ok(entries)  => entries,
                    Err(e) => return reply.error(EACCES),
                };
                for (index, result) in entries.into_iter().enumerate().skip(offset as usize) {
                    match result {
                        Ok(entry) => {
                            let metadata = self.op.stat(entry.path()).unwrap();
                            let child_path = parent_path.join(entry.name());
                            let inode = self.inodes.insert_metadata(&child_path, &metadata);
                            reply.add(inode.attr.ino, index as i64 + offset + 2, inode.attr.kind, entry.name());
        
                            match metadata.mode() {
                                EntryMode::FILE => {
                                    println!("Handling file");
                                    // reply.add(inode, i + offset + 2, FileType::RegularFile, child_path);
                                }
                                EntryMode::DIR => {
                                    println!("Handling dir {} {}", entry.path(), entry.name());
                                    // reply.add(inode, i + offset + 2, FileType::Directory, child_path);
                                }
                                EntryMode::Unknown => continue,
                            };
                        },
                        Err(..) => {
                            return reply.error(ENOENT);
                        },
                    };
                }
            }
        };

        // Mark this node visited
        let mut inodes = &mut self.inodes;
        let mut dir_inode = inodes.get_mut(ino).expect("inode missing for dir just listed");
        dir_inode.visited = true;

        reply.ok();
    }
}
