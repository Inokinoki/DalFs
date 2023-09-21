use time::Timespec;
use fuse::{
    FileType,
    FileAttr,
    Filesystem,
    Request,
    ReplyData,
    ReplyEntry,
    ReplyAttr,
    ReplyDirectory,
    ReplyEmpty,
    ReplyWrite,
    ReplyOpen,
    ReplyCreate,
};

use opendal::EntryMode;
use opendal::Metadata;
use opendal::BlockingOperator;

use libc::ENOENT;
use libc::EACCES;
use libc::ENOSYS;
use libc::EIO;
use std::ffi::OsStr;
use std::ffi::OsString;
use std::path::Path;

use chrono::DateTime;
use chrono::Utc;
use std::time::{
    SystemTime,
    Duration,
    UNIX_EPOCH,
};
use std::result::Result;

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

// Derivated from OpenDAL util
pub fn parse_datetime_from_from_timestamp_millis(s: i64) -> DateTime<Utc> {
    let st = UNIX_EPOCH
        .checked_add(Duration::from_millis(s as u64)).unwrap();
    st.into()
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

    fn mkdir(&mut self, _req: &Request, parent: u64, name: &OsStr, _mode: u32, reply: ReplyEntry) {
        println!("mkdir(parent={}, name={:?}, mode=0o{:o})", parent, name, _mode);

        let path_ref = self.inodes[parent].path.join(&name);
        let path = path_ref.to_str().unwrap();
        match self.op.create_dir(&(path.to_string() + "/")) {
            Ok(_) => {
                let mut meta = Metadata::new(EntryMode::DIR);
                let mut attr = self.inodes.insert_metadata(&path, &meta).attr;
                attr.perm = _mode as u16;
                reply.entry(&TTL, &attr, 0);
            },
            Err(err) => {
                println!("mkdir error - {}", err);
                reply.error(EACCES);
            },
        };
    }

    fn readdir(&mut self, _req: &Request, ino: u64, _fh: u64, offset: i64, mut reply: ReplyDirectory) {
        println!("readdir(ino={}, fh={}, offset={})", ino, _fh, offset);

        let dir_visited = self.inodes.get(ino).map(|n| n.visited).unwrap_or(false);
        if dir_visited {
            let cached_dir = self.cache_readdir(ino);
            if offset as usize > cached_dir.enumerate().count() {
                reply.ok();
                return;
            }
        }

        let parent_ino = match ino {
            1 => 1,
            _ => self.inodes.parent(ino).expect("inode has no parent").attr.ino,
        };

        reply.add(ino, 0, FileType::Directory, ".");
        reply.add(parent_ino, 1, FileType::Directory, "..");

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

    fn mknod(&mut self, _req: &Request, parent: u64, name: &OsStr, _mode: u32, _rdev: u32, reply: ReplyEntry) {
        println!("mknod(parent={}, name={:?}, mode=0o{:o})", parent, name, _mode);

        // TODO: check if we have write access to this dir in OpenDAL
        let path = self.inodes[parent].path.join(&name);
        let now = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap();

        let mut meta = Metadata::new(EntryMode::FILE);
        meta.set_last_modified(
            parse_datetime_from_from_timestamp_millis(
                now.as_secs() as i64 * 1000 + now.subsec_millis() as i64
            )
        );
        meta.set_content_length(0);

        // FIXME: cloning because it's quick-and-dirty
        let attr = self.inodes.insert_metadata(&Path::new(&path), &meta).attr.clone();

        let pathStr = path.to_str().unwrap();
        match self.op.write(pathStr, vec!()) {
            Ok(_) => reply.entry(&TTL, &attr, 0),
            Err(_) => reply.error(ENOENT),
        };
    }

    fn open(&mut self, _req: &Request, ino: u64, flags: u32, reply: ReplyOpen) {
        println!("open(ino={}, flags=0x{:x})", ino, flags);

        match self.inodes.get(ino) {
            Some(inode) => {
                // TODO: Create reader and/or writer
                reply.opened(0, flags);
            },
            None => reply.error(ENOENT),
        };
    }

    fn setattr(&mut self, _req: &Request, ino: u64, _mode: Option<u32>, uid: Option<u32>, gid: Option<u32>,
        size: Option<u64>, _atime: Option<Timespec>, _mtime: Option<Timespec>, _fh: Option<u64>,
        _crtime: Option<Timespec>, _chgtime: Option<Timespec>, _bkuptime: Option<Timespec>, flags: Option<u32>, reply: ReplyAttr) {
        println!("setattr(ino={}, mode={:?}, size={:?}, fh={:?}, flags={:?})", ino, _mode, size, _fh, flags);
        match self.inodes.get_mut(ino) {
            Some(mut inode) => {
                if let Some(new_size) = size {
                    inode.attr.size = new_size;
                }
                if let Some(new_uid) = uid {
                    inode.attr.uid = new_uid;
                }
                if let Some(new_gid) = gid {
                    inode.attr.gid = new_gid;
                }
                // TODO: is mode (u32) equivalent to attr.perm (u16)?
                reply.attr(&TTL, &inode.attr);
            }
            None => reply.error(ENOENT)
        }
    }

    fn write(&mut self, _req: &Request, ino: u64, fh: u64, offset: i64, data: &[u8], flags: u32, reply: ReplyWrite) {
        // TODO: check if in read-only mode: reply EROFS
        println!("write(ino={}, fh={}, offset={}, len={}, flags=0x{:x})", ino, fh, offset, data.len(), flags);

        let is_replace = (offset == 0) && (self.inodes.get(ino).unwrap().attr.size < data.len() as u64);

        // Open a reader and flush all data to writer if not replace
        if !is_replace {
            match self.inodes.get_mut(ino) {
                Some(mut inode) => {
                    // We assume to have reading perm with writing perm
                    let original_data = match self.op.read(inode.path.to_str().unwrap()) {
                        Ok(d) => d, // TODO: Do not copy all data
                        Err(_) => {
                            println!("Reading failed");
                            reply.error(ENOENT);
                            return;
                        },
                    };
                    let mut new_size = original_data.len() as u64;
                    // TODO: Validate the length

                    let mut writer = match self.op.writer(inode.path.to_str().unwrap()) {
                        Ok(writer) => writer,
                        Err(_) => {
                            println!("Writing failed");
                            reply.error(ENOENT);
                            return;
                        },
                    };

                    let _ = writer.write(original_data);
                    // Write new content
                    new_size = new_size + match writer.write(data.to_vec()) {
                        Ok(_) => {
                            reply.written(data.len() as u32);
                            data.len() as u64
                        },
                        Err(_) => {
                            println!("Writing failed");
                            reply.error(ENOENT);
                            0
                        },
                    };

                    let _ = writer.close();
                    inode.attr.size = new_size;
                    return;
                },
                None => {
                    println!("reading failed");
                    reply.error(ENOENT);
                    return;
                },
            }
        } else {
            // Replace the file
            let new_size = match self.inodes.get_mut(ino) {
                Some(mut inode) => {
                    match self.op.write(inode.path.to_str().unwrap(), data.to_vec()) {
                        Ok(_) => {
                            reply.written(data.len() as u32);
                            data.len() as u64
                        },
                        Err(_) => {
                            println!("Writing failed");
                            reply.error(ENOENT);
                            0
                        },
                    }
                },
                None => {
                    println!("write failed to read file");
                    reply.error(ENOENT);
                    return;
                },
            };

            let ref mut inode = self.inodes[ino];
            inode.attr.size = new_size;
        }
    }

    fn flush(&mut self, _req: &Request<'_>, ino: u64, fh: u64, _lock_owner: u64, reply: ReplyEmpty) {
        println!("flush(ino={}, fh={})", ino, fh);
        // TODO: find a way to flush reader and/or writer
        reply.error(ENOSYS);
    }

    fn release(&mut self, _req: &Request<'_>, ino: u64, fh: u64, flags: u32, _lock_owner: u64, flush: bool, reply: ReplyEmpty) {
        println!("release(ino={}, fh={}, flags={}, flush={})", ino, fh, flags, flush);
        // TODO: close writer and reader
        reply.ok();
    }

    fn unlink(&mut self, _req: &Request, parent: u64, name: &OsStr, reply: ReplyEmpty) {
        println!("unlink(parent={}, name={:?})", parent, name);

        let ino_opt = self.inodes.child(parent, &name).map(|inode| inode.attr.ino);
        let path_ref = self.inodes[parent].path.join(&name);
        let path = path_ref.to_str().unwrap();
        match self.op.delete(path) {
            Ok(_) => {
                ino_opt.map(|ino| {
                    self.inodes.remove(ino);
                });
                reply.ok()
            },
            Err(err) => {
                println!("Delete failed: {}", err);
                reply.error(EIO);
            }
        }
    }
}
