use time::Timespec;
use fuse::{FileType, FileAttr, Filesystem, Request, ReplyData, ReplyEntry, ReplyAttr, ReplyDirectory};

use opendal::EntryMode;
use opendal::BlockingOperator;

use libc::ENOENT;
use std::ffi::OsStr;

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
    uid: 501,
    gid: 20,
    rdev: 0,
    flags: 0,
};

const HELLO_TXT_CONTENT: &'static str = "Hello World!\n";

const HELLO_TXT_ATTR: FileAttr = FileAttr {
    ino: 2,
    size: 13,
    blocks: 1,
    atime: CREATE_TIME,
    mtime: CREATE_TIME,
    ctime: CREATE_TIME,
    crtime: CREATE_TIME,
    kind: FileType::RegularFile,
    perm: 0o644,
    nlink: 1,
    uid: 501,
    gid: 20,
    rdev: 0,
    flags: 0,
};
const TEST_TXT_ATTR: FileAttr = FileAttr {
    ino: 3,
    size: 13,
    blocks: 1,
    atime: CREATE_TIME,
    mtime: CREATE_TIME,
    ctime: CREATE_TIME,
    crtime: CREATE_TIME,
    kind: FileType::RegularFile,
    perm: 0o644,
    nlink: 1,
    uid: 501,
    gid: 20,
    rdev: 0,
    flags: 0,
};

pub struct DalFs {
    pub op: BlockingOperator,
}

impl Filesystem for DalFs {
    fn lookup(&mut self, _req: &Request, parent: u64, name: &OsStr, reply: ReplyEntry) {
        if parent == 1 && name.to_str() == Some("he.txt")  {
            reply.entry(&TTL, &HELLO_TXT_ATTR, 0);
        } else if parent == 1 && name.to_str() == Some("test.txt") {
            reply.entry(&TTL, &TEST_TXT_ATTR, 0);
        } else {
            reply.error(ENOENT);
        }
    }

    fn getattr(&mut self, _req: &Request, ino: u64, reply: ReplyAttr) {
        match ino {
            1 => reply.attr(&TTL, &HELLO_DIR_ATTR),
            2 => reply.attr(&TTL, &HELLO_TXT_ATTR),
            _ => reply.error(ENOENT),
        }
    }

    fn read(&mut self, _req: &Request, ino: u64, _fh: u64, offset: i64, _size: u32, reply: ReplyData) {
        if ino == 2 {
            reply.data(&HELLO_TXT_CONTENT.as_bytes()[offset as usize..]);
        } else {
            reply.error(ENOENT);
        }
    }

    fn readdir(&mut self, _req: &Request, ino: u64, _fh: u64, offset: i64, mut reply: ReplyDirectory) {
        if ino == 1 {
            if offset == 0 {
                reply.add(1, 0, FileType::Directory, ".");
                reply.add(1, 1, FileType::Directory, "..");
                reply.add(2, 2, FileType::Directory, "he.txt");
                // dal_listdir(reply);
                let entries = self.op.list("/").unwrap();
                let mut inode = 3;
                let mut offset: i64 = 3;
                for result in entries {
                    let entry = result.unwrap();
                    let metadata = self.op.stat(entry.path()).unwrap();
                    match metadata.mode() {
                        EntryMode::FILE => {
                            println!("Handling file");
                            reply.add(inode, offset, FileType::RegularFile, entry.name());
                        }
                        EntryMode::DIR => {
                            println!("Handling dir {} {}", entry.path(), entry.name());
                            reply.add(inode, offset, FileType::Directory, entry.name());
                        }
                        EntryMode::Unknown => continue,
                    }

                    inode += 1;
                    offset += 1;
                }
            }
            reply.ok();
        } else {
            reply.error(ENOENT);
        }
    }
}
