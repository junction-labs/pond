# Pond

Pond is a version-based, S3-backed filesystem that makes working with object
storage feel like working with local files. With Pond, you read and write plain
files while all of your data lives safely versioned in S3.
 
Pond works by committing snapshots of your datasets, like a Git commit for your
filesystem. This makes datasets immutable, reproducible, and safe to share
across jobs or collaborators. It’s built for write-once, read-many (WORM)
workloads, common in data and ML pipelines.

Pond eliminates the usual object storage plumbing, while still giving you the
speed and scale of S3. Stop worrying about multipart uploads or accidentally
overwriting data and just do things. 

## Core concepts

Pond is built around two  key ideas:
- Volume – a collection of files managed by Pond, like a dataset or project
  directory.
- Version – a read-only version of a volume at a specific point in time, similar
  to a Git commit.

To use a volume, you mount it as part of your filesystem using FUSE. You can
read and write files as normal with any of your favorite tools. When you’re
ready to share changes, you commit them to save a new version. New versions are
saved as deltas, so you don’t have to worry about duplicating your data.  

- Mount – how you access a version as a normal filesystem using FUSE.
- Commit – the act of saving your current changes as a new version.

You work inside a mounted version of a volume, make edits locally, and when
you’re ready, commit to create a new immutable version on S3.

## Getting Started

> [!NOTE] Pond is experimental and still alpha-quality software. 

### System Setup

Pond today only runs on Linux. You’ll need FUSE for filesystem mounting, which
on modern distros is usually preinstalled.

For best performance, use a Linux cloud VM close to your S3 bucket for the
lowest latency, and the highest bandwidth. However, local on your laptop works
fine, it's just slower.

While Pond is experimental, you'll need a working Rust toolchain. We recommend
[Rustup](https://rustup.rs/).

Make sure the cargo binary directory is in your path: 
```
$ export PATH=$PATH:$HOME/.cargo/bin/
```

### Cloud Credentials

If you’re on an AWS VM, Pond auto-detects instance credentials.

Otherwise to connect to AWS use the [standard AWS SDK environment
variables](https://docs.aws.amazon.com/sdkref/latest/guide/environment-variables.html).

Pond does not support other clouds at this point in time. If you’re interested
in support for a specific cloud, please file a new GitHub issue.

Building and installing
```
$ git clone https://github.com/junction-labs/pond.git
$ cargo install --path ./pond/pond-fs
```
### Create a volume

Once you've got Pond installed, you can create a volume. This command
initializes the contents from the samples directory
`./pond/examples/hello-pond-src/`:
```
$ BUCKETNAME=my-bucket  ## use your already existing S3 bucket name here
$ pond create ./pond/examples/hello-pond-src/ s3://$BUCKETNAME/hello-pond v1
```
You can examine the raw contents of the newly created volume with the pond CLI:
```
$ pond versions s3://$BUCKETNAME/hello-pond
v1
$ pond list s3://$BUCKETNAME/hello-pond
              25 a -> 6c6e118152a67856.data @ 0
              32 b -> 6c6e118152a67856.data @ 25
              31 c -> 6c6e118152a67856.data @ 57
```
This shows that the 3 original files have been backed into a single pond file
named `6c6e118152a67856.data`, with a `v1` label.

### Mount and Use the Volume

Now to use Pond you don't access these S3 files directly, instead you use FUSE
to mount the volume into your filesystem: 
```
$ pond mount s3://$BUCKETNAME/hello-pond /tmp/hello-pond
```
And then use it just like normal files:
```
$ ls /tmp/hello-pond
total 0
-rw-r--r-- 1 inowland inowland 25 Dec 31  1969 a
-rw-r--r-- 1 inowland inowland 32 Dec 31  1969 b
-rw-r--r-- 1 inowland inowland 31 Dec 31  1969 c
$
$ grep -r file /tmp/hello-pond/
grep: /tmp/hello-pond/.clearcache: Operation not permitted
grep: /tmp/hello-pond/.commit: Operation not permitted
/tmp/hello-pond/a:A file to test pond with.
/tmp/hello-pond/b:A second file to test pond with.
/tmp/hello-pond/c:A third file to test pond with.
```
Note that the output of the grep shows a couple of special control files you can
use to communicate with pond, which we explain below.

### Commit a new version

Once you're ready to save your changes, commit a new version by writing a name
for the commit into the special /tmp/hello-pond/.commit file. The commit label
can be any utf-8 string that's 64 characters or less:
```
$ echo "a fourth pond file" > /tmp/hello-pond/d
$ echo "v2" > /tmp/hello-pond/.commit
```

You should now see two versions of the example volume saved in object storage:
```
$ pond versions s3://$BUCKETNAME/hello-pond
v1
v2
```
If you mount that version somewhere else, you'll be able to see the file we just
wrote
```
$ pond mount s3://$BUCKETNAME/hello-pond /tmp/another-pond --version v2
$ cat /tmp/another-pond/d
a fourth pond file
```

### Interacting with snapshots

You can still mount and use the original version like nothing has changed. If we
remount v1 of our volume, we can see that `d` does not exist, even though we
wrote it in v2:
```
$ umount /tmp/hello-pond
$ pond mount s3://$BUCKETNAME/hello-pond /tmp/hello-pond --version v1
$ cat /tmp/hello-pond/d
cat: /tmp/hello-pond/d: No such file or directory
```

### Unmounting pond

To get rid of our mounts, we can’t just remove the directories, instead we must
use the umount command:
```
$ umount /tmp/hello-pond
$ umount /tmp/another-pond
```

## Performance

Where Pond shines versus other s3 mounting options is for small to medium sized
files, serially accessed (i.e. your application just wants to process all files
in a directory). For these types of application, performance wins of 10x are
possible in terms of reduced latency and incresed throughput. We have a
[benchmark here](./examples/benchmark).

## Advanced Usage

### Auto unmounting

Pond can automatically unmount itself when the userspace process exits. To
enable automatic unmounting, you need to change some of the FUSE options on your
system.

By default `user_allow_other` is unset in `/etc/fuse.conf`, restricting access
to the mounted fuse to just you. If you want to`enable auto_umount` or allow
other users to access the mounted fuse, you must set `user_allow_other` by
modifying `/etc/fuse.conf` and uncommenting the `user_allow_other` parameter.
```
# The file /etc/fuse.conf allows for the following parameters:
#
# user_allow_other - Using the allow_other mount option works fine as root, in
# order to have it work as user you need user_allow_other in /etc/fuse.conf as
# well. (This option allows users to use the allow_other option.) You need
# allow_other if you want users other than the owner to access a mounted fuse.
# This option must appear on a line by itself. There is no value, just the
# presence of the option.
```

## Roadmap

- [ ] Documentation
- [ ] GCS support
- [ ] Optional disk-based caching
- [ ] Optimizing filesystem metadata
- [ ] Better compaction on `.commit`
- [ ] A Rust API for access without FUSE
- [ ] A Python API and `fsspec` bindings for access without FUSE
- [ ] Reloading without remounting

## Contributing

To report a bug or suggest a feature, open an issue on this repository. Bug
reports are extremely welcome.

While Pond is under active development, we're not accepting pull requests for
new features and will close any pull request without a linked issue.
