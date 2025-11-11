# Pond

Pond is a version-based, S3-backed filesystem that feels like working with 
local files. You read and write plain files while all data lives safely in 
S3. Each version captures a complete view of your data, like a Git commit 
for your filesystem.

This makes datasets immutable, reproducible, and safe to share across jobs 
or collaborators. It’s built for write-once, read-many (WORM) workloads, common
in data and ML pipelines.

Pond eliminates the usual S3 plumbing — no separate upload steps, no SDK 
boilerplate, no risk of inconsistent copies. Instead its just versioned data 
that behaves like a local filesystem.

## Core concepts

Pond has four key ideas:

- Volume – a collection of files managed by Pond, like a dataset or project 
  directory.
- Version – a read-only version of a volume at a specific point in time, 
  similar to a Git commit.
- Mount – how you access a version as a normal filesystem using FUSE.
- Commit – the act of saving your current changes as a new version.

You work inside a mounted version of a volume, make edits locally, and when 
you’re ready, commit to create a new immutable version on S3.

## Getting Started

> [!NOTE]
> Pond is experimental and still alpha-quality software. 

### System Setup

Pond runs on Linux or macOS.

For best performance, use a Linux cloud VM close to your S3 bucket for the 
lowest latency, and the highest bandwidth. However, local on your laptop works
fine, it's just slower.

While Pond is experimental, you'll need a working Rust toolchain. We recommend 
[Rustup](https://rustup.rs/).

Make sure the cargo binary directory is in your path:
```
$ export PATH=$PATH:$HOME/.cargo/bin/
```

Finally, you’ll need FUSE for filesystem mounting:
- Linux: usually preinstalled
- macOS: install [Macfuse](https://macfuse.github.io/)

### Cloud Credentials

If you’re on an AWS VM, Pond auto-detects instance credentials.

Otherwise to connect to AWS use the [standard AWS SDK environment
variables](https://docs.aws.amazon.com/sdkref/latest/guide/environment-variables.html).

Pond does not support other clouds at this point in time.

### Building and installing

```
$ git clone https://github.com/junction-labs/pond.git
$ cargo install --path ./pond/pond-fs
```

### Create a volume

Once you've got Pond installed, you can create a volume. This command initializes the 
contents from the existing local directory `./pond/hello-pond/src`:

```
$ BUCKETNAME=my-bucket  ## use your already existing S3 bucket name here
$ pond create ./pond/examples/hello-pond-src/ s3://$BUCKETNAME/hello-pond v1
```

You can examine the _raw_ contents of the newly created volume with 
the `pond` CLI:

```
$ pond versions s3://$BUCKETNAME/hello-pond
v1
$ pond list s3://$BUCKETNAME/hello-pond
              25 a -> 6c6e118152a67856.data @ 0
              32 b -> 6c6e118152a67856.data @ 25
              31 c -> 6c6e118152a67856.data @ 57
```

What this is showing that the 3 originial files have been backed into a single 
pond file named `13092866a26922a2.data`, with a `v1` label.

### Mount and Use the Volume

Now to use pond you don't access these S3 files directly, instead you use FUSE 
to mount them into a filesystem:

```
pond mount s3://$BUCKETNAME/hello-pond /tmp/hello-pond
```

And then use it like a normal filesystem:

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

Note one thing the output of the second shows is a couple of special files.


### Commit a new version

Once you're ready to save your changes, commit a new version by writing a name 
for the commit into the special `/tmp/hello-pond/.commit` file. The commit 
label can be any utf-8 string that's 64 characters or less:

```
$ echo "a fourth pond file" > /tmp/hello-pond/d
$ echo "v2" > /tmp/hello-pond/.commit
```

You should now see two versions of the example volume saved in object
storage:

```
$ pond versions s3://$BUCKETNAME/hello-pond
v1
v2
```
If you mount that version somewhere else, you'll be able to see the file 
we just wrote

```
$ pond mount s3://$BUCKETNAME/hello-pond /tmp/another-pond --version v2
$ cat /tmp/another-pond/d
a fourth pond file
```

### Interacting with snapshots

You can still mount and use the original version like nothing has changed:

```
$ umount /tmp/hello-pond
$ pond mount s3://$BUCKETNAME/hello-pond /tmp/hello-pond --version v1
$ cat /tmp/hello-pond/d
cat: /tmp/hello-pond/d: No such file or directory
```

### Unmounting pond

To get rid of our mounts, we cant just remove the directories, instead we must 
use the umount command:
```
$ umount /tmp/hello-pond
$ umount /tmp/another-pond
```

## Advanced Usage

### Auto unmounting

By default `user_allow_other` is unset in `/etc/fuse.conf`, restricting access 
to the mounted fuse to just you. If you want to enable `auto_umount` or allow
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


## Roadmap

Pond is under active development. We're currently working on:

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
