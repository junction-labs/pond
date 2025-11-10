# Pond

Pond is an S3-backed filesystem that lets you work with plain old files while
storing data in S3. It lets you stop thinking about object storage and just do
things.

Pond is a snapshot-based distributed filesystem designed for write-once
read-many (WORM) workloads. When you run Pond locally, you see a snapshot of a
normal looking POSIX filesystem, anyone with access to the S3 bucket can load
and mount the same Pond snapshot. When making changes to a Pond filesystem, you
write to local disk; your changes aren't saved until you upload a new snapshot
and they don't affect what anyone else reads until they explicilty load the
updated snapshot.

Pond snapshots are optimized for reading. In the background, Pond does
prefetching and caching so your files feel like they're on your VM, even when
you need to work with huge files or millions of tiny ones.

### A Snapshot Based Filesystem

Pond is snapshot-based. Using Pond looks almost exactly like using any other
unix filesystem, but differs in that everything you write is ephemeral until you
explicitly save it. You can still read and write your own changes, but until you
commit a snapshot it won't get saved back to storage.

Using snapshots means that you never have to worry about accidentally changing
or overwriting data - the data in a running job can never change and even if you
commit a mistake, old versions of your volume are still available. Versioned
data means it's easy to make your analysis reproducible - you can guarantee
every run is using the same input data by using the same Pond snapshot.

### Designed for Object Storage

Pond is designed to work with object storage. Like a [lot][tpuf]
[of][warpstream] [other][neon] [smart people][slatedb] we believe that building
on object storage gives Pond the best possible scale and durability you can get
in the public cloud. However, it's not always easy to use object storage well.
For example, if you have to store a large number of small objects, the latency
and cost of object storage can become extremely painful.

Pond takes care all of that for you. When you commit a snapshot, it rewrites
your data so that it's easy to read back later. Every time you read from Pond,
it does request chunking, caching, prefetching, and readahead so you don't have
to think about how to lay out your data - you can just do things.

[tpuf]: https://turbopuffer.com
[warpstream]: https://warpstream.com
[neon]: https://neon.tech
[slatedb]: https://slatedb.io

## Installing

> [!NOTE]
> Pond is experimental and still alpha-quality software. While Pond is
  experimental, you'll need a working Rust toolichain to try it out. We
  recommend installing Rustup from [the official website](https://rustup.rs/).

Before installing Pond, make sure FUSE is set up on your operating system. The
instructions vary, but on most distros FUSE is available by default.

By default `user_allow_other` is unset in `/etc/fuse.conf`, restricting access 
to the mounted fuse to just you. If you want to enable `auto_umount` or allow
other users to access the mounted fuse, you must set `user_allow_other`.

Install Pond from source by cloning the git repo and running `cargo install`:

```
https://github.com/junction-labs/pond.git
cargo install --path ./pond/pond-fs
```

Verify that Pond is installed:

```
pond version
```


## Getting Started Locally

To try out Pond, install it from source on a Linux machine. If you're using
object storage, we recommend running Pond inside your cloud account, so you're
not uploading or downloading data over the public internet. To get started,
we'll use a volume backed by the local filesystem, so you can get started on
your laptop.

First, install Pond from source:

```
https://github.com/junction-labs/pond.git
cargo install --path ./pond/pond-fs
```

Once you've got Pond installed, you can create and save a volume from any
local directory. Pond will walk a local directory, pack it into its
internal format, and save it.

```
mkdir -p /tmp/hello-pond
pond create pond /tmp/hello-pond v1
```

You can examine the contents of the local volume with the `pond` CLI to
see what just got packed.

```
$ pond versions /tmp/hello-pond
v1
$ pond list /tmp/hello-pond
f    ./d47f401a6f82c8ef.data                             0      153 Cargo.lock
f    ./d47f401a6f82c8ef.data                           153      498 Cargo.toml
...
```

Now you can mount the volume we just created as a local filesystem:

```
$ mkdir /tmp/pond
$pond mount /tmp/hello-pond /tmp/pond
```

And then use it like a filesystem from another terminal:

```
$ $ ls /tmp/pond
Cargo.lock  Cargo.toml  fbs/  src/
$ grep -r pond /tmp/pond
grep: /tmp/pond/.clearcache: Operation not permitted
grep: /tmp/pond/.commit: Operation not permitted
/tmp/pond/Cargo.lock:name = "pond-core"
/tmp/pond/Cargo.toml:name = "pond"
/tmp/pond/src/metadata.rs:/// `VolumeMetadata` holds all of file and directory metadata for a pond
/tmp/pond/src/storage.rs:            .prefix(".pond")
/tmp/pond/src/storage.rs:            .prefix(".pond")
/tmp/pond/src/storage.rs:            .prefix(".pond")
```

To update the volume, write data into a file just as you normally would. Once
you're ready to save your changes, commit a new version by writing a name for
the commit into the special `/tmp/pond.commit` file. The commit label can be any
utf-8 string that's 64 characters or less:

```
$ echo "hi pond" > /tmp/pond/hi.txt
$ echo "v2" > /tmp/pond/.commit
```

You should now see two versions of the example volume saved in object
storage. If you mount that version somewhere else, you'll be able to
see the file we just wrote:

```
$ pond versions /tmp/hello-pond
v1
v2

$ mkdir /tmp/another-pond
$ pond mount /tmp/hello-pond /tmp/another-pond --version v2
$ cat /tmp/pond-example/hi.txt
hi pond
```

You can still mount and use the original version like nothing has changed:

```
$ pond mount /tmp/hello-pond /tmp/pond --version v1
$ cat /tmp/pond/hi.txt
cat: hi.txt: No such file or directory
```

## Getting started in AWS

Using Pond in AWS is just as easy as using it on your laptop. If you're on a VM
with instance credentials, Pond will automatically detect them. To explicitly
set your credentials, use the [standard AWS SDK environment
variables](https://docs.aws.amazon.com/sdkref/latest/guide/environment-variables.html).

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
