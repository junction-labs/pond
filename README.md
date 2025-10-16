# Pond

Pond is a snapshot based FUSE filesystem that stores data in object storage.
Pond intelligently compacts and caches your data so that you can work with
terabytes of data or millions of small files without worrying about exactly how
your dataset is formatted.

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

Before installing, Pond make sure FUSE is set up on your operating system. The
instructions vary, but on most distros FUSE is available by default.

Install Pond from source by cloning the git repo and running `cargo install`:

```
https://github.com/junction-labs/pond.git
cargo install --path ./pond/pond-fuse
```

Verify that Pond is installed:

```
pond version
```


## Getting Started

To try out Pond, install it from source on a Linux machine. If you're using
object storage, we recommend running Pond inside your cloud account, so you're
not uploading or downloading data over the public internet. To get started,
we'll use a volume backed by the local filesystem, so you can get started on
your laptop.

First, install Pond from source:

```
https://github.com/junction-labs/pond.git
cargo install --path ./pond/pond-fuse
```

Once you've got Pond installed, you can create and save a volume from any
local directory. Pond will walk a local directory, pack it into its
internal format, and save it.

```
mkdir -p /tmp/hello-pond
pond pack pond/pond-core /tmp/hello-pond v1
```

You can examine the contents of the local volume with the `pond` CLI to
see what just got packed.

```
$ pond list /tmp/hello-pond
$ pond dump /tmp/hello-pond
```

Now you can mount the volume we just created as a local filesystem:

```
mkdir /tmp/pond
pond mount /tmp/hello-pond /tmp/pond
ls /tmp/pond/
grep "mount the volume" /tmp/pond-example/README.md
```

To update the volume, write data into a file just you normally would. Once
you're ready to save your changes, commit a new version by writing a name
for the commit into the special `/tmp/pond.commit` file. The commit label
can be any utf-8 string that's 64 characters or less:

```
echo "hi pond" > /tmp/pond/hi.txt
echo "v2" > /tmp/pond/.commit
```

You should now see two versions of the example volume saved in object
storage. If you mount that version somewhere else, you'll be able to
see the file we just wrote:

```
$ pond list /tmp/hello-pond
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
