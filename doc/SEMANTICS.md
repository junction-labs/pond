# Pond behavior

## Staged vs. committed files

Staged files are temporary local files on disk. A staged file is a file that
you've created and haven't committed yet, or previously committed files that
you've modified. If you do not commit these files, your changes will be lost.

Committed files are files that live on S3. They are durable. You can view the
change history of a committed file by loading the Volume at each version it
existed in.

## Reading and writing files

Staged files are temporary local files on disk, so reading and writing to them
is exactly the same as if you were operating on a file in your local filesystem.
There is no file content caching done by Pond for staged files. Some operating
systems can utilize the kernel's page cache to optimize file reading performance
here however.

Committed files are slightly different. Reading from a committed file requires 
us to fetch the contents from S3. Exactly how much data we fetch from S3 depends
on a combination of things:

1. (FUSE) the request read buffer size (which itself is dependent on things like
hardware, filesystem block size, available memory)
2. configuration of Pond's cache (chunk size, cache size)
3. configuration of Pond's readahead

Writes to committed files are set to overwrite its contents. Opening a committed
file with write permissions will always truncate it to an empty file.

## Fetching from S3, caching and readaheads

TBD.

## Committing snapshots

Pond allows you to commit a snapshot of your dataset as a new version. The new
version is saved as a set of deltas on top of the base version. 

When you commit a snapshot, a copy of the deltas in the dataset at that point
in time is uploaded to S3. To perform consistent and deterministic commits,
we enforce that there cannot be open staged files at the start of the commit
process. The commit itself can take many seconds (or minutes) to complete,
especially if the deltas are large in size, so we relax the constraint on open
staged files after we finish aggregating the deltas. The upload to S3 afterwards
is asynchronous and does not block other Volume operations.
