# Examples

This directory contains example notebooks that demonstrate using Pond to run an image classification model. It shows off how easy it is to run a FUSE filesystem to load data from S3. It also benchmarks Pond against [Amazon's Mountpoint-S3 filesystem](https://github.com/awslabs/mountpoint-s3).

## Notebooks

The notebooks use [PyTorch's DataLoader](https://docs.pytorch.org/docs/stable/data.html#torch.utils.data.DataLoader) to load the [18.2GiB 2017 train images dataset](http://images.cocodataset.org/zips/train2017.zip) from [COCO](https://cocodataset.org/). The dataset is made up of 118287 jpg files, the median file in it being ~152KiB.

The notebooks are run against different configurations of Pond and Mountpoint, and run using different access patterns, to compare ballpark performance.

Each notebook can be broken down into the following:

1. Mount a filesystem to run against
2. Set up a static seed for more determinism (so random access actually uses the same "random" order across notebooks)
3. Load the data from the mounted filesystem using a random access pattern
4. Perform image transformations (using `torchvision.transforms`)
5. Dump the resulting transformed image into the abyss (`/dev/null`), alternatively this is where you'd feed it into the model for training.
6. (3), (4), and (5) but using a serial access pattern

- **[mountpoint.ipynb](./mountpoint.ipynb)** - configured to use the AWS Elastic Block Store (EBS) general purpose (gp3) volume attached to the EC2 instance as local cache
- **[mountpoint-nocache.ipynb](./mountpoint-nocache.ipynb)**  - configured to use no cache at all
- **[pond-2GiB.ipynb](./pond-2GiB.ipynb)** - configured with a 2GiB in-memory cache, chunks of 1MiB and readaheads of 4MiB
- **[pond-19GiB.ipynb](./pond-19GiB.ipynb)** - configured with a 19GiB in-memory cache, chunks of 8MiB and readaheads of 32MiB

## Results

| FUSE | Access Pattern | Cache | Empty Cache Time | Speedup | Warm Cache Time | Speedup |
| :--: | :------------: | :--------: | ---------------: | :------: | --------------: | :-------: |
| mountpoint | Random access  | None | ~61m (3686.9s) | 0% | ~61m (3697.2s) | 0% |
| mountpoint | Random access  | EBS volume (gp3) | ~55m (3214.7s) | 12.8% |  ~25m (1498.0s) | 59.5% |
| pond | Random access  | 2GiB inmemory | ~70m (4205.0s) | -14.1% | ~60m (3618.8s) | 2.1% |
| pond | Random access  | 19GiB inmemory | ~5m (309.9s) | 91.6% | ~4m (231.2s) | 93.7% |
| mountpoint | Serial access  | None | ~59m (3531.9s) | 0% | ~58m (3487.7s) | 0% |
| mountpoint | Serial access  | EBS volume (gp3) | ~52m (3130.3s) | 11.4% | ~28m (1667.0s) | 52.2% |
| pond | Serial access  | 2GiB inmemory | ~5m (287.3s) | 91.9% | ~5m (300.1s) | 91.4% |
| pond | Serial access  | 19GiB inmemory | ~4m (226.3s) | 93.6% | ~4m (227.3s) | 93.5% |


> [!NOTE]
> The speedups are calculated using mountpoint with no cache as the baseline.

## Notes
- Pond mounts were built at commit [6b7b2076bde44bb4da8535b7c439d48c742c6bfd](https://github.com/junction-labs/pond/commit/6b7b2076bde44bb4da8535b7c439d48c742c6bfd).
- The [Mountpoint](https://github.com/awslabs/mountpoint-s3) version we used is `1.19.0`.
- All notebooks were run on an AWS EC2 `t4g.2xlarge` instance.

These are not strict benchmarks, just ballpark performance numbers!
