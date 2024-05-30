# Benchmarking

Periodically we will be publishing results of benchmarking the Synapse Python Client
compared to directly working with AWS S3. The purpose of these benchmarks is to make
data driven decisions on where to spend time optimizing the client. Additionally, it
will give us a way to measure the impact of changes to the client.

## Results

### 05/10/2024: Uploading files to Synapse
These benchmarking results were collected due to the following changes:
- The upload algorithm for the Synapseutils `syncToSynapse` being re-written to take
advantage of the new AsyncIO upload algorithm for individual files.
- An updated limit on concurrent file transfers to match `max_threads * 2`


| Test                | Total Transfer Size | Synapseutils | OOP Models Interface | syn.Store() | S3 Sync CLI |
|---------------------|---------------------|--------------|----------------------|-------------|-------------|
| 10 File/10GiB ea    | 100GiB              | 1656.64s     | 1656.77s             | 1674.63s    | 1519.75s    |
| 1 File/10GiB ea     | 10GiB               | 166.83s      | 166.41s              | 167.21      | 149.55s     |
| 10 File/1GiB ea     | 10GiB               | 168.74s      | 167.15s              | 184.78s     | 166.39s     |
| 100 File/100 MiB ea | 10GiB               | 158.98       | 125.98s              | 293.07s     | 162.57s     |
| 10 File/100 MiB ea  | 1GiB                | 16.55s       | 14.37s               | 29.23s      | 19.18s      |
| 100 File/10 MiB ea  | 1GiB                | 15.92s       | 15.49s               | 129.90s     | 18.66s      |
| 1000 File/1 MiB ea  | 1GiB                | 135.77s      | 137.15s              | 1021.32s    | 26.03s      |

#### A high level overview of the differences between each of the upload methods:
- **OOP Models Interface:** Uploads all files and 8MB chunks of each file in parallel using a new upload algorithm
- **Synapseutils:** Uploads all files and 8MB chunks of each file in parallel using a new upload algorithm
- **syn.Store():** Uploads files sequentally, but 8MB chunks in parallel using a new upload algorithm
- **S3 Sync CLI:** Executing the `aws s3 sync` command through Python `subprocess.run()`

### 04/01/2024: Uploading files to Synapse
These benchmarking results bring together some important updates to the Upload logic. It
has been re-written to bring a focus to concurrent file uploads and more effecient use
of available threads. As a result of this change it is not reccommended to
increase `max_threads` manually. Based on the available CPU cores this python package
will use `multiprocessing.cpu_count() + 4`. For this testing the default thread size
for the machine testing took place on was `6`.

The results were created on a `t3a.micro` EC2 instance with a 200GB disk size running
in us-east-1. The script that was run can be found in `docs/scripts/uploadBenchmark.py`.


#### Some insights:
- The use of the
[Object-Orientated interfaces/Models Interface](../reference/oop/models.md) results in
the best performance out of the box and will scale based on the hardware it's run on.
- Increasing the number of threads did increase performance for the previous upload
logic and Synapseutils functionality. It may also increase performance for new uploads,
however, it was not tested again. Increasing the number of threads in use also has
inconsistent stability as found in previous benchmarks.
- The new upload algorithm follows a similar pattern to the S3 client upload times.

| Test                | Total Transfer Size | OOP Models Interface | syn.Store(), New Upload | S3 Sync CLI | syn.Store(), Old Upload | Synapseutils | Synapseutils (25 Threads) | syn.Store(), Old Upload (25 Threads) |
|---------------------|---------------------|----------------------|-------------------------|-------------|-------------------------|--------------|---------------------------|--------------------------------------|
| 10 File/10GiB ea    | 100GiB              | 1652.61s             | 1680.27s                | 1515.31s    | 2174.65s                | 2909.62s     | 1658.34s                  | 1687.17s                             |
| 1 File/10GiB ea     | 10GiB               | 168.39s              | 167s                    | 152.35s     | 223s                    | 255.99s      | 169s                      | 166s                                 |
| 10 File/1GiB ea     | 10GiB               | 168.78s              | 172.48s                 | 155.52s     | 224.59s                 | 291.72s      | 167.9s                    | 175.99s                              |
| 100 File/100 MiB ea | 10GiB               | 124.14s              | 248.57s                 | 150.46s     | 320.50s                 | 227.75s      | 170.82s                   | 294.69s                              |
| 10 File/100 MiB ea  | 1GiB                | 15.13s               | 28.38s                  | 18.14s      | 33.74s                  | 26.64s       | 17.69s                    | 32.80s                               |
| 100 File/10 MiB ea  | 1GiB                | 19.24s               | 141.23s                 | 19.59s      | 139.31s                 | 48.50s       | 18.34s                    | 138.14s                              |
| 1000 File/1 MiB ea  | 1GiB                | 152.65s              | 1044.42s                | 25.03s      | 1101.90s                | 340.07s      | 100.94s                   | 1106.40s                             |

#### A high level overview of the differences between each of the upload methods:
- **OOP Models Interface:** Uploads all files and 8MB chunks of each file in parallel using a new upload algorithm
- **Synapseutils:** Uploads all files in parallel and 8MB chunks of each file in parallel using the old upload algorithm
- **syn.Store(), New Upload:** Uploads files sequentally, but 8MB chunks in parallel using a new upload algorithm
- **syn.Store(), Old Upload:** Uploads files sequentally, but 8MB chunks in parallel using the old upload algorithm

### 12/12/2023: Downloading files from Synapse

The results were created on a `t3a.micro` EC2 instance with a 200GB disk size running in us-east-1. The script that was run can be found in `docs/scripts/downloadBenchmark.py` and `docs/scripts/uploadTestFiles.py`.

During this download test I tried various thread counts to see what performance looked like at different levels. What I found was that going over the default count of threads during download of large files (10GB and over) led to signficantly unstable performance. The client would often crash or hang during execution. As a result the general reccomendation is as follows:

- For files over 1GB use the default number of threads: `multiprocessing.cpu_count() + 4`
- For a large number of files 1GB and under 40-50 threads worked best

| Test                      | Thread Count | Synapseutils Sync | syn.getChildren + syn.get | S3 Sync  | Per file size |
|---------------------------|--------------|-------------------|---------------------------|----------|---------------|
| 25 Files 1MB total size   | 40           | 1.30s             | 5.48s                     | 1.49s    | 40KB          |
| 775 Files 10MB total size | 40           | 19.17s            | 161.46s                   | 12.02s   | 12.9KB        |
| 10 Files 1GB total size   | 40           | 14.74s            | 21.91s                    | 11.72s   | 100MB         |
| 10 Files 100GB total size | 6            | 3859.66s          | 2006.53s                  | 1023.57s | 10GB          |
| 10 Files 100GB total size | 40           | Wouldn't complete | Wouldn't complete         | N/A      | 10GB          |

### 12/06/2023: Uploading files to Synapse, Varying thread count, 5 annotations per file

The results were created on a `t3a.micro` EC2 instance with a 200GB disk size running in us-east-1. The script that was run can be found in `docs/scripts`. The time to create the files on disk is not included.

This test includes adding 5 annotations to each file, a Text, Integer, Floating Point, Boolean, and Date.

S3 was not benchmarked again.

As a result of these tests the sweet spot for thread count is around 50 threads. It is not recommended to go over 50 threads as it resulted in signficant instability in the client.

| Test                      | Thread Count | Synapseutils Sync | os.walk + syn.store | Per file size |
|---------------------------|--------------|-------------------|---------------------|---------------|
| 25 Files 1MB total size   | 6            | 10.75s            | 10.96s              | 40KB          |
| 25 Files 1MB total size   | 25           | 6.79s             | 11.31s              | 40KB          |
| 25 Files 1MB total size   | 50           | 6.05s             | 10.90s              | 40KB          |
| 25 Files 1MB total size   | 100          | 6.14s             | 10.89s              | 40KB          |
| 775 Files 10MB total size | 6            | 268.33s           | 298.12s             | 12.9KB        |
| 775 Files 10MB total size | 25           | 162.63s           | 305.93s             | 12.9KB        |
| 775 Files 10MB total size | 50           | 86.46s            | 304.40s             | 12.9KB        |
| 775 Files 10MB total size | 100          | 85.55s            | 304.71s             | 12.9KB        |
| 10 Files 1GB total size   | 6            | 27.17s            | 36.25s              | 100MB         |
| 10 Files 1GB total size   | 25           | 22.26s            | 12.77s              | 100MB         |
| 10 Files 1GB total size   | 50           | 22.24s            | 12.26s              | 100MB         |
| 10 Files 1GB total size   | 100          | Wouldn't complete | Wouldn't complete   | 100MB         |

### 11/14/2023: Uploading files to Synapse, Default thread count

The results were created on a `t3a.micro` EC2 instance with a 200GB disk size running in us-east-1.
The script that was run can be found in `docs/scripts`. The time to create the files on disk is not included.

This test uses the default number of threads in the client: `multiprocessing.cpu_count() + 4`

| Test                      | Synapseutils Sync | os.walk + syn.store | S3 Sync | Per file size |
|---------------------------|-------------------|---------------------|---------|---------------|
| 25 Files 1MB total size   | 10.43s            | 8.99s               | 1.83s   | 40KB          |
| 775 Files 10MB total size | 243.57s           | 257.27s             | 7.64s   | 12.9KB        |
| 10 Files 1GB total size   | 27.18s            | 33.73s              | 16.31s  | 100MB         |
| 10 Files 100GB total size | 3211s             | 3047s               | 3245s   | 10GB          |
