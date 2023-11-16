*****************
Benchmarking
*****************

Periodically we will be publishing results of benchmarking the Synapse Python Client
compared to directly working with AWS S3. The purpose of these benchmarks is to make
data driven decisions on where to spend time optimizing the client. Additionally, it will
give us a way to measure the impact of changes to the client.

===================
Results
===================


11/14/2023
==========================
The results were created on a `t3a.micro` EC2 instance with a 200GB disk size running in us-east-1.
The script that was run can be found in `docs/scripts`. The time to create the files on disk is not included.


+---------------------------+-------------------+---------------------+---------+---------------+
| Test                      | Synapseutils Sync | os.walk + syn.store | S3 Sync | Per file size |
+===========================+===================+=====================+=========+===============+
| 25 Files 1MB total size   | 10.43s            | 8.99s               | 1.83s   | 40KB          |
+---------------------------+-------------------+---------------------+---------+---------------+
| 775 Files 10MB total size | 243.57s           | 257.27s             | 7.64s   | 12.9KB        |
+---------------------------+-------------------+---------------------+---------+---------------+
| 10 Files 1GB total size   | 27.18s            | 33.73s              | 16.31s  | 100MB         |
+---------------------------+-------------------+---------------------+---------+---------------+
| 10 Files 100GB total size | 3211s             | 3047s               | 3245s   | 10GB          |
+---------------------------+-------------------+---------------------+---------+---------------+
