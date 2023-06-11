## Project overview:

Parallel sorting using threads is a technique where a sorting algorithm is executed simultaneously on multiple threads to improve performance and take advantage of parallel processing capabilities. This approach can be beneficial for large datasets or computationally intensive sorting tasks.

Parallel sort (`psort`) will take three command-line arguments.

input: The input file to read records for sort

output: The output file where records will be written after sort

numThreads: Number of threads that shall perform the sort operation.

prompt> ./psort input output 4

The input file will consist of records; within each record is a key. The key is the first four bytes of the record. The records are fixed-size, and are each 100 bytes (which includes the key). A successful sort will read all the records into memory from the input file, sort them by key, and then write out the “key, value records” to output file. The exact implementation of the parallel sort operation is left to you, the only constraint we impose is that your code should be a parallel algorithm.

## Main files:

psort.c

## Main coding language:

C

## Attributions:

psort.c was written by me (Zelong Jiang) and my partner Dipaksi Attraya.
Performance Grpah.jpg was made by my partner Dipaksi Attraya.
