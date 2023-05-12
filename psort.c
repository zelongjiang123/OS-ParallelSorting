////////////////////////////////////////////////////////////////////////////////
// Main File:        psort.c
// This File:        psort.c
// Other Files:      none
// Semester:         CS 537 Spring 2023
// Instructor:       Shivaram
//
// Author:           Zelong Jiang, Dipaksi Attraya
// Email:            zjiang287@wisc.edu, attraya@wisc.edu
// CS Login:         zjiang, dipaksi
//
/////////////////////////// OTHER SOURCES OF HELP //////////////////////////////
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdbool.h>
#include <unistd.h>
#include <sys/wait.h>
#include <fcntl.h>
#include <sys/types.h>
#include <pthread.h>
#include <sys/time.h>

#define RECORD_SIZE 100

typedef struct{
    int key;
    int data[24];
}Record;  // a structure that stores the key and value of the record

int num_records; 

// swap two record elements
void swap(Record** a, Record** b) {
    Record* t = *a;
    *a = *b;
    *b = t;
}


// partition the records from l to h (inclusive)
// the elements that are smaller than pivot are on the left side of the pivot
// the elements that are greater than or equal to pivot are on the right side of the pivot
int partition(Record** records, int l, int h) {
    int pivot = records[h]->key;
    //printf("pivot %d\n", pivot);
    int i = (l - 1);
  
    for (int j = l; j <= h - 1; j++) {
        if (records[j]->key < pivot) {
            i++;
            swap(&records[i], &records[j]);
        }
    }
    swap(&records[i + 1], &records[h]);
    return (i + 1);  // return the position of the pivot
}

// sort the record from l to h (inclusive)
void quicksort(Record** records, int l, int h) {
    //printf("low: %d, high %d\n", l, h);
    if (l < h) {
        int pi = partition(records, l, h);
        quicksort(records, l, pi - 1);
        quicksort(records, pi + 1, h);
    }
} 


typedef struct{
   Record** records;
   int  l;
   int  h;
}thread_data;  //single thread sorting arguments

void *thread_function(void* args)  // thread sorting function
{
    thread_data * my_data = (thread_data *) args;
    quicksort(my_data->records, my_data->l, my_data->h);
    return NULL;
}

typedef struct{
   Record** records;
   int  l1;
   int l2;
   int  h;
}merge_thread_data; // thread merging function arguments

/*
 * merge the records,
 * the first half starts from l1
 * the second half starts from l2, end at h (inclusive)
 * */

void merge_in_thread(Record **records, int l1, int l2, int h)
{
    int index = 0, left = l1, right = l2;
    Record **temp_records = malloc((h - l1 + 1) * sizeof(Record*));
    while(left < l2 && right <= h)
    {
        if(records[left]->key <= records[right]->key)
	{
	    //printf("%d ", records[left].key);
	    temp_records[index++] = records[left++];
	}
        else
	{
            //printf("%d ", records[right].key);
            temp_records[index++] = records[right++];
        }

    }
     if(left == l2)
    {
        
        for(int i=0; i<index; i++)
            records[i+l1] = temp_records[i];
    }
    else
    {
        int j = h;
        for(int i=l2-1; i>=left; i--)
            records[j--] = records[i];
        for(int i=0; i<index; i++)
            records[i+l1] = temp_records[i];
    }
    
    // printf("\n");
}

void *merge_thread_function(void*args)   // thread merging function
{
    merge_thread_data * my_data = (merge_thread_data *) args;
    merge_in_thread(my_data->records, my_data->l1, my_data->l2, my_data->h);
    return NULL;
}

/*
 * create multiple threads to merge the data
 * each thread has length chunklength (the last thread may have fewer length)
 * */
void merge(Record **records, int chunklength)
{
   // printf("merge %d\n", chunklength);
    int numThreads = num_records / chunklength + 1;
    pthread_t threads[numThreads]; 
    merge_thread_data threads_args[numThreads];
    int start = 0;
    
    // create merge threads
    for (int i = 0; i < numThreads; i++) {
	if(start >= num_records)
        {
            numThreads--;
            break;
        }
        threads_args[i].records = records;
        threads_args[i].l1 = start;
        if(start + chunklength / 2 >= num_records)
        {
            numThreads--;
            break;
        }
            
        threads_args[i].l2 = start + chunklength / 2;
        if(start + chunklength - 1 >= num_records)
            threads_args[i].h = num_records - 1;
        else
            threads_args[i].h = start + chunklength - 1;
//	printf("thread %d, l1: %d, l2: %d, h: %d\n", i,threads_args[i].l1, threads_args[i].l2, threads_args[i].h);
        pthread_create(&threads[i], NULL, merge_thread_function, (void *)&threads_args[i]);
    
	 start += chunklength;

    }
    
    // wait for threads to finish
    for (int i = 0; i < numThreads; i++) {
        pthread_join(threads[i], NULL);
    }
}

int main(int argc, char **argv)
{
    if(argc != 4)  // check the argument count
    {
        fprintf(stderr, "Usage: ./psort input output 4");
        exit(1);
    }

    FILE *input_file = fopen(argv[1], "r");  // open the input file
    if(input_file == NULL)
    {
        fprintf(stderr, "Can't open input file %s!\n", argv[1]);
        exit(1);
    }
    // Get the size of the input file
    fseek(input_file, 0, SEEK_END);
    long file_size = ftell(input_file);
    fseek(input_file, 0, SEEK_SET);
    
    // Calculate the number of records in the file
    num_records = file_size / RECORD_SIZE;



    Record **records = malloc(num_records * sizeof(Record*));

    int num;
    // Read in each record from the input file
    for (int i = 0; i < num_records; i++) {
	records[i] = malloc(sizeof(Record));
        num = fread(&records[i]->key, sizeof(int), 1, input_file);
        if(!num)
        {
            fclose(input_file);
        }
        num = fread(records[i]->data, sizeof(int), 24, input_file);
        if(!num)
        {
            fclose(input_file);
        }
    }

    fclose(input_file);

    int numThreads = atoi(argv[3]); // get the number of threads
    if(numThreads > num_records)
        numThreads = num_records;
    int chunklength = num_records / numThreads; // divide the input records to chunks
    pthread_t threads[numThreads]; 
    thread_data threads_args[numThreads];

    int start = 0;
   
    // variables that calculate the time
    struct timeval tv; 
    struct timeval start_tv;

    gettimeofday(&start_tv, NULL); // start the timer

    double elapsed = 0.0;

    
    // create threads, within each thread, a chunk of records are sorted
    for (int i = 0; i < numThreads; i++) {
        threads_args[i].records = records;
        threads_args[i].l = start;
        if(i < numThreads - 1)
            threads_args[i].h = start + chunklength - 1;
        else // the last thread may have a bigger chunk
            threads_args[i].h = num_records - 1;
        start += chunklength;
        pthread_create(&threads[i], NULL, thread_function, (void *)&threads_args[i]);
    }

    // wait for threads to finish
    for (int i = 0; i < numThreads; i++) {
        pthread_join(threads[i], NULL);
    }

    //gettimeofday(&start_tv, NULL);
    chunklength *= 2;

    // merge the data
    while(chunklength < num_records * 2)
    {
        merge(records, chunklength);
        chunklength *= 2;
    }

    gettimeofday(&tv, NULL);  // end the timer
    elapsed = (tv.tv_sec - start_tv.tv_sec) + (tv.tv_usec - start_tv.tv_usec) / 1000000.0;
    printf("time elapsed: %f\n", elapsed);
    
   

    // output the records into the output file 
    FILE *output_file = fopen(argv[2], "w"); // open the output file
    if(output_file == NULL)
    {
        fprintf(stderr, "Can't open output file %s!\n", argv[2]);
        exit(1);
    }
    // write each record to the output file
    for (int i = 0; i < num_records; i++) {
        fwrite(&records[i]->key, sizeof(int), 1, output_file);
        fwrite(records[i]->data, sizeof(int), 24, output_file);
    }

    fsync(fileno(output_file));
    fclose(output_file);

    return 0;
}
