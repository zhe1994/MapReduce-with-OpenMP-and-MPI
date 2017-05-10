# MapReduce-with-OpenMP-and-MPI
Implemented MapReduce using OpenMP and MPI  

<img src="https://drive.google.com/uc?id=0B-N_s7wEHZ8LbFlNcnEzaWNtNFE" width="400">

## Overview
This project is intended to do map reduce on multiple nodes using MPI for communication, and for each node using OpenMP to read, map, and reduce input text on multiple threads parallelly.

## How to Use
Input text files should be stored in right directory (./ranknumber/), for example, files to be read by rank0 should be stored in ./0/ .  
Input text files should have valid name as 0number.txt, for example, 01.txt, 09.txt, 010.txt, 0101.txt.   
The work will be divided evenly to all the nodes and work per threads will be divided dynamically to all threads on this node.    

To compile, use:    
~~~~
mpicxx -fopenmp mapreduce_ver1.0.cpp -o mapreduce_ver1.0    
~~~~
To run on different nodes, edit statement in mapreduce_ver1.0.sub:    
~~~~
mpiexec -n /*number of nodes*/ ./mapreduce_ver1.0   
~~~~
or: 
~~~~
mpirun -np /*number of nodes*/ ./mapreduce_ver1.0   
~~~~

To set number of threads on each node, change the value in mapreduce_ver1.0.cpp:   
~~~~
#define NUMOFTHREADS 4 //set number of threads    
/*Set the Macros below to 0 0 1 1 if the number of threads is 2, leave them alone if it's 1*/   
#define MIN_READER 0 //set beginning tid for reader or writer   
#define MAX_READER 1 //set end tid for reader or writer   
#define MIN_MAPPER 2 //set beginning tid for mapper or reducer    
#define MAX_MAPPER 3 //set end tid for mapper or reducer    
~~~~

To set worksize, change the value in mapreduce_ver1.0.cpp:    
~~~~
#define WORKSIZE 80   
~~~~
