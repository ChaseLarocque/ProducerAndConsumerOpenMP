# Producer & Consumer Using OpenMP For AUCSC 450
### AUCSC 450 - Parallel and Distributed Computing At the University of Alberta, Augustana Campus.

Program will simulate the Producer/Consumer problem present in message passing
using OpenMP. File initiates an array of queues, with queue belonging to a
thread. Then, each thread will read a file in the CURRENT DIRECTORY
and send each line to a random thread's queue. We will then tokenize
the lines and print to stdout. 
