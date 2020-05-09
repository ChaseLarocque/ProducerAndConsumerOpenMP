/*
Chase Larocque
AUCSC 450 - Assignment 3
December 9th 2019

File will simulate the Producer/Consumer problem present in message passing
using OpenMP. File initiates an array of queues, with queue belonging to a
thread. Then, each thread will read a file in the CURRENT DIRECTORY
and send each line to a random thread's queue. We will then tokenize
the lines and print to stdout. 

NOTE: The queue implementation was taken from https://www.geeksforgeeks.org/queue-linked-list-implementation/,
and edited to fit my needs. 

File Contains:
struct QNode -> Linked list node
struct Queue -> Queue to store nodes
newNode -> function to create a new link list node with given string
createQueue -> Function to allocate space for queue and initialize queue's lock
enQueue -> Function to enqueue to a thread's queue with a given string
deQueue -> Function to dequeue a thread's queue
tryReceive -> Function to try and dequeue a thread's queue and token string if any node dequeued
sendMessages -> Function to send each line of a file to a random thread
done -> termination condition checking
*/
#include <stdio.h> 
#include <stdlib.h> 
#include <string.h>
#include <omp.h>
#include <unistd.h>
#include <dirent.h> 

//Function prototypes=============

struct QNode* newNode(char* newLine);
struct Queue* createQueue();
struct QNode* newNode(char* newLine);
void enQueue(struct Queue* q, char newString[]);
struct QNode* deQueue(struct Queue* q);
void tryReceive(struct Queue* queue, int myRank);
void sendMessages(struct Queue* arrayOfQueues[], int numThreads, char* fileName, int myRank);
int done(struct Queue* q, int doneSending, int numThreads);

//Structs for queue and queue node=======
// A linked list (LL) node to store a queue entry 
struct QNode { 
    char* line;//char line[100];
    struct QNode* next; 
}; 
  
// The queue, front stores the front node of LL and rear stores the 
// last node of LL 
struct Queue { 
    struct QNode *front, *rear; 
    omp_lock_t lock;
    int enqueued;
    int dequeued;
}; 

// A utility function to create a new linked list node. 
struct QNode* newNode(char* newLine) { 
    struct QNode* temp = malloc(sizeof(struct QNode));
    temp->line = malloc(strlen (newLine) + 1);
    strcpy(temp->line, newLine);
    temp -> next = NULL; 
    return temp; 
} //newNode
  
// A utility function to create an empty queue 
struct Queue* createQueue() { 
    struct Queue* q = malloc(sizeof(struct Queue)); 
    q -> enqueued = q -> dequeued = 0;
    omp_init_lock(&q->lock);
    q -> front = q -> rear = NULL; 
    return q; 
}//createQueue
  
//enQueue to add a new line of text into the queue 
void enQueue(struct Queue* q, char newString[]) { 
    // Create a new LL node 
    struct QNode* temp = newNode(newString);
  
    // If queue is empty, then new node is front and rear both 
    if (q->rear == NULL) { 
        q->front = q->rear = temp; 
        q->enqueued++;
        return; 
    } //if
    
    //since the function is already locked,
    //there no need to make sure this is locked too. 
    q->enqueued++;
    // Add the new node at the end of queue and change rear 
    q->rear->next = temp; 
    q->rear = temp; 
}//enQueue
  
// Function to remove a key from given queue q 
struct QNode* deQueue(struct Queue* q) { 
    // If queue is empty, return NULL. 
    if (q->front == NULL){
        return NULL; 
    }
  
    // Store previous front and move front one node ahead 
    struct QNode* temp = q->front; 
    //free(temp); 

    //since only the owner can dequeue it's queue, there's
    //no need to make this critical
    q->dequeued++;
  
    q->front = q->front->next; 
  
    // If front becomes NULL, then change rear also as NULL 
    if (q->front == NULL) 
        q->rear = NULL; 
    return temp; 
}//deQueue

/**
tryReceive(struct Queue*, int) -> Void

Function will look into the queue and dequeue a node if possible. If it does dequeue a node, 
function will handle tokenizing the string and printing each token to 
stdout
*/
void tryReceive(struct Queue* queue, int myRank) {
    int queueSize;
    char* savePointer;
    char* token;


    //queue->dequeued can't be updated until later in this function, but queue->enqueued
    //can be updated at any point. This will not be a problem since if 
    //queue->enqueued is updated after this expression, we will actually be
    //working with a queueSize that's smaller than the actual queue size. 
    //The issues are small and will not cause a problem, but might cause
    //some inefficiences. The problems will arise if:
    //-queueSize = 0 but the actual queue has 1 element. In this case, 
    //we will just return, but we will miss the dequeue. This isn't a big problem
    //as we will find it later. 
    //-queueSize = 1 but the actual queue has 2 element. In this case, 
    //we will get the lock even though we don't need it. This again isn't a 
    //problem, just a little inefficient. 
    //-queueSize = 2 and above, it's no issue.
    //Therefore, no atomic or critical needed.
    
    queueSize = queue->enqueued - queue->dequeued;
    struct QNode* deQueuedNode;


    if (queueSize == 0) 
        return;

    else if (queueSize == 1) {
        omp_set_lock(&queue->lock);
        deQueuedNode = deQueue(queue);  
        omp_unset_lock(&queue->lock);
    } else
        deQueuedNode = deQueue(queue);

    token = strtok_r(deQueuedNode->line, " ", &savePointer);
    
    while(token != 0){
        printf("%s\n", token);
        token = strtok_r(NULL, " ", &savePointer);
    }//while
        
}//tryReceive

/**
sendMessages(struct Queue*, int, char*, int) -> void
Function opens the file name passed to it and sends each line of the file to a random 
thread. Function will call tryReceive after each line is sent to try and dequeue
from it's own queue. 

*/
void sendMessages(struct Queue* arrayOfQueues[], int numThreads, char* fileName, int myRank) {
    //open file and read a line at a time. 
    
    FILE* file = fopen(fileName, "r");

    if (file == NULL){
        fprintf(stderr, "Could not open current directory\n");
        exit(EXIT_FAILURE);
    }//if

    char lineToSend[1000];

    //while loop will send a line to a random thread, then check to see if it can read a message
    while (fgets(lineToSend, sizeof(lineToSend), file)) {
        //to remove new line from line
        char *position;
        if ((position=strchr(lineToSend, '\n')) != NULL)
            *position = '\0';

        int destinationThread = random() % numThreads;
        struct Queue* destinationQueue = arrayOfQueues[destinationThread];
        omp_set_lock(&destinationQueue->lock);
        enQueue(destinationQueue, lineToSend);
        omp_unset_lock(&destinationQueue->lock); 

        //check to see if there's anything in it's queue before heading onto a new line in the file               
        tryReceive(arrayOfQueues[myRank], myRank); 
    }//while
}//sendMessages

/**
done(struct Queue*, int, int) -> int

Function checks the queue size for the thread it's in and the shared doneSending variable. 
If this thread's queueSize == 0 and doneSending == numThreads,
then this thread will return 1 and this thread's execution will end. 

*/
int done(struct Queue* q, int doneSending, int numThreads) {
    int queueSize;
    /*
    This does not need to be atomic. My explanation why starts with the fact that dequeued can only be updated by
    the owner thread. This function will be called by a thread and the queue for that thread will be analyzed,
    meaning that this function can not happen at the same time that a queue will be dequeueing. This means that 
    it is not possible for queueSize to actually be smaller than what it's supposed to be. It is, however possible for 
    a thread to enqueue to a thread's queue once queueSize has already been calculated (eg. if thread 0 sends a message to thread 1
    and thread 1 is in this function and has already calculated queueSize the queueSize variable could be 0, but the queue 
    size could be actually 1). This will not cause a problem, since if this happens, that means that it's not 
    possible for doneSending(the argument in this function) == numThreads since we still have a thread sending messages. 

    Even if the message that thread 1 sends  is it's last, we're calling this function with the doneSending variable 
    one less than what it is once threads 1 is done. 
    That means that we will return 0 and we will try and receive again. In conclusion, we don't need to make
    this calculation atomic/critical, since if we do end up getting a queueSize thats invalid, we will have a doneSending value
    that will not equal numThreads, and we will still read the message. 

    eg: if thread 0 is running done and it calculates queueSize to be zero at this line but thread 1 sends a message
    to thread 0 after queueSize has been calculated, we will have queueSize = 0, but we will actually have 1 element in the
    queue. If this happened queueSize does == 0, since at the time of this function call, thread 1 will still be active 
    and the done Sending argument of this function will reflect this. 
    */
    queueSize = q-> enqueued - q-> dequeued;
    
    if (queueSize == 0 && doneSending == numThreads){
        return 1;
    }else 
        return 0;
} //done


/**
main(int, char*) -> int
Main function handles checking the arguments and making sure they're in the right range. 
Function will then count the number of files in the current directory and will
start forking the threads. Each thread will look at a file in the directory (in a round
robin scheme) and if it's a .txt file, will go on and start sending each line to 
a random thread. This function will also handle calling the tryReceive function
after the thread has sent all of it's file so we can keep getting the lines
that have been sent to threads that are finished sending. 
*/
int main(int argc, char* argv[]){
    int doneSending = 0;
    int fileCount = 0;

    if (argc != 2){
        fprintf(stderr, "Incorrect Number Of Arguments. Only Argument Should Be Number of Threads!\n");
        fprintf(stderr, "./<Program_Name> <Number of Threads>\n");
        exit(EXIT_FAILURE);
    }//if

    int numThreads = strtol(argv[1], NULL, 10);
    
    if (numThreads < 0){
        fprintf(stderr, "Threads Must Be Greater Than 0!\n");
        exit(EXIT_FAILURE);
    }//if

    //==================Code to count number of files in directory
    struct dirent *de;
    DIR *dr = opendir("."); 
    // opendir returns NULL if couldn't open directory 
    if (dr == NULL){
        fprintf(stderr, "Could not open current directory\n"); 
        return 0; 
    }//if
    while ((de = readdir(dr)) != NULL){ 
        fileCount++;
    }//while
    closedir(dr);
    //==================

    //new struct to open directory in order to actually read the files
    struct dirent *de2;
    DIR *dr2 = opendir("."); 
    if (dr2 == NULL){ 
        fprintf(stderr, "Could not open current directory\n"); 
        return 0; 
    }//if

    //allocate space for arrayOfQueues
    struct Queue** arrayOfQueues = malloc(numThreads * sizeof (struct Queue) );


    # pragma omp parallel num_threads(numThreads) private(de2) shared(doneSending)
    {
    int myRank = omp_get_thread_num();
    arrayOfQueues[myRank] = createQueue();

    # pragma omp barrier //we need to make sure all threads have message queues before we continue

    //threads will open all files ending with .txt in a round robin fashion.
    //NOTE: it is possible that not all threads will get equal work, if not all files in a
    //directory are .txt files. 
    # pragma omp for schedule(static, 1)
    for(int i = 0; i < fileCount; i++){
        if ((de2 = readdir(dr2)) != NULL){
            char* dot = strrchr(de2->d_name, '.');
            if (dot && !strcmp(dot, ".txt")){
                sendMessages(arrayOfQueues, numThreads, de2->d_name, myRank); //contains the call to tryReceive
            }//if
        }//if
    }//for

    #pragma omp atomic
    doneSending++;
   
    //continue checking for messages until all threads are done sending
    while (!done(arrayOfQueues[myRank], doneSending, numThreads))
        tryReceive(arrayOfQueues[myRank], myRank);
    }//parallel
    return 0; 
}//main














