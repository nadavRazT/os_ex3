#include "MapReduceClient.h"
#include <pthread.h>
#include <algorithm>
#include "Barrier/Barrier.cpp"
#include "atomic"
#include "iostream"

#include "SampleClient.cpp"


typedef void *JobHandle;

//enum stage_t {
//    UNDEFINED_STAGE = 0, MAP_STAGE = 1, SHUFFLE_STAGE = 2, REDUCE_STAGE = 3
//};

//typedef struct {
//    stage_t stage;
//    float percentage;
//} JobState;

typedef struct {
    std::atomic<int> *atomic_counter;
    std::vector<std::pair<K2*, V2*>> ds2;
    std::vector<std::pair<K3*, V3*>> ds3;
    const InputVec *inputVector;
    const MapReduceClient *client;
    Barrier* barrier;
    pthread_mutex_t *mutex;
} ThreadContext;

typedef struct {
    std::vector<ThreadContext> thread_contexts;
    std::vector<pthread_t> threads;
    pthread_mutex_t mutex;
    JobState job_state;
    int num_threads;
} JobContext;


void emit2(K2 *key, V2 *value, void *context) {
    ThreadContext *thread_context = static_cast<ThreadContext*>(context);
    thread_context->ds2.push_back(std::pair<K2*, V2*>(key, value));
}

void emit3(K3 *key, V3 *value, void *context) {
    ThreadContext *thread_context = static_cast<ThreadContext*>(context);
    thread_context->ds3.push_back(std::pair<K3*, V3*>(key, value));
}

void *threadMapReduce(void* context)
{
    ThreadContext *thread_context = static_cast<ThreadContext*>(context);
    // map
    while(true)
    {
        std::cout << *(thread_context->atomic_counter)<<std::endl;

        pthread_mutex_lock(thread_context->mutex);
        if(*(thread_context->atomic_counter) == (thread_context->inputVector)->size() - 1)
        {
            pthread_mutex_unlock(thread_context->mutex);
            break;
        }
        int old_value = (*(thread_context->atomic_counter))++;
        const InputVec *input_vec = thread_context->inputVector;
        InputPair input_pair = input_vec->at(old_value);
        pthread_mutex_unlock(thread_context->mutex);
        thread_context->client->map(input_pair.first, input_pair.second, thread_context);
    }

    for(int i = 0; i < thread_context->ds2.size(); i++)
    {
        std::cout << ((thread_context->ds2.at(i)).second) << std::endl;
    }
    // sort
    std::sort((thread_context->ds2).begin(), (thread_context->ds2).end());
//    std::cout << thread_context->ds2 << std::endl;

    thread_context->barrier->barrier();
    // shuffle

    //reduce
}

JobHandle startMapReduceJob(const MapReduceClient &client,
                            const InputVec &inputVec, OutputVec &outputVec,
                            int multiThreadLevel) {
    pthread_t threads[multiThreadLevel];
    std::vector<ThreadContext> thread_contexts;
    ThreadContext thread_context_arr[multiThreadLevel];
    std::atomic<int> atomic_counter(0);
    Barrier barrier(multiThreadLevel);
    pthread_mutex_t mutex(PTHREAD_MUTEX_INITIALIZER);
    for (int i = 0; i < multiThreadLevel; ++i) {
        auto *ds2 = new std::vector<std::pair<K2*, V2*>>();
        auto *ds3 = new std::vector<std::pair<K3*, V3*>>();

        ThreadContext tc = {&atomic_counter, *ds2, *ds3, &inputVec, &client, &barrier, &mutex};
        thread_context_arr[i] = tc;
        thread_contexts.push_back(tc);
    }

    for (int i = 0; i < multiThreadLevel; ++i) {

        pthread_create(threads + i, NULL, threadMapReduce, thread_context_arr + i);
    }

    // wait for job to finish
//    while (true)
//    {
//       std::cout << "stil here" <<std::endl;
//    }
    // reduce K3 V3 from all threads
}

void waitForJob(JobHandle job){};

void getJobState(JobHandle job, JobState *state){};

void closeJobHandle(JobHandle job){};



