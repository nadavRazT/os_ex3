#include "MapReduceClient.h"
#include <pthread.h>
#include "Barrier/Barrier.h"

typedef void *JobHandle;

enum stage_t {
    UNDEFINED_STAGE = 0, MAP_STAGE = 1, SHUFFLE_STAGE = 2, REDUCE_STAGE = 3
};

typedef struct {
    stage_t stage;
    float percentage;
} JobState;

typedef struct {
    std::atomic<int> *atomic_counter;
    std::vector<std::pair<K2, V2>> ds2;
    std::vector<std::pair<K3, V3>> ds3;
    InputVec &inputVector;
    MapReduceClient &client;
    Barrier* barrier;
} ThreadContext;

typedef struct {
    ThreadContext[] thread_contexts;
    pthread_t[] threads;
    pthread_mutex_t mutex;
    JobState job_state;
    int num_threads;
} JobContext;


void emit2(K2 *key, V2 *value, void *context) {
    ThreadContext *thread_context = static_cast<ThreadContext>(context);
    thread_context->ds2.push_back(std::pair<K2, V2>(key, value));
}

void emit3(K3 *key, V3 *value, void *context) {
    ThreadContext *thread_context = static_cast<ThreadContext>(context);
    thread_context->ds3.push_back(std::pair<K3, V3>(key, value));
}

void threadMapReduce(void* context)
{
    ThreadContext *thread_context = static_cast<ThreadContext>(context);
    // map
    while(thread_context->atomic_counter != inputVec.size())
    {
        int old_value = (*(thread_context->atomic_counter))++;
        MapReduceClient &client = thread_context->client;
        InputVec input_vec = thread_context->inputVector;
        InputPair input_pair = input_vec[old_value];
        client.map(input_pair.first, input_pair.second, thread_context);
    }
    // sort
    std::sort((thread_context->ds2).begin(), (thread_context->ds2).end(), K2::operator<());
    std::cout<< thread_context->ds2 << std::endl;

    thread_context->barrier->barrier();
    // shuffle

    //reduce
}

JobHandle startMapReduceJob(const MapReduceClient &client,
                            const InputVec &inputVec, OutputVec &outputVec,
                            int multiThreadLevel) {
    pthread_t threads[MT_LEVEL];
    ThreadContext thread_contexts[MT_LEVEL];
    std::atomic<int> atomic_counter(0);
    Barrier barrier(multiThreadLevel);
    for (int i = 0; i < multiThreadLevel; ++i) {
        std::vector<std::pair<K2, V2>> ds2 = new std::vector<K2, V2>();
        std::vector<std::pair<K3, V3>> ds3 = new std::vector<K3, V3>();
        thread_contexts[i] = {&atomic_counter, &ds2, &ds3, &inputVec, &client, &barrier};
    }

    for (int i = 0; i < multiThreadLevel; ++i) {

        pthread_create(threads + i, NULL, threadMapReduce, thread_contexts + i);
    }

    // wait for job to finish

    // reduce K3 V3 from all threads
}

void waitForJob(JobHandle job);

void getJobState(JobHandle job, JobState *state);

void closeJobHandle(JobHandle job);



