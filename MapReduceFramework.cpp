#include "MapReduceClient.h"
#include <pthread.h>
#include <algorithm>
#include "Barrier/Barrier.cpp"
#include "atomic"
#include "iostream"

typedef void *JobHandle;
//typedef struct JobContext;

enum stage_t {
    UNDEFINED_STAGE = 0, MAP_STAGE = 1, SHUFFLE_STAGE = 2, REDUCE_STAGE = 3
};

typedef struct {
    stage_t stage;
    float percentage;
} JobState;

typedef struct {
    std::vector<std::pair<K2 *, V2 *>> *ds2;
    std::vector<std::pair<K3 *, V3 *>> *ds3;
    JobHandle job_context;
    int threadId;
} ThreadContext;

typedef struct {
    std::vector<ThreadContext *> *thread_contexts;
//    std::vector<pthread_t> threads;
    JobState job_state;
    int num_threads;
    const InputVec *inputVector;
    const MapReduceClient *client;
    Barrier *barrier;
    pthread_mutex_t *mutex;
    std::atomic<int> *atomic_counter;
} JobContext;


void emit2(K2 *key, V2 *value, void *context) {
    ThreadContext *thread_context = static_cast<ThreadContext *>(context);
    thread_context->ds2->push_back(std::pair<K2 *, V2 *>(key, value));
}

void emit3(K3 *key, V3 *value, void *context) {
    ThreadContext *thread_context = static_cast<ThreadContext *>(context);
    thread_context->ds3->push_back(std::pair<K3 *, V3 *>(key, value));
}


K2 *find_next_key(JobContext *context) {
    if (context->thread_contexts->size() == 0) {
        return nullptr;
    }

    K2 *min = context->thread_contexts->at(0)->ds2->at(0).first;
    for (int i = 0; i < context->num_threads; i++) {
        if (context->thread_contexts->at(i)->ds2->size() > 0 && context->thread_contexts->at(i)->ds2->at(0)
                                                                        .first < min) {
            min = context->thread_contexts->at(i)->ds2->at(0).first;
        }
    }
    return min;
}

bool is_all_empty(JobContext *context) {
    for (int i = 0; i < context->num_threads; i++) {
        if (context->thread_contexts->at(i)->ds2->size() > 0) {
            return false;
        }
    }
    return true;
}


void map_helper(ThreadContext *thread_context, JobContext *job_context) {
    // map
    while (true) {
        pthread_mutex_lock(job_context->mutex);
        if (*(job_context->atomic_counter) == (job_context->inputVector)->size()) {
            pthread_mutex_unlock(job_context->mutex);
            break;
        }
        std::cout << "thread " << thread_context->threadId << " took " << *(job_context->atomic_counter)
                  << std::endl;

        int old_value = (*(job_context->atomic_counter))++;
        const InputVec *input_vec = job_context->inputVector;
        InputPair input_pair = input_vec->at(old_value);
        pthread_mutex_unlock(job_context->mutex);
        job_context->client->map(input_pair.first, input_pair.second, thread_context);
    }
    // sort
    std::sort((thread_context->ds2)->begin(), (thread_context->ds2)->end());
}

void shuffle_helper(ThreadContext *thread_context, JobContext *job_context) {
    if (thread_context->threadId == 1) {
        auto *shuffled = new std::vector<std::vector<std::pair<K2 *, V2 *>>>();
        while (!is_all_empty(job_context)) {
            K2 *next_key = find_next_key(job_context);
            auto *next_vector = new std::vector<std::pair<K2 *, V2 *>>();
            for (int i = 0; i < job_context->num_threads; i++) {
                std::vector<std::pair<K2 *, V2 *>> *thread_vector = job_context->thread_contexts->at(i)->ds2;
                while (thread_vector->size() > 0 && thread_vector->at(0).first == next_key) {
                    next_vector->push_back(thread_vector->at(0));
                    thread_vector->erase(thread_vector->begin());
//                    thread_vector(thread_vector[0]);
                    thread_vector;
                }
            }
            shuffled->push_back(*next_vector);
        }
    }
}

void *threadMapReduce(void *context) {
    ThreadContext *thread_context = static_cast<ThreadContext *>(context);
    JobContext *job_context = static_cast<JobContext *>(thread_context->job_context);
    // map
    map_helper(thread_context, job_context);
    std::cout << "BEFORE " << thread_context->threadId << std::endl;
    job_context->barrier->barrier();
    std::cout << "AFTER " << thread_context->threadId << std::endl;

    // shuffle
    shuffle_helper(thread_context, job_context);
    job_context->barrier->barrier();
    //reduce
}

JobHandle startMapReduceJob(const MapReduceClient &client,
                            const InputVec &inputVec, OutputVec &outputVec,
                            int multiThreadLevel) {
    pthread_t threads[multiThreadLevel];
    ThreadContext thread_context_arr[multiThreadLevel];
    auto *thread_contexts = new std::vector<ThreadContext *>();
    std::atomic<int> atomic_counter(0);
    Barrier *barrier = new Barrier(multiThreadLevel);
    pthread_mutex_t mutex(PTHREAD_MUTEX_INITIALIZER);
    pthread_mutex_init(&mutex, NULL);
    JobState job_state = {UNDEFINED_STAGE, 0};


    JobContext *job_context = new JobContext{thread_contexts, job_state, multiThreadLevel, &inputVec, &client, barrier,
                                             &mutex,
                                             &atomic_counter};
    for (int i = 0; i < multiThreadLevel; ++i) {
        auto *ds2 = new std::vector<std::pair<K2 *, V2 *>>();
        auto *ds3 = new std::vector<std::pair<K3 *, V3 *>>();
        JobHandle job_handle = (void *) job_context;
        ThreadContext *tc = new ThreadContext{ds2, ds3, job_handle, i + 1};
        job_context->thread_contexts->push_back(tc);
    }

    for (int i = 0; i < multiThreadLevel; ++i) {

        pthread_create(threads + i, NULL, threadMapReduce, job_context->thread_contexts->at(i));
    }

    // wait for job to finish
//    while (true)
//    {
//       std::cout << "stil here" <<std::endl;
//    }
    // reduce K3 V3 from all threads
    return NULL;
}

void waitForJob(JobHandle job) {};

void getJobState(JobHandle job, JobState *state) {};

void closeJobHandle(JobHandle job) {};



