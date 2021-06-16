#include "MapReduceClient.h"
#include <pthread.h>
#include <algorithm>
#include "Barrier/Barrier.cpp"
#include "atomic"
#include "iostream"

#define MIDDLE_MASK 0x7fffffff
#define FIRST_MASK 0x7ffffff

//TODO complicated atomic counter
// TODO semaphore
// todo remove
class VString : public V1 {
public:
    VString(std::string content) : content(content) {}

    std::string content;
};

class VCount : public V2, public V3 {
public:
    VCount(int count) : count(count) {}

    int count;
};


class KChar : public K2, public K3 {
public:
    KChar(char c) : c(c) {}

    virtual bool operator<(const K2 &other) const {
        return c < static_cast<const KChar &>(other).c;
    }

    virtual bool operator<(const K3 &other) const {
        return c < static_cast<const KChar &>(other).c;
    }

    char c;
};


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
    JobHandle job_context;
    int threadId;
} ThreadContext;

typedef struct {
    std::vector<ThreadContext *> *thread_contexts;
    JobState *job_state;
    int num_threads;
    const InputVec *inputVector;
    const MapReduceClient *client;
    Barrier *barrier;
    pthread_mutex_t *mutex;
    std::atomic<uint64_t> *atomic_counter;
    std::vector<std::vector<IntermediatePair>> *shuffledVector;
    OutputVec *output_vec;
    std::vector<pthread_t *> *threads;
    pthread_mutex_t *waitMutex;
    pthread_t *thread_arr;
} JobContext;

void printAtomic(JobContext *job_context) {
    printf("atomic counter first 31 bit: %d\n", job_context->atomic_counter->load() & (0x7ffffff));
    printf("atomic counter next 31 bit: %d\n", job_context->atomic_counter->load() >> 31 & (0x7fffffff));
    printf("atomic counter last 2 bit: %d\n", job_context->atomic_counter->load() >> 62);
}


void printPair2(std::pair<K2 *, V2 *> pair, ThreadContext *thread_context, int i) {
    K2 *k2 = pair.first;
    V2 *v2 = pair.second;
    KChar *ck2 = (KChar *) k2;
    VCount *cv2 = (VCount *) v2;
    std::cout << "THREAD " << thread_context->threadId << " key: " << ck2->c << " value: " << cv2->count << " index: "
              << i << std::endl;
}


void printInterVec(std::vector<IntermediatePair> *v, ThreadContext *thread_context) {
    for (int i = 0; i < v->size(); i++) {
        printPair2(v->at(i), thread_context, i);
    }
}


void printPair3(OutputPair pair, ThreadContext *thread_context, int i) {
    K3 *k = pair.first;
    V3 *v = pair.second;
    KChar *ck2 = (KChar *) k;
    VCount *cv2 = (VCount *) v;
    std::cout << "THREAD " << thread_context->threadId << " key: " << ck2->c << " value: " << cv2->count << " index: "
              << i << std::endl;
}


void printOutputVec(OutputVec *v, ThreadContext *thread_context) {
    for (int i = 0; i < v->size(); i++) {
        printPair3(v->at(i), thread_context, i);
    }
}


void emit2(K2 *key, V2 *value, void *context) {
    ThreadContext *thread_context = static_cast<ThreadContext *>(context);
    thread_context->ds2->push_back(std::pair<K2 *, V2 *>(key, value));
}

void emit3(K3 *key, V3 *value, void *context) {
    ThreadContext *thread_context = static_cast<ThreadContext *>(context);
    JobContext *job_context = static_cast<JobContext *>(thread_context->job_context);
    pthread_mutex_lock(job_context->mutex);
    (*(job_context->atomic_counter))++;
    job_context->output_vec->push_back(std::pair<K3 *, V3 *>(key, value));
    pthread_mutex_unlock(job_context->mutex);
}

bool compareKeys(K2 *k1, K2 *k2) {
    bool gt = k1->operator<(*k2);
    bool st = k2->operator<(*k1);
    return (not gt) and (not st);
}

bool compFunc(IntermediatePair p1, IntermediatePair p2) {
    return p1.first->operator<(*p2.first);
}


K2 *find_next_key(JobContext *context) {
    if (context->thread_contexts->size() == 0) {
        return nullptr;
    }
    IntermediatePair *min = nullptr;
    for (int i = 0; i < context->num_threads; i++) {
        if (context->thread_contexts->at(i)->ds2->size() == 0) {
            continue;
        }
        IntermediatePair *p = &context->thread_contexts->at(i)->ds2->at(0);
        if (min == nullptr || compFunc(*p, *min)) {
            min = p;
        }
    }
    return min->first;
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
    *job_context->atomic_counter |= ((uint64_t) MAP_STAGE << 62);
    // map
    while (true) {
        pthread_mutex_lock(job_context->mutex);

        if (((job_context->atomic_counter->load()) & FIRST_MASK) >= (job_context->inputVector)->size()) {
            pthread_mutex_unlock(job_context->mutex);
            break;
        }

        uint64_t old_value = (*(job_context->atomic_counter))++;
        old_value &= FIRST_MASK;
        InputPair input_pair = job_context->inputVector->at(old_value);
        pthread_mutex_unlock(job_context->mutex);
        job_context->client->map(input_pair.first, input_pair.second, thread_context);
    }

    // sort
    std::sort((thread_context->ds2)->begin(), (thread_context->ds2)->end(), compFunc);
//    std::cout << "---------- MAPPING ---------" << std::endl;
//    printInterVec(thread_context->ds2, thread_context);
}


int numOfShuffleTasks(JobContext *job_context) {
    int n = 0;
    for (int i = 0; i < job_context->thread_contexts->size(); i++) {
        n += job_context->thread_contexts->at(i)->ds2->size();
    }
    return n;
}


void shuffle_helper(ThreadContext *thread_context, JobContext *job_context) {
    if (thread_context->threadId == 1) {

        *job_context->atomic_counter += ((uint64_t) 1 << 62);
        *job_context->atomic_counter &= ((uint64_t) 3 << 62);
        int num_of_shuffle_tasks = numOfShuffleTasks(job_context);
        *job_context->atomic_counter += (uint64_t) num_of_shuffle_tasks << 31;
        auto *shuffled = new std::vector<std::vector<std::pair<K2 *, V2 *>>>();
        while (!is_all_empty(job_context)) {
            K2 *next_key = find_next_key(job_context);
            auto *next_vector = new std::vector<std::pair<K2 *, V2 *>>();
            for (int i = 0; i < job_context->num_threads; i++) {
                std::vector<std::pair<K2 *, V2 *>> *thread_vector = job_context->thread_contexts->at(i)->ds2;
                while (thread_vector->size() > 0 && compareKeys(thread_vector->at(0).first, next_key)) {
                    next_vector->push_back(thread_vector->at(0));
                    (*(job_context->atomic_counter))++;
                    thread_vector->erase(thread_vector->begin());
                }
            }
            shuffled->push_back(*next_vector);
        }
        job_context->shuffledVector = shuffled;
//        std::cout << "----------SHUFFLING---------" << std::endl;
//        for (int i = 0; i < shuffled->size(); i++) {
//            std::cout << "vec" << i << std::endl;
//            printInterVec(&shuffled->at(i), thread_context);
//        }

        *job_context->atomic_counter &= ((uint64_t) 3 << 62);
        *job_context->atomic_counter |= ((uint64_t) 3 << 62);
        *job_context->atomic_counter += (uint64_t) shuffled->size() << 31;
//        std::cout << thread_context->threadId <<(job_context->shuffledVector)->size() << ((job_context->atomic_counter->load()) & FIRST_MASK) <<std::endl;

    }
}

void reduce_helper(ThreadContext *thread_context, JobContext *job_context) {
    *(job_context->atomic_counter)|= ((uint64_t) 1 << 62);
    while (true) {
        pthread_mutex_lock(job_context->mutex);
        //std::cout << (job_context->shuffledVector)->size() << ((job_context->atomic_counter->load()) & FIRST_MASK) <<std::endl;
        if ((job_context->shuffledVector)->empty()) {
            pthread_mutex_unlock(job_context->mutex);
            break;
        }
//        uint64_t old_value = (job_context->atomic_counter)->load();
//        old_value &= FIRST_MASK;
//        std::cout<<old_value<<std::endl;
        const MapReduceClient *client = job_context->client;

        auto to_reduce = (job_context->shuffledVector->front());
//        auto masheo = *to_reduce;
        job_context->shuffledVector->erase(job_context->shuffledVector->begin());
        pthread_mutex_unlock(job_context->mutex);

        client->reduce(&to_reduce, thread_context);

    }
//    if (thread_context->threadId == 1) {
//        std::cout << "---------- REDUCING ---------" << std::endl;
//        printInterVec(job_context->output_vec, thread_context);
//    }
}

void *threadMapReduce(void *context) {
    ThreadContext *thread_context = static_cast<ThreadContext *>(context);
    JobContext *job_context = static_cast<JobContext *>(thread_context->job_context);
    // map
    uint64_t numOfMapJob = job_context->inputVector->size();
    *job_context->atomic_counter |= numOfMapJob << 31;
    map_helper(thread_context, job_context);
    job_context->barrier->barrier();
//    std::cout << "AFTER MAP" << thread_context->threadId << std::endl;

    // shuffle
    shuffle_helper(thread_context, job_context);
    job_context->barrier->barrier();

//    std::cout << "AFTER SHUFFEL" << std::endl;
    //reduce
    reduce_helper(thread_context, job_context);
    job_context->barrier->barrier();
}

JobHandle startMapReduceJob(const MapReduceClient &client,
                            const InputVec &inputVec, OutputVec &outputVec,
                            int multiThreadLevel) {
    pthread_t *threads_arr = new pthread_t [multiThreadLevel];
    auto *thread_contexts = new std::vector<ThreadContext *>();
    auto *threads_vec = new std::vector<pthread_t *>();

    std::atomic<uint64_t>* atomic_counter = new std::atomic<uint64_t>(0);
    Barrier *barrier = new Barrier(multiThreadLevel);
    pthread_mutex_t* mutex = new pthread_mutex_t;
    pthread_mutex_init(mutex, NULL);
    pthread_mutex_t* waitMutex = new pthread_mutex_t;
    pthread_mutex_init(waitMutex, NULL);

    JobState *job_state = new JobState{UNDEFINED_STAGE, 0};


    JobContext *job_context = new JobContext{thread_contexts, job_state, multiThreadLevel, &inputVec, &client, barrier,
                                             mutex, atomic_counter, nullptr, &outputVec, threads_vec,
                                             waitMutex, threads_arr};
    JobHandle job_handle = (void *) job_context;
    for (int i = 0; i < multiThreadLevel; ++i) {
        auto *ds2 = new std::vector<std::pair<K2 *, V2 *>>();
        ThreadContext *tc = new ThreadContext{ds2, job_handle, i + 1};
        job_context->thread_contexts->push_back(tc);
        threads_vec->push_back(threads_arr + i);
    }

    for (int i = 0; i < multiThreadLevel; ++i) {
        pthread_create(threads_arr + i, NULL, threadMapReduce, job_context->thread_contexts->at(i));
    }
    return job_handle;
}

void waitForJob(JobHandle job) {
    JobContext *job_context = static_cast<JobContext *>(job);
    pthread_mutex_lock(job_context->waitMutex);
    for (int i = 0; i < job_context->num_threads; ++i) {
       pthread_join(*(job_context->threads->at(i)), NULL);
    }
    pthread_mutex_unlock(job_context->waitMutex);
}

void getJobState(JobHandle job, JobState *state) {
    JobContext *job_context = static_cast<JobContext *>(job);
    uint64_t counter = *job_context->atomic_counter;
    int done = counter & FIRST_MASK;
    int numOfTasks = ( counter >> 31) & MIDDLE_MASK;
    int cur_state =  counter >> 62;
    state->stage = static_cast<stage_t>(cur_state);
    if (numOfTasks != 0)
    {
        state->percentage = (float) done / numOfTasks;
    } else
    {
        state->percentage = 0;
    }
    state->percentage *= 100;

};

void closeJobHandle(JobHandle job) {
    JobContext *job_context = static_cast<JobContext *>(job);
    waitForJob(job);
    //TODO: maybe well have to free shuffled
    delete job_context->thread_arr;
    delete job_context->threads;
    delete job_context->atomic_counter;
    delete job_context->barrier;
    pthread_mutex_destroy(job_context->mutex);
    pthread_mutex_destroy(job_context->waitMutex);
    delete job_context->job_state;
    for (int i = 0; i < job_context->num_threads; i++)
    {
        delete job_context->thread_contexts->at(i)->ds2;
        delete job_context->thread_contexts->at(i);
    }
    delete job_context->thread_contexts;
    delete job_context;
    return;
};



