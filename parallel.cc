#include <queue>
#include <iostream>
#include <string>
#include <fstream>
#include <stdlib.h>
#include <pthread.h>
#include <errno.h>
#include <unistd.h>
#include <string.h>
#include <sys/time.h>

using namespace std;

typedef struct {
    struct timeval ts_;
    unsigned long long nbytes_;
} tStat;

void usage();
bool init_from_args(int argc, char *argv[]);
void run();
static void * thread_func(void *iArgv);
void read_file(string iFileName);
void update_stats(int iBytes);
void flush_stats(tStat * iCurrentStat);

// queue of files to read
queue<string> gFileQueue;
// number of threads
int gNumOfThreads;
// read buffer size
int gBufSize;
// queue mutex
pthread_mutex_t gQueueLock;
// statistics lock
pthread_mutex_t gStatsLock;
// thread count lock
pthread_mutex_t gThreadCountLock;
// current thread count
int gThreadCount;
// some stats
tStat gStat;

// main function
int main(int argc, char *argv[]) {

    if (!init_from_args(argc, argv)) {
        cerr << "arguments error, exiting" << endl;
        usage();
        return -1;
    }

    run();

    return 0;
}

// usage
void usage() {
    cout << "usage: parallel_read <filelist> <threads> <buffer size in kb>" << endl; 
    cout << "    example: ./parallel_read /tmp/files.txt 32 128" << endl;
    cout << "    this will use /tmp/files.txt as list of files, will use 32 threads, buffer size 128KB" << endl;
}

// initialize queue of filenames and variables
bool init_from_args(int iArgc, char *iArgv[]) {
    if (iArgc < 3 || iArgc > 4) {
        return false;
    }

    // first argument is file list
    ifstream aFile(iArgv[1]);
    if (!aFile) {
        cerr << "couldn't open file " << iArgv[1] << ", exiting" << endl;
        return false;
    }
   
    cout << "loading filenames from " << iArgv[1] << " ... ";
 
    string aLine;
    while (getline(aFile, aLine)) {
        gFileQueue.push(aLine);
    }

    cout << "loaded " << gFileQueue.size() << " filenames" << endl;

    // second argument is number of threads
    gNumOfThreads = atoi(iArgv[2]);
    if (gNumOfThreads <= 0) {
        cerr << "wrong number of threads" << endl;
        return false;
    } else {
        cout << "number of threads: " << gNumOfThreads << endl;
    }

    // third argument is buffer size
    if (iArgc == 4) {
        gBufSize = atoi(iArgv[3])*1024;
        if (gBufSize <= 0) {
            cerr << "invalid buffer size specified: " << iArgv[3] << ", exiting" << endl;
            return false;
        }
    } else {
        gBufSize = 1024*1024;
    }
    cout << "buffer size: " << gBufSize << endl;

    return true;
}

// execute all threads, periodically print some stats, close on completion
void run() {
    pthread_t aThreads[gNumOfThreads];
    tStat aCurrentStat;

    // reset stats
    memset(&gStat, 0, sizeof(tStat));
    memset(&aCurrentStat, 0, sizeof(tStat));
    gettimeofday(&gStat.ts_, NULL);
    gettimeofday(&aCurrentStat.ts_, NULL);

    // start all threads
    for (int i = 0; i < gNumOfThreads; ++i) {
        if (pthread_create(&aThreads[i], NULL, &thread_func, NULL) != 0) {
            cerr << "pthread_create error for thread " << i << ", error: " << errno << endl;
        }
    }

    for (;;) {
        sleep (1);
        pthread_mutex_lock(&gQueueLock);
        if (gFileQueue.empty()) {
            pthread_mutex_unlock(&gQueueLock);
            break;
        } else {
            flush_stats(&aCurrentStat);
        }
        pthread_mutex_unlock(&gQueueLock);
    }

    cout << "no more files pending, joining threads and exiting" << endl;
    void *pResult;
    for (int i = 0; i < gNumOfThreads; ++i) {
        pthread_join(aThreads[i], &pResult);
    }
}

// thread function
void * thread_func(void *iArgv) {

    pthread_mutex_lock(&gThreadCountLock);
    gThreadCount++;
    pthread_mutex_unlock(&gThreadCountLock);

    string aFileName;

    for (;;) {
        pthread_mutex_lock(&gQueueLock);
        if (gFileQueue.empty()) {
            pthread_mutex_unlock(&gQueueLock);
            break;
        } else {
            aFileName = gFileQueue.front();
            gFileQueue.pop();
        }
        pthread_mutex_unlock(&gQueueLock);

        read_file(aFileName);
    }

    pthread_mutex_lock(&gThreadCountLock);
    gThreadCount--;
    pthread_mutex_unlock(&gThreadCountLock);

    return NULL;
}

// open, read and close the file
void read_file(string iFileName) {
    char aBuffer[gBufSize];

    ifstream aFile(iFileName.c_str(), ifstream::binary);

    if (!aFile) {
        cerr << "couldn't open file " << iFileName << endl;
        return;
    }

    while (aFile) {
        aFile.read(aBuffer, gBufSize);
        update_stats(aFile.gcount());
    }

    aFile.close();
}

// update number of bytes read
void update_stats(int iBytes) {
    pthread_mutex_lock(&gStatsLock);
    gStat.nbytes_ += iBytes;
    pthread_mutex_unlock(&gStatsLock);
}

// print total and current interval stats
void flush_stats(tStat * iCurrentStat) {
    struct timeval aTime;
    struct timeval aTimeSinceLastSnapshot;
    struct timeval aTotalTime;
    gettimeofday(&aTime, NULL);

    pthread_mutex_lock(&gStatsLock);
    unsigned long long aBytes = gStat.nbytes_ - iCurrentStat->nbytes_;
    timersub(&aTime, &iCurrentStat->ts_, &aTimeSinceLastSnapshot);
    timersub(&aTime, &gStat.ts_, &aTotalTime);
    unsigned int aCurThroughput = 
        (aBytes / 1048576 * 1000000) / (aTimeSinceLastSnapshot.tv_sec * 1000000 + aTimeSinceLastSnapshot.tv_usec);
    unsigned int aTotalThroughput =
        (gStat.nbytes_ / 1048576 * 1000000) / (aTotalTime.tv_sec * 1000000 + aTotalTime.tv_usec);
    iCurrentStat->nbytes_ = gStat.nbytes_;
    memcpy(&iCurrentStat->ts_, &aTime, sizeof(struct timeval));
    pthread_mutex_unlock(&gStatsLock);

    cout << "[" << gThreadCount << " active threads, " << gFileQueue.size() << " files pending] throughput: current: " 
        << aCurThroughput << " MB/sec, total: " << aTotalThroughput << " MB/sec" << endl;
}
