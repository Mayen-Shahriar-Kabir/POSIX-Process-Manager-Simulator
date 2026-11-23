#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <unistd.h>
#include <sched.h>

#define MAX_PROCESSES 64
#define MAX_CHILDREN 64
#define MAX_THREADS 32
#define LINE_SIZE 256
#define MAX_SNAPSHOTS 256
#define SNAPSHOT_SIZE 4096

typedef enum {
    RUNNING,
    WAITING,
    ZOMBIE
} State;

typedef struct {
    int pid;
    int ppid;
    State state;
    int exit_status;
    int killed;
    int waiting_for;
    int children[MAX_CHILDREN];
    int child_count;
    pthread_cond_t wait_cond;
} PCB;

typedef struct {
    char filename[128];
    int tid;
} ThreadArg;

PCB process_table[MAX_PROCESSES];
int process_count = 0;
int next_pid = 2;

pthread_mutex_t table_mutex = PTHREAD_MUTEX_INITIALIZER;
pthread_cond_t monitor_cond = PTHREAD_COND_INITIALIZER;

char snapshot_queue[MAX_SNAPSHOTS][SNAPSHOT_SIZE];
int q_front = 0;
int q_back = 0;

int workers_done = 0;
int total_workers = 0;

FILE *snapshot_file;

char* state_to_string(State s) {
    switch (s) {
        case RUNNING: return "RUNNING";
        case WAITING: return "WAITING";
        case ZOMBIE: return "ZOMBIE";
    }
    return "";
}

int find_process_index(int pid) {
    for (int i = 0; i < process_count; i++) {
        if (process_table[i].pid == pid)
            return i;
    }
    return -1;
}

void queue_snapshot(const char *action) {
    char snapshot[SNAPSHOT_SIZE];
    int offset = 0;

    offset += sprintf(snapshot + offset, "%s\n", action);
    offset += sprintf(snapshot + offset,
                      "PID\tPPID\tSTATE\t\tEXIT_STATUS\n");

    for (int i = 0; i < process_count; i++) {
        PCB *p = &process_table[i];

        offset += sprintf(snapshot + offset,
                          "%d\t%d\t%s\t\t",
                          p->pid,
                          p->ppid,
                          state_to_string(p->state));

        if (p->state == ZOMBIE && !p->killed)
            offset += sprintf(snapshot + offset,
                              "%d\n",
                              p->exit_status);
        else
            offset += sprintf(snapshot + offset, "-\n");
    }

    offset += sprintf(snapshot + offset, "\n");

    strcpy(snapshot_queue[q_back], snapshot);
    q_back++;

    pthread_cond_signal(&monitor_cond);
}

void* monitor(void *arg) {
    while (1) {
        pthread_mutex_lock(&table_mutex);

        while (q_front == q_back &&
               workers_done < total_workers) {
            pthread_cond_wait(&monitor_cond, &table_mutex);
        }

        while (q_front < q_back) {
            fprintf(snapshot_file, "%s", snapshot_queue[q_front]);
            fflush(snapshot_file);
            q_front++;
        }

        if (workers_done == total_workers &&
            q_front == q_back) {
            pthread_mutex_unlock(&table_mutex);
            break;
        }

        pthread_mutex_unlock(&table_mutex);
    }

    return NULL;
}

void init_process_manager() {
    PCB *init = &process_table[0];

    init->pid = 1;
    init->ppid = 0;
    init->state = RUNNING;
    init->exit_status = -1;
    init->killed = 0;
    init->waiting_for = -2;
    init->child_count = 0;

    pthread_cond_init(&init->wait_cond, NULL);

    process_count = 1;

    pthread_mutex_lock(&table_mutex);
    queue_snapshot("Initial Process Table");
    pthread_mutex_unlock(&table_mutex);
}

void pm_fork(int parent_pid, int tid) {
    pthread_mutex_lock(&table_mutex);

    int pidx = find_process_index(parent_pid);
    if (pidx == -1 || process_count >= MAX_PROCESSES) {
        pthread_mutex_unlock(&table_mutex);
        return;
    }

    PCB *child = &process_table[process_count];

    child->pid = next_pid++;
    child->ppid = parent_pid;
    child->state = RUNNING;
    child->exit_status = -1;
    child->killed = 0;
    child->waiting_for = -2;
    child->child_count = 0;

    pthread_cond_init(&child->wait_cond, NULL);

    process_table[pidx]
        .children[process_table[pidx].child_count++] = child->pid;

    process_count++;

    char msg[256];
    sprintf(msg, "Thread %d calls pm_fork %d", tid, parent_pid);

    queue_snapshot(msg);

    pthread_mutex_unlock(&table_mutex);
}

void pm_exit(int pid, int status, int tid) {
    pthread_mutex_lock(&table_mutex);

    int idx = find_process_index(pid);
    if (idx == -1) {
        pthread_mutex_unlock(&table_mutex);
        return;
    }

    PCB *child = &process_table[idx];
    child->state = ZOMBIE;
    child->exit_status = status;
    child->killed = 0;

    char msg[256];
    sprintf(msg, "Thread %d calls pm_exit %d %d", tid, pid, status);

    queue_snapshot(msg);

    int parent_idx = find_process_index(child->ppid);
    if (parent_idx != -1) {
        pthread_cond_signal(&process_table[parent_idx].wait_cond);
        sched_yield();
    }

    pthread_mutex_unlock(&table_mutex);
}

void pm_kill(int pid, int tid) {
    pthread_mutex_lock(&table_mutex);

    int idx = find_process_index(pid);
    if (idx == -1) {
        pthread_mutex_unlock(&table_mutex);
        return;
    }

    PCB *p = &process_table[idx];
    p->state = ZOMBIE;
    p->killed = 1;

    char msg[256];
    sprintf(msg, "Thread %d calls pm_kill %d", tid, pid);

    queue_snapshot(msg);

    int parent_idx = find_process_index(p->ppid);
    if (parent_idx != -1) {
        pthread_cond_signal(&process_table[parent_idx].wait_cond);
        sched_yield();
    }

    pthread_mutex_unlock(&table_mutex);
}

void reap_process(int idx) {
    for (int i = idx; i < process_count - 1; i++) {
        process_table[i] = process_table[i + 1];
    }
    process_count--;
}

void pm_wait(int parent_pid, int child_pid, int tid) {
    pthread_mutex_lock(&table_mutex);

    int pidx = find_process_index(parent_pid);
    if (pidx == -1) {
        pthread_mutex_unlock(&table_mutex);
        return;
    }

    PCB *parent = &process_table[pidx];
    int wait_logged = 0;

    while (1) {
        int zombie_idx = -1;

        for (int i = 0; i < process_count; i++) {
            if (process_table[i].ppid == parent_pid &&
                process_table[i].state == ZOMBIE &&
                (child_pid == -1 ||
                 process_table[i].pid == child_pid)) {
                zombie_idx = i;
                break;
            }
        }

        if (zombie_idx != -1) {
            int reaped_pid = process_table[zombie_idx].pid;

            reap_process(zombie_idx);

            parent->state = RUNNING;

            char msg[256];
            sprintf(msg,
                    "Thread %d calls pm_wait %d %d (reaped child %d)",
                    tid, parent_pid, child_pid, reaped_pid);

            queue_snapshot(msg);

            pthread_mutex_unlock(&table_mutex);
            return;
        }

        parent->state = WAITING;

        if (!wait_logged) {
            char msg[256];
            sprintf(msg,
                    "Thread %d calls pm_wait %d %d",
                    tid, parent_pid, child_pid);

            queue_snapshot(msg);
            wait_logged = 1;
        }

        pthread_cond_wait(&parent->wait_cond, &table_mutex);
    }
}


void* worker(void *arg) {
    ThreadArg *t = (ThreadArg*)arg;

    FILE *fp = fopen(t->filename, "r");
    if (!fp) return NULL;

    char line[LINE_SIZE];
    int a, b;

    while (fgets(line, sizeof(line), fp)) {
        if (sscanf(line, "fork %d", &a) == 1)
            pm_fork(a, t->tid);

        else if (sscanf(line, "exit %d %d", &a, &b) == 2)
            pm_exit(a, b, t->tid);

        else if (sscanf(line, "wait %d %d", &a, &b) == 2)
            pm_wait(a, b, t->tid);

        else if (sscanf(line, "kill %d", &a) == 1)
            pm_kill(a, t->tid);

        else if (sscanf(line, "sleep %d", &a) == 1)
            usleep(a * 1000);
    }

    fclose(fp);

    pthread_mutex_lock(&table_mutex);
    workers_done++;
    pthread_cond_signal(&monitor_cond);
    pthread_mutex_unlock(&table_mutex);

    return NULL;
}

int main(int argc, char *argv[]) {
    if (argc < 2) return 1;

    total_workers = argc - 1;

    snapshot_file = fopen("snapshots.txt", "w");
    if (!snapshot_file) return 1;

    init_process_manager();

    pthread_t mon;
    pthread_t workers[MAX_THREADS];
    ThreadArg args[MAX_THREADS];

    pthread_create(&mon, NULL, monitor, NULL);

    for (int i = 0; i < total_workers; i++) {
        strcpy(args[i].filename, argv[i + 1]);
        args[i].tid = i;
        pthread_create(&workers[i], NULL, worker, &args[i]);
    }

    for (int i = 0; i < total_workers; i++) {
        pthread_join(workers[i], NULL);
    }

    pthread_join(mon, NULL);

    fclose(snapshot_file);
    return 0;
}

