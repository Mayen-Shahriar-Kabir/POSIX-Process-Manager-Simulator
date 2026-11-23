# POSIX Process Manager Simulator

## 🖥️ Overview
This project implements a simulated process management subsystem for an operating system, written entirely in C. Instead of creating actual OS processes, the system simulates process lifecycles and state transitions within a shared Global Process Table. The core challenge and focus of this project is handling high concurrency: multiple worker threads simultaneously read task scripts and attempt to create, modify, and terminate processes, requiring robust synchronization using POSIX threads (pthreads), mutexes, and condition variables.

## ⚙️ Core Architecture

### 1. Global Process Table & Process Control Blocks (PCBs)
The system maintains a centralized process table capable of holding up to 64 simulated processes. Each process is represented by a PCB containing:
*   **PID & PPID:** Unique process identifier and parent process identifier.
*   **State:** The current lifecycle phase of the process.
*   **Exit Status:** Stored upon termination until the process is reaped.
*   **Children Tracking:** An array tracking all child PIDs.
*   **Synchronization Primitives:** Condition variables tied to specific PCBs to handle parent-child wait/wakeup mechanics.

### 2. Process States
Processes transition between three primary states:
*   **RUNNING:** The process is active immediately after creation via a fork.
*   **WAITING:** A parent process is blocked, waiting for a specific child (or any child) to terminate.
*   **ZOMBIE:** A process has terminated via an exit or kill command but has not yet been reaped by its waiting parent.

### 3. Concurrency & Threading Model
*   **Worker Threads:** The simulator spawns a worker thread for each input script provided via the command line. These threads concurrently parse their assigned scripts and apply operations to the shared process table.
*   **Monitor Thread:** A dedicated, automated thread observes the process table. Instead of polling, it sleeps on a condition variable and wakes up only when a worker thread signals a state change. It then flushes a formatted snapshot of the process table to `snapshots.txt`.

## 📜 Supported Script Commands
Worker threads read text files line-by-line. The script interpreter supports the following instructions:

*   `fork [parent_pid]`: Creates a new child process under the specified parent and assigns it the next available PID.
*   `exit [pid] [status]`: Terminates the target process, transitions it to a ZOMBIE state, records the exit status, and signals any waiting parent.
*   `wait [parent_pid] [child_pid]`: Blocks the parent process until the specified child terminates. If `-1` is passed as the child PID, the parent waits for *any* of its children to exit. Once a child is found in the ZOMBIE state, it is reaped from the process table.
*   `kill [pid]`: Sends a termination request, forcefully transitioning the target process to a ZOMBIE state.
*   `sleep [milliseconds]`: Pauses the worker thread's execution for the given duration, useful for testing race conditions and concurrency.

## 🚀 Build and Execution

### Prerequisites
Ensure you have a C compiler (such as GCC) installed and a POSIX-compliant environment (Linux, macOS, or WSL).

### Compilation
Link the pthread library during compilation to enable multithreading:
`gcc -pthread pm_sim.c -o pm_sim`

### Running the Simulator
Execute the compiled binary by passing one or more task scripts as arguments. Each file initializes a new concurrent worker thread:
`./pm_sim thread0.txt thread1.txt thread2.txt`

### Viewing the Output
The system generates a `snapshots.txt` file in the same directory. You can open this file in any text editor or view it directly in the terminal to review the chronological logs and tabular snapshots of the process table after every atomic modification:
`cat snapshots.txt`
