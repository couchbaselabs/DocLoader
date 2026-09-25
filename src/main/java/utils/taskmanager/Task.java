package utils.taskmanager;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public abstract class Task implements Runnable{

    public String taskName;
    public Boolean result;
    // Cumulative ops completed so far, updated by the running task and readable
    // without blocking on the task's Future (see TaskManager.getTaskProgress()).
    public AtomicLong completedOps = new AtomicLong(0);
    // Position of this task within the group of workers created for one load
    // request (0 for the first worker, 1 for the second, ...). TaskManager
    // schedules low indexes first, so every load gets its first worker running
    // before any load gets its second. Left at 0 for standalone tasks, which
    // makes their scheduling plain FIFO exactly as before.
    public int workerIndex = 0;
    // True when this task was cancelled before it ever ran because the work it
    // would have done was already finished by a sibling. Not a failure.
    public volatile boolean skipped = false;
    // True once a pool thread has actually begun executing this task. A task that has
    // never been dequeued is waiting for a thread, which is a queueing question, not a
    // hang - callers polling for progress must not time it out as a stall.
    public volatile boolean started = false;
    // Set only for workers that share one document generator with their siblings.
    public TaskGroup group = null;
    // Won by exactly one of: the thread about to execute this task, or skipTask()
    // deciding the task has nothing left to do. Whoever loses stands down.
    // Future.cancel(false) alone is not enough to decide this: a FutureTask stays in
    // state NEW while its runnable executes, so cancelling a *running* task succeeds
    // and makes get() throw immediately while the work is still in flight.
    private final AtomicBoolean claimed = new AtomicBoolean(false);

    /**
     * @return true if the caller now owns this task. A worker that loses the claim must
     *         return without doing any work; skipTask() that loses it must leave the
     *         running task alone.
     */
    public boolean claimForExecution() {
        return this.claimed.compareAndSet(false, true);
    }

    public Task(String taskName) {
        super();
        this.taskName = taskName;
        this.result = false;
    }

    public String getTaskName() {
        return taskName;
    }

    public void setTaskName(String taskName) {
        this.taskName = taskName;
    }

    /**
     * Called by a worker that finds the shared generator empty, so that siblings still
     * waiting for a thread are not left to be scheduled just to observe the same thing.
     * No-op for tasks that do not share a generator.
     */
    protected void notifyWorkExhausted() {
        TaskGroup taskGroup = this.group;
        if (taskGroup != null) {
            taskGroup.markWorkExhausted();
        }
    }

    /**
     * Final on purpose. skipTask() is only safe while every task acquires the claim
     * before doing any work - a subclass that forgot to would let skipTask() cancel it
     * mid-flight, and the caller would see CancellationException while documents were
     * still being written. Claiming here means no subclass can forget.
     */
    @Override
    public final void run() throws RuntimeException {
        if (!this.claimForExecution()) {
            // skipTask() got here first: the work this task would have done is already
            // finished, so there is nothing left to do.
            this.result = true;
            return;
        }
        this.started = true;
        this.runTask();
    }

    /**
     * The task body. Runs only once this task has won its claim.
     */
    protected abstract void runTask() throws RuntimeException;

}