package utils.taskmanager;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * The set of workers created for one load request that share a single document
 * generator.
 *
 * Only the workers that get a thread do any work: they pull batches from the shared
 * generator until it is empty. Once it is empty, every worker of the group that has not
 * started yet would do nothing but observe the empty generator and exit - but it still
 * has to be scheduled before its Future completes, and on a wide run the queue ahead of
 * it holds every other load's work. Callers wait for all of their workers, so those
 * no-op stragglers keep a finished load "in progress" for the rest of the run.
 *
 * Marking the group exhausted cancels the workers that never started, so the load
 * completes as soon as its data is loaded.
 *
 * Only wired up where workers genuinely share a generator. Loads whose workers own
 * disjoint doc ranges (SIFT/MSMARCO) must not use this - there, every worker has its
 * own range to load and none of them is redundant.
 */
public class TaskGroup {

    private final List<Task> members = new CopyOnWriteArrayList<>();
    private final AtomicBoolean exhausted = new AtomicBoolean(false);
    private volatile TaskManager manager;

    public void add(Task task) {
        this.members.add(task);
        task.group = this;
    }

    public void setManager(TaskManager manager) {
        this.manager = manager;
    }

    /**
     * Called by the first worker that finds the shared generator empty.
     */
    public void markWorkExhausted() {
        if (this.manager == null) {
            return;
        }
        if (!this.exhausted.compareAndSet(false, true)) {
            return;
        }
        for (Task member : this.members) {
            this.manager.skipTask(member);
        }
    }
}
