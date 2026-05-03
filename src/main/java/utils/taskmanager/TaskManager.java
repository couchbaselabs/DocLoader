package utils.taskmanager;

import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class TaskManager {
    private int workers;
    private ExecutorService poolExecutor;
    private ConcurrentHashMap<String, Future> tasks = new ConcurrentHashMap<String, Future>();

    public TaskManager(int workers) {
        this.workers = workers;
        this.poolExecutor = Executors.newFixedThreadPool(this.workers);
    }

    public void shutdown() {
        this.poolExecutor.shutdownNow();
    }

    public void submit(Task task) {
        Future future = this.poolExecutor.submit(task);
        this.tasks.put(task.taskName, future);
    }

    public void getAllTaskResult() {
        for (String taskName : this.tasks.keySet()) {
            try {
                this.tasks.get(taskName).get();
                this.tasks.remove(taskName);
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }
        }
    }

    public boolean check_if_task_with_name_exists(String task_name) {
        for (String taskName : this.tasks.keySet()) {
            if (task_name.equals(taskName)) {
                return true;
            }
        }
        return false;
    }

    public boolean getTaskResult(Task task) {
        Future future = this.tasks.get(task.taskName);
        if (future == null) {
            System.out.println("Task '" + task.taskName + "' not found in task manager. "
                    + "It may not have been submitted or was already consumed.");
            task.result = false;
            return false;
        }
        try {
            future.get();
            this.tasks.remove(task.taskName);
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
            this.tasks.remove(task.taskName);
        }
        return task.result;
    }

    public void abortTask(Task task) {
        Future future = this.tasks.get(task.taskName);
        if (future != null) {
            future.cancel(true);
        } else {
            System.out.println("Task '" + task.taskName + "' not found during abort. "
                    + "It may not have been submitted or was already consumed.");
        }
    }

    public void abortAllTasks() {
        for (Entry<String, Future> task : this.tasks.entrySet()) {
            this.tasks.get(task.getKey()).cancel(true);
        }
    }
}
