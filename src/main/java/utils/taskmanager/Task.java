package utils.taskmanager;

public abstract class Task implements Runnable{

    public String taskName;
    public Boolean result;

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

    @Override
    public abstract void run() throws RuntimeException;

}