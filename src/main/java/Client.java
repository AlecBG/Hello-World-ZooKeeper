import lombok.Getter;
import lombok.extern.log4j.Log4j;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.Closeable;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Collectors;

import static org.apache.zookeeper.ZooDefs.Ids.OPEN_ACL_UNSAFE;

@Log4j
public class Client implements Watcher, Closeable {

    private ZooKeeper zooKeeper;
    private final String hostPort;
    @Getter volatile boolean connected = false;
    @Getter volatile boolean expired = false;

    protected ConcurrentHashMap<String, Object> contextMap = new ConcurrentHashMap<>();

    Client(String hostPort) {
        this.hostPort = hostPort;
    }

    void startZooKeeper() throws IOException {
        zooKeeper = new ZooKeeper(hostPort, 15_000, this);
    }

    public void process(WatchedEvent event) {
        System.out.println(event);
        if(event.getType() == Event.EventType.None){
            switch (event.getState()) {
                case SyncConnected:
                    connected = true;
                    break;
                case Disconnected:
                    connected = false;
                    break;
                case Expired:
                    expired = true;
                    connected = false;
                    System.out.println("Session expired.");
                default:
                    break;
            }
        }
    }

    @Override
    public void close() throws IOException {
        log.info("Closing client");
        try {
            zooKeeper.close();
        } catch (InterruptedException e) {
            log.warn("ZooKeeper interrupted while closing");
        }
    }

    void submitTask(String task, TaskObject taskContext) {
        taskContext.setTask(task);
        zooKeeper.create("/tasks/task-", task.getBytes(StandardCharsets.UTF_8), OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_SEQUENTIAL, createTaskCallback, taskContext);
    }

    AsyncCallback.StringCallback createTaskCallback = new AsyncCallback.StringCallback() {
        @Override
        public void processResult(int rc, String path, Object ctx, String name) {
            KeeperException.Code code = KeeperException.Code.get(rc);
            TaskObject taskContext = (TaskObject) ctx;
            switch (code) {
                case CONNECTIONLOSS:
                    submitTask(taskContext.getTask(), taskContext);  // Note we may end up duplicating the task.
                    break;
                case OK:
                    log.info("Have created task " + taskContext.getTask() + " with name " + name);
                    taskContext.setTaskName(name);
                    watchStatus("/status/" + name.replace("/tasks/", ""), ctx);
                    break;
                default:
                    log.error("Error when creating task", KeeperException.create(code, path));
            }
        }
    };

    void watchStatus(String path, Object context) {
        contextMap.put(path, context);
        zooKeeper.exists(path, statusWatcher, existsCallback, context);
    }

    Watcher statusWatcher = new Watcher() {
        @Override
        public void process(WatchedEvent event) {
            if (event.getType() == Event.EventType.NodeCreated) {
                assert event.getPath().contains("/status/task-");
                zooKeeper.getData(event.getPath(), false, getDataCallback, contextMap.get(event.getPath()));
            }
        }
    };

    AsyncCallback.StatCallback existsCallback = new AsyncCallback.StatCallback() {
        @Override
        public void processResult(int rc, String path, Object ctx, Stat stat) {
            KeeperException.Code code = KeeperException.Code.get(rc);
            switch (code) {
                case CONNECTIONLOSS:
                    watchStatus(path, ctx);
                    break;
                case OK:
                    if (stat != null) {
                        zooKeeper.getData(path, false, getDataCallback, ctx);
                    }
                    break;
                case NONODE:
                    break;
                default:
                    log.error("Error checking if status node at " + path + " exists", KeeperException.create(code, path));
            }
        }
    };

    AsyncCallback.DataCallback getDataCallback = new AsyncCallback.DataCallback() {
        @Override
        public void processResult(int rc, String path, Object ctx, byte[] data, Stat stat) {
            KeeperException.Code code = KeeperException.Code.get(rc);
            switch (code) {
                case CONNECTIONLOSS:
                    zooKeeper.getData(path, false, getDataCallback, contextMap.get(path));
                    break;
                case OK:
                    String taskResult = new String(data, StandardCharsets.UTF_8);
                    log.info("Task: " + path + ", result: " + taskResult);
                    assert (ctx != null);
                    TaskObject taskObject = (TaskObject) ctx;
                    taskObject.setStatus(taskResult.contains("done"));
                    zooKeeper.delete(path, -1, taskDeleteCallback, null);
                    contextMap.remove(path);
                    log.info("Task: " + taskObject.getTask() +
                            "\tis done: " + taskObject.isDone() +
                            "\tis successful: " + taskObject.isSuccesful());
                    break;
                case NONODE:
                    log.warn("Status node: " + path + " is gone!");
                    break;
                default:
                    log.error("Error getting the data from " + path, KeeperException.create(code, path));
            }
        }
    };

    AsyncCallback.VoidCallback taskDeleteCallback = new AsyncCallback.VoidCallback() {
        @Override
        public void processResult(int rc, String path, Object ctx) {
            KeeperException.Code code = KeeperException.Code.get(rc);
            switch (code) {
                case CONNECTIONLOSS:
                    zooKeeper.delete(path, -1, taskDeleteCallback, null);
                    break;
                case OK:
                    log.info("Successfully deleted " + path);
                    break;
                default:
                    log.error("Error trying to delete task " + path, KeeperException.create(code, path));
            }
        }
    };

    static class TaskObject {
        private String task;
        private String taskName;
        private boolean done = false;
        private boolean succesful = false;
        private CountDownLatch latch = new CountDownLatch(1);

        String getTask () {
            return task;
        }

        void setTask (String task) {
            this.task = task;
        }

        void setTaskName(String name){
            this.taskName = name;
        }

        String getTaskName (){
            return taskName;
        }

        void setStatus (boolean status){
            succesful = status;
            done = true;
            latch.countDown();
        }

        void waitUntilDone () {
            try{
                latch.await();
            } catch (InterruptedException e) {
                log.warn("InterruptedException while waiting for task to get done");
            }
        }

        synchronized boolean isDone(){
            return done;
        }

        synchronized boolean isSuccesful(){
            return succesful;
        }
    }

    public static void main(String[] args) throws Exception {
        log.info("Client initialising.");
        Client client = new Client(args[0]);
        client.startZooKeeper();
        List<String> elementsToPrint = new ArrayList<>(args.length - 1);
        elementsToPrint.addAll(Arrays.asList(args).subList(1, args.length));

        while (!client.isConnected()) {
            Thread.sleep(100);
        }
        log.info("Client connected.");

        List<TaskObject> tasks = elementsToPrint.stream().map(x -> new TaskObject()).collect(Collectors.toList());
        for (int i = 0; i < elementsToPrint.size(); i++) {
            client.submitTask(elementsToPrint.get(i), tasks.get(i));
        }
        for (TaskObject task: tasks) {
            task.waitUntilDone();
        }
        client.close();
    }
}
