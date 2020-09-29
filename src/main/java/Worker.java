import lombok.Getter;
import lombok.extern.log4j.Log4j;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.Closeable;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.zookeeper.ZooDefs.Ids.OPEN_ACL_UNSAFE;

@Log4j
public class Worker implements Watcher, Closeable {

    private ZooKeeper zooKeeper;
    private final String hostPort;
    private final String serverId = Integer.toHexString(new Random().nextInt());
    private final String name = "worker-" + serverId;

    private String status;
    private final ThreadPoolExecutor executor;
    private AtomicInteger executionCount = new AtomicInteger(0);
    @Getter private volatile boolean connected = false;
    @Getter private volatile boolean expired = false;

    public Worker(String hostPort) {
        this.hostPort = hostPort;
        this.executor = new ThreadPoolExecutor(1, 1, 1000L, TimeUnit.MILLISECONDS, new ArrayBlockingQueue<>(200));
    }

    void startZooKeeper() throws IOException {
        zooKeeper = new ZooKeeper(hostPort, 15_000, this);
    }

    @Override
    public void process(WatchedEvent event) {
        log.info(event.toString() + ", " + hostPort);
        if (event.getType() == Event.EventType.None) {
            switch (event.getState()) {
                case SyncConnected:
                    connected = true;
                    bootstrap();
                    break;
                case Disconnected:
                    connected = false;
                    break;
                case Expired:
                    expired = true;
                    connected = false;
                    log.error("Session expired");
                default:
                    break;
            }
        }
    }

    public void bootstrap() {
        createAssignNode();
    }

    void createAssignNode() {
        zooKeeper.create("/assign/" + name, new byte[0], OPEN_ACL_UNSAFE, CreateMode.PERSISTENT, createAssignCallback, null);
    }

    AsyncCallback.StringCallback createAssignCallback = new AsyncCallback.StringCallback() {
        @Override
        public void processResult(int rc, String path, Object ctx, String name) {
            KeeperException.Code code = KeeperException.Code.get(rc);
            switch (code) {
                case CONNECTIONLOSS:
                    createAssignNode();
                    break;
                case OK:
                    log.info("Assign node for " + name + " has been created");
                    register();
                    break;
                case NODEEXISTS:
                    log.warn("Assign node for " + name + " already registered");
                    break;
                default:
                    log.error("Error when trying to create assign node for " + name, KeeperException.create(code, path));
            }
        }
    };

    void register() {
        zooKeeper.create("/workers/" + name, "Idle".getBytes(StandardCharsets.UTF_8), OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL, createWorkerCallback, null);
    }

    AsyncCallback.StringCallback createWorkerCallback = new AsyncCallback.StringCallback() {
        @Override
        public void processResult(int rc, String path, Object ctx, String name) {
            KeeperException.Code code = KeeperException.Code.get(rc);
            switch (code) {
                case CONNECTIONLOSS:
                    register();
                    break;
                case OK:
                    log.info("Registered successfully: " + serverId);
                    getTasks();
                    break;
                case NODEEXISTS:
                    log.warn("Already registered: " + serverId);
                    break;
                default:
                    log.error("Something went wrong: " + KeeperException.create(code, path));
            }
        }
    };

    public void setStatus(String newStatus) {
        status = newStatus;
        updateStatus(newStatus);
    }

    synchronized private void updateStatus(String status) {
        if (this.status.equals(status)) {
            zooKeeper.setData("/workers/" + name, status.getBytes(StandardCharsets.UTF_8), -1, statusUpdateCallback, status);
        }
    }

    AsyncCallback.StatCallback statusUpdateCallback = new AsyncCallback.StatCallback() {
        @Override
        public void processResult(int rc, String path, Object ctx, Stat stat) {
            KeeperException.Code code = KeeperException.Code.get(rc);
            String newStatus = (String) ctx;
            switch (code) {
                case CONNECTIONLOSS:
                    updateStatus(newStatus);
                    break;
                case OK:
                    log.info("Successfully updated the status to " + newStatus);
                    break;
                default:
                    log.error("Error when updating the status of " + name + " to " + newStatus, KeeperException.create(code, path));
            }
        }
    };


    Watcher newTaskWatcher = new Watcher() {
        public void process(WatchedEvent event) {
            if (event.getType() == Event.EventType.NodeChildrenChanged) {
                assert ("/assign/" + name).equals(event.getPath());
                log.info("Tasks assigned to me have changed. Alerted by watch.");
                getTasks();
            }
        }
    };

    void getTasks() {
        zooKeeper.getChildren("/assign/" + name, newTaskWatcher, tasksGetChildrenCallback, null);
    }

    AsyncCallback.ChildrenCallback tasksGetChildrenCallback = new AsyncCallback.ChildrenCallback() {
        @Override
        public void processResult(int rc, String path, Object ctx, List<String> childrenOfTasks) {
            KeeperException.Code code = KeeperException.Code.get(rc);
            switch (code) {
                case CONNECTIONLOSS:
                    getTasks();
                    break;
                case OK:
                    if (childrenOfTasks != null) {
                        log.info("Have " + childrenOfTasks.size() + " tasks assigned to me.");
                        RunnableTasks runnableTasks = new RunnableTasks(childrenOfTasks, taskDataCallback);
                        executor.execute(runnableTasks);
                    }
                    break;
                default:
                    log.error("Error trying to call getChildren on " + path, KeeperException.create(code, path));
            }
        }
    };

    @Override
    public void close() throws IOException {
        log.info("Closing");
        if (zooKeeper != null) {
            try{
                zooKeeper.close();
            } catch (InterruptedException e) {
                log.warn("Interrupted while closing ZooKeeper session", e);
            }
        }
    }

    class RunnableTasks implements Runnable {
        List<String> tasks;
        AsyncCallback.DataCallback dataCallback;

        public RunnableTasks(List<String> children, AsyncCallback.DataCallback dataCallback) {
            this.tasks = children;
            this.dataCallback = dataCallback;
        }

        @Override
        public void run() {
            if (tasks == null) {
                return;
            }
            log.info("Looping into tasks");
            setStatus("Working");
            for (String task: tasks) {
                zooKeeper.getData("/assign/" + name + "/" + task, false, dataCallback, task);
            }
        }
    }

    /**
     * The task is chosen to be print the data.
     */
    class TheTask implements Runnable {
        byte[] data;
        String taskName;

        TheTask(byte[] data, Object context) {
        this.data = data;
        this.taskName = (String) context;
        }

        @Override
        public void run() {
            log.info("Executing the task: " + new String(data));
            setStatusForTaskToDone(taskName);
        }
    }

    AsyncCallback.DataCallback taskDataCallback = new AsyncCallback.DataCallback() {
        @Override
        public void processResult(int rc, String path, Object ctx, byte[] data, Stat stat) {
            KeeperException.Code code = KeeperException.Code.get(rc);
            switch (code) {
                case CONNECTIONLOSS:
                    zooKeeper.getData(path, false, taskDataCallback, null);
                    break;
                case OK:
                    log.info("About to execute a task");
                    TheTask theTask = new TheTask(data, ctx);
                    executor.execute(theTask);
                    log.info("I have executed " + executionCount.incrementAndGet() + " tasks.");
                    break;
                case NONODE:
                    log.warn("The node has been deleted. Either something went wrong or it has already been done.");
                    break;
                default:
                    log.error("Failed to get task data for task: " + path, KeeperException.create(code, path));

            }
        }
    };

    private void setStatusForTaskToDone(String taskName) {
        zooKeeper.create("/status/" + taskName, "done".getBytes(StandardCharsets.UTF_8), OPEN_ACL_UNSAFE,
                CreateMode.PERSISTENT, taskStatusCreateCallback, taskName.getBytes(StandardCharsets.UTF_8));
    }

    private void deleteAssignation(String taskName) {
        zooKeeper.delete("/assign/" + name + "/" + taskName, -1, taskAssignmentDeletionCallback, taskName.getBytes(StandardCharsets.UTF_8));
    }

    AsyncCallback.StringCallback taskStatusCreateCallback = new AsyncCallback.StringCallback() {
        @Override
        public void processResult(int rc, String path, Object ctx, String name) {
            KeeperException.Code code = KeeperException.Code.get(rc);
            String taskName = new String((byte[]) ctx, StandardCharsets.UTF_8);
            switch (code) {
                case CONNECTIONLOSS:
                    setStatusForTaskToDone(taskName);
                    break;
                case OK:
                    log.info("Created status znode correctly for task " + taskName);
                    deleteAssignation(taskName);
                    break;
                case NODEEXISTS:
                    log.warn("Staus node already exists: " + path);
                    break;
                default:
                    log.error("Error trying to set status for task " + taskName, KeeperException.create(code, path));
            }
        }
    };

    AsyncCallback.VoidCallback taskAssignmentDeletionCallback = new AsyncCallback.VoidCallback() {
        @Override
        public void processResult(int rc, String path, Object ctx) {
            KeeperException.Code code = KeeperException.Code.get(rc);
            String taskName = new String((byte[]) ctx, StandardCharsets.UTF_8);
            switch (code) {
                case CONNECTIONLOSS:
                    deleteAssignation(taskName);
                    break;
                case OK:
                    log.info("Successfully deleted assignment of task " + taskName + " to " + name);
                    break;
                default:
                    log.error("Error in trying to delete task assignation of " + taskName + " to " + name, KeeperException.create(code, path));
            }
        }
    };

    public static void main(String[] args) throws InterruptedException, IOException {
        log.info("Worker initialising.");
        Worker worker = new Worker(args[0]);
        worker.startZooKeeper();

        while(!worker.isConnected()){
            Thread.sleep(100);
        }
        log.info("Worker connected.");

        while(!worker.isExpired()){
            Thread.sleep(1000);
        }
        worker.close();
    }
}
