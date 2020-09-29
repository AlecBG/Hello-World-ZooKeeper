import lombok.Getter;
import lombok.extern.log4j.Log4j;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.Closeable;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.stream.Collectors;

import static org.apache.zookeeper.ZooDefs.Ids.OPEN_ACL_UNSAFE;

@Log4j
public class Master implements Watcher, Closeable {

    private static final Random RNG = new Random();

    private final String serverId = Integer.toString(new Random().nextInt());
    private final String hostPort;

    @Getter private volatile MasterStates state = MasterStates.RUNNING;
    private ZooKeeper zooKeeper;
    ChildrenCache workersCache;
    @Getter private volatile boolean connected = false;
    @Getter private volatile boolean expired = false;
    private volatile boolean assigningTasks = false;

    Master(String hostPort) {
        this.hostPort = hostPort;
    }

    void startZooKeeper() throws IOException {
        zooKeeper = new ZooKeeper(hostPort, 15_000, this);
    }

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

    void stopZooKeeper() throws InterruptedException, IOException {
        zooKeeper.close();
    }

    @Override
    public void process(WatchedEvent watchedEvent) {
        log.info(watchedEvent.toString() + ", " + hostPort);
        if(watchedEvent.getType() == Event.EventType.None){
            switch (watchedEvent.getState()) {
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
                    log.error("Session expiration");
                default:
                    break;
            }
        }
    }

    public void bootstrap() {
        createParent("/workers", new byte[0], createWorkersCallback);
    }

    private void createParent(String path, byte[] data, AsyncCallback.StringCallback createParentCallback) {
        zooKeeper.create(path, data, OPEN_ACL_UNSAFE, CreateMode.PERSISTENT, createParentCallback, data);
    }

    private AsyncCallback.StringCallback createWorkersCallback = new AsyncCallback.StringCallback() {
        @Override
        public void processResult(int rc, String path, Object ctx, String name) {
            switch (KeeperException.Code.get(rc)) {
                case CONNECTIONLOSS:
                    createParent(path, (byte[]) ctx, createWorkersCallback);
                    break;

                case OK:
                    log.info("Parent " + path + " created");
                    createParent("/assign", new byte[0], createAssignCallback);
                    break;

                case NODEEXISTS:
                    log.warn("Parent already registered " + path);
                    createParent("/assign", new byte[0], createAssignCallback);
                    break;

                default:
                    log.error("Error creating znode: ", KeeperException.create(KeeperException.Code.get(rc), path));
            }
        }
    };


    private AsyncCallback.StringCallback createAssignCallback = new AsyncCallback.StringCallback() {
        @Override
        public void processResult(int rc, String path, Object ctx, String name) {
            switch (KeeperException.Code.get(rc)) {
                case CONNECTIONLOSS:
                    createParent(path, (byte[]) ctx, createAssignCallback);
                    break;

                case OK:
                    log.info("Parent " + path + " created");
                    createParent("/tasks", new byte[0], createTasksCallback);
                    break;

                case NODEEXISTS:
                    log.warn("Parent already registered " + path);
                    createParent("/tasks", new byte[0], createTasksCallback);
                    break;

                default:
                    log.error("Error creating znode: ", KeeperException.create(KeeperException.Code.get(rc), path));
            }
        }
    };

    private AsyncCallback.StringCallback createTasksCallback = new AsyncCallback.StringCallback() {
        @Override
        public void processResult(int rc, String path, Object ctx, String name) {
            switch (KeeperException.Code.get(rc)) {
                case CONNECTIONLOSS:
                    createParent(path, (byte[]) ctx, createTasksCallback);
                    break;

                case OK:
                    log.info("Parent " + path + " created");
                    createParent("/status", new byte[0], createStatusCallback);
                    break;

                case NODEEXISTS:
                    log.warn("Parent already registered " + path);
                    createParent("/status", new byte[0], createStatusCallback);
                    break;

                default:
                    log.error("Error creating znode: ", KeeperException.create(KeeperException.Code.get(rc), path));
            }
        }
    };

    private AsyncCallback.StringCallback createStatusCallback = new AsyncCallback.StringCallback() {
        @Override
        public void processResult(int rc, String path, Object ctx, String name) {
            switch (KeeperException.Code.get(rc)) {
                case CONNECTIONLOSS:
                    createParent(path, (byte[]) ctx, createStatusCallback);
                    break;

                case OK:
                    log.info("Parent " + path + " created");
                    runForMaster();
                    break;

                case NODEEXISTS:
                    log.warn("Parent already registered " + path);
                    runForMaster();
                    break;

                default:
                    log.error("Error creating znode: ", KeeperException.create(KeeperException.Code.get(rc), path));
            }
        }
    };

    void runForMaster() {
        zooKeeper.create("/master", serverId.getBytes(StandardCharsets.UTF_8), OPEN_ACL_UNSAFE,
                CreateMode.EPHEMERAL, masterCreateCallback, null);
    }

    AsyncCallback.StringCallback masterCreateCallback = new AsyncCallback.StringCallback() {
        @Override
        public void processResult(int rc, String path, Object ctx, String name) {
            switch(KeeperException.Code.get(rc)) {
                case CONNECTIONLOSS:
                    checkMaster();
                    return;
                case OK:
                    state = MasterStates.ELECTED;
                    // Get relevant info and set-up watches for changes on tasks and workers.
                    getTasks();
                    getWorkers();
                    break;
                case NODEEXISTS:
                    state = MasterStates.NOT_ELECTED;
                    log.info("It seems that someone else is the leader");
                    masterExists();  // Sets a watch on master.
                    break;
                default:
                    state = MasterStates.NOT_ELECTED;
                    log.error("Something went wrong when running for master.", KeeperException.create(KeeperException.Code.get(rc), path));
            }
            log.info("I'm " + (state == MasterStates.ELECTED ? "" : "not ") + "the leader.");
        }
    };

    void checkMaster() {
        zooKeeper.getData("/master", false, masterCheckCallBack, null);
    }

    AsyncCallback.DataCallback masterCheckCallBack = new AsyncCallback.DataCallback() {
        @Override
        public void processResult(int rc, String path, Object ctx, byte[] data, Stat stat) {
            switch(KeeperException.Code.get(rc)) {
                case CONNECTIONLOSS:
                    checkMaster();
                    return;
                case NONODE:
                    runForMaster();
            }
        }
    };

    void masterExists() {
        zooKeeper.exists("/master", masterExistsWatcher, masterExistsCallback, null);
    }

    Watcher masterExistsWatcher = new Watcher() {
        @Override
        public void process(WatchedEvent event) {
            if (event.getType() == Event.EventType.NodeDeleted) {
                assert "/master".equals(event.getPath());
                runForMaster();  // If /master znode has been deleted, run for master.
            }
        }
    };

    AsyncCallback.StatCallback masterExistsCallback = new AsyncCallback.StatCallback() {

        @Override
        public void processResult(int rc, String path, Object ctx, Stat stat) {
            switch (KeeperException.Code.get(rc)) {
                case CONNECTIONLOSS:
                    masterExists();
                    break;
                case OK:
                    break;
                case NONODE:
                    state = MasterStates.RUNNING;
                    log.info("It seems like there is no master, so I am running for master.");
                    runForMaster();
                    break;
                default:
                    checkMaster();
                    break;
            }
        }
    };

    void getTasks() {
        zooKeeper.getChildren("/tasks", tasksChangeWatcher, tasksGetChildrenCallback, null);
    }

    Watcher tasksChangeWatcher = new Watcher() {
        @Override
        public void process(WatchedEvent event) {
            if (event.getType() == Event.EventType.NodeChildrenChanged) {
                assert "/tasks".equals(event.getPath());
                log.info("Children of tasks have changed. Alerted by watch.");
                getTasks();
            }
        }
    };

    AsyncCallback.ChildrenCallback tasksGetChildrenCallback = new AsyncCallback.ChildrenCallback() {
        @Override
        public void processResult(int rc, String path, Object ctx, List<String> childrenOfTasks) {
            switch (KeeperException.Code.get(rc)) {
                case CONNECTIONLOSS:
                    getTasks();
                    break;
                case OK:
                    if (childrenOfTasks != null) {
                        if (assigningTasks) {
                            getTasks();
                            break;
                        }
                        assigningTasks = true;
                        log.info("Assigning " + childrenOfTasks.size() + " tasks to workers.");
                        getDataOfTasks(childrenOfTasks);
                    }
                    break;
                default:
                    log.error("getChildren failed.", KeeperException.create(KeeperException.Code.get(rc), path));
            }
        }
    };

    void getWorkers() {
        zooKeeper.getChildren("/workers", workersChangeWatcher, workersGetChildrenCallback, null);
    }

    Watcher workersChangeWatcher = new Watcher() {
        @Override
        public void process(WatchedEvent event) {
            if (event.getType() == Event.EventType.NodeChildrenChanged) {
                assert "/workers".equals(event.getPath());
                log.info("Children of /workers has changed. Alerted by watch.");
                getWorkers();
            }
        }
    };

    AsyncCallback.ChildrenCallback workersGetChildrenCallback = new AsyncCallback.ChildrenCallback() {
        @Override
        public void processResult(int rc, String path, Object ctx, List<String> workers) {
            switch (KeeperException.Code.get(rc)) {
                case CONNECTIONLOSS:
                    getWorkers();
                    break;
                case OK:
                    log.info("Successfully got a list of workers: " + workers.size() + " workers.");
                    reassignAndSet(workers);
                    break;
                default:
                    log.error("getChildren failed on /workers", KeeperException.create(KeeperException.Code.get(rc), path));
            }
        }
    };

    /**
     * See if any workers have died. If so, reassign their tasks.
     *
     * @param workers The current list of workers.
     */
    void reassignAndSet(List<String> workers) {
        List<String> removedWorkers;
        if (workersCache == null) {
            workersCache = new ChildrenCache(workers);
            removedWorkers = null;
        } else {
            log.info("Removing and setting workers");
            removedWorkers = workersCache.removedAndSet(workers);
        }

        if (removedWorkers != null) {
            log.info("Have " + removedWorkers.size() + " removed workers. I am reassigning their tasks.");
            for (String worker: removedWorkers) {
                getAbsentWorkerTasks(worker);  // We need to reassign absent worker tasks.
            }
        }
    }

    /**
     * Get the tasks assigned to worker.
     *
     * @param worker Absent worker
     */
    void getAbsentWorkerTasks(String worker) {
        zooKeeper.getChildren("/assign/" + worker, false, workerAssignmentCallback, null);
    }

    AsyncCallback.ChildrenCallback workerAssignmentCallback = new AsyncCallback.ChildrenCallback() {
        @Override
        public void processResult(int rc, String path, Object ctx, List<String> assignedTasks) {
            KeeperException.Code code = KeeperException.Code.get(rc);
            switch (code) {
                case CONNECTIONLOSS:
                    getAbsentWorkerTasks(path);
                    break;
                case OK:
                    log.info("Successfully got a list of assignments: " + assignedTasks.size() + " tasks");

                    for (String task: assignedTasks) {
                        getDataReassign(path + "/" + task, task);
                    }
                    break;
                default:
                    log.error("getChildren failed on tasks of " + path, KeeperException.create(code, path));
            }
        }
    };

    /**
     * Get reassigned task data.
     *
     * @param path Path of assigned task
     * @param task Task name excluding the path prefix
     */
    void getDataReassign(String path, String task) {
        zooKeeper.getData(path, false, getDataReassignCallback, task);
    }

    AsyncCallback.DataCallback getDataReassignCallback = new AsyncCallback.DataCallback() {
        @Override
        public void processResult(int rc, String path, Object ctx, byte[] data, Stat stat) {
            KeeperException.Code code = KeeperException.Code.get(rc);
            String task = (String) ctx;
            switch (code) {
                case CONNECTIONLOSS:
                    getDataReassign(path, task);
                    break;
                case OK:
                    log.info("Have got data of dead worker's task to be reassigned.");
                    recreateTaskOfDeadWorker(new RecreateTaskCtx(path, task, data));
                    break;
                default:
                    log.error("Something went wrong when getting data of task " + task, KeeperException.create(code, path));
            }
        }
    };

    /**
     * Recreate the task that was assigned to worker that died before it finished the task.
     *
     * @param recreateTaskContext The necessary information for the task.
     */
    void recreateTaskOfDeadWorker(RecreateTaskCtx recreateTaskContext) {
        zooKeeper.create("/tasks/" + recreateTaskContext.task, recreateTaskContext.data, OPEN_ACL_UNSAFE,
                CreateMode.PERSISTENT, recreateTaskOfDeadWorkerCallback, recreateTaskContext);
    }

    AsyncCallback.StringCallback recreateTaskOfDeadWorkerCallback = new AsyncCallback.StringCallback() {
        @Override
        public void processResult(int rc, String path, Object ctx, String name) {
            KeeperException.Code code = KeeperException.Code.get(rc);
            RecreateTaskCtx recreateTaskContext = (RecreateTaskCtx) ctx;
            switch (code) {
                case CONNECTIONLOSS:
                    recreateTaskOfDeadWorker(recreateTaskContext);
                    break;
                case OK:
                    log.info("Have successfully put dead worker's task back into /tasks. I am deleting " +
                            "the stale assignment.");
                    deleteAssignment(recreateTaskContext.path);
                    break;
                case NODEEXISTS:
                    log.info("Node exists already, but if it hasn't been deleted, " +
                            "then it will eventually, so we keep trying: " + path);
                    recreateTaskOfDeadWorker(recreateTaskContext);
                default:
                    log.error("Error when trying to recreate task " + recreateTaskContext.task, KeeperException.create(code, path));
            }
        }
    };

    /**
     * Delete assignment of absent worker
     *
     * @param path Path of znode to be deleted
     */
    void deleteAssignment(String path){
        zooKeeper.delete(path, -1, taskAssignmentDeletionCallback, null);
    }

    AsyncCallback.VoidCallback taskAssignmentDeletionCallback = new AsyncCallback.VoidCallback() {
        @Override
        public void processResult(int rc, String path, Object ctx) {
            KeeperException.Code code = KeeperException.Code.get(rc);
            switch (code) {
                case CONNECTIONLOSS:
                    deleteAssignment(path);
                    break;
                case OK:
                    log.info("Task assignment to dead worker correctly deleted: " + path);
                    break;
                case NONODE:
                    log.info("Task assignment to dead worker was already deleted.");
                    break;
                default:
                    log.error("Error when trying to delete task data " + path, KeeperException.create(code, path));
            }
        }
    };

    void getDataOfTasks(List<String> tasks) {
        List<Op> getDataOperations = tasks.stream()
                .map(t -> "/tasks/" + t)
                .map(Op::getData)
                .collect(Collectors.toList());
        zooKeeper.multi(getDataOperations, getTaskDataCallback, tasks);
    }

    AsyncCallback.MultiCallback getTaskDataCallback = new AsyncCallback.MultiCallback() {
        @Override
        public void processResult(int rc, String path, Object ctx, List<OpResult> opResults) {
            KeeperException.Code code = KeeperException.Code.get(rc);
            List<String> tasks = (List<String>) ctx;
            List<byte[]> data = opResults.stream()
                    .map(x -> (OpResult.GetDataResult) x)
                    .map(OpResult.GetDataResult::getData)
                    .collect(Collectors.toList());
            switch (code) {
                case CONNECTIONLOSS:
                    getDataOfTasks(tasks);
                    break;
                case OK:
                    if (workersCache.getList().size() == 0) {
                        log.warn("No workers to assign the tasks to.");
                        try {
                            Thread.sleep(100);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                        getTasks();
                        break;
                    }
                    createAssignments(tasks, data);
                    break;
                default:
                    assigningTasks = false;
                    log.error("Error when trying to get data of tasks: " + tasks, KeeperException.create(code, path));
            }
        }
    };

    void createAssignments(List<String> tasks, List<byte[]> data) {
        assert tasks.size() == data.size();
        List<String> workers = workersCache.getList();
        List<String> assignmentPaths = tasks.stream()
                .map(t -> {
                    int workerIndex = RNG.nextInt(workers.size());
                    String designatedWorker = workers.get(workerIndex);
                    return "/assign/" + designatedWorker + "/" + t;
                })
                .collect(Collectors.toList());
        List<Op> createOperations = new ArrayList<>(data.size());
        for (int i = 0; i < data.size(); i++) {
            createOperations.add(Op.create(assignmentPaths.get(i), data.get(i), OPEN_ACL_UNSAFE, CreateMode.PERSISTENT));
        }
        Object[] pathsData = new Object[2];
        pathsData[0] = tasks;
        pathsData[1] = data;
        zooKeeper.multi(createOperations, createAssignmentsCallback, pathsData);
    }

    AsyncCallback.MultiCallback createAssignmentsCallback = new AsyncCallback.MultiCallback() {
        @Override
        public void processResult(int rc, String path, Object ctx, List<OpResult> opResults) {
            KeeperException.Code code = KeeperException.Code.get(rc);
            Object[] pathsData = (Object[]) ctx;
            List<String> tasks = (List<String>) pathsData[0];
            List<byte[]> data = (List<byte[]>) pathsData[1];
            switch (code) {
                case CONNECTIONLOSS:
                    createAssignments(tasks, data);
                    break;
                case OK:
                    List<String> taskNames = opResults.stream()
                            .map(d -> (OpResult.CreateResult) d)
                            .map(OpResult.CreateResult::getPath)
                            .map(p -> {
                                String[] pathComponents = p.split("/");
                                return pathComponents[pathComponents.length - 1];
                            })
                            .collect(Collectors.toList());

                    log.info("Tasks assigned correctly: " + taskNames);
                    deleteTasks(taskNames);
                    break;
                default:
                    assigningTasks = false;
                    log.error("Error when trying to assign the tasks " + tasks, KeeperException.create(code, path));

            }
        }
    };

    void deleteTasks(List<String> taskNames) {
        List<String> pathsToDelete = taskNames.stream()
                .map(s -> "/tasks/" + s)
                .collect(Collectors.toList());
        List<Op> deleteOperations = pathsToDelete.stream()
                .map(p -> Op.delete(p, -1))
                .collect(Collectors.toList());
        zooKeeper.multi(deleteOperations, deleteTasksCallback, taskNames);
    }

    AsyncCallback.MultiCallback deleteTasksCallback = new AsyncCallback.MultiCallback() {
        @Override
        public void processResult(int rc, String path, Object ctx, List<OpResult> opResults) {
            KeeperException.Code code = KeeperException.Code.get(rc);
            List<String> taskNames = (List<String>) ctx;
            switch (code) {
                case CONNECTIONLOSS:
                    deleteTasks(taskNames);
                    break;
                case OK:
                    assigningTasks = false;
                    log.info("Assigned tasks correctly deleted: " + taskNames);
                    break;
                default:
                    assigningTasks = false;
                    log.error("Error when trying to delete assigned tasks " + taskNames, KeeperException.create(code, path));
            }
        }
    };

    static class RecreateTaskCtx {
        String path;
        String task;
        byte[] data;

        RecreateTaskCtx(String path, String task, byte[] data) {
            this.path = path;
            this.task = task;
            this.data = data;
        }
    }

    public static void main(String[] args) throws Exception {
        log.info("Master initialising.");
        Master master = new Master(args[0]);
        master.startZooKeeper();

        while(!master.isConnected()){
            Thread.sleep(100);
        }
        log.info("Master connected.");

        while(!master.isExpired()){
            Thread.sleep(1000);
        }

        master.stopZooKeeper();
    }
}
