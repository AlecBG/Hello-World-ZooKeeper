import lombok.extern.log4j.Log4j;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import java.io.Closeable;
import java.io.IOException;
import java.util.Date;

@Log4j
public class AdminClient implements Watcher, Closeable {

    private final String hostPort;
    private ZooKeeper zooKeeper;

    AdminClient(String hostPort) {
        this.hostPort = hostPort;
    }

    void startZooKeeper() throws IOException {
        zooKeeper = new ZooKeeper(hostPort, 15_000, this);
    }

    public void close() {
        log.info("Closing");
        if (zooKeeper != null) {
            try{
                zooKeeper.close();
            } catch (InterruptedException e) {
                log.warn("Interrupted while closing ZooKeeper session", e);
            }
        }
    }

    public void listState() throws KeeperException, InterruptedException {
        try {
            Stat stat = new Stat();
            byte[] masterData = zooKeeper.getData("/master", false, stat);
            Date startDate = new Date(stat.getCtime());
            log.info("Master: " + new String(masterData) + " since " + startDate);
        } catch (KeeperException.NoNodeException e) {
            log.info("No master");
        }

        log.info("Workers:");
        for (String worker: zooKeeper.getChildren("/workers", false)) {
            byte[] data = zooKeeper.getData("/workers/" + worker, false, null);
            String state = new String(data);
            log.info("\t" + worker + state);
        }

        log.info("Tasks:");
        for (String t: zooKeeper.getChildren("/assign", false)) {
            log.info("\t" + t);
        }
    }

    public void process(WatchedEvent e) {
        log.info(e.toString() + ", " + hostPort);
    }

    public static void main(String[] args) throws IOException, KeeperException, InterruptedException {
        AdminClient adminClient = new AdminClient(args[0]);
        adminClient.startZooKeeper();
        adminClient.listState();
        adminClient.close();
    }
}
