import org.apache.zookeeper.*;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.logging.Logger;

/**
 * Created by jiangjunguo on 16-11-6.
 */
public class LeaderElection extends Thread {

    Logger logger = Logger.getLogger(this.getName());

    ZooKeeper zookeeper;

    private final String HOST = "127.0.0.1:2181";
    private final String PATH = "/leader";
    private final String PARENT_PATH = "/week5";
    private final byte[] DATA = "week5 homework".getBytes();
    private ZooKeeper zooKeeper;

    private String curPath;
    private int curPathNum;
    private List<String> childrens;
    private String prevPath;

    private boolean isLeader = false;
    private Callable<Object> callable;

    public LeaderElection(Callable<Object> callable) {
        this.callable = callable;
    }

    public static void main(String[] args) throws Exception {

        LeaderElection thread1 = new LeaderElection(new Callable<Object>() {
            public Object call() throws Exception {
                // TODO 异步回调
                return null;
            }
        });
        thread1.start();


        System.out.println("isLeader:" + thread1.syncElection());
    }

    // 同步选举
    public boolean syncElection() throws Exception {
        if(!isLeader) {
            synchronized (this) {
                Thread.sleep(2000);
                System.out.println("start");
                this.wait();
            }
        }
        return isLeader;
    }

    // 异步选举
    public boolean asyncElection() {
        return this.isLeader;
    }

    public void run() {
        try {
            // 创建zk客户端对象，并连接zk
            this.zooKeeper = new ZooKeeper(HOST, 5000, new Watcher() {
                public void process(WatchedEvent watchedEvent) {
                    logger.info("Zk connectting");
                }
            });

            // 创建 LeaderElection 节点
            this.curPath = createPath();

            // 获取当前节点的序号
            this.curPathNum = getPathNum(this.curPath);

            // 获取所有节点
            getAllChildrenPath();

            if (this.childrens.size() == 1) {
                // 我是老大
                this.publicLeaderMessage();
            } else {
                // 监听上一节点
                listenPrevPath();
            }

            System.in.read();

            this.zooKeeper.close();


        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (KeeperException e) {
            e.printStackTrace();
        }
    }

    private void getAllChildrenPath() throws KeeperException, InterruptedException {
        this.zooKeeper.sync(this.PARENT_PATH, null, null);
        this.childrens = zooKeeper.getChildren(this.PARENT_PATH, false);
    }

    private void listenPrevPath() throws KeeperException, InterruptedException {
        // 添加上一节点的监听
        getPrevNode();
        zooKeeper.exists(PARENT_PATH + "/" + this.prevPath, new Watcher() {
            public void process(WatchedEvent event) {
                switch (event.getType()) {
                    case NodeDeleted:
                        prevPathWasDeleted();
                        break;
                }
            }
        });
        logger.info(this.getSelfName() + ": I am flower!! " + this.curPath);
        logger.info(this.getSelfName() + ": start listen " + PARENT_PATH + "/" + this.prevPath);
    }

    private void prevPathWasDeleted() {
        if (isPrevPathLeader()) {
            this.publicLeaderMessage();
        } else {
            try {
                this.listenPrevPath();
            } catch (KeeperException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    private String createPath() throws KeeperException, InterruptedException {
        String path = this.zooKeeper.create(PARENT_PATH + PATH, DATA, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
        String[] paths = path.split("/");
        String res = paths[paths.length - 1];
        logger.info("path was created: " + res);
        return res;
    }

    /**
     * 获取上一个节点名称
     *
     * @return
     */
    private void getPrevNode() {
        sortChildrens();
        for (int i = 0; i < this.childrens.size(); i++) {
            if (curPath.equals(this.childrens.get(i))) {
                this.prevPath = this.childrens.get(i - 1);
                return;
            }
        }
        throw new RuntimeException("prev node path not found");
    }

    private void sortChildrens() {
        Collections.sort(this.childrens);
    }

    private void publicLeaderMessage() {
        logger.info(this.getSelfName() + ": I am Leader now!! " + this.curPath);
        this.isLeader = true;
        synchronized (this) {
            this.notify();
        }
        if(null != this.callable){
            try {
                callable.call();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    private int getPathNum(String path) {
        return Integer.valueOf(path.substring(path.length() - 10, path.length()));
    }

    public boolean isPrevPathLeader() {
        try {
            this.getAllChildrenPath();
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        this.sortChildrens();

        if (this.getPathNum(this.prevPath) < this.getPathNum(this.childrens.get(0))) {
            return true;
        }
        return false;
    }

    public String getSelfName() {
        return Thread.currentThread().getName() + "-" + this.curPath;
    }
}
