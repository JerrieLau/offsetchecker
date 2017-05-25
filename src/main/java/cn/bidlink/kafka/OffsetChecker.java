package cn.bidlink.kafka;

import com.google.gson.Gson;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Hello world!
 *
 * @author :<a href="mailto:liujie@ebnew.com">刘杰</a>
 * @date :2017-05-25 13:19:04
 */
public class OffsetChecker {

    /**
     * The constant connectStr.
     *
     * @author :<a href="mailto:liujie@ebnew.com">刘杰</a>
     * @date :2017-05-25 13:19:04
     */
    private static String connectStr = "10.4.0.74:2181,10.4.0.75:2181,10.4.0.76:2181/dckafka";
    /**
     * The constant topic.
     *
     * @author :<a href="mailto:liujie@ebnew.com">刘杰</a>
     * @date :2017-05-25 13:19:04
     */
    private static String topic = "bidlink_canal";
    /**
     * The constant consumerGroup.
     *
     * @author :<a href="mailto:liujie@ebnew.com">刘杰</a>
     * @date :2017-05-25 13:19:04
     */
    private static String consumerGroup = "dc-daemon-test";

    /**
     * The constant zooKeeper.
     *
     * @author :<a href="mailto:liujie@ebnew.com">刘杰</a>
     * @date :2017-05-25 13:19:04
     */
    private static ZooKeeper zooKeeper;
    /**
     * The constant offsetMap.
     *
     * @author :<a href="mailto:liujie@ebnew.com">刘杰</a>
     * @date :2017-05-25 13:19:04
     */
    private static Map<Integer, Offset> offsetMap = new HashMap<>();

    /**
     * The entry point of application.
     *
     * @param args the input arguments
     * @throws IOException          the io exception
     * @throws KeeperException      the keeper exception
     * @throws InterruptedException the interrupted exception
     * @author :<a href="mailto:liujie@ebnew.com">刘杰</a>
     * @date :2017-05-25 13:19:04
     */
    public static void main(String[] args) throws IOException, KeeperException, InterruptedException {
        //检测参数
        checkArgs(args);

        createZkConnect();

        //获取topic分区信息
        getTopicPartitionMsg(zooKeeper);

        //获取消费者消费位置
        getConsumerOffsetMsg(zooKeeper);

        //获取logSize
        getLogSize(zooKeeper);

        //计算Lag
        calculateLag();

        //打印结果
        printOffsetCheckMsg();

    }

    private static void createZkConnect() {
        try {
            zooKeeper = new ZooKeeper(connectStr, 600000, null);
        } catch (IOException e) {
            throw new IllegalArgumentException("按指定的zookeeper.connect不能建立连接，请检查指定的zookeeper.connect是否正确，请参考kafka中的zookeeper.connect参数!");
        }
    }

    /**
     * Check args.
     *
     * @param args the args
     * @author :<a href="mailto:liujie@ebnew.com">刘杰</a>
     * @date :2017-05-25 13:49:52
     */
    private static void checkArgs(String[] args) {
        if (args == null || args.length < 3) {
            throw new IllegalArgumentException("请指定正确的参数，需要指定zookeeper连接信息（kafka中的zookeeper.connect参数）、topic(消息主题)、消费者分组。" +
                    "\n格式: java -jar offsetchecker-1.0-SNAPSHOT-jar-with-dependencies.jar zookeeper连接信息 消息主题 消费者分组" +
                    "\n示例: java -jar offsetchecker-1.0-SNAPSHOT-jar-with-dependencies.jar 10.4.0.74:2181,10.4.0.75:2181,10.4.0.76:2181/dckafka bidlink_canal dc-daemon-test");
        }
        connectStr = args[0];
        topic = args[1];
        consumerGroup = args[2];
    }

    /**
     * Print offset check msg.
     *
     * @author :<a href="mailto:liujie@ebnew.com">刘杰</a>
     * @date :2017-05-25 13:26:01
     */
    private static void printOffsetCheckMsg() {
        System.out.println("\n\n===================检测结果=================");
        System.out.println("分区\t消息总数(条)\t已消费数(条)\t积压数(条)");
        long totalSize = 0;
        long totalConsumedSize = 0;
        for (Offset offset : offsetMap.values()) {
            totalSize += offset.getLogSize();
            totalConsumedSize += offset.getConsumerOffset();
            System.out.println(String.format("%3d\t%8d\t%8d\t%8d", offset.getPartition(), offset.getLogSize(), offset.getConsumerOffset(), offset.getLag()));
        }
        System.out.printf("\n\n汇总: 消息总共%d条，已消费%d条，积压%d条.\n", totalSize, totalConsumedSize, (totalSize - totalConsumedSize));
    }

    /**
     * Calculate lag.
     *
     * @author :<a href="mailto:liujie@ebnew.com">刘杰</a>
     * @date :2017-05-25 13:24:33
     */
    private static void calculateLag() {
        for (Offset offset : offsetMap.values()) {
            offset.setLag(offset.getLogSize() - offset.getConsumerOffset());
        }
    }

    /**
     * Gets log size.
     *
     * @param zooKeeper the zoo keeper
     * @throws KeeperException              the keeper exception
     * @throws InterruptedException         the interrupted exception
     * @throws UnsupportedEncodingException the unsupported encoding exception
     * @author :<a href="mailto:liujie@ebnew.com">刘杰</a>
     * @date :2017-05-25 13:21:45
     */
    private static void getLogSize(ZooKeeper zooKeeper) throws KeeperException, InterruptedException, UnsupportedEncodingException {
        String path = "/brokers/ids";
        Stat stat = zooKeeper.exists(path, false);
        if(stat == null) {
            throw new IllegalArgumentException(String.format("从\"%s\"上查询出brokers的实例节点不存在，请检查指定的zookeeper.connect连接地址是否正确，zookeeper.connect必须跟kafka配置文件server.xml中的zookeeper.connect一致!", connectStr));
        }

        List<String> brokers = zooKeeper.getChildren(path, false, null);
        Gson gson = new Gson();
        for (String brokerId : brokers) {
            //获取broker信息
            byte[] data = zooKeeper.getData("/brokers/ids/" + brokerId, false, null);
            Broker broker = gson.fromJson(new String(data, "UTF-8"), Broker.class);

            if (broker != null) {
                //获取logSize
                try {
                    for (Offset offset : offsetMap.values()) {
                        long logSize = KafkaHelper.getLogSize(broker.getHost(), broker.getPort(), topic, offset.getPartition());
                        offset.setLogSize(logSize);
                    }
                    break;
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }

    /**
     * Gets consumer offset msg.
     *
     * @param zooKeeper the zoo keeper
     * @throws KeeperException              the keeper exception
     * @throws InterruptedException         the interrupted exception
     * @throws UnsupportedEncodingException the unsupported encoding exception
     * @author :<a href="mailto:liujie@ebnew.com">刘杰</a>
     * @date :2017-05-25 13:19:04
     */
    private static void getConsumerOffsetMsg(ZooKeeper zooKeeper) throws KeeperException, InterruptedException, UnsupportedEncodingException {
        String path = "/consumers/" + consumerGroup + "/offsets/" + topic;
        Stat stat = zooKeeper.exists(path, false);
        if (stat == null) {
            throw new IllegalArgumentException(String.format("从\"%s\"上根据指定的消息费者组（%s）查询出Topic（%s）的节点不存在，请核对消费者分组是否正确!", connectStr, consumerGroup, topic));
        }

        List<String> consumerPartitions = zooKeeper.getChildren(path, false, null);
        if (consumerPartitions == null) {
            throw new IllegalArgumentException(String.format("从\"%s\"上根据指定的消息费者组（%s）查询出Topic（%s）的消费者位置为空，请核对消费者分组是否正确!", connectStr, consumerGroup, topic));
        }
        for (String consumerPartition : consumerPartitions) {
            String subPath = "/consumers/" + consumerGroup + "/offsets/" + topic + "/" + consumerPartition;
            byte[] data = zooKeeper.getData(subPath, false, null);
            if (data != null && data.length > 0) {
                String consumerOffset = new String(data, "UTF-8");
                Offset offset = offsetMap.get(Integer.valueOf(consumerPartition));
                if (offset != null) {
                    offset.setConsumerOffset(Long.valueOf(consumerOffset));
                } else {
                    throw new IllegalArgumentException(String.format("从\"%s\"上的消息费者组（%s）查询出的分区（%s）在Broker下的分区列表中未找到，请检查是否重新进行了分区!", connectStr, consumerGroup, consumerPartition));
                }
            } else {
                throw new IllegalArgumentException(String.format("从\"%s\"上根据指定的消息费者组（%s）查询出Topic（%s）下分区（%s）的消费记录为空，请检测是否重新进行了分区!", connectStr, consumerGroup, topic, consumerPartition));
            }
        }
    }

    /**
     * Gets topic partition msg.
     *
     * @param zooKeeper the zoo keeper
     * @throws KeeperException      the keeper exception
     * @throws InterruptedException the interrupted exception
     * @author :<a href="mailto:liujie@ebnew.com">刘杰</a>
     * @date :2017-05-25 13:17:42
     */
    private static void getTopicPartitionMsg(ZooKeeper zooKeeper) throws KeeperException, InterruptedException {
        String path = "/brokers/topics/" + topic + "/partitions";
        Stat stat = zooKeeper.exists(path, false);
        if (stat == null) {
            throw new IllegalArgumentException(String.format("从\"%s\"中查询Topic（%s）的分区节点不存在，请检查Topic是否正确!", connectStr, topic));
        }

        List<String> partitionList = zooKeeper.getChildren(path, false, null);
        if (partitionList == null || partitionList.size() <= 0) {
            throw new IllegalArgumentException(String.format("从\"%s\"上的brokers中获取Topic（%s）的分区信息为空，请检查指定的zookeeper.connect连接地址是否正确，zookeeper.connect必须跟kafka配置文件server.xml中的zookeeper.connect一致!", connectStr, topic));
        }
        for (String partitionStr : partitionList) {
            Integer partition = Integer.valueOf(partitionStr);
            Offset offset = new Offset();
            offset.setPartition(partition);
            offset.setTopic(topic);

            offsetMap.put(partition, offset);
        }
    }
}
