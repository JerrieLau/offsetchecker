package cn.bidlink.kafka;

import lombok.Data;

/**
 * Created by jerrie on 17-5-25.
 *
 * @author :<a href="mailto:liujie@ebnew.com">刘杰</a>
 * @date :2017-05-25 14:22:10
 */
@Data
public class Offset {

    /**
     * The Topic.
     *
     * @author :<a href="mailto:liujie@ebnew.com">刘杰</a>
     * @date :2017-05-25 14:22:10
     */
    private String topic;

    /**
     * The Partition.
     *
     * @author :<a href="mailto:liujie@ebnew.com">刘杰</a>
     * @date :2017-05-25 14:22:10
     */
    private int partition;

    /**
     * The Log size.
     *
     * @author :<a href="mailto:liujie@ebnew.com">刘杰</a>
     * @date :2017-05-25 14:22:10
     */
    private long logSize;

    /**
     * The Consumer offset.
     *
     * @author :<a href="mailto:liujie@ebnew.com">刘杰</a>
     * @date :2017-05-25 14:22:10
     */
    private long consumerOffset;

    /**
     * The Lag.
     *
     * @author :<a href="mailto:liujie@ebnew.com">刘杰</a>
     * @date :2017-05-25 14:22:10
     */
    private long lag;

}
