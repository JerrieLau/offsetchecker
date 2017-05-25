package cn.bidlink.kafka;

import lombok.Data;

/**
 * Created by jerrie on 17-5-25.
 */
@Data
public class Broker {

    private int jmx_port;

    private long timestamp;

    private String host;

    private int version;

    private int port;


}
