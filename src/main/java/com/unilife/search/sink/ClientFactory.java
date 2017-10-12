package com.unilife.search.sink;

import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.List;

public class ClientFactory {
    private final static Logger logger = LoggerFactory.getLogger(ClientFactory.class);

    private static String clusterName;

    private static List<String> hostList;

    private static TransportClient client;

    public static synchronized TransportClient newInstance() {

        if (client != null) {
            return client;
        }

        Settings settings = Settings.builder().put("cluster.name", clusterName).build();
        //创建client
        try {
            InetSocketTransportAddress transportAddress = null;
            if (hostList == null || hostList.size() < 1) {
                throw new RuntimeException("es地址配置不能为空");
            }
            for (String hostPort : hostList) {
                String ip = hostPort.split(":")[0];
                int port = 9300;
                if (hostPort.split(":").length == 2) {
                    port = Integer.valueOf(hostPort.split(":")[1]);
                }
                transportAddress = new InetSocketTransportAddress(InetAddress.getByName(ip), port);
            }
            client = new PreBuiltTransportClient(settings)
                    .addTransportAddress(transportAddress);
        } catch (UnknownHostException e) {
            logger.error("", e);
            throw new RuntimeException("es地址配置错误");
        }
        return client;
    }

    public static void init(String clusterName, String hosts) {
        if (clusterName == null || "".equals(clusterName) || hosts == null || "".equals(hosts)) {
            throw new RuntimeException("clusterName和hosts不能为空！");
        }
        ClientFactory.clusterName = clusterName;
        ClientFactory.hostList = Arrays.asList(hosts.split(","));
        logger.info("es配置：{}，{}", clusterName, hostList);
    }

    public String getClusterName() {
        return clusterName;
    }

}