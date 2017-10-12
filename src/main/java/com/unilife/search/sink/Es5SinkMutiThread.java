package com.unilife.search.sink;

import com.google.common.base.Preconditions;
import org.apache.flume.*;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

public class Es5SinkMutiThread extends AbstractSink implements Configurable {
    private final static Logger logger= LoggerFactory.getLogger(Es5SinkMutiThread.class);
    private String hosts;
    private String cluster;
    private int bulkSize;
    private int size = 2000;
    private int indexThreadSize = 5;

    private int commitInterval = 10;

    private ExecutorService service;

    private static ConcurrentMap<String, List<String>> indexData = new ConcurrentHashMap<>();

    @Override
    public void configure(Context context) {
        logger.info("=========== 配置：{}", context);

        hosts = context.getString("hosts");
        Preconditions.checkNotNull(hosts, "hostname must be set !!");

        cluster = context.getString("cluster");
        Preconditions.checkNotNull(cluster, "cluster must be set !!");

        bulkSize = context.getInteger("bulkSize");
        Preconditions.checkNotNull(bulkSize, "size must be set !!");

        Integer size = context.getInteger("size");
        if (size != null && size > 0) {
            this.size = size;
        }

        Integer indexThreadSize = context.getInteger("indexThreadSize");
        if (indexThreadSize != null && indexThreadSize > 0) {
            this.indexThreadSize = indexThreadSize;
        }

        Integer commitInterval = context.getInteger("commitInterval");
        if (commitInterval != null && commitInterval > 0) {
            this.commitInterval = commitInterval;
        }
    }

    @Override
    public void start(){
        logger.info("=============== Es5Sink start running ===============");
        logger.info("hosts = {}", hosts);
        logger.info("cluster = {} ", cluster);
        logger.info("bulkSize = {}", bulkSize);

        logger.info("=============== 开始初始化elasticsearch ===============");
        ClientFactory.init(cluster, hosts);
        ClientFactory.newInstance();
        logger.info("=============== 初始化elasticsearch成功 ===============");
        service = Executors.newFixedThreadPool(indexThreadSize);
        logger.info("=============== 初始化建索引线程池成功 ===============");

        Executors.newSingleThreadScheduledExecutor().scheduleWithFixedDelay(new Runnable() {
            @Override
            public void run() {
                for (Map.Entry<String, List<String>> next : indexData.entrySet()) {
                    String key = next.getKey();
                    String[] strings = key.split("SEPARATE");
                    String index = strings[0];
                    String type = strings[1];
                    List<String> value = next.getValue();
                    if (value.size() <= 0) {
                        continue;
                    }
                    logger.info("周期提交任务开始，索引：{}，数量：{}==========", key, value.size());

                    boolean submit = submit(index, type, value);
                    if (!submit) {
                        logger.info("周期提交任务失败：{}", key);
                    }
                }
            }
        }, commitInterval, commitInterval, TimeUnit.SECONDS);
    }

    @Override
    public Status process() throws EventDeliveryException {

        Status result = Status.READY;
        Channel channel = getChannel();
        Transaction transaction = channel.getTransaction();
        Event event;
        String content;

        transaction.begin();
        try{
            for (int i = 0;i < size;i++) {
                event = channel.take();
                content = new String(event.getBody());
                Map<String, String> headers = event.getHeaders();
                String index = headers.get("data_type");
                String type = headers.get("data_type");
                if (index == null ||
                        index.equals("") ||
                        type == null ||
                        type.equals("")) {
                    continue;
                }
                logger.info("content:" + content);
                putData(index, type, content);
            }
            transaction.commit();
        } catch (Throwable e) {
            logger.error("Fail to show", e);
            transaction.rollback();
            return Status.BACKOFF;
        } finally {
            transaction.close();
        }
        return result;
    }

    private void putData(final String index, final String type, String json) {
        String key = index + "SEPARATE" + type;
        List<String> list = indexData.get(key);
        if (list == null) {
            list = Collections.synchronizedList(new LinkedList<String>());
            indexData.put(key, list);
        }

        list.add(json);

        if (list.size() >= bulkSize) {
            logger.info("数量满足提交任务开始==========");
            boolean submit = submit(index, type, list);
            if (!submit) {
                logger.info("数量满足提交任务失败==========");
            }
        }
    }

    private boolean submit(final String index, final String type, final List<String> finalList) {
        String key = index + "SEPARATE" + type;
        boolean replace = indexData.replace(key, finalList, Collections.synchronizedList(new LinkedList<String>()));
        if (!replace) {
            return false;
        }
        service.submit(new Runnable() {
            @Override
            public void run() {
                long start = System.currentTimeMillis();
                DataManager.bulkCreate(index, type, finalList);
                logger.info("批量提交数据完成，数量：{}，耗时：{}", finalList.size(), System.currentTimeMillis() - start);
            }
        });
        return true;
    }

    @Override
    public void stop(){
        logger.info("function stop running");
    }

    public int size(){
        return size;
    }

}