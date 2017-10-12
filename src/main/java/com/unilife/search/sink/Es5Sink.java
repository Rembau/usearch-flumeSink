package com.unilife.search.sink;

import com.google.common.base.Preconditions;
import org.apache.flume.*;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.MessageFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class Es5Sink extends AbstractSink implements Configurable {
    private final static Logger logger= LoggerFactory.getLogger(Es5Sink.class);
    private String hosts;
    private String cluster;
    private int bulkSize;
    private int size;

    private int commitInterval = 10;

    private AtomicBoolean atOnce = new AtomicBoolean(false);

    private static ConcurrentMap<String, List<String>> indexData = new ConcurrentHashMap<>();

    @Override
    public void configure(Context context) {
        logger.info("=========== 配置：{}", context);

        hosts = context.getString("hosts");
        Preconditions.checkNotNull(hosts, "hostname must be set !!");

        cluster = context.getString("cluster");
        Preconditions.checkNotNull(cluster, "cluster must be set !!");

        bulkSize = context.getInteger("bulkSize");
        if (bulkSize <= 0) {
            bulkSize = 2000;
        }

        size = context.getInteger("size");
        if (size <= 0) {
            size = 5000;
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

        Executors.newSingleThreadScheduledExecutor().scheduleWithFixedDelay(new Runnable() {
            @Override
            public void run() {
                atOnce.set(true);
            }
        }, commitInterval, commitInterval, TimeUnit.SECONDS);
    }

    @Override
    public Status process() throws EventDeliveryException {

        logger.info("开始process=====================");
        Status result = Status.READY;
        Channel channel = getChannel();
        Transaction transaction = channel.getTransaction();
        Event event = null;
        String content;
        MessageFormat messageFormat = new MessageFormat("{0}'{'{1}'}'");

        transaction.begin();
        try{
            for (int i = 0;i < size;i++) {
                if (atOnce.compareAndSet(true, false)) {
                    break;
                }

                event = channel.take();
                if (event == null) {    //超时无数据，默认发一个null数据，作为心跳
                    continue;
                }
                content = new String(event.getBody());
                logger.debug("header:{}=======content:{}", event.getHeaders(), content);
                Map<String, String> headers = event.getHeaders();

                String application = headers.get("application");
                if (application == null) {
                    application = "";
                }
                String index = headers.get("index");
                if (index == null || "".equals(index)) {
                    continue;
                }

                if (index.matches(".+\\{.+}")) {
                    try {
                        Object[] objects = messageFormat.parse(index);
                        SimpleDateFormat simpleDateFormat = new SimpleDateFormat(objects[1].toString());
                        index = objects[0] + simpleDateFormat.format(new Date());
                    } catch (ParseException e) {
                        logger.error("", e);
                    }
                }

                index = application + "-" + index;
                String type = headers.get("type");
                if (type == null ||
                        type.equals("")) {
                    continue;
                }

                putData(index, type, content);
            }

            boolean submit = submit();

            if (!submit) {
                transaction.rollback();
                return Status.BACKOFF;
            }

            transaction.commit();
        } catch (Throwable e) {
            logger.error("Fail:" + event, e);
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
    }

    private boolean submit() {
        for (Map.Entry<String, List<String>> next : indexData.entrySet()) {
            String key = next.getKey();
            String[] strings = key.split("SEPARATE");
            String index = strings[0];
            String type = strings[1];
            List<String> value = next.getValue();
            if (value.size() <= 0) {
                continue;
            }
            logger.info("提交任务开始，索引：{}，数量：{}==========", key, value.size());

            boolean submit = submitIndex(index, type, value);
            if (!submit) {
                logger.info("提交任务失败：{}", key);
                return false;
            }
        }
        return true;
    }

    private boolean submitIndex(final String index, final String type, final List<String> finalList) {
        String key = index + "SEPARATE" + type;
        List<String> newValue = Collections.synchronizedList(new LinkedList<String>());
        boolean replace = indexData.replace(key, finalList, newValue);
        if (!replace) {
            return false;
        }

        long start = System.currentTimeMillis();
        List<String> commitList = new LinkedList<>();
        for (int i = 0;i < finalList.size();i++) {
            if (i > 0 && i % bulkSize == 0) {
                DataManager.bulkCreate(index, type, commitList);
                commitList.clear();
            }
            commitList.add(finalList.get(i));
        }
        if (commitList.size() != 0) {
            DataManager.bulkCreate(index, type, commitList);
        }

        logger.info("批量提交数据完成，数量：{}，耗时：{}", finalList.size(), System.currentTimeMillis() - start);

        return true;
    }

    @Override
    public void stop(){
        logger.info("function stop running");
    }
}