package com.unilife.search.sink;

import org.apache.flume.*;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestMessage extends AbstractSink implements Configurable {
    private final static Logger logger= LoggerFactory.getLogger(TestMessage.class);

    private String hostname;
    private String port;
    private String username;
    private String password;
    private int size = 100;

    public TestMessage(){
        logger.info("Case_1 start...");
    }

    @Override
    public void configure(Context context){

    }

    @Override
    public void start(){
        logger.info("function start running");
        logger.info("hostname: " + hostname);
        logger.info("port: " + port);
        logger.info("username: " + username);
        logger.info("password: " + password);
    }

    @Override
    public void stop(){
        logger.info("function stop running");
    }

    public int size(){
        return size;
    }

    @Override
    public Status process() throws EventDeliveryException{
        logger.info("testMessage ======================================== ");
        Status result = Status.READY;
        Channel channel = getChannel();
        Transaction transaction = channel.getTransaction();
        Event event;
        String content;

        transaction.begin();
        try{
            for(int n=0; n < size; n++){
                event = channel.take();

                if (event == null) {    //超时无数据，默认发一个null数据，作为心跳
                    continue;
                }

                content = new String(event.getBody());
                logger.info("content:"+content);
            }
            transaction.commit();
        } catch (Throwable e) {
            logger.error("Fail to show");
            transaction.rollback();
        } finally {
            transaction.close();
        }
        logger.info("testMessage ======================================== END=============");
        return result;
    }
}