package com.unilife.search.sink;

import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Created by rembau on 2017/3/16.
 */
public class DataManager {
    private final static Logger logger = LoggerFactory.getLogger(DataManager.class);


    public static BulkResponse bulkCreate(String index, String type, List<String> jsonList) {
        long start = System.currentTimeMillis();
        BulkRequestBuilder bulkRequest = ClientFactory.newInstance().prepareBulk();

        for (String json : jsonList) {
            IndexRequestBuilder requestBuilder = ClientFactory.newInstance().prepareIndex(index, type);
            bulkRequest.add(requestBuilder.setSource(json));
        }
        BulkResponse responses = bulkRequest.execute().actionGet();
        if (responses.hasFailures()) {
            logger.error("批量创建索引，失败信息：{}", responses.buildFailureMessage());
        }
        logger.info("批量创建索引，数量：{}，responses：{}，耗时：" + (System.currentTimeMillis() - start), jsonList.size(), !responses.hasFailures());
        return responses;
    }

}
