/*
 * Copyright © 2017 Tesla Motors, Inc. All rights reserved.
 */

package com.shopify.presto.eventlisteners;

import static com.shopify.presto.eventlisteners.QueryLogEventListener.DATE_FORMAT;

import com.shopify.presto.eventlisteners.kafka.AvroKafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.json.JSONObject;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.Date;
import java.util.UUID;

public class TestAvroTranslation {

    private static final String TOPIC = "some-topic";

    @Test
    public void testBasicQuery() throws Exception {
        sendQuery(getSuccessfulQuery());
    }

    @Test
    public void testQueryWithMetadata() throws Exception {
        JSONObject query = getSuccessfulQuery();
        query.put("query_operation", "EXPLAIN");
        query.put("query_target_table", "dest");
        query.put("query_from_tables", new String[]{"source"});
        sendQuery(query);

        query.put("query_from_tables", new String[]{"source", "other-source"});
        sendQuery(query);
    }

    private void sendQuery(JSONObject query) throws IOException {
        Producer mock = Mockito.mock(Producer.class);
        AvroKafkaProducer producer = new AvroKafkaProducer(mock);
        producer.send(TOPIC,query);
        Mockito.verify(mock).send(Mockito.any(ProducerRecord.class));
    }

    private JSONObject getSuccessfulQuery(){
        JSONObject queryEventJson = new JSONObject();
        queryEventJson.put("service_name", "some service");
        queryEventJson.put("query_id", UUID.randomUUID().toString());
        queryEventJson.put("cpu_time", 100);
        queryEventJson.put("wall_time", 101);
        queryEventJson.put("start_time", "1234");
        queryEventJson.put("end_time", "1235");
        queryEventJson.put("queued_time", 102);
        queryEventJson.put("query_text", "select * from table1");
        queryEventJson.put("query_status", "SUCCESS");
        queryEventJson.put("failure_message", (String) null);
        queryEventJson.put("user", "foo");
        queryEventJson.put("event_timestamp", DATE_FORMAT.format(new Date(System.currentTimeMillis())));
        return queryEventJson;
    }
}
