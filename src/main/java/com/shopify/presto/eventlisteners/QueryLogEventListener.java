/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.shopify.presto.eventlisteners;

import com.facebook.presto.spi.eventlistener.EventListener;
import com.facebook.presto.spi.eventlistener.QueryCompletedEvent;
import io.airlift.log.Logger;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.json.JSONObject;

import java.util.Map;
import java.util.Properties;

public class QueryLogEventListener implements EventListener
{
    private static final Logger log = Logger.get(QueryLogEventListener.class);
    private Producer<String, String> producer;
    private static String TOPIC_NAME;

    public QueryLogEventListener(Map<String, String> config) {
        String kafka_broker = config.get("kafka-broker");
        TOPIC_NAME = config.get("kafka-topic-name");


        Properties props = new Properties();
        props.put("bootstrap.servers", kafka_broker + ":9092");
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", StringSerializer.class);
        props.put("value.serializer", StringSerializer.class);

        producer = new KafkaProducer<>(props);
    }

    @Override
    public void queryCompleted(QueryCompletedEvent queryCompletedEvent)
    {
        JSONObject queryEventJson = new JSONObject();

        boolean queryFailed = queryCompletedEvent.getFailureInfo().isPresent();

        queryEventJson.put("query_id", queryCompletedEvent.getMetadata().getQueryId());
        queryEventJson.put("cpu_time", queryCompletedEvent.getStatistics().getCpuTime().getSeconds());
        queryEventJson.put("wall_time", queryCompletedEvent.getStatistics().getWallTime().getSeconds());
        queryEventJson.put("start_time", queryCompletedEvent.getCreateTime().toString());
        queryEventJson.put("end_time", queryCompletedEvent.getEndTime().toString());
        queryEventJson.put("queued_time", queryCompletedEvent.getStatistics().getQueuedTime().getSeconds());
        queryEventJson.put("query_text", queryCompletedEvent.getMetadata().getQuery());
        queryEventJson.put("query_status", queryFailed ? "FAILURE" : "SUCCESS");
        queryEventJson.put("failure_message", queryFailed ? queryCompletedEvent.getFailureInfo().get().getErrorCode().getName() : null);
        queryEventJson.put("user", queryCompletedEvent.getContext().getUser());

        producer.send(new ProducerRecord<>(TOPIC_NAME, queryCompletedEvent.getMetadata().getQueryId(), queryEventJson.toString()));
        log.info("Sending to Kafka: " + queryEventJson.toString());
        log.info("Topic Name:" + TOPIC_NAME);
        log.info("QID " + queryCompletedEvent.getMetadata().getQueryId() + " text `" + queryCompletedEvent.getMetadata().getQuery() + "`");
        log.info("QID " + queryCompletedEvent.getMetadata().getQueryId() + " cpu time (minutes): " + queryCompletedEvent.getStatistics().getCpuTime().getSeconds()/60 + " wall time (minutes): " + queryCompletedEvent.getStatistics().getWallTime().getSeconds()/60.0);
    }
}
