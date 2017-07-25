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
import java.text.SimpleDateFormat;
import java.util.Date;

public class QueryLogEventListener implements EventListener
{
    private static final Logger log = Logger.get(QueryLogEventListener.class);
    private static String TOPIC_NAME;
    private static String SERVICE_NAME;
    private Producer<String, String> producer;

    public QueryLogEventListener(Map<String, String> config)
    {
        if (!config.containsKey("kafka-broker-list") || !config.containsKey("kafka-topic-name") || !config.containsKey("service-name")) {
            log.warn("Event listener plugin properties are not set.");
            return;
        }
        String kafkaBrokerList = config.get("kafka-broker-list");
        String acksValue = config.getOrDefault("acks", "0");
        Properties props = new Properties();
        TOPIC_NAME = config.get("kafka-topic-name");
        SERVICE_NAME = config.get("service-name");

        props.put("bootstrap.servers", kafkaBrokerList);
        props.put("acks", acksValue);
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
        if (producer == null) {
            log.debug("Kafka Producer is not set. Presto logs will not be written to Kafka!");
            return;
        }
        JSONObject queryEventJson = new JSONObject();
        boolean queryFailed = queryCompletedEvent.getFailureInfo().isPresent();
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");

        queryEventJson.put("service_name", SERVICE_NAME);
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
        queryEventJson.put("event_timestamp", getCurrentTimeStamp(sdf));

        log.debug("Sending " + queryEventJson.toString() + " to Kafka Topic: " + TOPIC_NAME);

        producer.send(new ProducerRecord<>(TOPIC_NAME, queryCompletedEvent.getMetadata().getQueryId(), queryEventJson.toString()));
        log.debug("QID " + queryCompletedEvent.getMetadata().getQueryId() + " text `" + queryCompletedEvent.getMetadata().getQuery() + "`");
        log.debug("QID " + queryCompletedEvent.getMetadata().getQueryId() + " cpu time (minutes): " + queryCompletedEvent.getStatistics().getCpuTime().getSeconds()/60 + " wall time (minutes): " + queryCompletedEvent.getStatistics().getWallTime().getSeconds()/60.0);
    }

    public static String getCurrentTimeStamp(SimpleDateFormat sdf)
    {
        long currentTimeMilliseconds = System.currentTimeMillis();
        Date resultDate = new Date(currentTimeMilliseconds);
        return sdf.format(resultDate);
    }
}
