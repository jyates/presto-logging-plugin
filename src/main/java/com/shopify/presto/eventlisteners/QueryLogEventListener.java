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
import com.google.cloud.ServiceOptions;
import com.google.pubsub.v1.ProjectTopicName;
import io.airlift.log.Logger;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutureCallback;
import com.google.api.core.ApiFutures;
import com.google.api.gax.rpc.ApiException;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;


import org.json.JSONObject;

import java.io.IOException;
import java.util.Map;
import java.util.Properties;
import java.text.SimpleDateFormat;
import java.util.Date;

public class QueryLogEventListener implements EventListener {
    private static final Logger LOG = Logger.get(QueryLogEventListener.class);
    private static final String PROJECT_ID = ServiceOptions.getDefaultProjectId();
    private static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
    private static String TOPIC_NAME;
    private static String SERVICE_NAME;
    private static boolean USE_KAFKA;
    private static boolean USE_PUBSUB;
    private Producer<String, String> producer;
    private Publisher publisher = null;


    public QueryLogEventListener(Map<String, String> config) {
        USE_KAFKA = Boolean.valueOf(config.getOrDefault("use-kafka", "false"));
        USE_PUBSUB = Boolean.valueOf(config.getOrDefault("use-pubsub", "false"));

        if(!config.containsKey("service-name")) {
            LOG.warn("Logging Plugin requires [service-name] to be set. Logging plugin will be ignored.");
            return;
        }

        if (!USE_KAFKA && !USE_PUBSUB) {
            LOG.warn("Logging Plugin requires [use-kafka] or [use-pubsub] set! Logging plugin will be ignored.");
            return;
        }

        if(USE_KAFKA) {
            producer = kafkaProducer(config);
            if(producer == null) {
                LOG.warn("Kafka Producer not setup. Ignoring Producing to Kafka.");
                USE_KAFKA = false;
            }
        }

        if(USE_PUBSUB) {
            try {
                publisher = pubSubPublisher(config);
            } catch (IOException e) {
                LOG.error("PubSub Publisher not setup. Ignoring Publishing to PubSub.");
                USE_PUBSUB = false;
            }
        }
    }

    private Publisher pubSubPublisher(Map<String, String> config) throws IOException {
        if(!config.containsKey("pubsub-topic-name")) {
            LOG.warn("Logging Plugin requires [pubsub-topic-name] when using PubSub.");
            return null;
        }
        String topicId = config.get("pubsub-topic-name");
        ProjectTopicName topicName = ProjectTopicName.of(PROJECT_ID, topicId);
        LOG.debug("topicName: " + topicName.toString());
        Publisher pub = Publisher.newBuilder(topicName).build();
        return pub;
    }

    private Producer<String, String> kafkaProducer(Map<String, String> config) {
        if (!config.containsKey("kafka-broker-list") || !config.containsKey("kafka-topic-name")) {
            LOG.warn("Logging Plugin requires [kafka-broker-list] and [kafka-topic-name] when using Kafka.");
            return null;
        }
        LOG.info("Setting up Kafka Producer");
        Properties props = new Properties();
        TOPIC_NAME = config.get("kafka-topic-name");
        SERVICE_NAME = config.get("service-name");
        props.put("bootstrap.servers", config.get("kafka-broker-list"));
        props.put("acks", "0"); //must be a string for some reason
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", StringSerializer.class);
        props.put("value.serializer", StringSerializer.class);
        props.put("ssl.client.auth", "requested");
        props.put("security.protocol", config.getOrDefault("security-protocol", "PLAINTEXT"));
        props.put("ssl.keystore.location", config.getOrDefault("ssl-keystore-location", null));
        props.put("ssl.keystore.password", config.getOrDefault("ssl-keystore-password", null));
        props.put("ssl.key.password", config.getOrDefault("ssl-key-password", null));

        return new KafkaProducer<>(props);
    }

    private JSONObject createQueryEventJson(QueryCompletedEvent queryCompletedEvent) {
        JSONObject queryEventJson = new JSONObject();
        boolean queryFailed = queryCompletedEvent.getFailureInfo().isPresent();

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
        queryEventJson.put("event_timestamp", DATE_FORMAT.format(new Date(System.currentTimeMillis())));

        return queryEventJson;
    }

    @Override
    public void queryCompleted(QueryCompletedEvent queryCompletedEvent) {
        String qid = queryCompletedEvent.getMetadata().getQueryId();
        String message = createQueryEventJson(queryCompletedEvent).toString();

        if(USE_KAFKA) {
            LOG.debug("Attempting to send query " + qid + " to Kafka.");
            producer.send(new ProducerRecord<>(TOPIC_NAME, qid, message));
        }

        if(USE_PUBSUB) {
            ByteString data = ByteString.copyFromUtf8(message);
            PubsubMessage pubsubMessage = PubsubMessage.newBuilder()
                    .setData(data)
                    .build();
            ApiFuture<String> future = publisher.publish(pubsubMessage);
            ApiFutures.addCallback(future, new ApiFutureCallback<String>() {

                @Override
                public void onFailure(Throwable throwable) {
                    if (throwable instanceof ApiException) {
                        ApiException apiException = ((ApiException) throwable);
                        // details on the API exception
                        LOG.info("Status Code: " + apiException.getStatusCode().getCode());
                        LOG.info("Retryable: " + apiException.isRetryable());
                    }
                    LOG.error("Error publishing message: " + message);
                }

                @Override
                public void onSuccess(String messageId) {
                    // Once published, returns server-assigned message ids (unique within the topic)
                    LOG.info("submitted message id: " + messageId);
                }
            });
        }
    }
}
