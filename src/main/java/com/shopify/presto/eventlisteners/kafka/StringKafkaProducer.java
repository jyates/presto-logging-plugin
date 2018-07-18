/*
 * Copyright © 2017 Tesla Motors, Inc. All rights reserved.
 */

package com.shopify.presto.eventlisteners.kafka;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.json.JSONObject;

public class StringKafkaProducer implements KafkaProducer {

    private final Producer<String, String> delegate;

    public StringKafkaProducer(Producer<String, String> delegate) {
        this.delegate = delegate;
    }

    @Override
    public void send(String topic, JSONObject record) {
        String message = record.toString();
        String qid = record.getString("query_id");
        delegate.send(new ProducerRecord<>(topic, qid, message));
    }
}
