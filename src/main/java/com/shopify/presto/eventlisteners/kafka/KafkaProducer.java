/*
 * Copyright © 2017 Tesla Motors, Inc. All rights reserved.
 */

package com.shopify.presto.eventlisteners.kafka;

import org.json.JSONObject;

public interface KafkaProducer {

    void send(String topic, JSONObject record);
}
