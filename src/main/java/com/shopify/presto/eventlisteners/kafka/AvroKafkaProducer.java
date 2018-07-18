/*
 * Copyright © 2017 Tesla Motors, Inc. All rights reserved.
 */

package com.shopify.presto.eventlisteners.kafka;

import com.google.common.collect.ImmutableList;
import com.google.common.io.Resources;
import io.airlift.log.Logger;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

public class AvroKafkaProducer implements KafkaProducer {
    private static final Logger LOG = Logger.get(AvroKafkaProducer.class);

    private final Producer<GenericRecord, GenericRecord> delegate;
    private final GenericRecordBuilder valueBuilder;
    private final GenericRecordBuilder keyBuilder;
    private final Schema keySchema;
    private final Schema valueSchema;

    public AvroKafkaProducer(Producer<GenericRecord, GenericRecord> producer) throws IOException {
        this.delegate = producer;
        this.keySchema = readSchema("query_completed_key.avsc");
        this.keyBuilder = new GenericRecordBuilder(keySchema);
        this.valueSchema = readSchema("query_completed_value.avsc");
        this.valueBuilder = new GenericRecordBuilder(valueSchema);
    }

    @Override
    public void send(String topic, JSONObject record) {
        String query_id = record.getString("query_id");
        keyBuilder.set("query_id", query_id);
        GenericRecord key = buildRecord(keyBuilder, keySchema, record);
        GenericRecord value = buildRecord(valueBuilder, valueSchema, record);
        this.delegate.send(new ProducerRecord<>(topic, key, value));
    }

    private GenericRecord buildRecord(GenericRecordBuilder builder, Schema schema, JSONObject record) {
        for (Schema.Field avroField : schema.getFields()) {
            Object value = getFieldValue(avroField, record);
            if (value != null) {
                try {
                    Schema type = avroField.schema();
                    if (isOfTypeOrOfUnionType(type, Schema.Type.LONG)) {
                        builder.set(avroField.name(), ((Number) value).longValue());
                    } else if (isOfTypeOrOfUnionType(type, Schema.Type.INT)) {
                        builder.set(avroField.name(), ((Number) value).intValue());
                    } else if (isOfTypeOrOfUnionType(type, Schema.Type.DOUBLE)) {
                        builder.set(avroField.name(), ((Number) value).doubleValue());
                    } else if (isOfTypeOrOfUnionType(type, Schema.Type.FLOAT)) {
                        builder.set(avroField.name(), ((Number) value).floatValue());
                    } else if (isOfTypeOrOfUnionType(type, Schema.Type.STRING)) {
                        // your schema says its a string. we won't argue, but just convert it into a string.
                        // this is useful for cases like numbers that won't fit into Long, but are sent unquoted.
                        builder.set(avroField.name(), value.toString());
                    } else if (avroField.schema().getType() == Schema.Type.ARRAY) {
                        if (value instanceof JSONArray) {
                            // just get the simple version of this field as strings. we could be fancies, but
                            // for right now just convert to strings
                            value = ImmutableList.of(((JSONArray)value).toList());
                        } else {
                            // it wasn't an array, even though was defined as one. Let's at least get the string
                            // representation of it and make that what we are storing.
                            value = ImmutableList.of(value.toString());
                        }
                        builder.set(avroField.name(), value);
                    }
                    else{
                        builder.set(avroField.name(), value);
                    }
                } catch (ClassCastException e) {
                    LOG.error("Failed to associate field {} with value {}", avroField, value);
                    throw e;
                }
            }
        }
        return builder.build();
    }

    private Object getFieldValue(Schema.Field field, JSONObject record) {
        Object value = getFieldValue(field.name(), record);
        // maybe one of the aliases has the value instead
        Set<String> aliases = field.aliases();
        if (!aliases.isEmpty()) {
            Iterator<String> aliasIter = aliases.iterator();
            while (value == null && aliasIter.hasNext()) {
                value = record.get(aliasIter.next());
            }
        }
        return value;
    }

    private Object getFieldValue(String fieldName, JSONObject record) {
        try {
            return record.get(fieldName);
        } catch (JSONException e) {
            LOG.debug("Failed to read field {} from record", fieldName);
            return null;
        }
    }

    static boolean isOfTypeOrOfUnionType(Schema schema, Schema.Type type) {
        return schema.getType() == type || (schema.getType() == Schema.Type.UNION && containsType(schema.getTypes(),
            type));
    }

    static boolean containsType(List<Schema> schemas, Schema.Type type) {
        for (Schema schema : schemas) {
            if (schema.getType() == type) {
                return true;
            }
        }
        return false;
    }

    /**
     * Read the {@link Schema} from the file or resource.
     *
     * @throws IOException if the schema cannot be read
     */
    public static Schema readSchema(String file) throws IOException {
        InputStream stream;
        try {
            stream = Resources.getResource(file).openStream();
        } catch (IllegalArgumentException e) {
            // its not a resource? Could be a system path file.
            stream = new FileInputStream(file);
        }
        return new Schema.Parser().parse(stream);
    }

    public static void main(String[] args) throws IOException {
        if(args.length > 0) {
            System.err.println("Check to see if the schema files load from the current classpath");
            return;
        }

        AvroKafkaProducer producer = new AvroKafkaProducer(null);
        System.out.println("Success!");
        System.out.println("Key schema: \n"+producer.keySchema.toString(true));
        System.out.println("Value schema: \n"+producer.valueSchema.toString(true));
    }
}
