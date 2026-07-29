package com.example.smt;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.transforms.Transformation;

import java.util.HashMap;
import java.util.Map;

/**
 * Maps the Debezium __op field values (c/u/d/r) to the Iceberg sink CDC
 * values (I/U/D). Applied in a sink connector after its unwrap SMT adds __op.
 *
 * Mapping: c/r -> I, u -> U, d -> D.
 */
public class DebeziumOpMapper<R extends ConnectRecord<R>> implements Transformation<R> {

    private static final String OP_FIELD = "__op";

    @Override
    public R apply(R record) {
        if (record.value() == null)
            return record;
        // Schema-based (Struct)
        if (record.valueSchema() != null && record.value() instanceof Struct) {
            Struct value = (Struct) record.value();

            // Check field exists
            if (value.schema().field(OP_FIELD) == null)
                return record;

            String op = value.getString(OP_FIELD);
            if (op == null)
                return record;

            String mapped = mapOp(op);
            if (mapped.equals(op))
                return record; // no change needed

            // Copy all fields into new Struct with updated __op
            Struct newValue = new Struct(record.valueSchema());
            for (Field field : record.valueSchema().fields()) {
                newValue.put(field, value.get(field));
            }
            newValue.put(OP_FIELD, mapped);

            return record.newRecord(
                    record.topic(), record.kafkaPartition(),
                    record.keySchema(), record.key(),
                    record.valueSchema(), newValue,
                    record.timestamp());
        }
        // Schemaless (Map)
        if (record.value() instanceof Map) {
            @SuppressWarnings("unchecked")
            Map<String, Object> value = (Map<String, Object>) record.value();

            Object op = value.get(OP_FIELD);
            if (op == null)
                return record;

            String mapped = mapOp(op.toString());
            if (mapped.equals(op.toString()))
                return record;

            Map<String, Object> newValue = new HashMap<>(value);
            newValue.put(OP_FIELD, mapped);

            return record.newRecord(
                    record.topic(), record.kafkaPartition(),
                    record.keySchema(), record.key(),
                    null, newValue,
                    record.timestamp());
        }

        return record;
    }

    private String mapOp(String op) {
        switch (op) {
            case "c":
            case "r":
                return "I";
            case "u":
                return "U";
            case "d":
                return "D";
            default:
                return op;
        }
    }

    @Override
    public ConfigDef config() {
        return new ConfigDef();
    }

    @Override
    public void configure(Map<String, ?> configs) {
    }

    @Override
    public void close() {
    }
}
