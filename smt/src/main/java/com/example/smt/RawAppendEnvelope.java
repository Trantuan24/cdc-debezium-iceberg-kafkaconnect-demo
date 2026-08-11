package com.example.smt;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.transforms.Transformation;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Map;

/**
 * Wraps an unchanged raw Kafka value in the three-column append-only contract:
 * id, record, ngay_cap_nhat.
 *
 * The connector must use StringConverter for the value so the Debezium JSON
 * reaches this transform without deserialization or reformatting.
 */
public class RawAppendEnvelope<R extends ConnectRecord<R>> implements Transformation<R> {

    private static final Schema VALUE_SCHEMA = SchemaBuilder.struct()
            .name("com.example.smt.RawCdcRecord")
            .field("id", Schema.STRING_SCHEMA)
            .field("record", Schema.STRING_SCHEMA)
            .field("ngay_cap_nhat", Schema.STRING_SCHEMA)
            .build();

    @Override
    public R apply(R record) {
        // Debezium delete tombstones have a null value and no raw JSON body.
        if (record.value() == null) {
            return null;
        }
        if (!(record instanceof SinkRecord)) {
            throw new IllegalArgumentException(
                    "RawAppendEnvelope can only be used by a sink connector");
        }
        if (!(record.value() instanceof String)) {
            throw new IllegalArgumentException(
                    "RawAppendEnvelope requires value.converter=org.apache.kafka.connect.storage.StringConverter");
        }

        SinkRecord sinkRecord = (SinkRecord) record;
        String id = record.topic()
                + "-" + record.kafkaPartition()
                + "-" + sinkRecord.kafkaOffset();

        Struct value = new Struct(VALUE_SCHEMA)
                .put("id", id)
                .put("record", record.value())
                .put("ngay_cap_nhat", Instant.now().truncatedTo(ChronoUnit.MILLIS).toString());

        return record.newRecord(
                record.topic(),
                record.kafkaPartition(),
                record.keySchema(),
                record.key(),
                VALUE_SCHEMA,
                value,
                record.timestamp());
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
