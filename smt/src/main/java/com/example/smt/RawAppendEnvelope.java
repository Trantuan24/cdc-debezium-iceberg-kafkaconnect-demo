package com.example.smt;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.transforms.Transformation;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.Date;
import java.util.Map;

/**
 * Builds the append-only CDC contract while preserving the original Debezium
 * JSON string in the data field.
 */
public class RawAppendEnvelope<R extends ConnectRecord<R>> implements Transformation<R> {

    private static final Map<String, String> EVENT_NAMES = Map.of(
            "r", "insert",
            "c", "insert",
            "u", "update",
            "d", "delete");

    private static final Schema VALUE_SCHEMA = SchemaBuilder.struct()
            .name("com.example.smt.RawCdcAppendRecordV2")
            .field("loainguon", Schema.STRING_SCHEMA)
            .field("manguondulieu", Schema.STRING_SCHEMA)
            .field("sukien", SchemaBuilder.string().optional().build())
            .field("phienban", Schema.INT32_SCHEMA)
            .field("body", SchemaBuilder.string().optional().build())
            .field("header", SchemaBuilder.string().optional().build())
            .field("data", Schema.STRING_SCHEMA)
            .field("ingest_date", org.apache.kafka.connect.data.Date.SCHEMA)
            .field("ingest_time", org.apache.kafka.connect.data.Timestamp.SCHEMA)
            .build();

    private final JsonConverter envelopeJsonConverter = new JsonConverter();
    private final JsonConverter headerJsonConverter = new JsonConverter();

    @Override
    public R apply(R record) {
        // Debezium tombstones contain no event body and must not create rows.
        if (record.value() == null) {
            return null;
        }
        if (!(record instanceof SinkRecord)) {
            throw new DataException(
                    "RawAppendEnvelope can only be used by a sink connector");
        }
        if (!(record.value() instanceof String)) {
            throw new DataException(
                    "RawAppendEnvelope requires value.converter=org.apache.kafka.connect.storage.StringConverter");
        }

        SinkRecord sinkRecord = (SinkRecord) record;
        if (record.kafkaPartition() == null) {
            throw new DataException("RawAppendEnvelope requires a Kafka partition");
        }

        String data = (String) record.value();
        String body = serializeKey(record.key());
        String headers = serializeHeaders(record.topic(), record.headers());
        String sourceId = record.topic() + "-" + record.kafkaPartition()
                + "-" + sinkRecord.kafkaOffset();
        Instant now = Instant.now().truncatedTo(ChronoUnit.MILLIS);
        Date ingestTime = Date.from(now);
        Date ingestDate = Date.from(now.atZone(ZoneOffset.UTC).toLocalDate()
                .atStartOfDay(ZoneOffset.UTC).toInstant());

        Struct value = new Struct(VALUE_SCHEMA)
                .put("loainguon", "cdc")
                .put("manguondulieu", sourceId)
                .put("sukien", extractEventName(record.topic(), data))
                .put("phienban", 1)
                .put("body", body)
                .put("header", headers)
                .put("data", data)
                .put("ingest_date", ingestDate)
                .put("ingest_time", ingestTime);

        return record.newRecord(
                record.topic(),
                record.kafkaPartition(),
                record.keySchema(),
                record.key(),
                VALUE_SCHEMA,
                value,
                record.timestamp());
    }

    private String extractEventName(String topic, String data) {
        try {
            SchemaAndValue envelope = envelopeJsonConverter.toConnectData(
                    topic, data.getBytes(StandardCharsets.UTF_8));
            if (!(envelope.value() instanceof Struct)) {
                throw new DataException("Debezium JSON envelope must be a struct");
            }

            Struct payload = (Struct) envelope.value();
            if (payload.schema().field("op") == null) {
                throw new DataException("Debezium JSON payload has no op field");
            }

            Object operation = payload.get("op");
            return operation == null ? null : EVENT_NAMES.get(operation.toString());
        } catch (DataException e) {
            throw e;
        } catch (RuntimeException e) {
            throw new DataException("Cannot parse Debezium JSON event", e);
        }
    }

    private String serializeKey(Object key) {
        if (key == null) {
            return "";
        }
        if (!(key instanceof String)) {
            throw new DataException(
                    "RawAppendEnvelope requires key.converter=org.apache.kafka.connect.storage.StringConverter");
        }
        return (String) key;
    }

    private String serializeHeaders(String topic, Iterable<Header> headers) {
        StringBuilder result = new StringBuilder();
        boolean hasHeaders = false;

        for (Header header : headers) {
            if (!hasHeaders) {
                result.append('[');
                hasHeaders = true;
            } else {
                result.append(',');
            }

            result.append("{\"key\":")
                    .append(quoteJson(header.key()))
                    .append(",\"value\":");

            if (header.value() == null) {
                result.append("null");
            } else {
                byte[] value = headerJsonConverter.fromConnectData(
                        topic, header.schema(), header.value());
                result.append(new String(value, StandardCharsets.UTF_8));
            }
            result.append('}');
        }

        return hasHeaders ? result.append(']').toString() : "";
    }

    private static String quoteJson(String value) {
        StringBuilder result = new StringBuilder(value.length() + 2).append('"');
        for (int i = 0; i < value.length(); i++) {
            char character = value.charAt(i);
            switch (character) {
                case '"':
                    result.append("\\\"");
                    break;
                case '\\':
                    result.append("\\\\");
                    break;
                case '\b':
                    result.append("\\b");
                    break;
                case '\f':
                    result.append("\\f");
                    break;
                case '\n':
                    result.append("\\n");
                    break;
                case '\r':
                    result.append("\\r");
                    break;
                case '\t':
                    result.append("\\t");
                    break;
                default:
                    if (character < 0x20) {
                        result.append(String.format("\\u%04x", (int) character));
                    } else {
                        result.append(character);
                    }
            }
        }
        return result.append('"').toString();
    }

    @Override
    public ConfigDef config() {
        return new ConfigDef();
    }

    @Override
    public void configure(Map<String, ?> configs) {
        envelopeJsonConverter.configure(
                Collections.singletonMap("schemas.enable", true), false);
        headerJsonConverter.configure(
                Collections.singletonMap("schemas.enable", false), false);
    }

    @Override
    public void close() {
        envelopeJsonConverter.close();
        headerJsonConverter.close();
    }
}
