package com.example.smt;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.transforms.Transformation;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Normalizes configured timestamp fields after Debezium unwrap and before the
 * Iceberg sink infers or writes the destination schema.
 *
 * Output format is UTC ISO-8601 seconds: yyyy-MM-ddTHH:mm:ssZ.
 */
public class IsoTimestampNormalizer<R extends ConnectRecord<R>> implements Transformation<R> {

    public static final String FIELDS_CONFIG = "fields";
    private static final DateTimeFormatter FORMATTER =
            DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss'Z'").withZone(ZoneOffset.UTC);
    private static final Pattern JSON_DATE_PATTERN = Pattern.compile("\\\"\\$date\\\"\\s*:\\s*(-?\\d+)");

    private final Map<Schema, Schema> schemaCache = new HashMap<>();
    private Set<String> fields = new HashSet<>();

    @Override
    public R apply(R record) {
        if (record.value() == null || fields.isEmpty()) {
            return record;
        }

        if (record.valueSchema() != null && record.value() instanceof Struct) {
            Struct oldValue = (Struct) record.value();
            Schema newSchema = normalizeSchema(record.valueSchema());
            if (newSchema == record.valueSchema()) {
                return record;
            }

            Struct newValue = new Struct(newSchema);
            for (Field oldField : record.valueSchema().fields()) {
                Object oldFieldValue = oldValue.get(oldField);
                Object newFieldValue = fields.contains(oldField.name())
                        ? toIsoUtcString(oldFieldValue)
                        : oldFieldValue;
                newValue.put(newSchema.field(oldField.name()), newFieldValue);
            }

            return record.newRecord(
                    record.topic(), record.kafkaPartition(),
                    record.keySchema(), record.key(),
                    newSchema, newValue,
                    record.timestamp());
        }

        if (record.value() instanceof Map) {
            @SuppressWarnings("unchecked")
            Map<String, Object> oldValue = (Map<String, Object>) record.value();
            Map<String, Object> newValue = new HashMap<>(oldValue);
            boolean changed = false;
            for (String field : fields) {
                if (newValue.containsKey(field)) {
                    newValue.put(field, toIsoUtcString(newValue.get(field)));
                    changed = true;
                }
            }
            if (!changed) {
                return record;
            }
            return record.newRecord(
                    record.topic(), record.kafkaPartition(),
                    record.keySchema(), record.key(),
                    null, newValue,
                    record.timestamp());
        }

        return record;
    }

    private Schema normalizeSchema(Schema oldSchema) {
        Schema cached = schemaCache.get(oldSchema);
        if (cached != null) {
            return cached;
        }

        boolean hasConfiguredField = false;
        SchemaBuilder builder = SchemaBuilder.struct().name(oldSchema.name());
        if (oldSchema.version() != null) {
            builder.version(oldSchema.version());
        }
        if (oldSchema.doc() != null) {
            builder.doc(oldSchema.doc());
        }
        if (oldSchema.isOptional()) {
            builder.optional();
        }

        for (Field field : oldSchema.fields()) {
            if (fields.contains(field.name())) {
                builder.field(field.name(), Schema.OPTIONAL_STRING_SCHEMA);
                hasConfiguredField = true;
            } else {
                builder.field(field.name(), field.schema());
            }
        }

        Schema newSchema = hasConfiguredField ? builder.build() : oldSchema;
        schemaCache.put(oldSchema, newSchema);
        return newSchema;
    }

    private String toIsoUtcString(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof Date) {
            return FORMATTER.format(((Date) value).toInstant());
        }
        if (value instanceof Number) {
            return formatEpochNumber(((Number) value).longValue());
        }
        if (value instanceof Struct) {
            Struct struct = (Struct) value;
            Field dateField = struct.schema().field("$date");
            if (dateField == null) {
                dateField = struct.schema().field("date");
            }
            if (dateField != null) {
                return toIsoUtcString(struct.get(dateField));
            }
        }
        if (value instanceof Map) {
            @SuppressWarnings("unchecked")
            Map<String, Object> map = (Map<String, Object>) value;
            if (map.containsKey("$date")) {
                return toIsoUtcString(map.get("$date"));
            }
            if (map.containsKey("date")) {
                return toIsoUtcString(map.get("date"));
            }
        }

        String text = value.toString().trim();
        if (text.isEmpty()) {
            return text;
        }
        if (text.matches("-?\\d+")) {
            return formatEpochNumber(Long.parseLong(text));
        }

        Matcher matcher = JSON_DATE_PATTERN.matcher(text);
        if (matcher.find()) {
            return formatEpochNumber(Long.parseLong(matcher.group(1)));
        }

        try {
            return FORMATTER.format(Instant.parse(text));
        } catch (RuntimeException ignored) {
            return text;
        }
    }

    private String formatEpochNumber(long value) {
        long abs = Math.abs(value);
        Instant instant;
        if (abs >= 100_000_000_000_000L) {
            long seconds = Math.floorDiv(value, 1_000_000L);
            long micros = Math.floorMod(value, 1_000_000L);
            instant = Instant.ofEpochSecond(seconds, micros * 1_000L);
        } else if (abs >= 100_000_000_000L) {
            instant = Instant.ofEpochMilli(value);
        } else {
            instant = Instant.ofEpochSecond(value);
        }
        return FORMATTER.format(instant);
    }

    @Override
    public ConfigDef config() {
        return new ConfigDef().define(
                FIELDS_CONFIG,
                ConfigDef.Type.LIST,
                "",
                ConfigDef.Importance.HIGH,
                "Comma-separated timestamp fields to normalize to UTC ISO strings.");
    }

    @Override
    public void configure(Map<String, ?> configs) {
        Object raw = configs.get(FIELDS_CONFIG);
        List<String> configured = new ArrayList<>();
        if (raw instanceof List) {
            for (Object item : (List<?>) raw) {
                addField(configured, item);
            }
        } else {
            addField(configured, raw);
        }
        this.fields = new HashSet<>(configured);
    }

    private void addField(List<String> configured, Object item) {
        if (item == null) {
            return;
        }
        for (String part : item.toString().split(",")) {
            String field = part.trim();
            if (!field.isEmpty()) {
                configured.add(field);
            }
        }
    }

    @Override
    public void close() {
        schemaCache.clear();
    }
}