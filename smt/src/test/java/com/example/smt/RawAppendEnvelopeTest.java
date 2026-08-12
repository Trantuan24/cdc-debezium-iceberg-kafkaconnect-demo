package com.example.smt;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.time.ZoneOffset;
import java.util.Collections;
import java.util.Date;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class RawAppendEnvelopeTest {

    private static final String TOPIC = "raw.mysql.mydb.orders";
    private RawAppendEnvelope<SinkRecord> transform;

    @Before
    public void setUp() {
        transform = new RawAppendEnvelope<>();
        transform.configure(Collections.emptyMap());
    }

    @After
    public void tearDown() {
        transform.close();
    }

    @Test
    public void buildsTheExpectedContractForEveryCdcOperation() {
        Map<String, String> events = Map.of(
                "r", "insert",
                "c", "insert",
                "u", "update",
                "d", "delete");

        long offset = 10L;
        for (Map.Entry<String, String> event : events.entrySet()) {
            String data = debeziumJson(event.getKey());
            SinkRecord result = transform.apply(record(data, offset));
            Struct value = (Struct) result.value();

            assertEquals("cdc", value.getString("loainguon"));
            assertEquals(TOPIC + "-0-" + offset, value.getString("manguondulieu"));
            assertEquals(event.getValue(), value.getString("sukien"));
            assertEquals(Integer.valueOf(1), value.getInt32("phienban"));
            assertEquals("{\"id\":10}", value.getString("body"));
            assertEquals("", value.getString("header"));
            assertSame(data, value.getString("data"));

            Date ingestDate = (Date) value.get("ingest_date");
            Date ingestTime = (Date) value.get("ingest_time");
            assertEquals(
                    ingestTime.toInstant().atZone(ZoneOffset.UTC).toLocalDate(),
                    ingestDate.toInstant().atZone(ZoneOffset.UTC).toLocalDate());

            Schema schema = result.valueSchema();
            assertFalse(schema.field("loainguon").schema().isOptional());
            assertFalse(schema.field("manguondulieu").schema().isOptional());
            assertTrue(schema.field("sukien").schema().isOptional());
            assertFalse(schema.field("phienban").schema().isOptional());
            assertTrue(schema.field("body").schema().isOptional());
            assertTrue(schema.field("header").schema().isOptional());
            assertFalse(schema.field("data").schema().isOptional());
            assertFalse(schema.field("ingest_date").schema().isOptional());
            assertFalse(schema.field("ingest_time").schema().isOptional());
            offset++;
        }
    }

    @Test
    public void skipsTombstones() {
        assertNull(transform.apply(record(null, 20L)));
    }

    @Test(expected = DataException.class)
    public void rejectsInvalidJson() {
        transform.apply(record("not-json", 30L));
    }

    @Test
    public void preservesHeaderOrderAndDuplicateNamesAsJsonArray() {
        SinkRecord source = record(debeziumJson("u"), 40L);
        source.headers().addString("traceparent", "00-test");
        source.headers().addInt("attempt", 2);
        source.headers().addString("traceparent", "00-retry");

        Struct value = (Struct) transform.apply(source).value();

        assertEquals(
                "[{\"key\":\"traceparent\",\"value\":\"00-test\"},"
                        + "{\"key\":\"attempt\",\"value\":2},"
                        + "{\"key\":\"traceparent\",\"value\":\"00-retry\"}]",
                value.getString("header"));
    }

    private static SinkRecord record(String value, long offset) {
        return new SinkRecord(TOPIC, 0, null, "{\"id\":10}", null, value, offset);
    }

    private static String debeziumJson(String operation) {
        return "{\"schema\":{\"type\":\"struct\",\"fields\":["
                + "{\"type\":\"struct\",\"fields\":[{\"type\":\"string\","
                + "\"optional\":true,\"field\":\"op\"}],\"optional\":true,"
                + "\"field\":\"after\"},{\"type\":\"string\",\"optional\":false,"
                + "\"field\":\"op\"}],\"optional\":false,"
                + "\"name\":\"io.debezium.connector.test.Envelope\"},"
                + "\"payload\":{\"after\":{\"op\":\"business-value\"},"
                + "\"op\":\"" + operation + "\"}}";
    }
}
