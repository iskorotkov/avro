package io.github.iskorotkov.avrobench.dataset;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.Test;

class JsonRecordsTest {
    private static final Schema SCHEMA = new Schema.Parser().parse("""
            {"type":"record","name":"R","fields":[
              {"name":"t","type":{"type":"long","logicalType":"timestamp-micros"}},
              {"name":"n","type":["null","long"]},
              {"name":"xs","type":{"type":"array","items":"float"}}]}""");

    @Test
    void convertsTimestampsUnionsAndArrays() throws Exception {
        var json = new ObjectMapper().readTree("{\"t\":\"2024-01-01T00:00:01.5Z\",\"n\":null,\"xs\":[1.5]}");

        var record = (GenericRecord) JsonRecords.convert(SCHEMA, json);

        assertEquals(1_704_067_201_500_000L, record.get("t"));
        assertNull(record.get("n"));
        assertEquals(1.5f, ((Iterable<?>) record.get("xs")).iterator().next());
    }
}
