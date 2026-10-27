package io.github.iskorotkov.avrobench.dataset;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.luben.zstd.ZstdInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.specific.SpecificData;

/** Loads the committed samples; decompression happens once, outside timing. */
public final class Samples {
    private Samples() {}

    public static Schema schema(Dataset dataset) {
        return SpecificData.get().getSchema(dataset.specificClass());
    }

    public static List<GenericRecord> load(Dataset dataset) {
        Schema schema = schema(dataset);
        var records = new ArrayList<GenericRecord>();
        try (InputStream in = new ZstdInputStream(Files.newInputStream(dataset.sample()))) {
            new ObjectMapper().readerFor(JsonNode.class).<JsonNode>readValues(in)
                    .forEachRemaining(n -> records.add((GenericRecord) JsonRecords.convert(schema, n)));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return records;
    }
}
