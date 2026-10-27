package io.github.iskorotkov.avrobench.subject;

import io.github.iskorotkov.avrobench.dataset.Dataset;
import io.github.iskorotkov.avrobench.dataset.Samples;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecord;

/** Builds subjects for the format names reported next to the Go ones. */
public final class Subjects {
    public static final String GENERIC = "java-avro-generic";
    public static final String SPECIFIC = "java-avro-specific";

    private Subjects() {}

    public static Subject<?> create(String format, Dataset dataset) {
        return switch (format) {
            case GENERIC -> generic(dataset);
            case SPECIFIC -> specific(dataset);
            default -> throw new IllegalArgumentException("unknown format " + format);
        };
    }

    static Subject<GenericRecord> generic(Dataset dataset) {
        Schema schema = Samples.schema(dataset);
        return new Subject<>(schema, Samples.load(dataset), new GenericDatumWriter<>(schema), new GenericDatumReader<>(schema));
    }

    static <T extends SpecificRecord> Subject<T> specific(Dataset dataset, Class<T> type) {
        Schema schema = Samples.schema(dataset);
        var writer = new SpecificDatumWriter<>(type);
        var reader = new SpecificDatumReader<>(type);
        return new Subject<>(schema, convert(Samples.load(dataset), schema, reader), writer, reader);
    }

    private static Subject<?> specific(Dataset dataset) {
        return specific(dataset, dataset.specificClass());
    }

    // Round-trips through the wire format so both APIs start from identical data.
    private static <T> List<T> convert(List<GenericRecord> records, Schema schema, SpecificDatumReader<T> reader) {
        var generic = new GenericDatumWriter<GenericRecord>(schema);
        var out = new ByteArrayOutputStream();
        BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
        BinaryDecoder decoder = null;
        var values = new ArrayList<T>(records.size());
        try {
            for (GenericRecord r : records) {
                out.reset();
                generic.write(r, encoder);
                encoder.flush();
                decoder = DecoderFactory.get().binaryDecoder(out.toByteArray(), decoder);
                values.add(reader.read(null, decoder));
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return values;
    }
}
