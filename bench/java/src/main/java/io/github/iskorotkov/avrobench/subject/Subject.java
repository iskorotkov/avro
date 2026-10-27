package io.github.iskorotkov.avrobench.subject;

import java.util.List;
import org.apache.avro.Schema;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;

/** Records of one dataset plus the Avro Java API used to encode and decode them. */
public record Subject<T>(Schema schema, List<T> values, DatumWriter<T> writer, DatumReader<T> reader) {}
