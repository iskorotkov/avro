package io.github.iskorotkov.avrobench.workload;

import io.github.iskorotkov.avrobench.subject.Subject;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;

/** Encoding and decoding primitives shared by the operations. */
final class Wire<T> {
    // Same block size as the Go OCF encoder.
    private static final int BLOCK_RECORDS = 100;

    private final Subject<T> subject;
    private final ByteArrayOutputStream out = new ByteArrayOutputStream();
    private BinaryEncoder encoder;
    private BinaryDecoder decoder;

    Wire(Subject<T> subject) {
        this.subject = subject;
    }

    /** Encodes one record into the reused buffer and returns its length. */
    int encode(T value) throws IOException {
        out.reset();
        encoder = EncoderFactory.get().binaryEncoder(out, encoder);
        subject.writer().write(value, encoder);
        encoder.flush();
        return out.size();
    }

    byte[] encodeOne(T value) throws IOException {
        encode(value);
        return out.toByteArray();
    }

    /** Encodes all records back to back with one encoder and returns the length. */
    int encodeAll() throws IOException {
        out.reset();
        encoder = EncoderFactory.get().binaryEncoder(out, encoder);
        for (T v : subject.values()) {
            subject.writer().write(v, encoder);
        }
        encoder.flush();
        return out.size();
    }

    byte[] encodedAll() throws IOException {
        encodeAll();
        return out.toByteArray();
    }

    T decode(byte[] payload) throws IOException {
        decoder = DecoderFactory.get().binaryDecoder(payload, decoder);
        return subject.reader().read(null, decoder);
    }

    List<T> decodeAll(byte[] data) throws IOException {
        var decoder = DecoderFactory.get().binaryDecoder(data, null);
        var values = new ArrayList<T>(subject.values().size());
        for (int i = 0; i < subject.values().size(); i++) {
            values.add(subject.reader().read(null, decoder));
        }
        return values;
    }

    int writeContainer(CodecFactory codec) throws IOException {
        out.reset();
        try (var writer = new DataFileWriter<>(subject.writer()).setCodec(codec)) {
            writer.create(subject.schema(), out);
            int n = 0;
            for (T v : subject.values()) {
                writer.append(v);
                if (++n % BLOCK_RECORDS == 0) {
                    writer.sync();
                }
            }
        }
        return out.size();
    }

    byte[] containerBytes(CodecFactory codec) throws IOException {
        writeContainer(codec);
        return out.toByteArray();
    }

    List<T> readContainer(byte[] data) throws IOException {
        var values = new ArrayList<T>(subject.values().size());
        try (var in = new DataFileStream<>(new ByteArrayInputStream(data), subject.reader())) {
            while (in.hasNext()) {
                values.add(in.next());
            }
        }
        return values;
    }
}
