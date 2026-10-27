package io.github.iskorotkov.avrobench.workload;

import io.github.iskorotkov.avrobench.subject.Subject;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.avro.file.CodecFactory;

/** Builds the operations named like the Go suite's ops. */
public final class Workloads {
    private static final Map<String, CodecFactory> CODECS = Map.of(
            "null", CodecFactory.nullCodec(),
            "deflate", CodecFactory.deflateCodec(6),
            "snappy", CodecFactory.snappyCodec(),
            "zstd", CodecFactory.zstandardCodec(3));

    private Workloads() {}

    public static <T> Workload create(Subject<T> subject, String op) throws IOException {
        var wire = new Wire<T>(subject);
        return switch (op) {
            case "encode" -> encode(subject, wire);
            case "decode" -> decode(subject, wire);
            case "batch-encode" -> batchEncode(subject, wire);
            case "batch-decode" -> batchDecode(subject, wire);
            default -> container(subject, wire, op);
        };
    }

    private static <T> Workload encode(Subject<T> s, Wire<T> wire) throws IOException {
        int size = encodedSize(s, wire);
        return new Workload(s.values().size(), size, () -> encodedSize(s, wire), () -> {
            var decoded = new ArrayList<T>();
            for (T v : s.values()) {
                decoded.add(wire.decode(wire.encodeOne(v)));
            }
            return same(s.values(), decoded);
        });
    }

    private static <T> int encodedSize(Subject<T> s, Wire<T> wire) throws IOException {
        int size = 0;
        for (T v : s.values()) {
            size += wire.encode(v);
        }
        return size;
    }

    private static <T> Workload decode(Subject<T> s, Wire<T> wire) throws IOException {
        var payloads = new ArrayList<byte[]>();
        long size = 0;
        for (T v : s.values()) {
            byte[] p = wire.encodeOne(v);
            payloads.add(p);
            size += p.length;
        }
        Workload.Step run = () -> {
            var decoded = new ArrayList<T>(payloads.size());
            for (byte[] p : payloads) {
                decoded.add(wire.decode(p));
            }
            return decoded;
        };
        return new Workload(payloads.size(), size, run, () -> same(s.values(), (List<?>) run.call()));
    }

    private static <T> Workload batchEncode(Subject<T> s, Wire<T> wire) throws IOException {
        return new Workload(s.values().size(), wire.encodeAll(), wire::encodeAll,
                () -> same(s.values(), wire.decodeAll(wire.encodedAll())));
    }

    private static <T> Workload batchDecode(Subject<T> s, Wire<T> wire) throws IOException {
        byte[] data = wire.encodedAll();
        return new Workload(s.values().size(), data.length, () -> wire.decodeAll(data),
                () -> same(s.values(), wire.decodeAll(data)));
    }

    private static <T> Workload container(Subject<T> s, Wire<T> wire, String op) throws IOException {
        boolean write = op.startsWith("container-write-");
        if (!write && !op.startsWith("container-read-")) {
            throw new IllegalArgumentException("unknown op " + op);
        }
        CodecFactory codec = CODECS.get(op.substring(op.lastIndexOf('-') + 1));
        if (codec == null) {
            throw new IllegalArgumentException("unknown codec in op " + op);
        }
        byte[] data = wire.containerBytes(codec);
        Workload.Step verify = () -> same(s.values(), wire.readContainer(data));
        if (write) {
            return new Workload(s.values().size(), data.length, () -> wire.writeContainer(codec), verify);
        }
        return new Workload(s.values().size(), data.length, () -> wire.readContainer(data), verify);
    }

    private static boolean same(List<?> want, List<?> got) {
        if (!want.equals(got)) {
            throw new IllegalStateException("round trip mismatch");
        }
        return true;
    }
}
