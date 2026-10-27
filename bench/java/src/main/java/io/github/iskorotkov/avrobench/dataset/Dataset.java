package io.github.iskorotkov.avrobench.dataset;

import java.nio.file.Path;
import org.apache.avro.specific.SpecificRecord;

/** A benchmark dataset shared with the Go suite: schema, zstd JSON Lines sample and generated class. */
public enum Dataset {
    SUPERHERO("superhero", com.model.Superhero.class),
    NYCTAXI("nyctaxi", nyc.tlc.yellow.Trip.class);

    private static final Path ROOT = Path.of(System.getenv().getOrDefault("BENCH_DATASETS", "../datasets"));

    private final String id;
    private final Class<? extends SpecificRecord> specificClass;

    Dataset(String id, Class<? extends SpecificRecord> specificClass) {
        this.id = id;
        this.specificClass = specificClass;
    }

    public String id() {
        return id;
    }

    public Class<? extends SpecificRecord> specificClass() {
        return specificClass;
    }

    public Path sample() {
        return ROOT.resolve(id).resolve("sample.jsonl.zst");
    }

    public static Dataset byId(String id) {
        for (Dataset d : values()) {
            if (d.id.equals(id)) {
                return d;
            }
        }
        throw new IllegalArgumentException("unknown dataset " + id);
    }
}
