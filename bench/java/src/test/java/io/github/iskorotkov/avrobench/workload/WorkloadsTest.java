package io.github.iskorotkov.avrobench.workload;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.iskorotkov.avrobench.dataset.Dataset;
import io.github.iskorotkov.avrobench.subject.Subjects;
import java.util.List;
import org.junit.jupiter.api.Test;

class WorkloadsTest {
    private static final List<String> OPS = List.of(
            "encode", "decode", "batch-encode", "batch-decode",
            "container-write-null", "container-read-deflate", "container-write-snappy", "container-read-zstd");

    @Test
    void everyOpRoundTripsOnBothApis() throws Exception {
        for (String format : List.of(Subjects.GENERIC, Subjects.SPECIFIC)) {
            var subject = Subjects.create(format, Dataset.SUPERHERO);
            for (String op : OPS) {
                var w = Workloads.create(subject, op);
                assertTrue((Boolean) w.verify().call(), format + " " + op);
                assertTrue(w.wireBytes() > 0, format + " " + op);
            }
        }
    }

    @Test
    void wireSizeMatchesAcrossApis() throws Exception {
        var generic = Workloads.create(Subjects.create(Subjects.GENERIC, Dataset.NYCTAXI), "batch-encode");
        var specific = Workloads.create(Subjects.create(Subjects.SPECIFIC, Dataset.NYCTAXI), "batch-encode");
        assertEquals(generic.wireBytes(), specific.wireBytes());
    }
}
