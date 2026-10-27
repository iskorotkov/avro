package io.github.iskorotkov.avrobench.jmh;

import io.github.iskorotkov.avrobench.dataset.Dataset;
import io.github.iskorotkov.avrobench.subject.Subjects;
import io.github.iskorotkov.avrobench.workload.Workload;
import io.github.iskorotkov.avrobench.workload.Workloads;
import java.util.concurrent.TimeUnit;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

/** One JMH op processes the whole sample, like one iteration of the Go suite. */
@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Fork(value = 2, jvmArgs = "-Xmx2g")
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
public class SuiteBenchmark {
    @Param({"superhero", "nyctaxi"})
    public String dataset;

    @Param({Subjects.GENERIC, Subjects.SPECIFIC})
    public String format;

    @Param({"encode", "decode", "batch-encode", "batch-decode",
            "container-write-null", "container-read-null",
            "container-write-deflate", "container-read-deflate",
            "container-write-snappy", "container-read-snappy",
            "container-write-zstd", "container-read-zstd"})
    public String op;

    private Workload workload;

    @Setup
    public void setup() throws Exception {
        workload = Workloads.create(Subjects.create(format, Dataset.byId(dataset)), op);
        workload.verify().call();
    }

    @Benchmark
    public Object run() throws Exception {
        return workload.run().call();
    }
}
