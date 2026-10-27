package io.github.iskorotkov.avrobench.workload;

/** One prepared operation over a whole dataset sample, like one Go benchmark iteration. */
public record Workload(int records, long wireBytes, Step run, Step verify) {
    /** A step that may fail with an I/O error. */
    @FunctionalInterface
    public interface Step {
        Object call() throws Exception;
    }
}
