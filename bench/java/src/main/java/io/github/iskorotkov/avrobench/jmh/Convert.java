package io.github.iskorotkov.avrobench.jmh;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.github.iskorotkov.avrobench.dataset.Dataset;
import io.github.iskorotkov.avrobench.subject.Subject;
import io.github.iskorotkov.avrobench.subject.Subjects;
import io.github.iskorotkov.avrobench.workload.Workload;
import io.github.iskorotkov.avrobench.workload.Workloads;
import java.io.File;
import java.util.HashMap;
import java.util.Map;

/** Adds the records and wireBytes params the Go report expects to native JMH JSON. */
public final class Convert {
    private Convert() {}

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            throw new IllegalArgumentException("usage: Convert <jmh.json> <report.json>");
        }
        var mapper = new ObjectMapper();
        var root = mapper.readTree(new File(args[0]));
        Map<String, Subject<?>> subjects = new HashMap<>();
        for (var benchmark : root) {
            var params = (ObjectNode) benchmark.get("params");
            String dataset = params.get("dataset").asText();
            String format = params.get("format").asText();
            Subject<?> subject = subjects.computeIfAbsent(dataset + "/" + format,
                    k -> Subjects.create(format, Dataset.byId(dataset)));
            Workload w = Workloads.create(subject, params.get("op").asText());
            params.put("records", String.valueOf(w.records()));
            params.put("wireBytes", String.valueOf(w.wireBytes()));
        }
        mapper.writerWithDefaultPrettyPrinter().writeValue(new File(args[1]), root);
    }
}
