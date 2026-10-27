package io.github.iskorotkov.avrobench.dataset;

import com.fasterxml.jackson.databind.JsonNode;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;

/** Converts plain JSON, as written by the Go suite, into generic Avro values. */
final class JsonRecords {
    private JsonRecords() {}

    static Object convert(Schema schema, JsonNode node) {
        return switch (schema.getType()) {
            case RECORD -> record(schema, node);
            case ARRAY -> array(schema, node);
            case UNION -> union(schema, node);
            case INT -> node.intValue();
            case LONG -> isTimestamp(schema) && node.isTextual() ? micros(node.textValue()) : node.longValue();
            case FLOAT -> node.floatValue();
            case DOUBLE -> node.doubleValue();
            case STRING -> node.textValue();
            case BOOLEAN -> node.booleanValue();
            default -> throw new IllegalArgumentException("unsupported type " + schema.getType());
        };
    }

    private static GenericData.Record record(Schema schema, JsonNode node) {
        var record = new GenericData.Record(schema);
        for (Schema.Field f : schema.getFields()) {
            JsonNode value = node.get(f.name());
            if (value == null) {
                throw new IllegalArgumentException("missing field " + f.name());
            }
            record.put(f.pos(), convert(f.schema(), value));
        }
        return record;
    }

    private static List<Object> array(Schema schema, JsonNode node) {
        var items = new GenericData.Array<Object>(node.size(), schema);
        node.forEach(e -> items.add(convert(schema.getElementType(), e)));
        return items;
    }

    private static Object union(Schema schema, JsonNode node) {
        if (node.isNull()) {
            return null;
        }
        var branches = schema.getTypes().stream().filter(b -> b.getType() != Schema.Type.NULL).toList();
        if (branches.size() != 1) {
            throw new IllegalArgumentException("only nullable unions are supported");
        }
        return convert(branches.get(0), node);
    }

    private static boolean isTimestamp(Schema schema) {
        return schema.getLogicalType() instanceof LogicalTypes.TimestampMicros;
    }

    private static long micros(String rfc3339) {
        return ChronoUnit.MICROS.between(Instant.EPOCH, Instant.parse(rfc3339));
    }
}
