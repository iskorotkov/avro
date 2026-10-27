package javadiff;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import org.apache.avro.Conversions;
import org.apache.avro.Schema;
import org.apache.avro.SchemaCompatibility;
import org.apache.avro.SchemaNormalization;
import org.apache.avro.data.TimeConversions;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.message.BinaryMessageDecoder;
import org.apache.avro.message.BinaryMessageEncoder;

/** Runs a manifest of conformance cases against Apache Avro Java and writes one result per case id. */
public final class Harness {
  private static final ObjectMapper JSON = new ObjectMapper();
  private static final HexFormat HEX = HexFormat.of();
  private static final GenericData MODEL = newModel();

  private Harness() {}

  public static void main(String[] args) throws IOException {
    if (args.length != 2) {
      System.err.println("usage: javadiff <manifest.json> <results.json>");
      System.exit(2);
    }
    JsonNode manifest = JSON.readTree(new File(args[0]));
    ObjectNode results = JSON.createObjectNode();
    for (JsonNode c : manifest.get("cases")) {
      results.set(c.get("id").asText(), run(c));
    }
    JSON.writerWithDefaultPrettyPrinter().writeValue(new File(args[1]), results);
  }

  private static ObjectNode run(JsonNode c) {
    ObjectNode out = JSON.createObjectNode();
    try {
      switch (c.get("op").asText()) {
        case "decode" -> decode(c, out);
        case "schema" -> schema(c, out);
        case "compat" -> compat(c, out);
        case "ocf-write" -> ocfWrite(c, out);
        case "ocf-read" -> ocfRead(c, out);
        case "soe-encode" -> soeEncode(c, out);
        case "soe-decode" -> soeDecode(c, out);
        default -> throw new IllegalArgumentException("unknown op " + c.get("op"));
      }
    } catch (Exception | StackOverflowError e) {
      out.removeAll();
      out.put("error", e.getClass().getSimpleName() + ": " + e.getMessage());
    }
    return out;
  }

  /** Decodes hex with the writer schema, resolved to the reader schema when given, and re-encodes it. */
  private static void decode(JsonNode c, ObjectNode out) throws IOException {
    Schema writer = parse(c.get("schema").asText());
    Schema reader = c.hasNonNull("reader") ? parse(c.get("reader").asText()) : writer;
    BinaryDecoder dec = DecoderFactory.get().binaryDecoder(HEX.parseHex(c.get("hex").asText()), null);
    Object datum = new GenericDatumReader<>(writer, reader, MODEL).read(null, dec);
    if (!dec.isEnd()) {
      throw new IOException("trailing bytes after datum");
    }
    out.put("hex", HEX.formatHex(encode(reader, datum)));
  }

  private static void schema(JsonNode c, ObjectNode out) throws NoSuchAlgorithmException {
    Schema s = parse(c.get("schema").asText());
    out.put("canonical", SchemaNormalization.toParsingForm(s));
    out.put("crc64", Long.toString(SchemaNormalization.parsingFingerprint64(s)));
    out.put("md5", HEX.formatHex(SchemaNormalization.parsingFingerprint("MD5", s)));
    out.put("sha256", HEX.formatHex(SchemaNormalization.parsingFingerprint("SHA-256", s)));
  }

  private static void compat(JsonNode c, ObjectNode out) {
    Schema writer = parse(c.get("schema").asText());
    Schema reader = parse(c.get("reader").asText());
    SchemaCompatibility.SchemaCompatibilityType type =
        SchemaCompatibility.checkReaderWriterCompatibility(reader, writer).getType();
    out.put("compatible", type == SchemaCompatibility.SchemaCompatibilityType.COMPATIBLE);
  }

  /** Writes the datum count times, one block each, to a container file. */
  private static void ocfWrite(JsonNode c, ObjectNode out) throws IOException {
    Schema schema = parse(c.get("schema").asText());
    Object datum = decodeDatum(schema, HEX.parseHex(c.get("hex").asText()));
    try (DataFileWriter<Object> w = new DataFileWriter<>(new GenericDatumWriter<>(schema, MODEL))) {
      w.setCodec(CodecFactory.fromString(c.get("codec").asText()));
      w.create(schema, new File(c.get("file").asText()));
      for (int i = 0; i < c.path("count").asInt(1); i++) {
        w.append(datum);
        w.sync();
      }
    }
    out.put("hex", c.get("hex").asText());
  }

  /** Reads a container file and re-encodes every datum with its writer schema, counting them. */
  private static void ocfRead(JsonNode c, ObjectNode out) throws IOException {
    try (DataFileReader<Object> r =
        new DataFileReader<>(new File(c.get("file").asText()), new GenericDatumReader<>(null, null, MODEL))) {
      out.put("codec", r.getMetaString("avro.codec"));
      StringBuilder hex = new StringBuilder();
      int count = 0;
      for (Object datum : r) {
        hex.append(HEX.formatHex(encode(r.getSchema(), datum)));
        count++;
      }
      out.put("hex", hex.toString());
      out.put("count", count);
    }
  }

  private static void soeEncode(JsonNode c, ObjectNode out) throws IOException {
    Schema schema = parse(c.get("schema").asText());
    Object datum = decodeDatum(schema, HEX.parseHex(c.get("hex").asText()));
    ByteBuffer msg = new BinaryMessageEncoder<>(MODEL, schema).encode(datum);
    byte[] b = new byte[msg.remaining()];
    msg.get(b);
    out.put("hex", HEX.formatHex(b));
  }

  private static void soeDecode(JsonNode c, ObjectNode out) throws IOException {
    Schema schema = parse(c.get("schema").asText());
    Object datum = new BinaryMessageDecoder<>(MODEL, schema).decode(HEX.parseHex(c.get("hex").asText()));
    out.put("hex", HEX.formatHex(encode(schema, datum)));
  }

  private static Schema parse(String json) {
    return new Schema.Parser().parse(json);
  }

  private static Object decodeDatum(Schema schema, byte[] data) throws IOException {
    return new GenericDatumReader<>(schema, schema, MODEL).read(null, DecoderFactory.get().binaryDecoder(data, null));
  }

  private static byte[] encode(Schema schema, Object datum) throws IOException {
    ByteArrayOutputStream buf = new ByteArrayOutputStream();
    BinaryEncoder enc = EncoderFactory.get().binaryEncoder(buf, null);
    new GenericDatumWriter<>(schema, MODEL).write(datum, enc);
    enc.flush();
    return buf.toByteArray();
  }

  /** Builds a data model that converts every logical type Java supports, so values are validated on the way through. */
  private static GenericData newModel() {
    GenericData m = new GenericData();
    m.addLogicalTypeConversion(new Conversions.DecimalConversion());
    m.addLogicalTypeConversion(new Conversions.UUIDConversion());
    m.addLogicalTypeConversion(new Conversions.DurationConversion());
    m.addLogicalTypeConversion(new TimeConversions.DateConversion());
    m.addLogicalTypeConversion(new TimeConversions.TimeMillisConversion());
    m.addLogicalTypeConversion(new TimeConversions.TimeMicrosConversion());
    m.addLogicalTypeConversion(new TimeConversions.TimestampMillisConversion());
    m.addLogicalTypeConversion(new TimeConversions.TimestampMicrosConversion());
    m.addLogicalTypeConversion(new TimeConversions.TimestampNanosConversion());
    m.addLogicalTypeConversion(new TimeConversions.LocalTimestampMillisConversion());
    m.addLogicalTypeConversion(new TimeConversions.LocalTimestampMicrosConversion());
    m.addLogicalTypeConversion(new TimeConversions.LocalTimestampNanosConversion());
    return m;
  }
}
