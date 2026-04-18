package io.goldusdt.tron.filesink;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.lang.reflect.Constructor;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.junit.Test;

public class SegmentedNdjsonWriterTest {

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  @Test
  public void writesGzipSegmentsWithRecordCapRotation() throws Exception {
    Path outputRoot = Files.createTempDirectory("filesink-writer-test");
    FileSinkConfig config =
        buildConfig(outputRoot, 10_000_000L, 2, 1, 1, 65536, "gzip");

    try (SegmentedNdjsonWriter writer = new SegmentedNdjsonWriter(config)) {
      writer.writeRecord(EventType.SOLIDITY_LOG, payload(1L, "tx-1", "u-1"));
      writer.writeRecord(EventType.SOLIDITY_LOG, payload(2L, "tx-2", "u-2"));
      writer.writeRecord(EventType.SOLIDITY_LOG, payload(3L, "tx-3", "u-3"));
    }

    List<Path> manifests = new ArrayList<Path>();
    try (Stream<Path> stream = Files.list(outputRoot.resolve("manifests"))) {
      stream.sorted(Comparator.naturalOrder()).forEach(manifests::add);
    }

    assertEquals(2, manifests.size());
    Map<?, ?> firstManifest = OBJECT_MAPPER.readValue(manifests.get(0).toFile(), Map.class);
    Map<?, ?> secondManifest = OBJECT_MAPPER.readValue(manifests.get(1).toFile(), Map.class);
    assertEquals(2, ((Number) firstManifest.get("record_count")).intValue());
    assertEquals(1, ((Number) secondManifest.get("record_count")).intValue());

    List<Path> segments = new ArrayList<Path>();
    try (Stream<Path> stream = Files.list(outputRoot.resolve("segments"))) {
      stream.sorted(Comparator.naturalOrder()).forEach(segments::add);
    }

    assertEquals(2, segments.size());
    assertTrue(segments.get(0).getFileName().toString().endsWith(".ndjson.gz"));
    assertTrue(Files.size(segments.get(0)) > 0L);
    assertTrue(Files.size(segments.get(1)) > 0L);
  }

  private static FileSinkConfig buildConfig(
      Path outputRoot,
      long segmentMaxBytes,
      int segmentMaxRecords,
      int flushEveryRecords,
      int gzipLevel,
      int gzipBufferBytes,
      String compression)
      throws Exception {
    Constructor<FileSinkConfig> constructor =
        FileSinkConfig.class.getDeclaredConstructor(
            String.class,
            String.class,
            String.class,
            String.class,
            String.class,
            String.class,
            String.class,
            Path.class,
            long.class,
            int.class,
            int.class,
            int.class,
            int.class,
            int.class,
            String.class,
            String.class,
            String.class,
            String.class,
            String.class);
    constructor.setAccessible(true);
    return constructor.newInstance(
        "run-1",
        "usdt_transfer",
        compression,
        EventType.SOLIDITY_LOG.getTriggerName(),
        "TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t",
        "ddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef",
        "i-test",
        outputRoot,
        segmentMaxBytes,
        segmentMaxRecords,
        flushEveryRecords,
        gzipLevel,
        gzipBufferBytes,
        10000,
        "bucket",
        "prefix/root",
        null,
        null,
        "test-build");
  }

  private static String payload(long blockNumber, String transactionId, String uniqueId) throws Exception {
    return OBJECT_MAPPER.writeValueAsString(
        Map.of(
            "triggerName", EventType.SOLIDITY_LOG.getTriggerName(),
            "blockNumber", blockNumber,
            "transactionId", transactionId,
            "uniqueId", uniqueId,
            "timeStamp", 1713446400000L + blockNumber));
  }
}
