package io.goldusdt.tron.filesink;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.security.DigestOutputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.zip.GZIPOutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class SegmentedNdjsonWriter implements AutoCloseable {

  private static final Logger log = LoggerFactory.getLogger(SegmentedNdjsonWriter.class);

  private final FileSinkConfig config;
  private final ObjectMapper objectMapper = new ObjectMapper();
  private int nextSequence;
  private OpenSegment currentSegment;

  SegmentedNdjsonWriter(FileSinkConfig config) throws IOException {
    this.config = config;
    Files.createDirectories(config.getSegmentsDir());
    Files.createDirectories(config.getManifestsDir());
    failIfOrphanedPartialExists();
    this.nextSequence = determineNextSequence();
  }

  void writeRecord(EventType eventType, String payload) throws IOException {
    TriggerEnvelopeMetadata metadata = TriggerEnvelopeMetadata.parse(objectMapper, payload, eventType);
    if (!config.getExpectedTriggerName().equals(metadata.getTriggerName())) {
      log.debug(
          "Ignoring trigger {} because expected trigger is {}",
          metadata.getTriggerName(),
          config.getExpectedTriggerName());
      return;
    }

    if (currentSegment == null) {
      currentSegment = openSegment(eventType);
    }

    if (currentSegment.shouldRotateBeforeWrite(payload)) {
      closeCurrentSegment();
      currentSegment = openSegment(eventType);
    }

    currentSegment.write(payload, metadata);

    if (currentSegment.shouldRotateAfterWrite()) {
      closeCurrentSegment();
      currentSegment = null;
    }
  }

  @Override
  public void close() throws IOException {
    closeCurrentSegment();
  }

  private OpenSegment openSegment(EventType eventType) throws IOException {
    int sequence = nextSequence++;
    String finalFileName = config.segmentFileName(sequence);
    return new OpenSegment(
        sequence,
        config.segmentId(sequence),
        eventType,
        config.getSegmentsDir().resolve(config.partialFileName(sequence)),
        config.getSegmentsDir().resolve(finalFileName),
        config.getManifestsDir().resolve(config.manifestFileName(sequence)));
  }

  private void closeCurrentSegment() throws IOException {
    if (currentSegment == null) {
      return;
    }
    currentSegment.close();
    currentSegment = null;
  }

  private void failIfOrphanedPartialExists() throws IOException {
    try (DirectoryStream<Path> stream = Files.newDirectoryStream(config.getSegmentsDir(), "*.partial")) {
      for (Path path : stream) {
        quarantineOrphan(path);
      }
    }
  }

  private void quarantineOrphan(Path path) throws IOException {
    String orphanName = path.getFileName().toString() + ".orphaned." + Instant.now().toEpochMilli();
    Path orphanPath = path.resolveSibling(orphanName);
    moveAtomically(path, orphanPath);
    throw new IllegalStateException(
        "Refusing to start with orphan partial segment. Moved to " + orphanPath.toString());
  }

  private int determineNextSequence() throws IOException {
    int maxSequence = 0;
    maxSequence = Math.max(maxSequence, scanSequence(config.getSegmentsDir(), ".ndjson"));
    maxSequence = Math.max(maxSequence, scanSequence(config.getSegmentsDir(), ".partial"));
    maxSequence = Math.max(maxSequence, scanSequence(config.getManifestsDir(), ".manifest.json"));
    return maxSequence + 1;
  }

  private int scanSequence(Path directory, String marker) throws IOException {
    int maxSequence = 0;
    try (DirectoryStream<Path> stream = Files.newDirectoryStream(directory)) {
      for (Path path : stream) {
        String fileName = path.getFileName().toString();
        if (!fileName.startsWith(config.getStreamName() + "_")) {
          continue;
        }
        if (!fileName.contains(marker)) {
          continue;
        }
        int underscore = fileName.lastIndexOf('_');
        int dot = fileName.indexOf('.', underscore);
        if (underscore < 0 || dot < 0 || dot <= underscore + 1) {
          continue;
        }
        try {
          int sequence = Integer.parseInt(fileName.substring(underscore + 1, dot));
          if (sequence > maxSequence) {
            maxSequence = sequence;
          }
        } catch (NumberFormatException ignored) {
          log.debug("Skipping non-standard segment filename {}", fileName);
        }
      }
    }
    return maxSequence;
  }

  private static String sha256Hex(byte[] digest) {
    StringBuilder builder = new StringBuilder(digest.length * 2);
    for (byte value : digest) {
      builder.append(String.format("%02x", value & 0xff));
    }
    return builder.toString();
  }

  private static void moveAtomically(Path source, Path target) throws IOException {
    try {
      Files.move(source, target, StandardCopyOption.ATOMIC_MOVE);
    } catch (IOException ignored) {
      Files.move(source, target);
    }
  }

  private static void failIfExists(Path path, String message) {
    if (Files.exists(path)) {
      throw new IllegalStateException(message + ": " + path.toString());
    }
  }

  private final class OpenSegment implements AutoCloseable {

    private final int sequence;
    private final String segmentId;
    private final EventType eventType;
    private final Path partialPath;
    private final Path finalPath;
    private final Path manifestPath;
    private final MessageDigest digest;
    private final BufferedWriter writer;
    private final Instant openedAt = Instant.now();

    private long approximateBytes;
    private int recordCount;
    private Long blockFrom;
    private Long blockTo;
    private String firstEventKey;
    private String lastEventKey;
    private String firstTxHash;
    private String lastTxHash;

    private OpenSegment(
        int sequence,
        String segmentId,
        EventType eventType,
        Path partialPath,
        Path finalPath,
        Path manifestPath)
        throws IOException {
      this.sequence = sequence;
      this.segmentId = segmentId;
      this.eventType = eventType;
      this.partialPath = partialPath;
      this.finalPath = finalPath;
      this.manifestPath = manifestPath;
      this.digest = createDigest();

      failIfExists(finalPath, "Refusing to overwrite existing sealed segment");
      failIfExists(manifestPath, "Refusing to overwrite existing segment manifest");
      failIfExists(partialPath, "Refusing to overwrite existing partial segment");

      OutputStream fileOutputStream =
          Files.newOutputStream(partialPath, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE);
      DigestOutputStream digestOutputStream = new DigestOutputStream(fileOutputStream, digest);
      OutputStream contentStream =
          config.isGzipEnabled() ? new GZIPOutputStream(digestOutputStream, 8192) : digestOutputStream;
      this.writer = new BufferedWriter(new OutputStreamWriter(contentStream, StandardCharsets.UTF_8));
    }

    boolean shouldRotateBeforeWrite(String payload) {
      if (recordCount == 0) {
        return false;
      }
      long predictedBytes = approximateBytes + payload.getBytes(StandardCharsets.UTF_8).length + 1L;
      return predictedBytes > config.getSegmentMaxBytes();
    }

    boolean shouldRotateAfterWrite() {
      return recordCount >= config.getSegmentMaxRecords();
    }

    void write(String payload, TriggerEnvelopeMetadata metadata) throws IOException {
      writer.write(payload);
      writer.newLine();
      approximateBytes += payload.getBytes(StandardCharsets.UTF_8).length + 1L;
      recordCount += 1;

      if (recordCount % config.getFlushEveryRecords() == 0) {
        writer.flush();
      }

      if (metadata.getBlockNumber() != null) {
        if (blockFrom == null) {
          blockFrom = metadata.getBlockNumber();
        }
        blockTo = metadata.getBlockNumber();
      }

      if (firstEventKey == null) {
        firstEventKey = metadata.getEventKey();
        firstTxHash = metadata.getTransactionId();
      }

      lastEventKey = metadata.getEventKey();
      lastTxHash = metadata.getTransactionId();
    }

    @Override
    public void close() throws IOException {
      writer.flush();
      writer.close();
      moveAtomically(partialPath, finalPath);

      Instant closedAt = Instant.now();
      long fileSizeBytes = Files.size(finalPath);
      String sha256 = sha256Hex(digest.digest());

      Map<String, Object> manifest = new LinkedHashMap<String, Object>();
      manifest.put("manifest_version", 1);
      manifest.put("kind", "segment_manifest");
      manifest.put("segment_id", segmentId);
      manifest.put("run_id", config.getRunId());
      manifest.put("segment_seq", sequence);
      manifest.put("stream_name", config.getStreamName());
      manifest.put("trigger_name", eventType.getTriggerName());
      manifest.put("topic0", config.getTopic0());
      manifest.put("contract_address", config.getContractAddress());
      manifest.put("block_from", blockFrom);
      manifest.put("block_to", blockTo);
      manifest.put("first_event_key", firstEventKey);
      manifest.put("last_event_key", lastEventKey);
      manifest.put("first_tx_hash", firstTxHash);
      manifest.put("last_tx_hash", lastTxHash);
      manifest.put("record_count", recordCount);
      manifest.put("file_size_bytes", fileSizeBytes);
      manifest.put("sha256", sha256);
      manifest.put("codec", config.getCodecName());
      manifest.put("local_path", finalPath.toString());
      manifest.put("relative_path", config.getOutputRoot().relativize(finalPath).toString());
      manifest.put("s3_bucket", config.getS3Bucket());
      manifest.put("s3_key", config.buildS3Key(finalPath.getFileName().toString()));
      manifest.put("extractor_instance_id", config.getExtractorInstanceId());
      manifest.put("created_at_utc", openedAt.toString());
      manifest.put("closed_at_utc", closedAt.toString());
      manifest.put("status", "sealed");

      Path tempManifestPath = manifestPath.resolveSibling(manifestPath.getFileName().toString() + ".partial");
      failIfExists(tempManifestPath, "Refusing to overwrite existing partial manifest");
      objectMapper.writerWithDefaultPrettyPrinter().writeValue(tempManifestPath.toFile(), manifest);
      moveAtomically(tempManifestPath, manifestPath);
    }

    private MessageDigest createDigest() throws IOException {
      try {
        return MessageDigest.getInstance("SHA-256");
      } catch (NoSuchAlgorithmException e) {
        throw new IOException("SHA-256 digest unavailable", e);
      }
    }
  }
}
