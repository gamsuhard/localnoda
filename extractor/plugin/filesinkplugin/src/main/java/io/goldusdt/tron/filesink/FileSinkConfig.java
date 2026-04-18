package io.goldusdt.tron.filesink;

import java.nio.file.Path;
import java.nio.file.Paths;

final class FileSinkConfig {

  private static final String ENV_PREFIX = "TRON_FILE_SINK_";

  private final String runId;
  private final String streamName;
  private final String compression;
  private final String expectedTriggerName;
  private final String contractAddress;
  private final String topic0;
  private final String extractorInstanceId;
  private final Path outputRoot;
  private final Path segmentsDir;
  private final Path manifestsDir;
  private final long segmentMaxBytes;
  private final int segmentMaxRecords;
  private final int flushEveryRecords;
  private final int gzipLevel;
  private final int gzipBufferBytes;
  private final int maxQueueRecords;
  private final String s3Bucket;
  private final String s3PrefixRoot;
  private final String sseMode;
  private final String kmsKeyArn;
  private final String pluginBuildId;

  private FileSinkConfig(
      String runId,
      String streamName,
      String compression,
      String expectedTriggerName,
      String contractAddress,
      String topic0,
      String extractorInstanceId,
      Path outputRoot,
      long segmentMaxBytes,
      int segmentMaxRecords,
      int flushEveryRecords,
      int gzipLevel,
      int gzipBufferBytes,
      int maxQueueRecords,
      String s3Bucket,
      String s3PrefixRoot,
      String sseMode,
      String kmsKeyArn,
      String pluginBuildId) {
    this.runId = runId;
    this.streamName = streamName;
    this.compression = compression;
    this.expectedTriggerName = expectedTriggerName;
    this.contractAddress = contractAddress;
    this.topic0 = topic0;
    this.extractorInstanceId = extractorInstanceId;
    this.outputRoot = outputRoot;
    this.segmentsDir = outputRoot.resolve("segments");
    this.manifestsDir = outputRoot.resolve("manifests");
    this.segmentMaxBytes = segmentMaxBytes;
    this.segmentMaxRecords = segmentMaxRecords;
    this.flushEveryRecords = flushEveryRecords;
    this.gzipLevel = gzipLevel;
    this.gzipBufferBytes = gzipBufferBytes;
    this.maxQueueRecords = maxQueueRecords;
    this.s3Bucket = s3Bucket;
    this.s3PrefixRoot = s3PrefixRoot;
    this.sseMode = sseMode;
    this.kmsKeyArn = kmsKeyArn;
    this.pluginBuildId = pluginBuildId;
  }

  static FileSinkConfig fromEnvironment(String pluginBuildId) {
    String expectedTriggerName = requiredEnv("EXPECTED_TRIGGER_NAME");
    if (!EventType.SOLIDITY_LOG.getTriggerName().equals(expectedTriggerName)) {
      throw new IllegalStateException(
          "TRON_FILE_SINK_EXPECTED_TRIGGER_NAME must equal " + EventType.SOLIDITY_LOG.getTriggerName());
    }

    Path outputRoot = Paths.get(requiredEnv("OUTPUT_ROOT"));
    if (!outputRoot.isAbsolute()) {
      throw new IllegalStateException("TRON_FILE_SINK_OUTPUT_ROOT must be an absolute path");
    }

    String sseMode = trimToNull(env("SSE_MODE"));
    String kmsKeyArn = trimToNull(env("KMS_KEY_ARN"));
    if ("aws:kms".equalsIgnoreCase(sseMode) && kmsKeyArn == null) {
      throw new IllegalStateException(
          "TRON_FILE_SINK_KMS_KEY_ARN is required when TRON_FILE_SINK_SSE_MODE=aws:kms");
    }

    return new FileSinkConfig(
        requiredEnv("RUN_ID"),
        requiredEnv("STREAM_NAME"),
        parseCompression(requiredEnv("COMPRESSION")),
        expectedTriggerName,
        requiredEnv("CONTRACT_ADDRESS"),
        normalizeTopic0(requiredEnv("TOPIC0")),
        requiredEnv("EXTRACTOR_INSTANCE_ID"),
        outputRoot,
        parsePositiveLong(requiredEnv("SEGMENT_MAX_BYTES"), "SEGMENT_MAX_BYTES"),
        parsePositiveInt(requiredEnv("SEGMENT_MAX_RECORDS"), "SEGMENT_MAX_RECORDS"),
        parsePositiveInt(requiredEnv("FLUSH_EVERY_RECORDS"), "FLUSH_EVERY_RECORDS"),
        parseOptionalGzipLevel(env("GZIP_LEVEL")),
        parseOptionalGzipBufferBytes(env("GZIP_BUFFER_BYTES")),
        parsePositiveInt(requiredEnv("MAX_QUEUE_RECORDS"), "MAX_QUEUE_RECORDS"),
        requiredEnv("S3_BUCKET"),
        normalizePrefix(requiredEnv("S3_PREFIX_ROOT")),
        sseMode,
        kmsKeyArn,
        firstNonBlank(trimToNull(pluginBuildId), "dev-build"));
  }

  String getRunId() {
    return runId;
  }

  String getStreamName() {
    return streamName;
  }

  String getCompression() {
    return compression;
  }

  String getCodecName() {
    return isGzipEnabled() ? "ndjson.gz" : "ndjson";
  }

  boolean isGzipEnabled() {
    return "gzip".equalsIgnoreCase(compression);
  }

  String getExpectedTriggerName() {
    return expectedTriggerName;
  }

  String getContractAddress() {
    return contractAddress;
  }

  String getTopic0() {
    return topic0;
  }

  String getExtractorInstanceId() {
    return extractorInstanceId;
  }

  Path getOutputRoot() {
    return outputRoot;
  }

  Path getSegmentsDir() {
    return segmentsDir;
  }

  Path getManifestsDir() {
    return manifestsDir;
  }

  long getSegmentMaxBytes() {
    return segmentMaxBytes;
  }

  int getSegmentMaxRecords() {
    return segmentMaxRecords;
  }

  int getFlushEveryRecords() {
    return flushEveryRecords;
  }

  int getGzipLevel() {
    return gzipLevel;
  }

  int getGzipBufferBytes() {
    return gzipBufferBytes;
  }

  int getMaxQueueRecords() {
    return maxQueueRecords;
  }

  String getS3Bucket() {
    return s3Bucket;
  }

  String getSseMode() {
    return sseMode;
  }

  String getKmsKeyArn() {
    return kmsKeyArn;
  }

  String getPluginBuildId() {
    return pluginBuildId;
  }

  String segmentFileName(int sequence) {
    return String.format("%s_%06d.ndjson%s", streamName, sequence, fileSuffix());
  }

  String manifestFileName(int sequence) {
    return String.format("%s_%06d.manifest.json", streamName, sequence);
  }

  String partialFileName(int sequence) {
    return segmentFileName(sequence) + ".partial";
  }

  String segmentId(int sequence) {
    return String.format("%s-seg-%06d", runId, sequence);
  }

  String buildS3Key(String fileName) {
    return s3PrefixRoot + "/runs/" + runId + "/segments/" + fileName;
  }

  private String fileSuffix() {
    return isGzipEnabled() ? ".gz" : "";
  }

  private static String env(String name) {
    return trimToNull(System.getenv(ENV_PREFIX + name));
  }

  private static String requiredEnv(String name) {
    String value = env(name);
    if (value == null) {
      throw new IllegalStateException("Missing required environment variable " + ENV_PREFIX + name);
    }
    return value;
  }

  private static String firstNonBlank(String... candidates) {
    for (String candidate : candidates) {
      String normalized = trimToNull(candidate);
      if (normalized != null) {
        return normalized;
      }
    }
    return null;
  }

  private static long parsePositiveLong(String raw, String fieldName) {
    try {
      long parsed = Long.parseLong(raw);
      if (parsed <= 0L) {
        throw new NumberFormatException("must be > 0");
      }
      return parsed;
    } catch (NumberFormatException e) {
      throw new IllegalStateException("TRON_FILE_SINK_" + fieldName + " must be a positive integer", e);
    }
  }

  private static int parsePositiveInt(String raw, String fieldName) {
    try {
      int parsed = Integer.parseInt(raw);
      if (parsed <= 0) {
        throw new NumberFormatException("must be > 0");
      }
      return parsed;
    } catch (NumberFormatException e) {
      throw new IllegalStateException("TRON_FILE_SINK_" + fieldName + " must be a positive integer", e);
    }
  }

  static int parseOptionalGzipLevel(String raw) {
    if (raw == null) {
      return 1;
    }
    try {
      int parsed = Integer.parseInt(raw);
      if (parsed < 1 || parsed > 9) {
        throw new NumberFormatException("must be between 1 and 9");
      }
      return parsed;
    } catch (NumberFormatException e) {
      throw new IllegalStateException("TRON_FILE_SINK_GZIP_LEVEL must be an integer between 1 and 9", e);
    }
  }

  static int parseOptionalGzipBufferBytes(String raw) {
    if (raw == null) {
      return 65536;
    }
    return parsePositiveInt(raw, "GZIP_BUFFER_BYTES");
  }

  private static String parseCompression(String value) {
    if ("gzip".equalsIgnoreCase(value)) {
      return "gzip";
    }
    if ("none".equalsIgnoreCase(value)) {
      return "none";
    }
    throw new IllegalStateException("TRON_FILE_SINK_COMPRESSION must be gzip or none");
  }

  private static String normalizePrefix(String prefix) {
    String normalized = trimToNull(prefix);
    while (normalized.endsWith("/")) {
      normalized = normalized.substring(0, normalized.length() - 1);
    }
    return normalized;
  }

  private static String normalizeTopic0(String value) {
    String normalized = trimToNull(value);
    if (normalized.startsWith("0x") || normalized.startsWith("0X")) {
      normalized = normalized.substring(2);
    }
    return normalized.toLowerCase();
  }

  private static String trimToNull(String value) {
    if (value == null) {
      return null;
    }
    String normalized = value.trim();
    return normalized.isEmpty() ? null : normalized;
  }
}
