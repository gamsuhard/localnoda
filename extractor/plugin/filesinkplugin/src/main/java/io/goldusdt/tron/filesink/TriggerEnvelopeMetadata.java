package io.goldusdt.tron.filesink;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;

final class TriggerEnvelopeMetadata {

  private final String triggerName;
  private final Long blockNumber;
  private final String transactionId;
  private final String uniqueId;
  private final String eventKey;
  private final Long timeStamp;

  private TriggerEnvelopeMetadata(
      String triggerName,
      Long blockNumber,
      String transactionId,
      String uniqueId,
      String eventKey,
      Long timeStamp) {
    this.triggerName = triggerName;
    this.blockNumber = blockNumber;
    this.transactionId = transactionId;
    this.uniqueId = uniqueId;
    this.eventKey = eventKey;
    this.timeStamp = timeStamp;
  }

  static TriggerEnvelopeMetadata parse(ObjectMapper objectMapper, String payload, EventType fallbackType)
      throws IOException {
    JsonNode root = objectMapper.readTree(payload);
    String triggerName = normalizeTriggerName(text(root, "triggerName", null), fallbackType);
    Long blockNumber = longValue(root.get("blockNumber"));
    String transactionId = text(root, "transactionId", null);
    String uniqueId = text(root, "uniqueId", null);
    Long timeStamp = longValue(root.get("timeStamp"));

    String eventKey = firstNonBlank(
        uniqueId,
        transactionId,
        triggerName + ":" + nullableToString(blockNumber) + ":" + nullableToString(timeStamp));

    return new TriggerEnvelopeMetadata(triggerName, blockNumber, transactionId, uniqueId, eventKey, timeStamp);
  }

  String getTriggerName() {
    return triggerName;
  }

  Long getBlockNumber() {
    return blockNumber;
  }

  String getTransactionId() {
    return transactionId;
  }

  String getUniqueId() {
    return uniqueId;
  }

  String getEventKey() {
    return eventKey;
  }

  Long getTimeStamp() {
    return timeStamp;
  }

  private static String text(JsonNode root, String fieldName, String fallback) {
    JsonNode node = root.get(fieldName);
    if (node == null || node.isNull()) {
      return fallback;
    }
    String value = node.asText();
    return value == null || value.trim().isEmpty() ? fallback : value;
  }

  private static Long longValue(JsonNode node) {
    if (node == null || node.isNull()) {
      return null;
    }
    if (node.canConvertToLong()) {
      return node.asLong();
    }
    String text = node.asText();
    if (text == null || text.trim().isEmpty()) {
      return null;
    }
    try {
      return Long.valueOf(text);
    } catch (NumberFormatException ignored) {
      return null;
    }
  }

  private static String nullableToString(Object value) {
    return value == null ? "null" : String.valueOf(value);
  }

  private static String normalizeTriggerName(String rawTriggerName, EventType fallbackType) {
    if (rawTriggerName == null || rawTriggerName.trim().isEmpty()) {
      return fallbackType.getTriggerName();
    }
    if (fallbackType == EventType.SOLIDITY_LOG
        && EventType.CONTRACT_LOG.getTriggerName().equals(rawTriggerName)) {
      return fallbackType.getTriggerName();
    }
    if (fallbackType == EventType.SOLIDITY_EVENT
        && EventType.CONTRACT_EVENT.getTriggerName().equals(rawTriggerName)) {
      return fallbackType.getTriggerName();
    }
    return rawTriggerName;
  }

  private static String firstNonBlank(String... values) {
    for (String value : values) {
      if (value != null && !value.trim().isEmpty()) {
        return value;
      }
    }
    return null;
  }
}
