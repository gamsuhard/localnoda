package io.goldusdt.tron.filesink;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
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
    String triggerName = null;
    Long blockNumber = null;
    String transactionId = null;
    String uniqueId = null;
    Long timeStamp = null;

    try (JsonParser parser = objectMapper.getFactory().createParser(payload)) {
      if (parser.nextToken() != JsonToken.START_OBJECT) {
        throw new IOException("Trigger payload must be a JSON object");
      }
      while (parser.nextToken() != JsonToken.END_OBJECT) {
        String fieldName = parser.currentName();
        if (fieldName == null) {
          parser.skipChildren();
          continue;
        }
        JsonToken valueToken = parser.nextToken();
        if (valueToken == null) {
          break;
        }
        if ("triggerName".equals(fieldName)) {
          triggerName = textValue(parser);
        } else if ("blockNumber".equals(fieldName)) {
          blockNumber = longValue(parser);
        } else if ("transactionId".equals(fieldName)) {
          transactionId = textValue(parser);
        } else if ("uniqueId".equals(fieldName)) {
          uniqueId = textValue(parser);
        } else if ("timeStamp".equals(fieldName)) {
          timeStamp = longValue(parser);
        } else {
          parser.skipChildren();
        }
      }
    }

    triggerName = normalizeTriggerName(triggerName, fallbackType);

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

  private static String textValue(JsonParser parser) throws IOException {
    if (parser.currentToken() == JsonToken.VALUE_NULL) {
      return null;
    }
    String value = parser.getValueAsString();
    return value == null || value.trim().isEmpty() ? null : value;
  }

  private static Long longValue(JsonParser parser) throws IOException {
    JsonToken token = parser.currentToken();
    if (token == null || token == JsonToken.VALUE_NULL) {
      return null;
    }
    if (token == JsonToken.VALUE_NUMBER_INT || token == JsonToken.VALUE_NUMBER_FLOAT) {
      return parser.getLongValue();
    }
    String text = parser.getValueAsString();
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
