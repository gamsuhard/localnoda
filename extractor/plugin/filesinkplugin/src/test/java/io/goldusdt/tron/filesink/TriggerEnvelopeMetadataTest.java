package io.goldusdt.tron.filesink;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Test;

public class TriggerEnvelopeMetadataTest {

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  @Test
  public void parsesTopLevelFieldsFromPayload() throws Exception {
    String payload =
        "{"
            + "\"triggerName\":\"solidityLogTrigger\","
            + "\"blockNumber\":12345,"
            + "\"transactionId\":\"tx-1\","
            + "\"uniqueId\":\"tx-1_0\","
            + "\"timeStamp\":1713446400123,"
            + "\"ignored\":{\"nested\":true}"
            + "}";

    TriggerEnvelopeMetadata metadata =
        TriggerEnvelopeMetadata.parse(OBJECT_MAPPER, payload, EventType.SOLIDITY_LOG);

    assertEquals("solidityLogTrigger", metadata.getTriggerName());
    assertEquals(Long.valueOf(12345L), metadata.getBlockNumber());
    assertEquals("tx-1", metadata.getTransactionId());
    assertEquals("tx-1_0", metadata.getUniqueId());
    assertEquals("tx-1_0", metadata.getEventKey());
    assertEquals(Long.valueOf(1713446400123L), metadata.getTimeStamp());
  }

  @Test
  public void normalizesLegacyContractLogTriggerName() throws Exception {
    String payload = "{\"triggerName\":\"contractLogTrigger\",\"blockNumber\":12345}";

    TriggerEnvelopeMetadata metadata =
        TriggerEnvelopeMetadata.parse(OBJECT_MAPPER, payload, EventType.SOLIDITY_LOG);

    assertEquals("solidityLogTrigger", metadata.getTriggerName());
  }

  @Test
  public void fallsBackToSyntheticEventKeyWhenIdsMissing() throws Exception {
    String payload = "{\"blockNumber\":12345,\"timeStamp\":1713446400123}";

    TriggerEnvelopeMetadata metadata =
        TriggerEnvelopeMetadata.parse(OBJECT_MAPPER, payload, EventType.SOLIDITY_LOG);

    assertNull(metadata.getTransactionId());
    assertNull(metadata.getUniqueId());
    assertEquals("solidityLogTrigger:12345:1713446400123", metadata.getEventKey());
  }
}
