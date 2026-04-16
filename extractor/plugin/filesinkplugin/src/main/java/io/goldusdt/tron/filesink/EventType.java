package io.goldusdt.tron.filesink;

enum EventType {
  BLOCK(0, "blockTrigger"),
  TRANSACTION(1, "transactionTrigger"),
  CONTRACT_LOG(2, "contractLogTrigger"),
  CONTRACT_EVENT(3, "contractEventTrigger"),
  SOLIDITY(4, "solidityTrigger"),
  SOLIDITY_EVENT(5, "solidityEventTrigger"),
  SOLIDITY_LOG(6, "solidityLogTrigger");

  private final int code;
  private final String triggerName;

  EventType(int code, String triggerName) {
    this.code = code;
    this.triggerName = triggerName;
  }

  int getCode() {
    return code;
  }

  String getTriggerName() {
    return triggerName;
  }
}
