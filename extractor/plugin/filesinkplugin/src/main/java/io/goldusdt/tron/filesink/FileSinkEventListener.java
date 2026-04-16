package io.goldusdt.tron.filesink;

import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.pf4j.Extension;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tron.common.logsfilter.IPluginEventListener;

@Extension
public class FileSinkEventListener implements IPluginEventListener {

  private static final Logger log = LoggerFactory.getLogger(FileSinkEventListener.class);

  private volatile BlockingQueue<QueuedTrigger> triggerQueue;
  private volatile boolean running;
  private volatile boolean acceptingTriggers;
  private volatile Thread workerThread;
  private volatile SegmentedNdjsonWriter writer;
  private volatile FileSinkConfig config;
  private volatile RuntimeException fatalError;

  @Override
  public void setServerAddress(String address) {
    // Intentionally unused. The file sink is fully env-driven.
  }

  @Override
  public void setTopic(int eventType, String topic) {
    // Intentionally unused. Canonical topic0 is frozen via environment and manifests.
  }

  @Override
  public void setDBConfig(String dbConfig) {
    // Intentionally unused. The file sink is env-driven and does not depend on node DB config.
  }

  @Override
  public synchronized void start() {
    if (running) {
      return;
    }

    try {
      config = FileSinkConfig.fromEnvironment(packageVersion());
      triggerQueue = new LinkedBlockingQueue<QueuedTrigger>(config.getMaxQueueRecords());
      fatalError = null;
      writer = new SegmentedNdjsonWriter(config);
      acceptingTriggers = true;
      running = true;
      workerThread = new Thread(
          new Runnable() {
            @Override
            public void run() {
              processLoop();
            }
          },
          "tron-file-sink-writer");
      workerThread.setDaemon(true);
      workerThread.start();
      log.info(
          "Started file sink plugin with runId={} outputRoot={} expectedTrigger={} contract={} topic0={} maxQueue={}",
          config.getRunId(),
          config.getOutputRoot(),
          config.getExpectedTriggerName(),
          config.getContractAddress(),
          config.getTopic0(),
          Integer.valueOf(config.getMaxQueueRecords()));
    } catch (Exception e) {
      throw new IllegalStateException("Unable to start file sink plugin", e);
    }
  }

  @Override
  public synchronized void stop() {
    acceptingTriggers = false;
    running = false;

    Thread localWorkerThread = workerThread;
    if (localWorkerThread != null && localWorkerThread != Thread.currentThread()) {
      try {
        localWorkerThread.join(30000L);
        if (localWorkerThread.isAlive()) {
          log.warn("File sink worker did not drain within timeout; interrupting with pending={}",
              Integer.valueOf(getPendingSize()));
          localWorkerThread.interrupt();
          localWorkerThread.join(5000L);
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }

    closeWriterQuietly();
    workerThread = null;
    triggerQueue = null;
    config = null;
  }

  @Override
  public void handleBlockEvent(Object data) {
    enqueue(EventType.BLOCK, data);
  }

  @Override
  public void handleTransactionTrigger(Object data) {
    enqueue(EventType.TRANSACTION, data);
  }

  @Override
  public void handleContractLogTrigger(Object data) {
    enqueue(EventType.CONTRACT_LOG, data);
  }

  @Override
  public void handleContractEventTrigger(Object data) {
    enqueue(EventType.CONTRACT_EVENT, data);
  }

  @Override
  public void handleSolidityTrigger(Object trigger) {
    enqueue(EventType.SOLIDITY, trigger);
  }

  @Override
  public void handleSolidityLogTrigger(Object trigger) {
    enqueue(EventType.SOLIDITY_LOG, trigger);
  }

  @Override
  public void handleSolidityEventTrigger(Object trigger) {
    enqueue(EventType.SOLIDITY_EVENT, trigger);
  }

  @Override
  public int getPendingSize() {
    BlockingQueue<QueuedTrigger> queue = triggerQueue;
    return queue == null ? 0 : queue.size();
  }

  private void enqueue(EventType eventType, Object payload) {
    if (payload == null) {
      return;
    }
    ensureHealthy();
    if (!acceptingTriggers) {
      throw new IllegalStateException("File sink plugin is stopping; refusing new triggers");
    }

    BlockingQueue<QueuedTrigger> queue = triggerQueue;
    if (queue == null) {
      throw new IllegalStateException("File sink queue is not initialized");
    }

    QueuedTrigger trigger = new QueuedTrigger(eventType, String.valueOf(payload));
    if (!queue.offer(trigger)) {
      IllegalStateException failure =
          new IllegalStateException(
              "File sink queue capacity exceeded while enqueuing " + eventType.getTriggerName());
      registerFatalFailure(failure);
      throw failure;
    }
  }

  private void processLoop() {
    while (running || hasPendingTriggers()) {
      QueuedTrigger trigger = null;
      try {
        BlockingQueue<QueuedTrigger> queue = triggerQueue;
        if (queue == null) {
          break;
        }
        trigger = queue.poll(1L, TimeUnit.SECONDS);
        if (trigger == null) {
          continue;
        }
        SegmentedNdjsonWriter currentWriter = writer;
        if (currentWriter == null) {
          throw new IllegalStateException(
              "Writer unavailable while pending trigger " + trigger.eventType.getTriggerName() + " exists");
        }
        currentWriter.writeRecord(trigger.eventType, trigger.payload);
      } catch (InterruptedException e) {
        if (!running && !hasPendingTriggers()) {
          Thread.currentThread().interrupt();
          break;
        }
      } catch (Exception e) {
        registerFatalFailure(
            new IllegalStateException(
                "File sink writer loop failed for trigger "
                    + (trigger == null ? "<unknown>" : trigger.eventType.getTriggerName()),
                e));
        break;
      }
    }
  }

  private boolean hasPendingTriggers() {
    return getPendingSize() > 0;
  }

  private void ensureHealthy() {
    RuntimeException failure = fatalError;
    if (failure != null) {
      throw failure;
    }
  }

  private void registerFatalFailure(RuntimeException failure) {
    if (fatalError == null) {
      fatalError = failure;
      acceptingTriggers = false;
      running = false;
      log.error("File sink entered fatal state", failure);
      closeWriterQuietly();
    }
  }

  private void closeWriterQuietly() {
    SegmentedNdjsonWriter currentWriter = writer;
    if (currentWriter == null) {
      return;
    }
    try {
      currentWriter.close();
    } catch (IOException e) {
      log.error("Failed to close file sink writer cleanly", e);
    } finally {
      writer = null;
    }
  }

  private String packageVersion() {
    Package pluginPackage = FileSinkEventListener.class.getPackage();
    return pluginPackage == null ? null : pluginPackage.getImplementationVersion();
  }

  private static final class QueuedTrigger {

    private final EventType eventType;
    private final String payload;

    private QueuedTrigger(EventType eventType, String payload) {
      this.eventType = Objects.requireNonNull(eventType, "eventType");
      this.payload = Objects.requireNonNull(payload, "payload");
    }
  }
}
