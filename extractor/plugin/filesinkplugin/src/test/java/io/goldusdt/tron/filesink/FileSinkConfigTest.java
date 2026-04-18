package io.goldusdt.tron.filesink;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import org.junit.Test;

public class FileSinkConfigTest {

  @Test
  public void defaultsFastGzipKnobsWhenUnset() {
    assertEquals(1, FileSinkConfig.parseOptionalGzipLevel(null));
    assertEquals(65536, FileSinkConfig.parseOptionalGzipBufferBytes(null));
  }

  @Test
  public void parsesExplicitFastGzipKnobs() {
    assertEquals(3, FileSinkConfig.parseOptionalGzipLevel("3"));
    assertEquals(131072, FileSinkConfig.parseOptionalGzipBufferBytes("131072"));
  }

  @Test
  public void rejectsOutOfRangeGzipLevel() {
    try {
      FileSinkConfig.parseOptionalGzipLevel("10");
      fail("expected parseOptionalGzipLevel to reject values above 9");
    } catch (IllegalStateException expected) {
      assertEquals(
          "TRON_FILE_SINK_GZIP_LEVEL must be an integer between 1 and 9",
          expected.getMessage());
    }
  }
}
