package io.goldusdt.tron.filesink;

import java.io.IOException;
import java.io.OutputStream;
import java.util.zip.Deflater;
import java.util.zip.GZIPOutputStream;

final class TunedGzipOutputStream extends GZIPOutputStream {

  TunedGzipOutputStream(OutputStream outputStream, int bufferSize, int gzipLevel) throws IOException {
    super(outputStream, bufferSize);
    def.setLevel(gzipLevel);
  }
}
