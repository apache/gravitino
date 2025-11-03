package org.apache.gravitino.lance.common.utils;

import com.google.common.base.Preconditions;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.channels.Channels;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.types.pojo.Schema;

public class ArrowUtils {
  public static byte[] generateIpcStream(Schema arrowSchema) throws IOException {
    try (BufferAllocator allocator = new RootAllocator()) {

      // Create an empty VectorSchemaRoot with the schema
      try (VectorSchemaRoot root = VectorSchemaRoot.create(arrowSchema, allocator)) {
        // Allocate empty vectors (0 rows)
        root.allocateNew();
        root.setRowCount(0);

        // Write to IPC stream
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        try (ArrowStreamWriter writer =
            new ArrowStreamWriter(root, null, Channels.newChannel(outputStream))) {
          writer.start();
          writer.writeBatch();
          writer.end();
        }

        return outputStream.toByteArray();
      }
    } catch (Exception e) {
      throw new IOException("Failed to create empty Arrow IPC stream: " + e.getMessage(), e);
    }
  }

  public static org.apache.arrow.vector.types.pojo.Schema parseArrowIpcStream(byte[] stream) {
    org.apache.arrow.vector.types.pojo.Schema schema;
    try (BufferAllocator allocator = new RootAllocator();
        ByteArrayInputStream bais = new ByteArrayInputStream(stream);
        ArrowStreamReader reader = new ArrowStreamReader(bais, allocator)) {
      schema = reader.getVectorSchemaRoot().getSchema();
    } catch (Exception e) {
      throw new RuntimeException("Failed to parse Arrow IPC stream", e);
    }

    Preconditions.checkArgument(schema != null, "No schema found in Arrow IPC stream");
    return schema;
  }
}
