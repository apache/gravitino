/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.gravitino.trino.connector.util.json;

import static java.lang.Math.toIntExact;

import com.fasterxml.jackson.core.Base64Variants;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceOutput;
import io.airlift.slice.Slices;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockEncodingSerde;
import java.io.IOException;
import java.lang.reflect.Method;

/**
 * This class is reference to Trino source code io.trino.block.BlockJsonSerde, use refactoring to
 * call the key method to avoid class loader isolation
 */
public final class BlockJsonSerde {
  private static final String BLOCK_SERDE_UTIL_CLASS_NAME = "io.trino.block.BlockSerdeUtil";

  /**
   * Jackson serializer for Trino {@link Block} objects. Serializes block instances using Trino's
   * internal BlockSerdeUtil via reflection.
   */
  public static class Serializer extends JsonSerializer<Block> {
    private final BlockEncodingSerde blockEncodingSerde;
    private final Method writeBlock;

    /**
     * Constructs a serializer for {@link Block} using the specified encoding serde.
     *
     * @param blockEncodingSerde the encoding serde used to serialize blocks
     * @throws Exception if the internal writeBlock method cannot be resolved via reflection
     */
    public Serializer(BlockEncodingSerde blockEncodingSerde) throws Exception {
      this.blockEncodingSerde = blockEncodingSerde;
      Class<?> clazz =
          blockEncodingSerde.getClass().getClassLoader().loadClass(BLOCK_SERDE_UTIL_CLASS_NAME);
      this.writeBlock =
          clazz.getDeclaredMethod(
              "writeBlock", BlockEncodingSerde.class, SliceOutput.class, Block.class);
    }

    @Override
    public void serialize(
        Block block, JsonGenerator jsonGenerator, SerializerProvider serializerProvider)
        throws IOException {
      //  Encoding name is length prefixed as are many block encodings
      SliceOutput output =
          new DynamicSliceOutput(
              toIntExact(
                  block.getSizeInBytes() + block.getEncodingName().length() + (2 * Integer.BYTES)));

      try {
        writeBlock.invoke(null, blockEncodingSerde, output, block);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }

      Slice slice = output.slice();
      jsonGenerator.writeBinary(
          Base64Variants.MIME_NO_LINEFEEDS,
          slice.byteArray(),
          slice.byteArrayOffset(),
          slice.length());
    }
  }

  /**
   * Jackson deserializer for Trino {@link Block} objects. Deserializes block data using Trino's
   * internal BlockSerdeUtil via reflection.
   */
  public static class Deserializer extends JsonDeserializer<Block> {
    private final BlockEncodingSerde blockEncodingSerde;
    private final Method readBlock;

    /**
     * Constructs a deserializer for {@link Block} using the specified encoding serde.
     *
     * @param blockEncodingSerde the encoding serde used to deserialize blocks
     * @throws Exception if the internal readBlock method cannot be resolved via reflection
     */
    public Deserializer(BlockEncodingSerde blockEncodingSerde) throws Exception {
      this.blockEncodingSerde = blockEncodingSerde;
      Class<?> clazz =
          blockEncodingSerde.getClass().getClassLoader().loadClass(BLOCK_SERDE_UTIL_CLASS_NAME);
      readBlock = clazz.getDeclaredMethod("readBlock", BlockEncodingSerde.class, Slice.class);
    }

    @Override
    public Block deserialize(JsonParser jsonParser, DeserializationContext deserializationContext)
        throws IOException {
      byte[] decoded = jsonParser.getBinaryValue(Base64Variants.MIME_NO_LINEFEEDS);
      try {
        return (Block) readBlock.invoke(null, blockEncodingSerde, Slices.wrappedBuffer(decoded));
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }
}
