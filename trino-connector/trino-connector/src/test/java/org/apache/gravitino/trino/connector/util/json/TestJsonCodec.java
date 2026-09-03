/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.gravitino.trino.connector.util.json;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import io.trino.FeaturesConfig;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.BlockEncodingSerde;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.TypeManager;
import java.util.Set;
import org.junit.jupiter.api.Test;

class TestJsonCodec {

  static class NoArgManager {}

  static class FeaturesConfigManager {
    final FeaturesConfig featuresConfig;

    public FeaturesConfigManager(FeaturesConfig featuresConfig) {
      this.featuresConfig = featuresConfig;
    }
  }

  static class SetManager {
    final Set<?> encodings;

    public SetManager(Set<?> encodings) {
      this.encodings = encodings;
    }
  }

  static class FallbackManager {
    final int flag;

    private FallbackManager(int flag) {
      this.flag = flag;
    }
  }

  static class UnresolvableDependency {
    UnresolvableDependency(String required) {}
  }

  static class UnresolvableManager {
    final UnresolvableDependency dependency;

    private UnresolvableManager(UnresolvableDependency dependency) {
      this.dependency = dependency;
    }
  }

  private static final ClassLoader CLASS_LOADER = TestJsonCodec.class.getClassLoader();

  @Test
  void testPrefersNoArgConstructor() throws Exception {
    Object instance = JsonCodec.instantiateBlockEncodingManager(NoArgManager.class, CLASS_LOADER);
    assertThat(instance).isInstanceOf(NoArgManager.class);
  }

  @Test
  void testFallsBackToFeaturesConfigConstructor() throws Exception {
    Object instance =
        JsonCodec.instantiateBlockEncodingManager(FeaturesConfigManager.class, CLASS_LOADER);
    assertThat(instance).isInstanceOfSatisfying(FeaturesConfigManager.class, m -> {});
  }

  @Test
  void testFallsBackToSetConstructor() throws Exception {
    Object instance = JsonCodec.instantiateBlockEncodingManager(SetManager.class, CLASS_LOADER);
    assertThat(instance)
        .isInstanceOfSatisfying(SetManager.class, m -> assertThat(m.encodings).isEmpty());
  }

  @Test
  void testFallsBackToReflectiveScanForOtherConstructors() throws Exception {
    Object instance =
        JsonCodec.instantiateBlockEncodingManager(FallbackManager.class, CLASS_LOADER);
    assertThat(instance)
        .isInstanceOfSatisfying(FallbackManager.class, m -> assertThat(m.flag).isEqualTo(0));
  }

  @Test
  void testThrowsWhenNoConstructorIsResolvable() {
    assertThatThrownBy(
            () ->
                JsonCodec.instantiateBlockEncodingManager(UnresolvableManager.class, CLASS_LOADER))
        .isInstanceOf(NoSuchMethodException.class);
  }

  /**
   * Round-trips a Trino {@link Block} (including a null position) through {@link BlockJsonSerde} so
   * the full serde path is exercised against the real Trino runtime of whichever version-segment
   * module runs this shared test: the reflective BlockEncodingManager construction, the {@code
   * BlockSerdeUtil} write/read reflection, and the block null representation. This guards against
   * silent breakage of the block wire format across supported Trino versions (e.g. Trino 482-483).
   */
  @Test
  void testBlockJsonSerdeRoundTripsBlockWithNull() throws Exception {
    TypeManager typeManager = JsonCodec.createTypeManager(CLASS_LOADER);
    BlockEncodingSerde blockEncodingSerde = JsonCodec.createBlockEncodingSerde(typeManager);

    SimpleModule module = new SimpleModule();
    module.addSerializer(Block.class, new BlockJsonSerde.Serializer(blockEncodingSerde));
    module.addDeserializer(Block.class, new BlockJsonSerde.Deserializer(blockEncodingSerde));
    ObjectMapper mapper = new ObjectMapper().registerModule(module);

    // positions: 1, null, 42. Built via the type block builder (portable across Trino versions)
    // rather than a concrete block constructor, whose null representation changed in Trino 483.
    BlockBuilder builder = BigintType.BIGINT.createBlockBuilder(null, 3);
    BigintType.BIGINT.writeLong(builder, 1L);
    builder.appendNull();
    BigintType.BIGINT.writeLong(builder, 42L);
    Block block = builder.build();

    String json = mapper.writeValueAsString(block);
    Block roundTripped = mapper.readValue(json, Block.class);

    assertThat(roundTripped.getPositionCount()).isEqualTo(3);
    assertThat(roundTripped.isNull(0)).isFalse();
    assertThat(roundTripped.isNull(1)).isTrue();
    assertThat(roundTripped.isNull(2)).isFalse();
    assertThat(BigintType.BIGINT.getLong(roundTripped, 0)).isEqualTo(1L);
    assertThat(BigintType.BIGINT.getLong(roundTripped, 2)).isEqualTo(42L);
  }
}
