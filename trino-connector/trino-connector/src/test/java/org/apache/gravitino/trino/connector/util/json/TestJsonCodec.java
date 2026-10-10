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

import io.trino.FeaturesConfig;
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
}
