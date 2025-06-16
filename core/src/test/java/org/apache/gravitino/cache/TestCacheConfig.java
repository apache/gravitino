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

package org.apache.gravitino.cache;

import com.google.common.collect.Lists;
import java.util.ArrayList;
import java.util.Optional;
import org.apache.gravitino.Config;
import org.apache.gravitino.Configs;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestCacheConfig {
  @Test
  void testDefaultCacheConfig() {
    Config config = new Config(false) {};
    Assertions.assertFalse(config.get(Configs.CACHE_STATS_ENABLED));
    Assertions.assertTrue(config.get(Configs.CACHE_ENABLED));
    Assertions.assertTrue(config.get(Configs.CACHE_WEIGHER_ENABLED));
    Assertions.assertEquals(10_000, config.get(Configs.CACHE_MAX_ENTRIES));
    Assertions.assertEquals(3_600_000L, config.get(Configs.CACHE_EXPIRATION_TIME));
    Assertions.assertEquals(200_302_000L, EntityCacheWeigher.getMaxWeight());
  }

  @Test
  void test() {
    ArrayList<Integer> list = Lists.newArrayList();
    Optional<Integer> i =
        Optional.ofNullable(list).filter(entities -> !entities.isEmpty()).map(l -> l.get(0));
    Assertions.assertFalse(i.isPresent());
  }
}
