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

package org.apache.gravitino.auxiliary;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.google.common.collect.ImmutableMap;
import java.util.Collections;
import java.util.Map;
import org.apache.gravitino.Config;
import org.apache.gravitino.utils.IsolatedClassLoader;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestAuxiliaryServiceManager {

  static class DummyConfig extends Config {

    public static DummyConfig of(Map<String, String> m) {
      DummyConfig dummyConfig = new DummyConfig();
      dummyConfig.loadFromMap(m, k -> true);
      return dummyConfig;
    }
  }

  @Test
  public void testGravitinoAuxServiceManagerEmptyServiceName() throws Exception {
    AuxiliaryServiceManager auxServiceManager = new AuxiliaryServiceManager();
    auxServiceManager.serviceInit(
        DummyConfig.of(
            ImmutableMap.of(
                AuxiliaryServiceManager.GRAVITINO_AUX_SERVICE_PREFIX
                    + AuxiliaryServiceManager.AUX_SERVICE_NAMES,
                "")));
    auxServiceManager.serviceStart();
    auxServiceManager.serviceStop();
  }

  @Test
  public void testGravitinoAuxServiceNotSetClassPath() {
    AuxiliaryServiceManager auxServiceManager = new AuxiliaryServiceManager();
    Assertions.assertThrowsExactly(
        IllegalArgumentException.class,
        () ->
            auxServiceManager.serviceInit(
                DummyConfig.of(
                    ImmutableMap.of(
                        AuxiliaryServiceManager.GRAVITINO_AUX_SERVICE_PREFIX
                            + AuxiliaryServiceManager.AUX_SERVICE_NAMES,
                        "mock1"))));
  }

  @Test
  public void testGravitinoAuxServiceManager() throws Exception {
    GravitinoAuxiliaryService auxService = mock(GravitinoAuxiliaryService.class);
    GravitinoAuxiliaryService auxService2 = mock(GravitinoAuxiliaryService.class);

    IsolatedClassLoader isolatedClassLoader =
        new IsolatedClassLoader(
            Collections.emptyList(), Collections.emptyList(), Collections.emptyList());

    AuxiliaryServiceManager auxServiceManager = new AuxiliaryServiceManager();
    AuxiliaryServiceManager spyAuxManager = spy(auxServiceManager);

    doReturn(isolatedClassLoader).when(spyAuxManager).getIsolatedClassLoader(anyList());
    doReturn(auxService).when(spyAuxManager).loadAuxService("mock1", isolatedClassLoader);
    doReturn(auxService2).when(spyAuxManager).loadAuxService("mock2", isolatedClassLoader);

    spyAuxManager.serviceInit(
        DummyConfig.of(
            ImmutableMap.of(
                AuxiliaryServiceManager.GRAVITINO_AUX_SERVICE_PREFIX
                    + AuxiliaryServiceManager.AUX_SERVICE_NAMES,
                "mock1,mock2",
                "gravitino.mock1." + AuxiliaryServiceManager.AUX_SERVICE_CLASSPATH,
                "/tmp",
                "gravitino.mock2." + AuxiliaryServiceManager.AUX_SERVICE_CLASSPATH,
                "/tmp")));
    verify(auxService, times(1)).serviceInit(any());
    verify(auxService2, times(1)).serviceInit(any());

    spyAuxManager.serviceStart();
    verify(auxService, times(1)).serviceStart();
    verify(auxService2, times(1)).serviceStart();

    spyAuxManager.serviceStop();
    verify(auxService, times(1)).serviceStop();
    verify(auxService2, times(1)).serviceStop();
  }

  @Test
  void testAuxiliaryServiceConfigs() {
    Map<String, String> m =
        ImmutableMap.of(
            AuxiliaryServiceManager.GRAVITINO_AUX_SERVICE_PREFIX
                + AuxiliaryServiceManager.AUX_SERVICE_NAMES,
            "a,b",
            "gravitino.a.test1",
            "test1",
            AuxiliaryServiceManager.GRAVITINO_AUX_SERVICE_PREFIX + "a.test4",
            "test4",
            "gravitino.b.test2",
            "test2",
            "gravitino.aa.test3",
            "test3");
    DummyConfig config = new DummyConfig();
    config.loadFromMap(m, k -> true);
    Map<String, String> serviceConfigs =
        AuxiliaryServiceManager.extractAuxiliaryServiceConfigs(config);
    Assertions.assertEquals(
        ImmutableMap.of(
            AuxiliaryServiceManager.AUX_SERVICE_NAMES,
            "a,b",
            "a.test1",
            "test1",
            "a.test4",
            "test4",
            "b.test2",
            "test2"),
        serviceConfigs);

    // Test gravitino.a.test1 overwrite gravitino.auxService.a.test1
    m =
        ImmutableMap.of(
            AuxiliaryServiceManager.GRAVITINO_AUX_SERVICE_PREFIX
                + AuxiliaryServiceManager.AUX_SERVICE_NAMES,
            "a",
            "gravitino.a.test1",
            "test1",
            AuxiliaryServiceManager.GRAVITINO_AUX_SERVICE_PREFIX + "a.test1",
            "test4");
    DummyConfig config2 = new DummyConfig();
    config2.loadFromMap(m, k -> true);
    serviceConfigs = AuxiliaryServiceManager.extractAuxiliaryServiceConfigs(config2);
    Assertions.assertEquals(
        ImmutableMap.of(AuxiliaryServiceManager.AUX_SERVICE_NAMES, "a", "a.test1", "test1"),
        serviceConfigs);
  }
}
