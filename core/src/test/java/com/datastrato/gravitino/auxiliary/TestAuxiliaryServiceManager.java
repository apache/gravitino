/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.auxiliary;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.datastrato.gravitino.utils.IsolatedClassLoader;
import com.google.common.collect.ImmutableMap;
import java.util.Collections;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestAuxiliaryServiceManager {

  @Test
  public void testGravitinoAuxServiceManagerEmptyServiceName() throws Exception {
    AuxiliaryServiceManager auxServiceManager = new AuxiliaryServiceManager();
    auxServiceManager.serviceInit(ImmutableMap.of(AuxiliaryServiceManager.AUX_SERVICE_NAMES, ""));
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
                ImmutableMap.of(AuxiliaryServiceManager.AUX_SERVICE_NAMES, "mock1")));
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
        ImmutableMap.of(
            AuxiliaryServiceManager.AUX_SERVICE_NAMES,
            "mock1,mock2",
            "mock1." + AuxiliaryServiceManager.AUX_SERVICE_CLASSPATH,
            "/tmp",
            "mock2." + AuxiliaryServiceManager.AUX_SERVICE_CLASSPATH,
            "/tmp"));
    verify(auxService, times(1)).serviceInit(any());
    verify(auxService2, times(1)).serviceInit(any());

    spyAuxManager.serviceStart();
    verify(auxService, times(1)).serviceStart();
    verify(auxService2, times(1)).serviceStart();

    spyAuxManager.serviceStop();
    verify(auxService, times(1)).serviceStop();
    verify(auxService2, times(1)).serviceStop();
  }
}
