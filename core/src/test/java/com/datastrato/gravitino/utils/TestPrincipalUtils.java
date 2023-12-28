/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.utils;

import com.datastrato.gravitino.UserPrincipal;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestPrincipalUtils {

  @Test
  public void testNormal() throws Exception {
    UserPrincipal principal = new UserPrincipal("testNormal");
    PrincipalUtils.doAs(
        principal,
        () -> {
          Assertions.assertEquals("testNormal", PrincipalUtils.getCurrentPrincipal().getName());
          return null;
        });
  }

  @Test
  public void testThread() throws Exception {
    UserPrincipal principal = new UserPrincipal("testThread");
    PrincipalUtils.doAs(
        principal,
        () -> {
          Thread thread =
              new Thread(
                  () ->
                      Assertions.assertEquals(
                          "testThread", PrincipalUtils.getCurrentPrincipal().getName()));
          thread.start();
          thread.join();
          return null;
        });
  }

  @Test
  public void testThreadPool() throws Exception {
    UserPrincipal principal = new UserPrincipal("testThreadPool");
    ExecutorService executorService = Executors.newCachedThreadPool();
    PrincipalUtils.doAs(
        principal,
        () -> {
          Future<?> future =
              executorService.submit(
                  () ->
                      Assertions.assertEquals(
                          "testThreadPool", PrincipalUtils.getCurrentPrincipal().getName()));
          future.get();
          return null;
        });
    executorService.shutdownNow();
  }
}
