/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.graviton.integration.e2e.web.rest;

import com.datastrato.graviton.client.GravitonVersion;
import com.datastrato.graviton.integration.util.AbstractIT;
import com.datastrato.graviton.integration.util.CommandExecutor;
import com.datastrato.graviton.integration.util.ProcessData;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class VersionOperationsIT extends AbstractIT {
  @Test
  public void testGetVersion() {
    Object ret =
        CommandExecutor.executeCommandLocalHost(
            "git rev-parse HEAD", false, ProcessData.TypesOfData.OUTPUT);
    String gitCommitId = ret.toString().replace("\n", "");

    GravitonVersion gravitonVersion = client.getVersion();
    Assertions.assertEquals(System.getenv("PROJECT_VERSION"), gravitonVersion.version());
    Assertions.assertFalse(gravitonVersion.compileDate().isEmpty());
    Assertions.assertEquals(gitCommitId, gravitonVersion.gitCommit());
  }
}
