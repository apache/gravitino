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
package org.apache.gravitino.model;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.exceptions.ModelVersionAliasesAlreadyExistException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestModelCatalog {

  /** A minimal catalog whose linkModelVersion always fails, to exercise the failure path. */
  private static class FailingLinkModelCatalog implements ModelCatalog {
    final Set<NameIdentifier> models = Sets.newHashSet();
    int deleteModelCalls = 0;
    int versionsToReport = 0;
    boolean failDeleteModel = false;

    @Override
    public Model registerModel(
        NameIdentifier ident, String comment, Map<String, String> properties) {
      models.add(ident);
      return null;
    }

    @Override
    public boolean deleteModel(NameIdentifier ident) {
      deleteModelCalls++;
      if (failDeleteModel) {
        throw new RuntimeException("compensation delete failed");
      }
      return models.remove(ident);
    }

    @Override
    public void linkModelVersion(
        NameIdentifier ident,
        Map<String, String> uris,
        String[] aliases,
        String comment,
        Map<String, String> properties)
        throws ModelVersionAliasesAlreadyExistException {
      throw new ModelVersionAliasesAlreadyExistException("alias already exists");
    }

    @Override
    public NameIdentifier[] listModels(Namespace namespace) {
      return new NameIdentifier[0];
    }

    @Override
    public Model getModel(NameIdentifier ident) {
      return null;
    }

    @Override
    public int[] listModelVersions(NameIdentifier ident) {
      return new int[versionsToReport];
    }

    @Override
    public ModelVersion[] listModelVersionInfos(NameIdentifier ident) {
      return new ModelVersion[0];
    }

    @Override
    public ModelVersion getModelVersion(NameIdentifier ident, int version) {
      return null;
    }

    @Override
    public ModelVersion getModelVersion(NameIdentifier ident, String alias) {
      return null;
    }

    @Override
    public String getModelVersionUri(NameIdentifier ident, int version, String uriName) {
      return null;
    }

    @Override
    public String getModelVersionUri(NameIdentifier ident, String alias, String uriName) {
      return null;
    }

    @Override
    public boolean deleteModelVersion(NameIdentifier ident, int version) {
      return false;
    }

    @Override
    public boolean deleteModelVersion(NameIdentifier ident, String alias) {
      return false;
    }

    @Override
    public Model alterModel(NameIdentifier ident, ModelChange... changes) {
      return null;
    }

    @Override
    public ModelVersion alterModelVersion(
        NameIdentifier ident, int version, ModelVersionChange... changes) {
      return null;
    }

    @Override
    public ModelVersion alterModelVersion(
        NameIdentifier ident, String alias, ModelVersionChange... changes) {
      return null;
    }
  }

  @Test
  void testRegisterModelCleansUpWhenLinkModelVersionFails() {
    FailingLinkModelCatalog catalog = new FailingLinkModelCatalog();
    NameIdentifier ident = NameIdentifier.of("schema", "model1");

    Assertions.assertThrows(
        ModelVersionAliasesAlreadyExistException.class,
        () ->
            catalog.registerModel(
                ident,
                ImmutableMap.of("uri", "file:///tmp/m"),
                new String[] {"alias"},
                "comment",
                Collections.emptyMap()));

    // Before the fix, the just-registered model was left behind as an orphan with zero versions
    // when linking the version failed.
    Assertions.assertFalse(
        catalog.models.contains(ident),
        "failed registerModel must not leave the registered model behind");
    Assertions.assertEquals(1, catalog.deleteModelCalls);
  }

  @Test
  void testRegisterModelDoesNotRollBackModelWithConcurrentVersions() {
    FailingLinkModelCatalog catalog = new FailingLinkModelCatalog();
    catalog.versionsToReport = 1;
    NameIdentifier ident = NameIdentifier.of("schema", "model1");

    Assertions.assertThrows(
        ModelVersionAliasesAlreadyExistException.class,
        () ->
            catalog.registerModel(
                ident,
                ImmutableMap.of("uri", "file:///tmp/m"),
                new String[] {"alias"},
                "comment",
                Collections.emptyMap()));

    // Another actor linked a version to the model concurrently; deleteModel cascades to all
    // versions, so the rollback must not remove the model in this case.
    Assertions.assertTrue(catalog.models.contains(ident));
    Assertions.assertEquals(0, catalog.deleteModelCalls);
  }

  @Test
  void testRegisterModelKeepsLinkFailureWhenRollbackFails() {
    FailingLinkModelCatalog catalog = new FailingLinkModelCatalog();
    catalog.failDeleteModel = true;
    NameIdentifier ident = NameIdentifier.of("schema", "model1");

    ModelVersionAliasesAlreadyExistException thrown =
        Assertions.assertThrows(
            ModelVersionAliasesAlreadyExistException.class,
            () ->
                catalog.registerModel(
                    ident,
                    ImmutableMap.of("uri", "file:///tmp/m"),
                    new String[] {"alias"},
                    "comment",
                    Collections.emptyMap()));

    // The original linking failure stays the primary exception; the failed compensation is
    // recorded as a suppressed exception rather than discarded.
    Assertions.assertEquals(1, catalog.deleteModelCalls);
    Throwable[] suppressed = thrown.getSuppressed();
    Assertions.assertEquals(1, suppressed.length);
    Assertions.assertEquals("compensation delete failed", suppressed[0].getMessage());
  }
}
