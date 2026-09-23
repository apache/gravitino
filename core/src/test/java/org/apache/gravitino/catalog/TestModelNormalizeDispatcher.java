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
package org.apache.gravitino.catalog;

import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.MetadataObjects;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.model.Model;
import org.apache.gravitino.model.ModelChange;
import org.apache.gravitino.utils.NameIdentifierUtil;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class TestModelNormalizeDispatcher extends TestOperationDispatcher {
  private static ModelNormalizeDispatcher modelNormalizeDispatcher;
  private static SchemaNormalizeDispatcher schemaNormalizeDispatcher;

  @BeforeAll
  public static void initialize() throws IOException, IllegalAccessException {
    TestModelOperationDispatcher.initialize();
    schemaNormalizeDispatcher =
        new SchemaNormalizeDispatcher(
            TestModelOperationDispatcher.schemaOperationDispatcher, catalogManager);
    modelNormalizeDispatcher =
        new ModelNormalizeDispatcher(
            TestModelOperationDispatcher.modelOperationDispatcher, catalogManager);
  }

  @Test
  public void testRenameNameSpec() {
    String schemaName = "testRenameNameSpec";
    schemaNormalizeDispatcher.createSchema(
        NameIdentifier.of(metalake, catalog, schemaName), "comment", ImmutableMap.of("k1", "v1"));
    NameIdentifier modelIdent = NameIdentifierUtil.ofModel(metalake, catalog, schemaName, "model");
    modelNormalizeDispatcher.registerModel(modelIdent, "comment", ImmutableMap.of("k1", "v1"));

    String tooLongName = StringUtils.repeat("m", 129);
    Exception exception =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> modelNormalizeDispatcher.alterModel(modelIdent, ModelChange.rename(tooLongName)));
    Assertions.assertEquals(
        String.format("The MODEL name '%s' is illegal. Illegal name: %s", tooLongName, tooLongName),
        exception.getMessage());

    exception =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () ->
                modelNormalizeDispatcher.alterModel(
                    modelIdent, ModelChange.rename(MetadataObjects.METADATA_OBJECT_RESERVED_NAME)));
    Assertions.assertEquals(
        "The MODEL name '*' is reserved. Illegal name: *", exception.getMessage());

    exception =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> modelNormalizeDispatcher.alterModel(modelIdent, ModelChange.rename("a?")));
    Assertions.assertEquals(
        "The MODEL name 'a?' is illegal. Illegal name: a?", exception.getMessage());

    Model renamed = modelNormalizeDispatcher.alterModel(modelIdent, ModelChange.rename("model_1"));
    Assertions.assertEquals("model_1", renamed.name());
  }
}
