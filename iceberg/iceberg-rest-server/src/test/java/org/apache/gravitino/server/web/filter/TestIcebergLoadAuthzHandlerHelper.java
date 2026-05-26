/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package org.apache.gravitino.server.web.filter;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.lang.reflect.Method;
import org.apache.gravitino.Entity;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.server.authorization.annotations.AuthorizationExpression;
import org.apache.gravitino.server.authorization.annotations.AuthorizationMetadata;
import org.apache.gravitino.server.authorization.annotations.IcebergAuthorizationMetadata;
import org.apache.gravitino.server.web.filter.IcebergLoadAuthzHandlerHelper.LoadTarget;
import org.apache.iceberg.catalog.Namespace;
import org.junit.jupiter.api.Test;

/** Test for {@link IcebergLoadAuthzHandlerHelper}. */
public class TestIcebergLoadAuthzHandlerHelper {

  @Test
  public void testExtractLoadTarget() throws Exception {
    Method method =
        TestOperations.class.getMethod("loadTable", String.class, String.class, String.class);

    LoadTarget loadTarget =
        IcebergLoadAuthzHandlerHelper.extractLoadTarget(
            method.getParameters(),
            new Object[] {"test_catalog/", "test_schema", "orders%20table"},
            IcebergAuthorizationMetadata.RequestType.LOAD_TABLE);

    assertEquals("orders table", loadTarget.name());
    assertEquals(Namespace.of("test_schema"), loadTarget.namespace());
  }

  @Test
  public void testResolveExpression() throws Exception {
    Method method =
        TestOperations.class.getMethod("loadTable", String.class, String.class, String.class);
    AuthorizationExpression authorizationExpression =
        method.getAnnotation(AuthorizationExpression.class);

    assertEquals(
        "custom_primary",
        IcebergLoadAuthzHandlerHelper.resolveExpression(
            authorizationExpression, "default_primary"));
    assertEquals(
        "custom_existence",
        IcebergLoadAuthzHandlerHelper.resolveAllowCheckExistenceExpression(
            authorizationExpression, "default_existence"));
    assertEquals(
        "default_primary",
        IcebergLoadAuthzHandlerHelper.resolveExpression(null, "default_primary"));
    assertEquals(
        "default_existence",
        IcebergLoadAuthzHandlerHelper.resolveAllowCheckExistenceExpression(
            null, "default_existence"));
  }

  @SuppressWarnings("unused")
  public static class TestOperations {
    @AuthorizationExpression(
        expression = "custom_primary",
        allowCheckExistence = "custom_existence",
        accessMetadataType = MetadataObject.Type.TABLE)
    public void loadTable(
        @AuthorizationMetadata(type = Entity.EntityType.CATALOG) String prefix,
        @AuthorizationMetadata(type = Entity.EntityType.SCHEMA) String namespace,
        @IcebergAuthorizationMetadata(type = IcebergAuthorizationMetadata.RequestType.LOAD_TABLE)
            @AuthorizationMetadata(type = Entity.EntityType.TABLE)
            String table) {}
  }
}
