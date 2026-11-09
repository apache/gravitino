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
package org.apache.gravitino.server.web.rest.authorization;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.common.collect.ImmutableSet;
import java.lang.reflect.Method;
import org.apache.gravitino.server.authorization.annotations.AuthorizationExpression;
import org.apache.gravitino.server.authorization.expression.AuthorizationExpressionConverter;
import org.apache.gravitino.server.web.rest.MetadataObjectTagOperations;
import org.apache.gravitino.server.web.rest.TagOperations;
import org.junit.jupiter.api.Test;

/** Tests that metadata access cannot bypass tag read authorization. */
public class TestMetadataObjectTagAuthorizationExpression {

  /** Verify tag reads require both metadata access and tag permission on both REST routes. */
  @Test
  public void testGetTagRequiresTagPermission() throws Exception {
    for (Class<?> resource :
        new Class<?>[] {MetadataObjectTagOperations.class, TagOperations.class}) {
      Method method =
          resource.getMethod(
              "getTagForObject", String.class, String.class, String.class, String.class);
      String expression = method.getAnnotation(AuthorizationExpression.class).expression();
      for (String type : new String[] {"TABLE", "SEMANTIC_MODEL"}) {
        MockAuthorizationExpressionEvaluator evaluator =
            new MockAuthorizationExpressionEvaluator(
                AuthorizationExpressionConverter.replaceCanAccessMetadataPrivilege(expression)
                    .replace("entityType", "'" + type + "'"));
        String access =
            type.equals("TABLE") ? "TABLE::SELECT_TABLE" : "SEMANTIC_MODEL::USE_SEMANTIC_MODEL";
        ImmutableSet<String> metadataAccess =
            ImmutableSet.of("CATALOG::USE_CATALOG", "SCHEMA::USE_SCHEMA", access);
        assertFalse(evaluator.getResult(metadataAccess), resource.getSimpleName() + ": " + type);
        assertFalse(evaluator.getResult(ImmutableSet.of("TAG::VIEW_TAG")));
        for (String tagPrivilege : new String[] {"TAG::VIEW_TAG", "TAG::APPLY_TAG", "TAG::OWNER"}) {
          assertTrue(
              evaluator.getResult(
                  ImmutableSet.<String>builder().addAll(metadataAccess).add(tagPrivilege).build()));
        }
        assertFalse(
            evaluator.getResult(
                ImmutableSet.<String>builder()
                    .addAll(metadataAccess)
                    .add("TAG::VIEW_TAG", "TAG::DENY_VIEW_TAG")
                    .build()));
      }
    }
  }
}
