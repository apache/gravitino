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
package org.apache.gravitino.catalog.glue;

import org.apache.gravitino.exceptions.ForbiddenException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.glue.model.AccessDeniedException;
import software.amazon.awssdk.services.glue.model.GlueException;
import software.amazon.awssdk.services.glue.model.InvalidInputException;

public class TestGlueExceptionConverter {

  @Test
  public void testDefaultGlueErrorPreservesUpstreamMessage() {
    GlueException cause =
        GlueException.builder()
            .message("User is not authorized to perform glue:CreateDatabase")
            .build();

    RuntimeException converted = GlueExceptionConverter.toSchemaException(cause, "schema drop_me");

    Assertions.assertEquals(
        "Glue error: schema drop_me: User is not authorized to perform glue:CreateDatabase",
        converted.getMessage());
    Assertions.assertSame(cause, converted.getCause());
  }

  @Test
  public void testAccessDeniedMapsToForbidden() {
    AccessDeniedException cause =
        AccessDeniedException.builder()
            .message(
                "User: arn:aws:iam::123:user/a is not authorized to perform: glue:CreateDatabase")
            .build();

    RuntimeException converted = GlueExceptionConverter.toTableException(cause, "table ctas_test");

    Assertions.assertInstanceOf(ForbiddenException.class, converted);
    Assertions.assertTrue(
        converted
            .getMessage()
            .contains(
                "User: arn:aws:iam::123:user/a is not authorized to perform:"
                    + " glue:CreateDatabase"));
  }

  @Test
  public void testInvalidInputMapsToIllegalArgument() {
    InvalidInputException cause =
        InvalidInputException.builder().message("Name is too long").build();

    RuntimeException converted = GlueExceptionConverter.toSchemaException(cause, "schema bad");

    Assertions.assertInstanceOf(IllegalArgumentException.class, converted);
    Assertions.assertEquals("schema bad: Name is too long", converted.getMessage());
  }
}
