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

import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.gravitino.exceptions.ForbiddenException;
import org.apache.gravitino.exceptions.NoSuchSchemaException;
import org.apache.gravitino.exceptions.NoSuchTableException;
import org.apache.gravitino.exceptions.SchemaAlreadyExistsException;
import org.apache.gravitino.exceptions.TableAlreadyExistsException;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.awscore.exception.AwsErrorDetails;
import software.amazon.awssdk.services.glue.model.AccessDeniedException;
import software.amazon.awssdk.services.glue.model.AlreadyExistsException;
import software.amazon.awssdk.services.glue.model.EntityNotFoundException;
import software.amazon.awssdk.services.glue.model.GlueException;
import software.amazon.awssdk.services.glue.model.InvalidInputException;

/** Tests for {@link GlueExceptionConverter}. */
public class TestGlueExceptionConverter {

  private static final String IAM_MESSAGE =
      "User: arn:aws:iam::123456789012:user/gravitino is not authorized to perform: "
          + "glue:CreateDatabase on resource: "
          + "arn:aws:glue:us-east-2:123456789012:database/drop_me3 "
          + "because no identity-based policy allows the glue:CreateDatabase action";

  @Test
  public void testSchemaAccessDeniedKeepsAwsMessage() {
    AccessDeniedException e =
        AccessDeniedException.builder()
            .message(IAM_MESSAGE)
            .awsErrorDetails(
                AwsErrorDetails.builder()
                    .errorCode("AccessDeniedException")
                    .errorMessage(IAM_MESSAGE)
                    .build())
            .build();

    RuntimeException converted = GlueExceptionConverter.toSchemaException(e, "schema drop_me");

    assertInstanceOf(ForbiddenException.class, converted);
    assertSame(e, converted.getCause());
    String message = converted.getMessage();
    assertTrue(message.contains("schema drop_me"), message);
    assertTrue(message.contains("[AccessDeniedException] "), message);
    assertTrue(message.contains("glue:CreateDatabase"), message);
    assertTrue(message.contains("database/drop_me3"), message);
  }

  @Test
  public void testTableAccessDeniedKeepsAwsMessage() {
    AccessDeniedException e =
        AccessDeniedException.builder()
            .message(IAM_MESSAGE)
            .awsErrorDetails(
                AwsErrorDetails.builder()
                    .errorCode("AccessDeniedException")
                    .errorMessage(IAM_MESSAGE)
                    .build())
            .build();

    RuntimeException converted = GlueExceptionConverter.toTableException(e, "table ctas_test");

    assertInstanceOf(ForbiddenException.class, converted);
    assertSame(e, converted.getCause());
    String message = converted.getMessage();
    assertTrue(message.contains("table ctas_test"), message);
    assertTrue(message.contains("AccessDeniedException"), message);
    assertTrue(message.contains("glue:CreateDatabase"), message);
  }

  @Test
  public void testErrorMessageAloneIsSurfaced() {
    GlueException e =
        (GlueException)
            GlueException.builder()
                .awsErrorDetails(
                    AwsErrorDetails.builder().errorMessage("throttled by Glue").build())
                .build();

    RuntimeException converted = GlueExceptionConverter.toSchemaException(e, "schema db6a");

    assertTrue(converted.getMessage().contains("schema db6a"), converted.getMessage());
    assertTrue(converted.getMessage().contains("throttled by Glue"), converted.getMessage());
  }

  @Test
  public void testErrorCodeAloneIsSurfaced() {
    GlueException e =
        (GlueException)
            GlueException.builder()
                .awsErrorDetails(
                    AwsErrorDetails.builder().errorCode("InternalServiceException").build())
                .build();

    RuntimeException converted = GlueExceptionConverter.toSchemaException(e, "schema db6a");

    assertTrue(
        converted.getMessage().contains("[InternalServiceException]"), converted.getMessage());
  }

  @Test
  public void testFallsBackToExceptionMessageWithoutAwsErrorDetails() {
    GlueException e = (GlueException) GlueException.builder().message("connection reset").build();

    RuntimeException converted = GlueExceptionConverter.toSchemaException(e, "schema db6a");

    assertTrue(converted.getMessage().contains("schema db6a"), converted.getMessage());
    assertTrue(converted.getMessage().contains("connection reset"), converted.getMessage());
  }

  @Test
  public void testFallsBackWhenAwsErrorDetailsAreBlank() {
    GlueException e =
        (GlueException)
            GlueException.builder()
                .message("connection reset")
                .awsErrorDetails(AwsErrorDetails.builder().errorCode("").errorMessage("").build())
                .build();

    RuntimeException converted = GlueExceptionConverter.toSchemaException(e, "schema db6a");

    assertTrue(converted.getMessage().contains("connection reset"), converted.getMessage());
  }

  @Test
  public void testFallsBackToExceptionTypeWithoutAnyMessage() {
    GlueException e = (GlueException) GlueException.builder().build();

    RuntimeException converted = GlueExceptionConverter.toSchemaException(e, "schema db6a");

    assertTrue(converted.getMessage().contains("GlueException"), converted.getMessage());
  }

  @Test
  public void testRecognisedExceptionsKeepTheirMapping() {
    EntityNotFoundException notFound = EntityNotFoundException.builder().message("gone").build();
    AlreadyExistsException exists = AlreadyExistsException.builder().message("dup").build();
    InvalidInputException invalid = InvalidInputException.builder().message("bad name").build();

    assertInstanceOf(
        NoSuchSchemaException.class,
        GlueExceptionConverter.toSchemaException(notFound, "schema db6a"));
    assertInstanceOf(
        SchemaAlreadyExistsException.class,
        GlueExceptionConverter.toSchemaException(exists, "schema db6a"));
    assertInstanceOf(
        IllegalArgumentException.class,
        GlueExceptionConverter.toSchemaException(invalid, "schema db6a"));

    assertInstanceOf(
        NoSuchTableException.class,
        GlueExceptionConverter.toTableException(notFound, "table ctas_test"));
    assertInstanceOf(
        TableAlreadyExistsException.class,
        GlueExceptionConverter.toTableException(exists, "table ctas_test"));
    assertInstanceOf(
        IllegalArgumentException.class,
        GlueExceptionConverter.toTableException(invalid, "table ctas_test"));
  }
}
