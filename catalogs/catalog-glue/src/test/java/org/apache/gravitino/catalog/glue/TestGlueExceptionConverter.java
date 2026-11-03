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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.gravitino.exceptions.ConnectionFailedException;
import org.apache.gravitino.exceptions.ForbiddenException;
import org.apache.gravitino.exceptions.NoSuchSchemaException;
import org.apache.gravitino.exceptions.NoSuchTableException;
import org.apache.gravitino.exceptions.NotFoundException;
import org.apache.gravitino.exceptions.OptimisticLockException;
import org.apache.gravitino.exceptions.SchemaAlreadyExistsException;
import org.apache.gravitino.exceptions.ServiceUnavailableException;
import org.apache.gravitino.exceptions.TableAlreadyExistsException;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.awscore.exception.AwsErrorDetails;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.services.glue.model.AccessDeniedException;
import software.amazon.awssdk.services.glue.model.AlreadyExistsException;
import software.amazon.awssdk.services.glue.model.ConcurrentModificationException;
import software.amazon.awssdk.services.glue.model.EntityNotFoundException;
import software.amazon.awssdk.services.glue.model.GlueException;
import software.amazon.awssdk.services.glue.model.InvalidInputException;
import software.amazon.awssdk.services.glue.model.ThrottlingException;
import software.amazon.awssdk.services.s3.model.S3Exception;

/** Tests for {@link GlueExceptionConverter}. */
public class TestGlueExceptionConverter {

  private static final String IAM_MESSAGE =
      "User: arn:aws:iam::123456789012:user/gravitino is not authorized to perform: "
          + "glue:CreateDatabase on resource: "
          + "arn:aws:glue:us-east-2:123456789012:database/drop_me3 "
          + "because no identity-based policy allows the glue:CreateDatabase action";

  @Test
  public void testIsCredentialFailureMatchesChainExhaustedMessage() {
    SdkClientException e =
        SdkClientException.create(
            "Unable to load credentials from any of the providers in the chain "
                + "AwsCredentialsProviderChain(...)");

    assertTrue(GlueExceptionConverter.isCredentialFailure(e));
  }

  @Test
  public void testIsCredentialFailureRejectsUnrelatedMessage() {
    SdkClientException e = SdkClientException.create("connection refused");

    assertFalse(GlueExceptionConverter.isCredentialFailure(e));
  }

  @Test
  public void testIsCredentialFailureRejectsNullMessage() {
    SdkClientException e = SdkClientException.builder().message(null).build();

    assertFalse(GlueExceptionConverter.isCredentialFailure(e));
  }

  @Test
  public void testIsAuthenticationFailureMatchesAwsErrorCode() {
    GlueException e = invalidCredentialsException();

    assertTrue(GlueExceptionConverter.isAuthenticationFailure(e));
  }

  @Test
  public void testIsAuthenticationFailureRejectsAuthorizationError() {
    AccessDeniedException e =
        AccessDeniedException.builder()
            .awsErrorDetails(AwsErrorDetails.builder().errorCode("AccessDeniedException").build())
            .build();

    assertFalse(GlueExceptionConverter.isAuthenticationFailure(e));
  }

  @Test
  public void testToCredentialExceptionIncludesContextAndPropertyNames() {
    SdkClientException cause =
        SdkClientException.create("Unable to load credentials from any of the providers");

    RuntimeException ex = GlueExceptionConverter.toCredentialException(cause, "table mydb.mytbl");

    assertEquals(cause, ex.getCause());
    assertTrue(ex.getMessage().contains("table mydb.mytbl"));
    assertTrue(ex.getMessage().contains(GlueConstants.AWS_ACCESS_KEY_ID));
    assertTrue(ex.getMessage().contains(GlueConstants.AWS_SECRET_ACCESS_KEY));
  }

  @Test
  public void testRejectedCredentialsIncludePropertyNamesAndAwsError() {
    GlueException cause = invalidCredentialsException();

    RuntimeException converted = GlueExceptionConverter.toSchemaException(cause, "listing schemas");

    assertInstanceOf(ForbiddenException.class, converted);
    assertInstanceOf(
        ForbiddenException.class, GlueExceptionConverter.toTableException(cause, "table t"));
    assertSame(cause, converted.getCause());
    assertTrue(converted.getMessage().contains(GlueConstants.AWS_ACCESS_KEY_ID));
    assertTrue(converted.getMessage().contains(GlueConstants.AWS_SECRET_ACCESS_KEY));
    assertTrue(converted.getMessage().contains("UnrecognizedClientException"));
  }

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

  private static GlueException invalidCredentialsException() {
    return (GlueException)
        GlueException.builder()
            .message("The security token included in the request is invalid")
            .awsErrorDetails(
                AwsErrorDetails.builder()
                    .errorCode("UnrecognizedClientException")
                    .errorMessage("The security token included in the request is invalid")
                    .build())
            .build();
  }

  @Test
  public void testIcebergForbiddenWithNonGlueCauseStaysForbidden() {
    AwsServiceException s3Denial =
        AwsServiceException.builder()
            .message("Access Denied")
            .statusCode(403)
            .awsErrorDetails(
                AwsErrorDetails.builder()
                    .serviceName("S3")
                    .errorCode("AccessDenied")
                    .errorMessage("Access Denied")
                    .build())
            .build();
    RuntimeException raw =
        new org.apache.iceberg.exceptions.ForbiddenException(
            s3Denial, "Cannot commit db.t because S3 denied access");

    RuntimeException converted = GlueExceptionConverter.toIcebergException(raw, "table t");

    assertInstanceOf(ForbiddenException.class, converted);
    assertTrue(converted.getMessage().contains("Access Denied"), converted.getMessage());
  }

  @Test
  public void testIcebergRawGlueNotFoundIsNotFound() {
    RuntimeException raw =
        new RuntimeException(
            EntityNotFoundException.builder().message("Database db not found").build());

    assertInstanceOf(
        NotFoundException.class, GlueExceptionConverter.toIcebergException(raw, "table t"));
  }

  @Test
  public void testIcebergRenameOntoExistingTableIsAlreadyExists() {
    RuntimeException raw =
        new RuntimeException(
            AlreadyExistsException.builder()
                .message("Table already exists.")
                .awsErrorDetails(
                    AwsErrorDetails.builder()
                        .errorCode("AlreadyExistsException")
                        .errorMessage("Table already exists.")
                        .build())
                .build());

    assertInstanceOf(
        TableAlreadyExistsException.class,
        GlueExceptionConverter.toIcebergException(raw, "altering table t1"));
  }

  @Test
  public void testIcebergMissingNamespaceIsNoSuchSchema() {
    RuntimeException converted =
        GlueExceptionConverter.toIcebergException(
            new org.apache.iceberg.exceptions.NoSuchNamespaceException(
                "Cannot rename db.t to nodb.t because namespace nodb does not exist"),
            "altering table t");

    assertInstanceOf(NoSuchSchemaException.class, converted);
  }

  @Test
  public void testIcebergForbiddenKeepsAwsDenialWithoutCatalogName() {
    AccessDeniedException denial =
        AccessDeniedException.builder()
            .message(IAM_MESSAGE)
            .awsErrorDetails(
                AwsErrorDetails.builder()
                    .errorCode("AccessDeniedException")
                    .errorMessage(IAM_MESSAGE)
                    .build())
            .build();
    RuntimeException raw =
        new org.apache.iceberg.exceptions.ForbiddenException(
            denial,
            "Cannot commit %s.db.t because Glue cannot access the requested resources",
            GlueIcebergTableHelper.ICEBERG_CATALOG_NAME);

    RuntimeException converted = GlueExceptionConverter.toIcebergException(raw, "table t");

    assertInstanceOf(ForbiddenException.class, converted);
    assertTrue(converted.getMessage().contains("glue:CreateDatabase"), converted.getMessage());
    assertFalse(
        converted.getMessage().contains(GlueIcebergTableHelper.ICEBERG_CATALOG_NAME),
        converted.getMessage());
  }

  @Test
  public void testIcebergValidationUnderCommitFailureIsIllegalArgument() {
    RuntimeException raw =
        new org.apache.iceberg.exceptions.CommitStateUnknownException(
            new org.apache.iceberg.exceptions.ValidationException(
                "Invalid S3 URI, cannot determine scheme: /tmp/wh/db/t/metadata/0.json"));

    RuntimeException converted = GlueExceptionConverter.toIcebergException(raw, "table t");

    assertInstanceOf(IllegalArgumentException.class, converted);
    assertTrue(converted.getMessage().contains("/tmp/wh/db/t"), converted.getMessage());
  }

  @Test
  public void testIcebergCommitStateUnknownIsNotConnectionFailure() {
    RuntimeException raw =
        new org.apache.iceberg.exceptions.CommitStateUnknownException(
            SdkClientException.create("Read timed out"));

    RuntimeException converted = GlueExceptionConverter.toIcebergException(raw, "table t");

    assertFalse(converted instanceof ConnectionFailedException, converted.toString());
    assertTrue(converted.getMessage().contains("commit state is unknown"), converted.getMessage());
    assertTrue(converted.getMessage().contains("Read timed out"), converted.getMessage());
  }

  @Test
  public void testIcebergConcurrentCommitIsConflict() {
    RuntimeException raw =
        new org.apache.iceberg.exceptions.CommitFailedException(
            ConcurrentModificationException.builder().message("concurrent update").build(),
            "Cannot commit db.t because Glue detected concurrent update");

    assertInstanceOf(
        OptimisticLockException.class, GlueExceptionConverter.toIcebergException(raw, "table t"));
  }

  @Test
  public void testIcebergNotFoundIsNotFound() {
    RuntimeException raw =
        new org.apache.iceberg.exceptions.NotFoundException(
            EntityNotFoundException.builder().message("Database db not found").build(),
            "Cannot commit db.t because Glue cannot find the requested entity");

    assertInstanceOf(
        NotFoundException.class, GlueExceptionConverter.toIcebergException(raw, "table t"));
  }

  @Test
  public void testIcebergAlreadyExistsIsTableAlreadyExists() {
    RuntimeException converted =
        GlueExceptionConverter.toIcebergException(
            new org.apache.iceberg.exceptions.AlreadyExistsException(
                "Table already exists: %s.db.t", GlueIcebergTableHelper.ICEBERG_CATALOG_NAME),
            "table t");

    assertInstanceOf(TableAlreadyExistsException.class, converted);
    assertFalse(
        converted.getMessage().contains(GlueIcebergTableHelper.ICEBERG_CATALOG_NAME),
        converted.getMessage());
  }

  @Test
  public void testIcebergThrottlingCauseIsServiceUnavailable() {
    RuntimeException raw =
        new RuntimeException(ThrottlingException.builder().message("Rate exceeded").build());

    assertInstanceOf(
        ServiceUnavailableException.class,
        GlueExceptionConverter.toIcebergException(raw, "table t"));
  }

  @Test
  public void testIcebergUnknownFailureFallsBackWithoutNullMessage() {
    RuntimeException converted =
        GlueExceptionConverter.toIcebergException(new IllegalStateException(), "table t");

    assertEquals(RuntimeException.class, converted.getClass());
    assertTrue(converted.getMessage().contains("IllegalStateException"), converted.getMessage());
    assertFalse(converted.getMessage().contains("null"), converted.getMessage());
  }

  @Test
  public void testUnmodeledThrottlingStatusIsServiceUnavailable() {
    GlueException tooMany =
        (GlueException)
            GlueException.builder()
                .message("Too many requests")
                .statusCode(429)
                .awsErrorDetails(
                    AwsErrorDetails.builder()
                        .errorCode("TooManyRequestsException")
                        .errorMessage("Too many requests")
                        .build())
                .build();

    assertInstanceOf(
        ServiceUnavailableException.class,
        GlueExceptionConverter.toClientException(tooMany, "table t", null));
  }

  @Test
  public void testIcebergGravitinoExceptionsPassThrough() {
    NoSuchTableException original = new NoSuchTableException("gone");
    assertSame(original, GlueExceptionConverter.toIcebergException(original, "table t"));
    UnsupportedOperationException unsupported =
        new UnsupportedOperationException("Transform is not supported: bucket");
    assertSame(unsupported, GlueExceptionConverter.toIcebergException(unsupported, "table t"));
  }

  @Test
  public void testServiceScopeErrorIsNotRegionMismatch() {
    GlueException serviceError =
        (GlueException)
            GlueException.builder()
                .message("Credential should be scoped to correct service: 'glue'.")
                .awsErrorDetails(
                    AwsErrorDetails.builder()
                        .errorCode("InvalidSignatureException")
                        .errorMessage("Credential should be scoped to correct service: 'glue'.")
                        .build())
                .build();

    assertFalse(GlueExceptionConverter.isRegionScopeFailure(serviceError));
  }

  @Test
  public void testRegionScopeErrorIsNotReportedAsRejectedCredentials() {
    GlueException regionError =
        (GlueException)
            GlueException.builder()
                .message("Credential should be scoped to a valid region.")
                .awsErrorDetails(
                    AwsErrorDetails.builder()
                        .errorCode("InvalidSignatureException")
                        .errorMessage("Credential should be scoped to a valid region.")
                        .build())
                .build();

    assertFalse(GlueExceptionConverter.isAuthenticationFailure(regionError));
    String message = GlueExceptionConverter.toConnectionException(regionError).getMessage();
    assertTrue(message.contains(GlueConstants.AWS_REGION), message);
    assertFalse(message.contains("rejected the configured credentials"), message);
  }

  @Test
  void testIcebergS3FailuresMapByStatus() {
    assertInstanceOf(
        NotFoundException.class,
        GlueExceptionConverter.toIcebergException(s3Exception(404, "NoSuchBucket"), "table t"));
    assertInstanceOf(
        ServiceUnavailableException.class,
        GlueExceptionConverter.toIcebergException(s3Exception(503, "SlowDown"), "table t"));
    assertInstanceOf(
        IllegalArgumentException.class,
        GlueExceptionConverter.toIcebergException(
            s3Exception(301, "PermanentRedirect"), "table t"));
    RuntimeException other =
        GlueExceptionConverter.toIcebergException(s3Exception(500, "InternalError"), "table t");
    assertTrue(other.getMessage().startsWith("S3 error"), other.getMessage());
    assertTrue(other.getMessage().contains("[InternalError]"), other.getMessage());
  }

  private static RuntimeException s3Exception(int status, String code) {
    return new RuntimeException(
        "wrapped",
        S3Exception.builder()
            .statusCode(status)
            .awsErrorDetails(
                AwsErrorDetails.builder().errorCode(code).errorMessage("message").build())
            .build());
  }
}
