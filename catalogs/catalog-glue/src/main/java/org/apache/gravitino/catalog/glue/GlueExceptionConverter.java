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

import java.util.Set;
import javax.annotation.Nullable;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.exceptions.ConnectionFailedException;
import org.apache.gravitino.exceptions.ForbiddenException;
import org.apache.gravitino.exceptions.GravitinoRuntimeException;
import org.apache.gravitino.exceptions.NoSuchSchemaException;
import org.apache.gravitino.exceptions.NoSuchTableException;
import org.apache.gravitino.exceptions.NotFoundException;
import org.apache.gravitino.exceptions.OptimisticLockException;
import org.apache.gravitino.exceptions.SchemaAlreadyExistsException;
import org.apache.gravitino.exceptions.ServiceUnavailableException;
import org.apache.gravitino.exceptions.TableAlreadyExistsException;
import org.apache.gravitino.utils.ExceptionMessages;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.CommitStateUnknownException;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import software.amazon.awssdk.awscore.exception.AwsErrorDetails;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.exception.ApiCallAttemptTimeoutException;
import software.amazon.awssdk.core.exception.ApiCallTimeoutException;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.glue.GlueServiceClientConfiguration;
import software.amazon.awssdk.services.glue.model.AccessDeniedException;
import software.amazon.awssdk.services.glue.model.AlreadyExistsException;
import software.amazon.awssdk.services.glue.model.ConcurrentModificationException;
import software.amazon.awssdk.services.glue.model.EntityNotFoundException;
import software.amazon.awssdk.services.glue.model.GlueException;
import software.amazon.awssdk.services.glue.model.InvalidInputException;
import software.amazon.awssdk.services.glue.model.ThrottlingException;
import software.amazon.awssdk.services.glue.model.ValidationException;
import software.amazon.awssdk.services.s3.model.S3Exception;

/** Converts AWS Glue SDK exceptions to Gravitino exceptions. */
final class GlueExceptionConverter {

  private static final String NO_CREDENTIALS_MARKER =
      "Unable to load credentials from any of the providers";

  /** Markers AWS puts in a signature error when the request was signed for the wrong region. */
  private static final Set<String> REGION_SCOPE_MARKERS =
      Set.of(
          "Credential should be scoped to a valid region",
          "Credential should be scoped to correct region");

  /** AWS messages for invalid input can quote whole request values; longer ones are shortened. */
  private static final int MAX_AWS_MESSAGE_LENGTH = 1024;

  private static final Set<String> AUTHENTICATION_ERROR_CODES =
      Set.of(
          "AuthFailure",
          "ExpiredToken",
          "ExpiredTokenException",
          "IncompleteSignature",
          "InvalidAccessKeyId",
          "InvalidClientTokenId",
          "InvalidSignatureException",
          "RequestExpired",
          "SignatureDoesNotMatch",
          "TokenRefreshRequired",
          "UnrecognizedClientException");

  private GlueExceptionConverter() {}

  /**
   * Whether {@code e} is the AWS SDK's default-credential-chain-exhausted error, which otherwise
   * surfaces as a raw {@link SdkClientException} listing SDK-internal credential sources instead of
   * this connector's own {@code aws-access-key-id} / {@code aws-secret-access-key} properties.
   *
   * @param e the client exception raised while calling AWS Glue
   * @return true if {@code e} is a credential-resolution failure
   */
  static boolean isCredentialFailure(SdkClientException e) {
    return e.getMessage() != null && e.getMessage().contains(NO_CREDENTIALS_MARKER);
  }

  /**
   * Converts a credential-resolution {@link SdkClientException} into a message that names this
   * connector's own credential properties, so operators are not left guessing which environment
   * variable or IAM role the raw SDK message intended.
   *
   * @param e the credential-resolution failure
   * @param context description of the operation context for error messages
   * @return a runtime exception with an actionable message
   */
  static RuntimeException toCredentialException(SdkClientException e, String context) {
    return new RuntimeException(
        String.format(
            "Failed to authenticate with AWS Glue for %s. No usable AWS credentials were "
                + "found. Set both '%s' and '%s' catalog properties, or ensure the default AWS "
                + "credential chain can resolve credentials.",
            context, GlueConstants.AWS_ACCESS_KEY_ID, GlueConstants.AWS_SECRET_ACCESS_KEY),
        e);
  }

  /**
   * Whether AWS Glue rejected credentials that were successfully resolved by the configured
   * provider. Static credential providers can return any nonblank access-key pair locally, so only
   * an AWS service response can establish whether that pair is authentic.
   *
   * @param e the service exception raised by AWS Glue
   * @return true if AWS classified the failure as an authentication error
   */
  static boolean isAuthenticationFailure(GlueException e) {
    AwsErrorDetails details = e.awsErrorDetails();
    return details != null
        && StringUtils.isNotBlank(details.errorCode())
        && AUTHENTICATION_ERROR_CODES.contains(details.errorCode())
        && !isRegionScopeFailure(e);
  }

  /**
   * Whether AWS refused the request signature because it was scoped to a region other than the one
   * of the endpoint. The credentials themselves are valid in that case.
   *
   * @param e the service exception raised by AWS Glue
   * @return true if the signature was scoped to the wrong region
   */
  static boolean isRegionScopeFailure(GlueException e) {
    AwsErrorDetails details = e.awsErrorDetails();
    String message = details != null ? details.errorMessage() : null;
    String text = StringUtils.defaultIfBlank(message, e.getMessage());
    return text != null && REGION_SCOPE_MARKERS.stream().anyMatch(text::contains);
  }

  /**
   * Converts an AWS Glue SDK failure raised by a connection probe into a connection error.
   *
   * @param e the SDK failure raised by the connection probe
   * @return an actionable connection failure
   */
  static ConnectionFailedException toConnectionException(SdkException e) {
    return toConnectionException(e, null);
  }

  /**
   * Converts an AWS Glue SDK failure raised by a connection probe into a connection error. Known
   * credential and region failures name the connector properties that an operator can correct;
   * authorization and transport failures retain the AWS or SDK detail without claiming the
   * credentials are invalid.
   *
   * @param e the SDK failure raised by the connection probe
   * @param client the Glue client that made the call, used to name its region and endpoint
   * @return an actionable connection failure
   */
  static ConnectionFailedException toConnectionException(
      SdkException e, @Nullable GlueClient client) {
    if (e instanceof SdkClientException && isCredentialFailure((SdkClientException) e)) {
      return new ConnectionFailedException(
          e,
          "Failed to authenticate with AWS Glue. No usable AWS credentials were found. Set both "
              + "'%s' and '%s' catalog properties, or ensure the default AWS credential chain can "
              + "resolve credentials.",
          GlueConstants.AWS_ACCESS_KEY_ID,
          GlueConstants.AWS_SECRET_ACCESS_KEY);
    }
    if (e instanceof GlueException && isRegionScopeFailure((GlueException) e)) {
      return new ConnectionFailedException(
          e, "%s", regionMismatchMessage((GlueException) e, client));
    }
    if (e instanceof GlueException && isAuthenticationFailure((GlueException) e)) {
      return new ConnectionFailedException(
          e,
          "AWS Glue rejected the configured credentials. Verify the '%s' and '%s' catalog "
              + "properties, or the configured default AWS credential source. AWS error: %s",
          GlueConstants.AWS_ACCESS_KEY_ID,
          GlueConstants.AWS_SECRET_ACCESS_KEY,
          awsErrorDetail((GlueException) e));
    }

    String detail =
        e instanceof GlueException
            ? awsErrorDetail((GlueException) e)
            : StringUtils.defaultIfBlank(
                ExceptionMessages.usefulMessage(e), e.getClass().getSimpleName());
    return new ConnectionFailedException(
        e,
        "Failed to connect to AWS Glue at %s: %s",
        GlueClientProvider.describeEndpoint(client),
        detail);
  }

  /**
   * Converts the failures that do not depend on the object being operated on: missing, rejected or
   * mis-scoped credentials, throttling, invalid input, access denial, and any other client-side SDK
   * failure (timeouts, transport errors), which is reported as a connection failure.
   *
   * @param e the SDK failure
   * @param context description of the operation context for error messages
   * @param client the Glue client that made the call, used to name its region and endpoint; may be
   *     null
   * @return the converted exception, or null when the failure is specific to the object (for
   *     example not found or already exists) and must be converted by the caller
   */
  @Nullable
  static RuntimeException toClientException(
      SdkException e, String context, @Nullable GlueClient client) {
    if (e instanceof SdkClientException) {
      SdkClientException clientException = (SdkClientException) e;
      if (isCredentialFailure(clientException)) {
        return toCredentialException(clientException, context);
      }
      String endpoint = GlueClientProvider.describeEndpoint(client);
      String detail =
          StringUtils.defaultIfBlank(
              ExceptionMessages.usefulMessage(e), e.getClass().getSimpleName());
      if (e instanceof ApiCallTimeoutException || e instanceof ApiCallAttemptTimeoutException) {
        return new ConnectionFailedException(
            e, "AWS Glue at %s timed out for %s: %s", endpoint, context, detail);
      }
      return new ConnectionFailedException(
          e, "Failed to reach AWS Glue at %s for %s: %s", endpoint, context, detail);
    }
    if (!(e instanceof GlueException)) {
      return null;
    }

    GlueException glueException = (GlueException) e;
    if (isRegionScopeFailure(glueException)) {
      return new IllegalArgumentException(
          String.format(
              "Failed to call AWS Glue for %s. %s",
              context, regionMismatchMessage(glueException, client)),
          e);
    }
    if (isAuthenticationFailure(glueException)) {
      return new ForbiddenException(
          e,
          "Failed to authenticate with AWS Glue for %s. AWS rejected the configured "
              + "credentials. Verify the '%s' and '%s' catalog properties, or the configured "
              + "default AWS credential source. AWS error: %s",
          context,
          GlueConstants.AWS_ACCESS_KEY_ID,
          GlueConstants.AWS_SECRET_ACCESS_KEY,
          awsErrorDetail(glueException));
    }
    if (e instanceof ThrottlingException || glueException.isThrottlingException()) {
      return new ServiceUnavailableException(
          e,
          "AWS Glue throttled the request for %s; the request can be retried. AWS error: %s",
          context,
          awsErrorDetail(glueException));
    }
    if (e instanceof ConcurrentModificationException) {
      return new OptimisticLockException(
          e,
          "%s was changed concurrently; the request can be retried. AWS error: %s",
          context,
          awsErrorDetail(glueException));
    }
    if (e instanceof InvalidInputException || e instanceof ValidationException) {
      return new IllegalArgumentException(
          String.format(
              "AWS Glue rejected the input for %s: %s", context, awsErrorDetail(glueException)),
          e);
    }
    if (e instanceof AccessDeniedException) {
      return new ForbiddenException(
          e, "Glue error: %s: %s", context, awsErrorDetail(glueException));
    }
    return null;
  }

  /**
   * Converts an AWS Glue SDK failure that has no object-specific meaning to the caller.
   *
   * @param e the SDK failure
   * @param context description of the operation context for error messages
   * @param client the Glue client that made the call; may be null
   * @return a Gravitino or standard Java runtime exception
   */
  static RuntimeException toGenericException(
      SdkException e, String context, @Nullable GlueClient client) {
    RuntimeException converted = toClientException(e, context, client);
    if (converted != null) {
      return converted;
    }
    String detail =
        e instanceof GlueException
            ? awsErrorDetail((GlueException) e)
            : StringUtils.defaultIfBlank(e.getMessage(), e.getClass().getSimpleName());
    return new RuntimeException("Glue error: " + context + ": " + detail, e);
  }

  /**
   * Converts a {@link GlueException} to the appropriate Gravitino schema exception.
   *
   * @param e the Glue exception to convert
   * @param context description of the operation context for error messages
   * @return a Gravitino or standard Java runtime exception
   */
  static RuntimeException toSchemaException(GlueException e, String context) {
    if (e instanceof EntityNotFoundException) {
      return new NoSuchSchemaException(e, "%s does not exist", context);
    }
    if (e instanceof AlreadyExistsException) {
      return new SchemaAlreadyExistsException(e, "%s already exists", context);
    }
    return toGenericException(e, context, null);
  }

  /**
   * Converts a {@link GlueException} to the appropriate Gravitino table exception.
   *
   * @param e the Glue exception to convert
   * @param context description of the operation context for error messages
   * @return a Gravitino or standard Java runtime exception
   */
  static RuntimeException toTableException(GlueException e, String context) {
    if (e instanceof EntityNotFoundException) {
      return new NoSuchTableException(e, "%s does not exist", context);
    }
    if (e instanceof AlreadyExistsException) {
      return new TableAlreadyExistsException(e, "%s already exists", context);
    }
    return toGenericException(e, context, null);
  }

  /**
   * Converts a failure raised by the Iceberg SDK into a Gravitino exception. Iceberg reports Glue
   * and S3 errors either as its own exception types or wrapped around the AWS SDK exception, so the
   * cause chain is walked from the outside in and the first recognized type decides the result:
   * Iceberg not-found, already-exists, validation and forbidden errors map to the matching
   * Gravitino types, a commit conflict maps to {@link OptimisticLockException}, S3 failures map by
   * their HTTP status, and other AWS SDK causes go through {@link #toGenericException}. A commit
   * whose state is unknown is reported as such, because the change may have been applied. {@link
   * GravitinoRuntimeException}, {@link IllegalArgumentException} and {@link
   * UnsupportedOperationException} are returned as is.
   *
   * @param e the failure raised by an Iceberg catalog call
   * @param context description of the operation context for error messages
   * @return a Gravitino or standard Java runtime exception
   */
  static RuntimeException toIcebergException(RuntimeException e, String context) {
    if (e instanceof GravitinoRuntimeException
        || e instanceof IllegalArgumentException
        || e instanceof UnsupportedOperationException) {
      return e;
    }
    for (Throwable t = e; t != null; t = t.getCause() == t ? null : t.getCause()) {
      if (t instanceof CommitStateUnknownException) {
        // A local validation failure underneath means nothing was sent, so the commit did not
        // happen. Any other cause, such as a transport failure, does not mean the commit failed.
        Throwable invalid = findCause(t, org.apache.iceberg.exceptions.ValidationException.class);
        if (invalid != null) {
          return new IllegalArgumentException(
              String.format("Invalid request for %s: %s", context, describe(invalid)), e);
        }
        return new RuntimeException(
            String.format(
                "%s: the commit state is unknown and the change may have been applied; check the"
                    + " table before retrying: %s",
                context, describe(t.getCause() != null ? t.getCause() : t)),
            e);
      }
      if (t instanceof CommitFailedException) {
        Throwable awsCause = findCause(t, SdkException.class);
        return awsCause == null || awsCause instanceof ConcurrentModificationException
            ? new OptimisticLockException(
                e, "%s: the table was changed concurrently, retry: %s", context, describe(t))
            : toGenericException((SdkException) awsCause, context, null);
      }
      if (t instanceof AlreadyExistsException) {
        return new TableAlreadyExistsException(
            e, "%s: %s", context, awsErrorDetail((AlreadyExistsException) t));
      }
      if (t instanceof org.apache.iceberg.exceptions.AlreadyExistsException) {
        return new TableAlreadyExistsException(e, "%s: %s", context, describe(t));
      }
      if (t instanceof NoSuchNamespaceException) {
        return new NoSuchSchemaException(e, "%s: %s", context, describe(t));
      }
      if (t instanceof org.apache.iceberg.exceptions.NoSuchTableException) {
        return new NoSuchTableException(e, "%s: %s", context, describe(t));
      }
      if (t instanceof org.apache.iceberg.exceptions.NotFoundException) {
        // Iceberg wraps a Glue EntityNotFoundException here; it may be the table or its database.
        return new NotFoundException(e, "%s: %s", context, describe(t));
      }
      if (t instanceof org.apache.iceberg.exceptions.ValidationException) {
        return new IllegalArgumentException(
            String.format("Invalid request for %s: %s", context, describe(t)), e);
      }
      if (t instanceof org.apache.iceberg.exceptions.ForbiddenException) {
        // A Glue denial deeper in the chain keeps its AWS error code and message through the
        // shared mapping; a denial by another service (such as S3) stays forbidden.
        Throwable awsCause = findCause(t, SdkException.class);
        return awsCause instanceof GlueException
            ? toGenericException((SdkException) awsCause, context, null)
            : new ForbiddenException(
                e, "%s: %s", context, describe(awsCause != null ? awsCause : t));
      }
      if (t instanceof EntityNotFoundException) {
        // Not wrapped by Iceberg; it may be the table or its database.
        return new NotFoundException(e, "%s: %s", context, describe(t));
      }
      if (t instanceof S3Exception) {
        return toS3Exception((S3Exception) t, context, e);
      }
      if (t instanceof SdkException) {
        return toGenericException((SdkException) t, context, null);
      }
    }
    return new RuntimeException(context + ": " + describe(e), e);
  }

  /**
   * Converts an S3 failure met while reading or writing Iceberg metadata or data files. The status
   * decides the result, since S3 reports most failures under a few generic error codes.
   */
  private static RuntimeException toS3Exception(
      S3Exception s3Exception, String context, RuntimeException e) {
    String detail = awsErrorDetail(s3Exception);
    int status = s3Exception.statusCode();
    if (status == 403) {
      return new ForbiddenException(e, "S3 denied access for %s: %s", context, detail);
    }
    if (status == 404) {
      return new NotFoundException(e, "S3 object or bucket not found for %s: %s", context, detail);
    }
    if (status == 503 || s3Exception.isThrottlingException()) {
      return new ServiceUnavailableException(
          e, "S3 throttled the request for %s; the request can be retried: %s", context, detail);
    }
    if (status == 301 || (status >= 400 && status < 500)) {
      return new IllegalArgumentException(
          String.format("S3 rejected the request for %s: %s", context, detail), e);
    }
    return new RuntimeException(String.format("S3 error: %s: %s", context, detail), e);
  }

  private static String regionMismatchMessage(GlueException e, @Nullable GlueClient client) {
    GlueServiceClientConfiguration conf =
        client == null ? null : client.serviceClientConfiguration();
    String region = conf != null && conf.region() != null ? conf.region().id() : "unknown";
    String endpoint =
        conf != null && conf.endpointOverride() != null && conf.endpointOverride().isPresent()
            ? conf.endpointOverride().get().toString()
            : "(not set)";
    return String.format(
        "The request was signed for a region that does not match the AWS Glue endpoint. Check that"
            + " '%s' (%s) and '%s' (%s) refer to the same region. AWS error: %s",
        GlueConstants.AWS_REGION,
        region,
        GlueConstants.AWS_GLUE_ENDPOINT,
        endpoint,
        awsErrorDetail(e));
  }

  @Nullable
  private static Throwable findCause(Throwable t, Class<?> type) {
    for (Throwable c = t.getCause(); c != null; c = c.getCause() == c ? null : c.getCause()) {
      if (type.isInstance(c)) {
        return c;
      }
    }
    return null;
  }

  /**
   * Describes a throwable for an error message without the name of the internal Iceberg catalog,
   * which means nothing to the caller, falling back to the type name when there is no message.
   */
  private static String describe(Throwable t) {
    String message = StringUtils.defaultIfBlank(t.getMessage(), t.getClass().getSimpleName());
    return message.replace(GlueIcebergTableHelper.ICEBERG_CATALOG_NAME + ".", "");
  }

  /**
   * Renders the AWS-side detail of a Glue or S3 exception. AWS names the failing action and the
   * resource there, which is what the caller needs to act on; the error code is prefixed so the
   * failure can be classified at a glance. Long messages, which quote the offending request value,
   * are shortened in the middle so the violated constraint at the end stays visible.
   *
   * @param e the AWS service exception to describe
   * @return the AWS error code and message, or a best-effort description when they are unavailable
   */
  private static String awsErrorDetail(AwsServiceException e) {
    return StringUtils.abbreviateMiddle(rawAwsErrorDetail(e), " ... ", MAX_AWS_MESSAGE_LENGTH);
  }

  private static String rawAwsErrorDetail(AwsServiceException e) {
    AwsErrorDetails details = e.awsErrorDetails();
    if (details != null) {
      String code = details.errorCode();
      String message = details.errorMessage();
      if (StringUtils.isNotBlank(code) && StringUtils.isNotBlank(message)) {
        return "[" + code + "] " + message;
      }
      if (StringUtils.isNotBlank(message)) {
        return message;
      }
      if (StringUtils.isNotBlank(code)) {
        return "[" + code + "]";
      }
    }
    return StringUtils.isNotBlank(e.getMessage()) ? e.getMessage() : e.getClass().getSimpleName();
  }
}
