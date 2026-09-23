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
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.exceptions.ConnectionFailedException;
import org.apache.gravitino.exceptions.ForbiddenException;
import org.apache.gravitino.exceptions.NoSuchSchemaException;
import org.apache.gravitino.exceptions.NoSuchTableException;
import org.apache.gravitino.exceptions.SchemaAlreadyExistsException;
import org.apache.gravitino.exceptions.TableAlreadyExistsException;
import org.apache.gravitino.utils.ExceptionMessages;
import software.amazon.awssdk.awscore.exception.AwsErrorDetails;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.services.glue.model.AccessDeniedException;
import software.amazon.awssdk.services.glue.model.AlreadyExistsException;
import software.amazon.awssdk.services.glue.model.EntityNotFoundException;
import software.amazon.awssdk.services.glue.model.GlueException;
import software.amazon.awssdk.services.glue.model.InvalidInputException;

/** Converts AWS Glue SDK exceptions to Gravitino exceptions. */
final class GlueExceptionConverter {

  private static final String NO_CREDENTIALS_MARKER =
      "Unable to load credentials from any of the providers";

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
   * @return a Gravitino runtime exception with an actionable message
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
        && AUTHENTICATION_ERROR_CODES.contains(details.errorCode());
  }

  /**
   * Converts an AWS Glue SDK failure raised by a connection probe into a connection error. Known
   * credential failures name the connector properties that an operator can correct; authorization
   * and transport failures retain the AWS or SDK detail without claiming the credentials are
   * invalid.
   *
   * @param e the SDK failure raised by the connection probe
   * @return an actionable connection failure
   */
  static ConnectionFailedException toConnectionException(SdkException e) {
    if (e instanceof SdkClientException && isCredentialFailure((SdkClientException) e)) {
      return new ConnectionFailedException(
          e,
          "Failed to authenticate with AWS Glue. No usable AWS credentials were found. Set both "
              + "'%s' and '%s' catalog properties, or ensure the default AWS credential chain can "
              + "resolve credentials.",
          GlueConstants.AWS_ACCESS_KEY_ID,
          GlueConstants.AWS_SECRET_ACCESS_KEY);
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
            : StringUtils.defaultIfBlank(e.getMessage(), e.getClass().getSimpleName());
    return new ConnectionFailedException(e, "Failed to connect to AWS Glue: %s", detail);
  }

  /**
   * Converts a {@link GlueException} to the appropriate Gravitino schema exception.
   *
   * @param e the Glue exception to convert
   * @param context description of the operation context for error messages
   * @return a Gravitino or standard Java runtime exception
   */
  static RuntimeException toSchemaException(GlueException e, String context) {
    if (isAuthenticationFailure(e)) {
      return toAuthenticationException(e, context);
    }
    if (e instanceof EntityNotFoundException) {
      return new NoSuchSchemaException(e, "%s does not exist", context);
    }
    if (e instanceof AlreadyExistsException) {
      return new SchemaAlreadyExistsException(e, "%s already exists", context);
    }
    if (e instanceof InvalidInputException) {
      return ExceptionMessages.illegalArgument(context, e);
    }
    if (e instanceof AccessDeniedException) {
      return new ForbiddenException(e, "Glue error: %s: %s", context, awsErrorDetail(e));
    }
    return new RuntimeException("Glue error: " + context + ": " + awsErrorDetail(e), e);
  }

  /**
   * Converts a {@link GlueException} to the appropriate Gravitino table exception.
   *
   * @param e the Glue exception to convert
   * @param context description of the operation context for error messages
   * @return a Gravitino or standard Java runtime exception
   */
  static RuntimeException toTableException(GlueException e, String context) {
    if (isAuthenticationFailure(e)) {
      return toAuthenticationException(e, context);
    }
    if (e instanceof EntityNotFoundException) {
      return new NoSuchTableException(e, "%s does not exist", context);
    }
    if (e instanceof AlreadyExistsException) {
      return new TableAlreadyExistsException(e, "%s already exists", context);
    }
    if (e instanceof InvalidInputException) {
      return ExceptionMessages.illegalArgument(context, e);
    }
    if (e instanceof AccessDeniedException) {
      return new ForbiddenException(e, "Glue error: %s: %s", context, awsErrorDetail(e));
    }
    return new RuntimeException("Glue error: " + context + ": " + awsErrorDetail(e), e);
  }

  private static RuntimeException toAuthenticationException(GlueException e, String context) {
    return new RuntimeException(
        String.format(
            "Failed to authenticate with AWS Glue for %s. AWS rejected the configured "
                + "credentials. Verify the '%s' and '%s' catalog properties, or the configured "
                + "default AWS credential source. AWS error: %s",
            context,
            GlueConstants.AWS_ACCESS_KEY_ID,
            GlueConstants.AWS_SECRET_ACCESS_KEY,
            awsErrorDetail(e)),
        e);
  }

  /**
   * Renders the AWS-side detail of a Glue exception. AWS names the failing action and the resource
   * there, which is what the caller needs to act on; the error code is prefixed so the failure can
   * be classified at a glance.
   *
   * @param e the Glue exception to describe
   * @return the AWS error code and message, or a best-effort description when they are unavailable
   */
  private static String awsErrorDetail(GlueException e) {
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
