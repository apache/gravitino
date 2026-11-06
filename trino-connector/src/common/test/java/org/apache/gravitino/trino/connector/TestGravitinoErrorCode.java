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
package org.apache.gravitino.trino.connector;

import static io.trino.spi.StandardErrorCode.CATALOG_UNAVAILABLE;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.trino.spi.StandardErrorCode.PERMISSION_DENIED;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

import io.trino.spi.TrinoException;
import java.io.IOException;
import java.net.ConnectException;
import java.util.concurrent.ExecutionException;
import org.apache.gravitino.exceptions.ConnectionFailedException;
import org.apache.gravitino.exceptions.ForbiddenException;
import org.apache.gravitino.exceptions.MetalakeNotInUseException;
import org.apache.gravitino.exceptions.RESTException;
import org.junit.jupiter.api.Test;

public class TestGravitinoErrorCode {

  @Test
  public void testTrinoExceptionIsReturnedAsIs() {
    TrinoException original =
        new TrinoException(GravitinoErrorCode.GRAVITINO_TABLE_NOT_EXISTS, "missing");
    assertSame(original, GravitinoErrorCode.toTrinoException(original));
  }

  @Test
  public void testForbiddenKeepsFirstLine() {
    TrinoException e =
        GravitinoErrorCode.toTrinoException(
            new ForbiddenException("denied\n\tat org.apache.gravitino.Server"));
    assertEquals(PERMISSION_DENIED.toErrorCode(), e.getErrorCode());
    assertEquals("denied", e.getMessage());
  }

  @Test
  public void testNotInUseIsCatalogUnavailable() {
    TrinoException e =
        GravitinoErrorCode.toTrinoException(
            new MetalakeNotInUseException("Metalake m is not in use"));
    assertEquals(CATALOG_UNAVAILABLE.toErrorCode(), e.getErrorCode());
    assertEquals("Metalake m is not in use", e.getMessage());
  }

  @Test
  public void testIllegalArgumentIsIllegalArgument() {
    TrinoException e =
        GravitinoErrorCode.toTrinoException(new IllegalArgumentException("Illegal name: 9lives"));
    assertEquals(GravitinoErrorCode.GRAVITINO_ILLEGAL_ARGUMENT.toErrorCode(), e.getErrorCode());
  }

  @Test
  public void testUnsupportedOperationIsUnsupportedOperation() {
    TrinoException e =
        GravitinoErrorCode.toTrinoException(new UnsupportedOperationException("not supported"));
    assertEquals(
        GravitinoErrorCode.GRAVITINO_UNSUPPORTED_OPERATION.toErrorCode(), e.getErrorCode());
  }

  @Test
  public void testSourceConnectionFailureIsConnectionFailed() {
    TrinoException e =
        GravitinoErrorCode.toTrinoException(
            new ConnectionFailedException("Failed to connect to Hive Metastore"));
    assertEquals(GravitinoErrorCode.GRAVITINO_CONNECTION_FAILED.toErrorCode(), e.getErrorCode());
    assertEquals("Failed to connect to Hive Metastore", e.getMessage());
  }

  @Test
  public void testUnreachableServerIsConnectionFailed() {
    TrinoException e =
        GravitinoErrorCode.toTrinoException(
            new RESTException(
                new ConnectException("Connection refused"),
                "Failed to connect to Gravitino server"));
    assertEquals(GravitinoErrorCode.GRAVITINO_CONNECTION_FAILED.toErrorCode(), e.getErrorCode());
  }

  @Test
  public void testUnexpectedServerResponseIsInternalError() {
    TrinoException e =
        GravitinoErrorCode.toTrinoException(new RESTException("Unhandled error: %s", "code 9999"));
    assertEquals(GENERIC_INTERNAL_ERROR.toErrorCode(), e.getErrorCode());

    e =
        GravitinoErrorCode.toTrinoException(
            new RESTException(new IOException("Unexpected token"), "Failed to parse response"));
    assertEquals(GENERIC_INTERNAL_ERROR.toErrorCode(), e.getErrorCode());
  }

  @Test
  public void testUnclassifiedFailureIsInternalError() {
    TrinoException e = GravitinoErrorCode.toTrinoException(new RuntimeException("boom"));
    assertEquals(GENERIC_INTERNAL_ERROR.toErrorCode(), e.getErrorCode());
    assertEquals("boom", e.getMessage());
  }

  @Test
  public void testExecutionExceptionIsClassifiedByCause() {
    TrinoException e =
        GravitinoErrorCode.toTrinoException(
            new ExecutionException(new ForbiddenException("denied")));
    assertEquals(PERMISSION_DENIED.toErrorCode(), e.getErrorCode());
    assertEquals("denied", e.getMessage());
  }

  @Test
  public void testContextPrefixesOnlyUnclassifiedFailures() {
    TrinoException internal =
        GravitinoErrorCode.toTrinoException(
            new RuntimeException("boom"), "Drop catalog m.c failed");
    assertEquals(GENERIC_INTERNAL_ERROR.toErrorCode(), internal.getErrorCode());
    assertEquals("Drop catalog m.c failed: boom", internal.getMessage());

    TrinoException denied =
        GravitinoErrorCode.toTrinoException(
            new ForbiddenException("denied"), "Drop catalog m.c failed");
    assertEquals("denied", denied.getMessage());
  }

  @Test
  public void testEmptyMessageFallsBackToExceptionName() {
    TrinoException e = GravitinoErrorCode.toTrinoException(new IllegalArgumentException(""));
    assertEquals(GravitinoErrorCode.GRAVITINO_ILLEGAL_ARGUMENT.toErrorCode(), e.getErrorCode());
    assertEquals("java.lang.IllegalArgumentException: ", e.getMessage());
  }

  @Test
  public void testServerStackTraceIsDroppedFromMessage() {
    String stack = "\n\tat org.apache.gravitino.server.web.rest.TableOperations.createTable";
    TrinoException illegal =
        GravitinoErrorCode.toTrinoException(
            new IllegalArgumentException("Illegal name: 9lives" + stack));
    assertEquals("Illegal name: 9lives", illegal.getMessage());

    TrinoException internal =
        GravitinoErrorCode.toTrinoException(
            new RuntimeException("boom" + stack), "Drop catalog m.c failed");
    assertEquals("Drop catalog m.c failed: boom", internal.getMessage());
  }

  @Test
  public void testLeadingBlankLinesAreSkipped() {
    TrinoException e =
        GravitinoErrorCode.toTrinoException(new RuntimeException("\n  \nboom\nmore"), "Op failed");
    assertEquals("Op failed: boom", e.getMessage());
  }
}
