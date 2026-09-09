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
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;
import software.amazon.awssdk.core.exception.SdkClientException;

class TestGlueExceptionConverter {

  @Test
  void testIsCredentialFailureMatchesChainExhaustedMessage() {
    SdkClientException e =
        SdkClientException.create(
            "Unable to load credentials from any of the providers in the chain "
                + "AwsCredentialsProviderChain(...)");

    assertTrue(GlueExceptionConverter.isCredentialFailure(e));
  }

  @Test
  void testIsCredentialFailureRejectsUnrelatedMessage() {
    SdkClientException e = SdkClientException.create("connection refused");

    assertFalse(GlueExceptionConverter.isCredentialFailure(e));
  }

  @Test
  void testIsCredentialFailureRejectsNullMessage() {
    SdkClientException e = SdkClientException.builder().message(null).build();

    assertFalse(GlueExceptionConverter.isCredentialFailure(e));
  }

  @Test
  void testToCredentialExceptionIncludesContextAndPropertyNames() {
    SdkClientException cause =
        SdkClientException.create("Unable to load credentials from any of the providers");

    RuntimeException ex = GlueExceptionConverter.toCredentialException(cause, "table mydb.mytbl");

    assertEquals(cause, ex.getCause());
    assertTrue(ex.getMessage().contains("table mydb.mytbl"));
    assertTrue(ex.getMessage().contains(GlueConstants.AWS_ACCESS_KEY_ID));
    assertTrue(ex.getMessage().contains(GlueConstants.AWS_SECRET_ACCESS_KEY));
  }
}
