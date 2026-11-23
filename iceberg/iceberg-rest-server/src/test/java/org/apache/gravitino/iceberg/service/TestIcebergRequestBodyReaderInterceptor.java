/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.gravitino.iceberg.service;

import javax.ws.rs.NotAcceptableException;
import javax.ws.rs.ext.ReaderInterceptorContext;
import org.apache.iceberg.exceptions.BadRequestException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class TestIcebergRequestBodyReaderInterceptor {

  private final IcebergRequestBodyReaderInterceptor interceptor =
      new IcebergRequestBodyReaderInterceptor();

  @Test
  public void testParserRejectionsBecomeBadRequest() throws Exception {
    for (RuntimeException parserError :
        new RuntimeException[] {
          new UnsupportedOperationException(
              "Cannot convert metadata update action to json: no-such-action"),
          new IllegalArgumentException("Cannot parse missing field: term"),
          new IllegalStateException("Invalid state"),
          new NullPointerException()
        }) {
      ReaderInterceptorContext context = Mockito.mock(ReaderInterceptorContext.class);
      Mockito.when(context.proceed()).thenThrow(parserError);
      BadRequestException e =
          Assertions.assertThrows(
              BadRequestException.class, () -> interceptor.aroundReadFrom(context));
      Assertions.assertTrue(e.getMessage().startsWith("Invalid request body: "), e.getMessage());
      if (parserError.getMessage() != null) {
        Assertions.assertTrue(e.getMessage().contains(parserError.getMessage()), e.getMessage());
      }
    }
  }

  @Test
  public void testOtherOutcomesPassThrough() throws Exception {
    ReaderInterceptorContext ok = Mockito.mock(ReaderInterceptorContext.class);
    Object body = new Object();
    Mockito.when(ok.proceed()).thenReturn(body);
    Assertions.assertSame(body, interceptor.aroundReadFrom(ok));

    ReaderInterceptorContext webError = Mockito.mock(ReaderInterceptorContext.class);
    NotAcceptableException notAcceptable = new NotAcceptableException();
    Mockito.when(webError.proceed()).thenThrow(notAcceptable);
    Assertions.assertSame(
        notAcceptable,
        Assertions.assertThrows(
            NotAcceptableException.class, () -> interceptor.aroundReadFrom(webError)));
  }
}
