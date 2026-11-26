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

package org.apache.gravitino.server.web;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import javax.servlet.http.HttpServletRequest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/** Tests explicit header selection, startup validation, and bounded sanitization. */
public class TestRequestHeaderCapture {

  @Test
  void testDisabledDoesNotReadAnyHeaders() {
    HttpServletRequest request = mock(HttpServletRequest.class);
    assertTrue(new RequestHeaderCapture(Collections.emptyList()).capture(request).isEmpty());
    verifyNoInteractions(request);
  }

  @Test
  void testNormalizesDeduplicatesAndOnlyReadsAllowlist() {
    RequestHeaderCapture capture =
        new RequestHeaderCapture(Arrays.asList(" X-Job-Id ", "x-job-id", "X-Missing"));
    HttpServletRequest request = mock(HttpServletRequest.class);
    when(request.getHeader("x-job-id")).thenReturn("job-123");
    assertEquals(Collections.singletonMap("x-job-id", "job-123"), capture.capture(request));
    verify(request).getHeader("x-job-id");
    verify(request).getHeader("x-missing");
    verifyNoMoreInteractions(request);
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "Authorization",
        "PROXY-AUTHORIZATION",
        "X-Api-Key",
        "X-Auth-Token",
        "X-Amz-Security-Token",
        "Cookie",
        "Set-Cookie",
        "WWW-Authenticate",
        "Proxy-Authenticate",
        "",
        " ",
        "X Bad",
        "X:Bad",
        "*wildcard*?"
      })
  void testRejectsSensitiveAndInvalidNames(String name) {
    assertThrows(
        IllegalArgumentException.class,
        () -> new RequestHeaderCapture(Collections.singletonList(name)));
  }

  @Test
  void testBoundsConfiguration() {
    List<String> names =
        IntStream.rangeClosed(0, RequestHeaderCapture.MAX_HEADERS)
            .mapToObj(i -> "x-" + i)
            .collect(Collectors.toList());
    assertThrows(IllegalArgumentException.class, () -> new RequestHeaderCapture(names));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            new RequestHeaderCapture(
                Collections.singletonList(
                    String.join(
                        "", Collections.nCopies(RequestHeaderCapture.MAX_NAME_LENGTH + 1, "x")))));
  }

  @Test
  void testSanitizesAndTruncatesValues() {
    RequestHeaderCapture capture =
        new RequestHeaderCapture(Arrays.asList("X-Job-Id", "X-Execution-Id"));
    HttpServletRequest request = mock(HttpServletRequest.class);
    when(request.getHeader("x-job-id")).thenReturn("job\r\n\t\u0000\u2028\u2029end");
    String maximum =
        String.join("", Collections.nCopies(RequestHeaderCapture.MAX_VALUE_LENGTH, "x"));
    when(request.getHeader("x-execution-id")).thenReturn(maximum + "overflow");
    Map<String, String> result = capture.capture(request);
    assertEquals("job      end", result.get("x-job-id"));
    assertEquals(maximum + RequestHeaderCapture.TRUNCATED_SUFFIX, result.get("x-execution-id"));
    assertThrows(UnsupportedOperationException.class, () -> result.put("other", "value"));
  }
}
