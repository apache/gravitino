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

package org.apache.gravitino.server.web.ui;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import javax.servlet.FilterChain;
import javax.servlet.RequestDispatcher;
import javax.servlet.ServletException;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import org.junit.jupiter.api.Test;

public class WebUIFilterTest {
  @Test
  public void testNestedDirectoryRequestForwardsToIndexHtml() throws ServletException, IOException {
    WebUIFilter filter = new WebUIFilter();
    HttpServletRequest request = mock(HttpServletRequest.class);
    ServletResponse response = mock(ServletResponse.class);
    FilterChain chain = mock(FilterChain.class);
    RequestDispatcher dispatcher = mock(RequestDispatcher.class);

    when(request.getRequestURI()).thenReturn("/ui/section/subsection/");
    when(request.getRequestDispatcher("/ui/section/subsection/index.html")).thenReturn(dispatcher);

    filter.doFilter(request, response, chain);

    verify(dispatcher).forward(request, response);
    verify(chain, never()).doFilter(any(), any());
  }
}
