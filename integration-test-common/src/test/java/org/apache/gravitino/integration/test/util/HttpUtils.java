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
package org.apache.gravitino.integration.test.util;

import org.apache.hc.client5.http.classic.methods.HttpGet;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.apache.hc.core5.http.ClassicHttpResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HttpUtils {
  private static final Logger LOG = LoggerFactory.getLogger(HttpUtils.class);

  /**
   * Check if the http server is up. If http response status code is 200, then we're assuming the
   * server is up. Or else we assume the server is not ready.
   *
   * <p>Note: The method will ignore the response body and only check the status code.
   *
   * @param testUrl A url that we want to test ignore the response body.
   * @return true if the server is up, false otherwise.
   */
  public static boolean isHttpServerUp(String testUrl) {
    try (CloseableHttpClient httpClient = HttpClients.createDefault()) {
      HttpGet request = new HttpGet(testUrl);
      ClassicHttpResponse response = httpClient.execute(request, a -> a);
      return response.getCode() == 200;
    } catch (Exception e) {
      LOG.warn("Check HTTP server failed: url:{}, error message:{}", testUrl, e.getMessage());
      return false;
    }
  }
}
