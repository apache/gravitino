/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.gravitino.server.web.rest;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.apache.gravitino.dto.responses.BaseListResponse;
import org.apache.gravitino.dto.responses.ErrorConstants;
import org.apache.gravitino.dto.responses.ErrorResponse;
import org.junit.jupiter.api.Assertions;

/** Walks a paged list endpoint in REST tests and checks the paging contract on every page. */
final class PagedListWalker {

  private PagedListWalker() {}

  /**
   * Walks every page of a list endpoint with the given page size.
   *
   * @param base The endpoint, with any non-paging query parameters already set.
   * @param pageSize The page size to request.
   * @param responseClass The list response class.
   * @param items Extracts the page's entries from a response.
   * @return The entries of every page, in the order returned.
   */
  static <R extends BaseListResponse, T> List<T> walk(
      WebTarget base, int pageSize, Class<R> responseClass, Function<R, T[]> items) {
    List<T> walked = new ArrayList<>();
    String token = null;
    int pages = 0;
    do {
      WebTarget target = base.queryParam("pageSize", pageSize);
      if (token != null) {
        target = target.queryParam("pageToken", token);
      }
      Response resp =
          target
              .request(MediaType.APPLICATION_JSON_TYPE)
              .accept("application/vnd.gravitino.v1+json")
              .get();
      Assertions.assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());
      R page = resp.readEntity(responseClass);
      Assertions.assertEquals(0, page.getCode());
      T[] pageItems = items.apply(page);
      token = page.nextPageToken();
      Assertions.assertTrue(
          token == null || pageItems.length == pageSize,
          "only the last page may be short: " + Arrays.toString(pageItems));
      walked.addAll(Arrays.asList(pageItems));
      Assertions.assertTrue(++pages < 1000, "walk did not terminate");
    } while (token != null);
    return walked;
  }

  /**
   * Asserts that a request with an invalid page size and one with a malformed token are both
   * rejected with a 400 that names the parameter.
   *
   * @param base The endpoint, with any non-paging query parameters already set.
   */
  static void assertRejectsInvalidPaging(WebTarget base) {
    assertBadRequest(base.queryParam("pageSize", 0), "pageSize");
    assertBadRequest(base.queryParam("pageSize", 2).queryParam("pageToken", "bad"), "pageToken");
  }

  private static void assertBadRequest(WebTarget target, String parameter) {
    Response resp =
        target
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .get();
    Assertions.assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), resp.getStatus());
    ErrorResponse error = resp.readEntity(ErrorResponse.class);
    Assertions.assertEquals(ErrorConstants.ILLEGAL_ARGUMENTS_CODE, error.getCode());
    Assertions.assertTrue(
        error.getMessage().contains(parameter),
        "expected the message to name " + parameter + ": " + error.getMessage());
  }
}
