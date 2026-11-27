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
package org.apache.gravitino.paimon.service;

import com.google.common.base.Preconditions;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Pattern;
import javax.ws.rs.core.Response;
import org.apache.paimon.rest.RESTApi;
import org.apache.paimon.rest.RESTUtil;
import org.apache.paimon.rest.exceptions.BadRequestException;

/** Shared helpers for the Paimon REST resources: pagination, name patterns and JSON. */
public class PaimonRESTUtils {

  /** Default and maximum number of items returned per page, mirroring the Paimon client. */
  public static final int DEFAULT_MAX_RESULTS = 100;

  public static final String MAX_RESULTS = RESTApi.MAX_RESULTS;
  public static final String PAGE_TOKEN = RESTApi.PAGE_TOKEN;

  private PaimonRESTUtils() {}

  /**
   * Parses and validates the {@code maxResults} query parameter.
   *
   * @param parameters the query parameters of the request
   * @return the effective page size, defaulted to {@link #DEFAULT_MAX_RESULTS} and clamped to it
   */
  public static int getMaxResults(Map<String, String> parameters) {
    String strMaxResults = parameters.get(MAX_RESULTS);
    Integer maxResults = Objects.nonNull(strMaxResults) ? Integer.parseInt(strMaxResults) : null;
    if (Objects.isNull(maxResults) || maxResults <= 0) {
      return DEFAULT_MAX_RESULTS;
    }
    return Math.min(maxResults, DEFAULT_MAX_RESULTS);
  }

  /**
   * Validates that {@code maxResults} is a positive integer.
   *
   * @param parameters the query parameters of the request
   * @return true when the parameter is absent or a valid positive integer
   */
  public static boolean isValidMaxResults(Map<String, String> parameters) {
    String strMaxResults = parameters.get(MAX_RESULTS);
    if (strMaxResults == null) {
      return true;
    }
    try {
      return Integer.parseInt(strMaxResults) > 0;
    } catch (NumberFormatException e) {
      return false;
    }
  }

  /**
   * Sorts items by their pagination key and returns one page. Items strictly after {@code
   * pageToken} are returned, limited to {@code maxResults} entries. The next page token is the key
   * of the last returned item, or null when the result set is exhausted.
   *
   * @param entities the full list of items
   * @param keyExtractor a function deriving the opaque pagination key of an item
   * @param maxResults the page size
   * @param pageToken the token of the previously returned page, may be null
   * @param <T> the item type
   * @return the page of items
   */
  public static <T> List<T> buildPagedEntities(
      List<T> entities,
      java.util.function.Function<T, String> keyExtractor,
      int maxResults,
      String pageToken) {
    List<T> filtered =
        entities.stream()
            .sorted(Comparator.comparing(keyExtractor))
            .filter(
                entity -> pageToken == null || keyExtractor.apply(entity).compareTo(pageToken) > 0)
            .collect(java.util.stream.Collectors.toList());
    if (filtered.size() > maxResults) {
      return filtered.subList(0, maxResults);
    }
    return filtered;
  }

  /**
   * Filters a numerically ascending list for one page. Items whose numeric key is strictly greater
   * than {@code pageToken} are returned, limited to {@code maxResults} entries. Unlike {@link
   * #buildPagedEntities}, the page token is compared as a number so that keys do not need identical
   * digit lengths.
   *
   * @param entities the full list of items, ascending by numeric key
   * @param keyExtractor a function deriving the numeric key of an item
   * @param maxResults the page size
   * @param pageToken the token of the previously returned page, may be null
   * @param <T> the item type
   * @return the page of items
   */
  public static <T> List<T> buildPagedEntitiesNumeric(
      List<T> entities,
      java.util.function.ToLongFunction<T> keyExtractor,
      int maxResults,
      String pageToken) {
    List<T> filtered = entities;
    if (pageToken != null) {
      long token;
      try {
        token = Long.parseLong(pageToken);
      } catch (NumberFormatException e) {
        throw new BadRequestException(
            "Invalid input for queryParameter %s: %s", PAGE_TOKEN, pageToken);
      }
      filtered =
          entities.stream()
              .filter(entity -> keyExtractor.applyAsLong(entity) > token)
              .collect(java.util.stream.Collectors.toList());
    }
    if (filtered.size() > maxResults) {
      return filtered.subList(0, maxResults);
    }
    return filtered;
  }

  /**
   * Computes the next page token for a page of entities. Null when the result set is complete (the
   * page is smaller than the page size), matching the Paimon client's stop condition.
   *
   * @param page the returned page of items
   * @param keyExtractor a function deriving the opaque pagination key of an item
   * @param pageSize the page size used to produce the page
   * @return the key of the last item, or null when paging is complete
   */
  public static <T> String nextPageToken(
      List<T> page, java.util.function.Function<T, String> keyExtractor, int pageSize) {
    if (page == null || page.isEmpty() || page.size() < pageSize) {
      return null;
    }
    return keyExtractor.apply(page.get(page.size() - 1));
  }

  /**
   * Validates a SQL LIKE-style prefix name pattern as defined by the Paimon client: {@code %} may
   * only appear at the end of the pattern and {@code \} escapes the next character.
   *
   * @param pattern the pattern to validate, may be null
   * @return true when the pattern is valid
   */
  public static boolean isValidSqlLikePattern(String pattern) {
    if (pattern == null) {
      return true;
    }
    try {
      RESTUtil.validatePrefixSqlPattern(pattern);
      return true;
    } catch (IllegalArgumentException e) {
      return false;
    }
  }

  /**
   * Compiles a validated SQL LIKE-style prefix pattern into an anchored regular expression.
   *
   * @param pattern the validated pattern
   * @return the compiled pattern
   */
  public static Pattern sqlLikePatternToRegex(String pattern) {
    Preconditions.checkArgument(pattern != null, "pattern must not be null");
    StringBuilder regex = new StringBuilder();
    boolean escaped = false;
    for (int i = 0; i < pattern.length(); i++) {
      char c = pattern.charAt(i);
      if (escaped) {
        regex.append(Pattern.quote(String.valueOf(c)));
        escaped = false;
        continue;
      }
      if (c == '\\') {
        escaped = true;
        continue;
      }
      if (c == '%') {
        regex.append(".*");
      } else {
        // Quote literal characters so user supplied regex metacharacters stay literal.
        regex.append(Pattern.quote(String.valueOf(c)));
      }
    }
    return Pattern.compile("^" + regex + "$");
  }

  /**
   * Filters names by a validated SQL LIKE-style pattern.
   *
   * @param names the names to filter, in ascending order
   * @param pattern the pattern, may be null to keep all names
   * @return the filtered names
   */
  public static List<String> filterBySqlLikePattern(List<String> names, String pattern) {
    if (pattern == null || pattern.isEmpty()) {
      return names;
    }
    Pattern regex = sqlLikePatternToRegex(pattern);
    return names.stream()
        .filter(name -> regex.matcher(name).matches())
        .collect(java.util.stream.Collectors.toList());
  }

  /**
   * Serializes a Paimon REST response DTO to JSON using Paimon's shaded object mapper.
   *
   * @param response the DTO to serialize
   * @return the JSON string
   */
  public static String toJson(Object response) {
    try {
      return RESTApi.OBJECT_MAPPER.writeValueAsString(response);
    } catch (Exception e) {
      throw new BadRequestException("Failed to serialize response: %s", e.getMessage());
    }
  }

  /**
   * Deserializes a Paimon REST request DTO from JSON using Paimon's shaded object mapper.
   *
   * @param body the JSON request body
   * @param clazz the target DTO class
   * @param <T> the DTO type
   * @return the deserialized request
   */
  public static <T> T fromJson(String body, Class<T> clazz) {
    try {
      return RESTApi.OBJECT_MAPPER.readValue(body, clazz);
    } catch (Exception e) {
      throw new BadRequestException("Failed to parse request: %s", e.getMessage());
    }
  }

  /**
   * Builds a 200 OK response whose entity is a Paimon DTO serialized with the shaded mapper.
   *
   * @param entity the response DTO
   * @return the JAX-RS response
   */
  public static Response ok(Object entity) {
    return Response.ok(toJson(entity))
        .type(javax.ws.rs.core.MediaType.APPLICATION_JSON_TYPE)
        .build();
  }
}
