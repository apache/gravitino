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
package org.apache.gravitino.rest;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.io.UnsupportedEncodingException;
import java.net.ServerSocket;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Random;
import org.apache.gravitino.exceptions.GravitinoRuntimeException;

/**
 * Referred from Apache Iceberg's RESTUtil implementation
 * core/src/main/java/org/apache/iceberg/rest/RESTUtil.java
 *
 * <p>Utility class for working with REST related operations.
 */
public class RESTUtils {

  private static final Joiner.MapJoiner FORM_JOINER = Joiner.on("&").withKeyValueSeparator("=");
  private static final Splitter.MapSplitter FORM_SPLITTER =
      Splitter.on("&").withKeyValueSeparator("=");

  private RESTUtils() {}

  /**
   * Remove trailing slashes from a path.
   *
   * @param path The path to strip trailing slashes from.
   * @return The path with trailing slashes removed.
   */
  public static String stripTrailingSlash(String path) {
    if (path == null) {
      return null;
    }

    String result = path;
    while (result.endsWith("/")) {
      result = result.substring(0, result.length() - 1);
    }
    return result;
  }

  /**
   * Encode a map of form data into a URL encoded string.
   *
   * @param formData The form data to encode.
   * @return The URL encoded form data string.
   */
  public static String encodeFormData(Map<?, ?> formData) {
    ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();
    formData.forEach(
        (key, value) ->
            builder.put(encodeString(String.valueOf(key)), encodeString(String.valueOf(value))));
    return FORM_JOINER.join(builder.build());
  }

  /**
   * Decode a URL encoded form data string into a map.
   *
   * @param formString The URL encoded form data string.
   * @return The decoded form data map.
   */
  public static Map<String, String> decodeFormData(String formString) {
    return FORM_SPLITTER.split(formString).entrySet().stream()
        .collect(
            ImmutableMap.toImmutableMap(
                e -> decodeString(e.getKey()), e -> decodeString(e.getValue())));
  }

  /**
   * URL encode a string.
   *
   * @param toEncode The string to encode.
   * @return The URL encoded string.
   * @throws IllegalArgumentException If the input string is null.
   * @throws UncheckedIOException If URL encoding fails.
   */
  public static String encodeString(String toEncode) {
    Preconditions.checkArgument(toEncode != null, "Invalid string to encode: null");
    try {
      return URLEncoder.encode(toEncode, StandardCharsets.UTF_8.name());
    } catch (UnsupportedEncodingException e) {
      throw new GravitinoRuntimeException("Failed to encode string: %s", toEncode);
    }
  }

  /**
   * Decode a URL encoded string.
   *
   * @param encoded The URL encoded string to decode.
   * @return The decoded string.
   * @throws IllegalArgumentException if the input string is null.
   * @throws UncheckedIOException if URL decoding fails.
   */
  public static String decodeString(String encoded) {
    Preconditions.checkArgument(encoded != null, "Invalid string to decode: null");
    try {
      return URLDecoder.decode(encoded, StandardCharsets.UTF_8.name());
    } catch (UnsupportedEncodingException e) {
      throw new GravitinoRuntimeException("Failed to decode string: %s", encoded);
    }
  }

  /**
   * Find an available port in the port range.
   *
   * @param portRangeStart the start of the port range
   * @param portRangeEnd the end of the port range
   * @return the available port
   * @throws IOException if no available port in the port range
   */
  public static int findAvailablePort(final int portRangeStart, final int portRangeEnd)
      throws IOException {
    if (portRangeStart > portRangeEnd) {
      throw new IOException("Invalid port range: " + portRangeStart + ":" + portRangeEnd);
    } else if (portRangeStart == 0 && portRangeEnd == 0) {
      try (ServerSocket socket = new ServerSocket(0)) {
        return socket.getLocalPort();
      } catch (IOException e) {
        throw new IOException("Failed to allocate a random port", e);
      }
    } else if (portRangeStart == portRangeEnd) {
      try (ServerSocket socket = new ServerSocket(portRangeStart)) {
        return socket.getLocalPort();
      } catch (IOException e) {
        throw new IOException("Failed to allocate the specified port: " + portRangeStart, e);
      }
    }

    // valid user registered port https://en.wikipedia.org/wiki/Registered_port
    if (portRangeStart < 1024 || portRangeEnd > 65535) {
      throw new IOException("port number must be 0 or in [1024, 65535]");
    }

    Random random = new Random();
    final int maxRetry = 200;
    for (int i = portRangeStart; i <= portRangeEnd && i < portRangeStart + maxRetry; ++i) {
      int randomNumber = random.nextInt(portRangeEnd - portRangeStart + 1) + portRangeStart;
      try (ServerSocket socket = new ServerSocket(randomNumber)) {
        return socket.getLocalPort();
      } catch (IOException e) {
        // ignore this
      }
    }
    throw new IOException("No available port in the range: " + portRangeStart + ":" + portRangeEnd);
  }
}
