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

package org.apache.gravitino.auth;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashSet;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.commons.lang3.StringUtils;

/**
 * Parses the {@link AuthConstants#X_GRAVITINO_ACTIVE_ROLES_HEADER} header value into an {@link
 * ActiveRoles} declaration.
 *
 * <p>Accepted grammar:
 *
 * <ul>
 *   <li>an absent, empty, or whitespace-only value maps to {@link ActiveRoles#all()};
 *   <li>{@code ALL} maps to {@link ActiveRoles#all()};
 *   <li>{@code NONE} maps to {@link ActiveRoles#none()};
 *   <li>a role name or comma-separated list of role names maps to {@link ActiveRoles#of}.
 * </ul>
 *
 * <p>Entries are trimmed and de-duplicated, and role names are case-sensitive. {@code ALL} and
 * {@code NONE} are reserved keywords, matched exactly in upper case, and each must appear on its
 * own.
 *
 * <p>The value is read as UTF-8, and each role name may be percent-encoded ({@code %XX} over its
 * UTF-8 bytes), because many HTTP clients cannot send non-ASCII header values. Entries are split
 * and keywords matched before decoding, so {@code a%2Cb} names the single role {@code a,b} and
 * {@code %41LL} names a role literally called {@code ALL}. {@code +} is not decoded as a space, and
 * trimming happens before decoding, so an encoded space is part of the name. A name that is blank
 * or contains a control character after decoding is rejected.
 *
 * <p>This performs syntactic validation only. Checking that the caller actually holds a named role
 * happens later on the server.
 */
public final class ActiveRolesParser {

  /** The reserved keyword that activates every role the caller holds. */
  public static final String ALL_KEYWORD = "ALL";

  /** The reserved keyword that activates no role. */
  public static final String NONE_KEYWORD = "NONE";

  private ActiveRolesParser() {}

  /**
   * Parses a raw header value into an {@link ActiveRoles} declaration.
   *
   * @param rawValue the raw header value as exposed by the servlet container, or {@code null} when
   *     the header is absent
   * @return the parsed declaration
   * @throws IllegalActiveRolesException if the value is syntactically invalid (an empty entry, a
   *     reserved keyword combined with any other value, a malformed percent-escape, bytes that are
   *     not valid UTF-8, or a role name that is blank or has a control character after decoding)
   */
  public static ActiveRoles parse(@Nullable String rawValue) {
    if (rawValue == null) {
      return ActiveRoles.all();
    }
    String value = recoverUtf8(rawValue);
    String trimmed = value.trim();
    if (trimmed.isEmpty()) {
      return ActiveRoles.all();
    }

    // Use a negative limit so trailing empty entries (e.g. "analyst,") are kept and rejected below
    // instead of being silently dropped.
    String[] parts = trimmed.split(",", -1);
    Set<String> names = new LinkedHashSet<>();
    boolean sawAll = false;
    boolean sawNone = false;
    for (String part : parts) {
      String token = part.trim();
      if (token.isEmpty()) {
        throw invalid("empty role entry in value '" + value + "'");
      }
      if (ALL_KEYWORD.equals(token)) {
        sawAll = true;
      } else if (NONE_KEYWORD.equals(token)) {
        sawNone = true;
      } else {
        names.add(checkRoleName(decodeRoleName(token, value), value));
      }
    }

    // A reserved keyword must appear on its own. Every entry is non-empty here, so a reserved
    // keyword is valid only when it is the single entry.
    boolean sawReserved = sawAll || sawNone;
    if (sawReserved && parts.length > 1) {
      throw invalid(
          "reserved keyword "
              + ALL_KEYWORD
              + "/"
              + NONE_KEYWORD
              + " must appear alone in value '"
              + value
              + "'");
    }

    if (sawAll) {
      return ActiveRoles.all();
    }
    if (sawNone) {
      return ActiveRoles.none();
    }
    return ActiveRoles.of(names);
  }

  // Servlet containers such as Jetty expose each header byte as one ISO-8859-1 char. Re-read those
  // bytes as UTF-8 so a role name sent as raw UTF-8 matches. A value holding a char above U+00FF
  // was already decoded by the container and is kept as is.
  private static String recoverUtf8(String rawValue) {
    boolean hasNonAscii = false;
    for (int i = 0; i < rawValue.length(); i++) {
      char c = rawValue.charAt(i);
      if (c > 0xFF) {
        return rawValue;
      }
      hasNonAscii |= c >= 0x80;
    }
    return hasNonAscii
        ? decodeUtf8(rawValue.getBytes(StandardCharsets.ISO_8859_1), rawValue)
        : rawValue;
  }

  private static String decodeRoleName(String token, String value) {
    if (token.indexOf('%') < 0) {
      return token;
    }
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    int i = 0;
    while (i < token.length()) {
      if (token.charAt(i) == '%') {
        bytes.write(decodeEscape(token, i, value));
        i += 3;
        continue;
      }
      int next = token.indexOf('%', i);
      int end = next < 0 ? token.length() : next;
      byte[] literal = token.substring(i, end).getBytes(StandardCharsets.UTF_8);
      bytes.write(literal, 0, literal.length);
      i = end;
    }
    return decodeUtf8(bytes.toByteArray(), value);
  }

  // Decoding can produce names the raw header could not carry: reject blank names (e.g. "%20" or
  // Unicode spaces, matching role creation) here so they get a 400, and control characters so a
  // name cannot forge lines in logs.
  private static String checkRoleName(String name, String value) {
    if (StringUtils.isBlank(name)) {
      throw invalid("empty role entry in value '" + value + "'");
    }
    if (name.chars().anyMatch(Character::isISOControl)) {
      throw invalid("control character in a role name in value '" + value + "'");
    }
    return name;
  }

  private static int decodeEscape(String token, int index, String value) {
    int high = index + 2 < token.length() ? hexValue(token.charAt(index + 1)) : -1;
    int low = high >= 0 ? hexValue(token.charAt(index + 2)) : -1;
    if (low < 0) {
      throw invalid("malformed percent-escape in value '" + value + "'");
    }
    return (high << 4) | low;
  }

  // Only ASCII hex digits are accepted; Character.digit would also accept non-ASCII digits.
  private static int hexValue(char c) {
    if (c >= '0' && c <= '9') {
      return c - '0';
    }
    if (c >= 'a' && c <= 'f') {
      return c - 'a' + 10;
    }
    if (c >= 'A' && c <= 'F') {
      return c - 'A' + 10;
    }
    return -1;
  }

  private static String decodeUtf8(byte[] bytes, String value) {
    try {
      return StandardCharsets.UTF_8
          .newDecoder()
          .onMalformedInput(CodingErrorAction.REPORT)
          .onUnmappableCharacter(CodingErrorAction.REPORT)
          .decode(ByteBuffer.wrap(bytes))
          .toString();
    } catch (CharacterCodingException e) {
      throw invalid(
          "value '"
              + value
              + "' is not valid UTF-8; percent-encode the UTF-8 bytes of non-ASCII role names");
    }
  }

  private static IllegalActiveRolesException invalid(String detail) {
    return new IllegalActiveRolesException(
        "Invalid '" + AuthConstants.X_GRAVITINO_ACTIVE_ROLES_HEADER + "' header: " + detail);
  }
}
