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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertIterableEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import org.apache.gravitino.auth.ActiveRoles.Mode;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullSource;
import org.junit.jupiter.params.provider.ValueSource;

public class TestActiveRolesParser {

  @ParameterizedTest
  @NullSource
  @ValueSource(strings = {"", "   ", "\t"})
  public void testAbsentOrBlankIsAll(String value) {
    ActiveRoles roles = ActiveRolesParser.parse(value);
    assertEquals(Mode.ALL, roles.mode());
    assertTrue(roles.isAll());
    assertTrue(roles.roleNames().isEmpty());
  }

  @Test
  public void testAllKeyword() {
    assertEquals(Mode.ALL, ActiveRolesParser.parse("ALL").mode());
  }

  @Test
  public void testNoneKeyword() {
    ActiveRoles roles = ActiveRolesParser.parse("NONE");
    assertEquals(Mode.NONE, roles.mode());
    assertTrue(roles.isNone());
    assertTrue(roles.roleNames().isEmpty());
  }

  @Test
  public void testSingleRole() {
    ActiveRoles roles = ActiveRolesParser.parse("analyst");
    assertEquals(Mode.NAMED, roles.mode());
    assertIterableEquals(Collections.singletonList("analyst"), roles.roleNames());
  }

  @Test
  public void testCommaSeparatedList() {
    ActiveRoles roles = ActiveRolesParser.parse("analyst,reader");
    assertEquals(Mode.NAMED, roles.mode());
    assertIterableEquals(Arrays.asList("analyst", "reader"), roles.roleNames());
  }

  @Test
  public void testWhitespaceIsTrimmed() {
    ActiveRoles roles = ActiveRolesParser.parse("  analyst ,  reader  ");
    assertIterableEquals(Arrays.asList("analyst", "reader"), roles.roleNames());
  }

  @Test
  public void testDuplicatesCollapseAndPreserveOrder() {
    ActiveRoles roles = ActiveRolesParser.parse("reader, analyst, reader");
    assertIterableEquals(Arrays.asList("reader", "analyst"), roles.roleNames());
  }

  @Test
  public void testRoleNamesAreCaseSensitive() {
    ActiveRoles roles = ActiveRolesParser.parse("Analyst,analyst");
    assertIterableEquals(Arrays.asList("Analyst", "analyst"), roles.roleNames());
  }

  @Test
  public void testKeywordsAreCaseSensitiveSoLowercaseIsARoleName() {
    ActiveRoles roles = ActiveRolesParser.parse("all");
    assertEquals(Mode.NAMED, roles.mode());
    assertIterableEquals(Collections.singletonList("all"), roles.roleNames());
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "analyst,", // trailing comma
        ",analyst", // leading comma
        "analyst,,reader", // empty middle entry
        ",", // only a comma
        "ALL,analyst", // reserved keyword with a role name
        "analyst,ALL", // reserved keyword with a role name
        "NONE,reader", // reserved keyword with a role name
        "ALL,NONE", // two different reserved keywords
        "ALL,ALL", // repeated reserved keyword
        "NONE,NONE" // repeated reserved keyword
      })
  public void testMalformedValuesThrow(String value) {
    assertThrows(IllegalActiveRolesException.class, () -> ActiveRolesParser.parse(value));
  }

  @Test
  public void testMalformedExceptionIsIllegalArgument() {
    // The server maps this to 400 Bad Request, so it must remain an IllegalArgumentException.
    assertThrows(IllegalArgumentException.class, () -> ActiveRolesParser.parse("analyst,"));
  }

  @Test
  public void testPercentEncodedUtf8RoleName() {
    ActiveRoles roles = ActiveRolesParser.parse("vrf_%E8%A7%92%E8%89%B2");
    assertIterableEquals(Collections.singletonList("vrf_\u89d2\u8272"), roles.roleNames());
  }

  @Test
  public void testPercentEncodingIsCaseInsensitiveAndAppliesPerEntry() {
    ActiveRoles roles = ActiveRolesParser.parse("vrf_%e8%a7%92%e8%89%b2, r%C3%B4le");
    assertIterableEquals(Arrays.asList("vrf_\u89d2\u8272", "r\u00f4le"), roles.roleNames());
  }

  @Test
  public void testEncodedCommaAndPercentAreLiteral() {
    // Entries are split before decoding, so %2C is part of a single role name.
    ActiveRoles roles = ActiveRolesParser.parse("a%2Cb,100%25");
    assertIterableEquals(Arrays.asList("a,b", "100%"), roles.roleNames());
  }

  @Test
  public void testPlusIsNotDecodedAsSpace() {
    ActiveRoles roles = ActiveRolesParser.parse("a+b");
    assertIterableEquals(Collections.singletonList("a+b"), roles.roleNames());
  }

  @Test
  public void testEncodedKeywordIsARoleName() {
    // Keywords are matched before decoding, so an encoded form names a role literally called ALL.
    ActiveRoles roles = ActiveRolesParser.parse("%41LL");
    assertEquals(Mode.NAMED, roles.mode());
    assertIterableEquals(Collections.singletonList("ALL"), roles.roleNames());
  }

  @Test
  public void testDuplicatesCollapseAfterDecoding() {
    ActiveRoles roles = ActiveRolesParser.parse("a, %61");
    assertIterableEquals(Collections.singletonList("a"), roles.roleNames());
  }

  @Test
  public void testEncodedWhitespaceIsPartOfTheName() {
    // Trimming happens before decoding, so an encoded space is significant.
    ActiveRoles roles = ActiveRolesParser.parse(" %20a ");
    assertIterableEquals(Collections.singletonList(" a"), roles.roleNames());
  }

  @Test
  public void testEncodedNoneIsARoleNameAndKeywordsStillMustBeAlone() {
    assertIterableEquals(
        Collections.singletonList("NONE"), ActiveRolesParser.parse("%4EONE").roleNames());
    assertThrows(IllegalActiveRolesException.class, () -> ActiveRolesParser.parse("ALL,%41LL"));
  }

  @Test
  public void testRawUtf8BytesDecodedAsLatin1AreRecovered() {
    // Jetty exposes each header byte as one ISO-8859-1 char; raw UTF-8 must still resolve.
    String asReceived =
        new String(
            "vrf_\u89d2\u8272".getBytes(StandardCharsets.UTF_8), StandardCharsets.ISO_8859_1);
    ActiveRoles roles = ActiveRolesParser.parse(asReceived);
    assertIterableEquals(Collections.singletonList("vrf_\u89d2\u8272"), roles.roleNames());
  }

  @Test
  public void testSupplementaryCharacterInRawAndPercentForms() {
    String name = "r\ud83d\ude00"; // U+1F600, four UTF-8 bytes
    String raw = new String(name.getBytes(StandardCharsets.UTF_8), StandardCharsets.ISO_8859_1);
    assertIterableEquals(Collections.singletonList(name), ActiveRolesParser.parse(raw).roleNames());
    assertIterableEquals(
        Collections.singletonList(name), ActiveRolesParser.parse("r%F0%9F%98%80").roleNames());
  }

  @Test
  public void testControlCharacterFromRawUtf8IsRejected() {
    // U+0085 (NEL) sent as raw UTF-8 bytes C2 85 is a control character, like %0A.
    String raw =
        new String(new byte[] {'x', (byte) 0xC2, (byte) 0x85}, StandardCharsets.ISO_8859_1);
    assertThrows(IllegalActiveRolesException.class, () -> ActiveRolesParser.parse(raw));
  }

  @Test
  public void testAlreadyDecodedUnicodeIsKeptAsIs() {
    ActiveRoles roles = ActiveRolesParser.parse("vrf_\u89d2\u8272");
    assertIterableEquals(Collections.singletonList("vrf_\u89d2\u8272"), roles.roleNames());
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "a%", // truncated escape
        "a%4", // truncated escape
        "a%G1", // non-hex escape
        "vrf_%E8%A7", // truncated UTF-8 sequence
        "%FF", // invalid UTF-8 byte
        "vrf_r\u00f4le", // raw Latin-1 byte 0xF4 is not valid UTF-8
        "%20", // blank after decoding
        "a,%09", // blank after decoding
        "%E2%80%83", // Unicode whitespace (U+2003) only after decoding
        "%00", // control character
        "x%0Aforged", // control character
        "x%7F" // control character
      })
  public void testInvalidEncodingThrows(String value) {
    assertThrows(IllegalActiveRolesException.class, () -> ActiveRolesParser.parse(value));
  }
}
