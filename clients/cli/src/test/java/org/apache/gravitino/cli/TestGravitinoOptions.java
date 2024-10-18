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

package org.apache.gravitino.cli;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.commons.cli.Option;
import org.junit.jupiter.api.Test;

public class TestGravitinoOptions {

  @Test
  public void testCreateSimpleOption() {
    GravitinoOptions gravitinoOptions = new GravitinoOptions();
    Option helpOption =
        gravitinoOptions.createSimpleOption("h", GravitinoOptions.HELP, "help message");

    assertEquals("h", helpOption.getOpt(), "Simple option short name should be 'h'.");
    assertEquals("help", helpOption.getLongOpt(), "Simple option long name should be 'help'.");
    assertFalse(helpOption.hasArg(), "Simple option should not require an argument.");
    assertEquals(
        "help message",
        helpOption.getDescription(),
        "Simple option should have correct description.");
  }

  @Test
  public void testCreateArgOption() {
    GravitinoOptions gravitinoOptions = new GravitinoOptions();
    Option urlOption = gravitinoOptions.createArgOption("u", GravitinoOptions.URL, "url argument");

    assertEquals("u", urlOption.getOpt(), "Argument option short name should be 'u'.");
    assertEquals("url", urlOption.getLongOpt(), "Argument option long name should be 'url'.");
    assertTrue(urlOption.hasArg(), "Argument option should require an argument.");
    assertEquals(
        "url argument",
        urlOption.getDescription(),
        "Argument option should have correct description.");
  }
}
