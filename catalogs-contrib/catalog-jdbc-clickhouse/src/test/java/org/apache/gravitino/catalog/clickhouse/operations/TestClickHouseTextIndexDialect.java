/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.gravitino.catalog.clickhouse.operations;

import static org.apache.gravitino.catalog.clickhouse.ClickHouseConstants.IndexConstants.TEXT_NGRAM_SIZE;
import static org.apache.gravitino.catalog.clickhouse.ClickHouseConstants.IndexConstants.TOKENIZER;

import java.util.HashMap;
import java.util.Map;
import org.apache.gravitino.catalog.clickhouse.operations.ClickHouseTextIndexDialect.Grammar;
import org.apache.gravitino.catalog.clickhouse.operations.ClickHouseTextIndexDialect.ServerVersion;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestClickHouseTextIndexDialect {

  @Test
  public void testParseServerVersion() {
    ServerVersion version = ClickHouseTextIndexDialect.parseServerVersion("25.5.1.2782");
    Assertions.assertEquals(25, version.major());
    Assertions.assertEquals(5, version.minor());
    Assertions.assertEquals(1, version.patch());

    ServerVersion shortVersion = ClickHouseTextIndexDialect.parseServerVersion("24.8");
    Assertions.assertEquals(24, shortVersion.major());
    Assertions.assertEquals(8, shortVersion.minor());
    Assertions.assertEquals(0, shortVersion.patch());

    // An unclassifiable version must fail clearly rather than silently pick a grammar.
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> ClickHouseTextIndexDialect.parseServerVersion(null));
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> ClickHouseTextIndexDialect.parseServerVersion(" "));
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> ClickHouseTextIndexDialect.parseServerVersion("unknown-server"));
  }

  @Test
  public void testGrammarSelectionAtEveryBoundary() {
    Assertions.assertEquals(
        Grammar.INVERTED, ClickHouseTextIndexDialect.grammarFor(version("23.2.0")));
    Assertions.assertEquals(
        Grammar.INVERTED, ClickHouseTextIndexDialect.grammarFor(version("24.4.0")));
    // v24.5 switches from inverted to full_text.
    Assertions.assertEquals(
        Grammar.FULL_TEXT, ClickHouseTextIndexDialect.grammarFor(version("24.5.0")));
    Assertions.assertEquals(
        Grammar.FULL_TEXT, ClickHouseTextIndexDialect.grammarFor(version("24.8.14")));
    // v25.5 publishes the gin rename.
    Assertions.assertEquals(Grammar.GIN, ClickHouseTextIndexDialect.grammarFor(version("25.5.1")));
    // v25.6 starts the v2 key-value tokenizer grammar.
    Assertions.assertEquals(
        Grammar.TEXT_V2, ClickHouseTextIndexDialect.grammarFor(version("25.6.0")));
    Assertions.assertEquals(
        Grammar.TEXT_V2, ClickHouseTextIndexDialect.grammarFor(version("25.8.0")));
    // v25.9 introduces the v3 tokenizer grammar, including up to and beyond v26.2.
    Assertions.assertEquals(
        Grammar.TEXT_V3, ClickHouseTextIndexDialect.grammarFor(version("25.9.0")));
    Assertions.assertEquals(
        Grammar.TEXT_V3, ClickHouseTextIndexDialect.grammarFor(version("25.12.0")));
    Assertions.assertEquals(
        Grammar.TEXT_V3, ClickHouseTextIndexDialect.grammarFor(version("26.2.0")));

    // Text indexes do not exist before v23.2.
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> ClickHouseTextIndexDialect.grammarFor(version("23.1.0")));
  }

  @Test
  public void testLegacyPositionalForms() {
    Map<String, String> properties = new HashMap<>();
    properties.put(TEXT_NGRAM_SIZE, "3");

    Assertions.assertEquals(
        "inverted(3)",
        ClickHouseTextIndexDialect.buildTypeClause(version("24.4.0"), properties, "idx"));
    Assertions.assertEquals(
        "full_text(3)",
        ClickHouseTextIndexDialect.buildTypeClause(version("24.8.14"), properties, "idx"));
    Assertions.assertEquals(
        "gin(3)", ClickHouseTextIndexDialect.buildTypeClause(version("25.5.1"), properties, "idx"));
  }

  @Test
  public void testTokenizerGrammars() {
    Map<String, String> properties = new HashMap<>();
    properties.put(TOKENIZER, "ngram");
    properties.put(TEXT_NGRAM_SIZE, "4");

    // v25.6-v25.8 use the key-value tokenizer grammar.
    Assertions.assertEquals(
        "text(tokenizer = 'ngram', ngram_size = 4)",
        ClickHouseTextIndexDialect.buildTypeClause(version("25.6.0"), properties, "idx"));

    // v25.9+ uses the v3 tokenizer grammar.
    Assertions.assertEquals(
        "text(tokenizer = 'ngram', ngram_size = 4)",
        ClickHouseTextIndexDialect.buildTypeClause(version("25.9.0"), properties, "idx"));

    // The default granularity only differs for the v3 grammar.
    Assertions.assertEquals(1, ClickHouseTextIndexDialect.defaultGranularity(Grammar.TEXT_V2));
    Assertions.assertEquals(
        100000000, ClickHouseTextIndexDialect.defaultGranularity(Grammar.TEXT_V3));
  }

  @Test
  public void testTokenizerIsMandatoryFromV2512() {
    // A blank tokenizer must fail on v25.12+ because "default" is no longer accepted there.
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            ClickHouseTextIndexDialect.buildTypeClause(version("25.12.0"), new HashMap<>(), "idx"));

    // An explicit tokenizer is accepted.
    Map<String, String> properties = new HashMap<>();
    properties.put(TOKENIZER, "split");
    Assertions.assertEquals(
        "text(tokenizer = 'split')",
        ClickHouseTextIndexDialect.buildTypeClause(version("25.12.0"), properties, "idx"));

    // Unsupported tokenizer names are rejected.
    Map<String, String> bad = new HashMap<>();
    bad.put(TOKENIZER, "bogus");
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> ClickHouseTextIndexDialect.buildTypeClause(version("25.9.0"), bad, "idx"));
  }

  @Test
  public void testNgramSizeValidation() {
    Map<String, String> tooSmall = new HashMap<>();
    tooSmall.put(TEXT_NGRAM_SIZE, "1");
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> ClickHouseTextIndexDialect.buildTypeClause(version("24.8.14"), tooSmall, "idx"));

    Map<String, String> tooLarge = new HashMap<>();
    tooLarge.put(TEXT_NGRAM_SIZE, "9");
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> ClickHouseTextIndexDialect.buildTypeClause(version("24.8.14"), tooLarge, "idx"));

    Map<String, String> notANumber = new HashMap<>();
    notANumber.put(TEXT_NGRAM_SIZE, "abc");
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> ClickHouseTextIndexDialect.buildTypeClause(version("24.8.14"), notANumber, "idx"));

    // The accepted range is 2-8.
    Map<String, String> lowerBound = new HashMap<>();
    lowerBound.put(TEXT_NGRAM_SIZE, "2");
    Assertions.assertEquals(
        "full_text(2)",
        ClickHouseTextIndexDialect.buildTypeClause(version("24.8.14"), lowerBound, "idx"));

    Map<String, String> upperBound = new HashMap<>();
    upperBound.put(TEXT_NGRAM_SIZE, "8");
    Assertions.assertEquals(
        "full_text(8)",
        ClickHouseTextIndexDialect.buildTypeClause(version("24.8.14"), upperBound, "idx"));
  }

  private static ServerVersion version(String raw) {
    return ClickHouseTextIndexDialect.parseServerVersion(raw);
  }
}
