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

package org.apache.gravitino.job;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.gravitino.meta.JobTemplateEntity;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestJobTemplatePlaceholderUtils {

  @Test
  public void testResolveWithValues() {
    String template = "Hello, {{name}}! Welcome to {{place}}.";
    Map<String, String> conf = ImmutableMap.of("name", "Alice", "place", "Wonderland");
    Assertions.assertEquals("Hello, Alice! Welcome to Wonderland.", replace(template, conf));

    Assertions.assertEquals("Hello, World!", replace("Hello, World!", conf));
    Assertions.assertEquals("", replace("", conf));
    Assertions.assertNull(replace(null, conf));

    Assertions.assertEquals("Alice is Alice", replace("{{name}} is {{name}}", conf));

    // Names may contain letters, digits, '_', '.' and '-'.
    Assertions.assertEquals(
        "a-b-c",
        replace(
            "{{user_name}}-{{user.name}}-{{score-1}}",
            ImmutableMap.of("user_name", "a", "user.name", "b", "score-1", "c")));
  }

  @Test
  public void testResolveKeepsTextThatIsNotAPlaceholder() {
    Map<String, String> conf = ImmutableMap.of("name", "Dave", "value", "42");
    Assertions.assertEquals("{{name}! 42", replace("{{name}! {{value}}", conf));
    Assertions.assertEquals("{{score%}}", replace("{{score%}}", conf));
    Assertions.assertEquals("{{ name }}", replace("{{ name }}", conf));
    Assertions.assertEquals("{42}", replace("{{{value}}}", conf));
    Assertions.assertEquals("{{42}}", replace("{{{{value}}}}", conf));
  }

  @Test
  public void testResolveMissingValueFails() {
    IllegalArgumentException e =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> replace("Hello, {{name}}!", Collections.emptyMap()));
    Assertions.assertTrue(e.getMessage().contains("name"), e.getMessage());

    // A null value in the job configuration counts as no value.
    Map<String, String> conf = new HashMap<>();
    conf.put("name", null);
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> replace("Hello, {{name}}!", conf));
  }

  @Test
  public void testResolveDefaultValues() {
    String template = "--strategy {{strategy:-binpack}} --where {{where:-}}";
    Assertions.assertEquals(
        "--strategy binpack --where ", replace(template, Collections.emptyMap()));

    // A provided value, including an explicit empty string, wins over the default value.
    Assertions.assertEquals(
        "--strategy sort --where a > 1",
        replace(template, ImmutableMap.of("strategy", "sort", "where", "a > 1")));
    Assertions.assertEquals(
        "--strategy  --where ", replace(template, ImmutableMap.of("strategy", "")));

    // A default value may contain ':', '-', spaces, '$' and backslashes, all used as is.
    Assertions.assertEquals(
        "local[*] a:-b x y $1 C:\\tmp",
        replace(
            "{{master:-local[*]}} {{a:-a:-b}} {{b:-x y}} {{c:-$1}} {{d:-C:\\tmp}}",
            Collections.emptyMap()));
  }

  @Test
  public void testResolveEscapedPlaceholder() {
    Map<String, String> conf = ImmutableMap.of("ds", "2026-09-22");
    Assertions.assertEquals("echo {{ds}} 2026-09-22", replace("echo \\{{ds}} {{ds}}", conf));
    Assertions.assertEquals("{{missing}}", replace("\\{{missing}}", Collections.emptyMap()));
  }

  @Test
  public void testResolveValueIsNotScanned() {
    // Values are used as is: placeholders, escapes and replacement syntax in them stay literal.
    Map<String, String> conf =
        ImmutableMap.of("conf", "path=C:\\tmp and cost=$5 {{other}} \\{{x}}", "other", "x");
    Assertions.assertEquals(
        "config=path=C:\\tmp and cost=$5 {{other}} \\{{x}}", replace("config={{conf}}", conf));

    Assertions.assertEquals(
        "config=p$1x", replace("config={{conf}}", ImmutableMap.of("conf", "p$1x")));
  }

  @Test
  public void testParametersCollectsAllFields() {
    JobTemplateEntity.TemplateContent content =
        JobTemplateEntity.TemplateContent.builder()
            .withJobType(JobTemplate.JobType.SPARK)
            .withExecutable("{{exec}}")
            .withArguments(Lists.newArrayList("--a", "{{arg:-1}}", "\\{{escaped}}"))
            .withEnvironments(ImmutableMap.of("{{env_key}}", "{{env_value:-}}"))
            .withCustomFields(ImmutableMap.of("field", "{{custom}}"))
            .withClassName("{{class_name:-Main}}")
            .withJars(Lists.newArrayList("{{jar}}"))
            .withFiles(Lists.newArrayList("{{file:-}}"))
            .withArchives(Lists.newArrayList("{{archive:-}}"))
            .withConfigs(ImmutableMap.of("spark.master", "{{spark_master:-local[*]}}"))
            .build();

    Map<String, Optional<String>> parameters = JobTemplatePlaceholderUtils.parseParameters(content);
    Map<String, Optional<String>> expected = new LinkedHashMap<>();
    expected.put("exec", Optional.empty());
    expected.put("arg", Optional.of("1"));
    expected.put("env_key", Optional.empty());
    expected.put("env_value", Optional.of(""));
    expected.put("custom", Optional.empty());
    expected.put("class_name", Optional.of("Main"));
    expected.put("jar", Optional.empty());
    expected.put("file", Optional.of(""));
    expected.put("archive", Optional.of(""));
    expected.put("spark_master", Optional.of("local[*]"));
    Assertions.assertEquals(expected, parameters);
  }

  @Test
  public void testDefaultValueAppliesToAllOccurrences() {
    JobTemplateEntity.TemplateContent content =
        shellContent(
            Lists.newArrayList("--catalog", "{{catalog}}", "--table", "{{catalog:-ice}}.{{t}}"));
    Map<String, Optional<String>> parameters = JobTemplatePlaceholderUtils.parseParameters(content);
    Assertions.assertEquals(Optional.of("ice"), parameters.get("catalog"));

    Map<String, String> conf = ImmutableMap.of("t", "db.t");
    Assertions.assertEquals(
        "ice", JobTemplatePlaceholderUtils.replacePlaceholders("{{catalog}}", conf, parameters));
  }

  @Test
  public void testConflictingDefaultValuesFail() {
    JobTemplateEntity.TemplateContent content =
        shellContent(Lists.newArrayList("{{mode:-all}}", "{{mode:-stats}}"));
    IllegalArgumentException e =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> JobTemplatePlaceholderUtils.parseParameters(content));
    Assertions.assertTrue(e.getMessage().contains("mode"), e.getMessage());

    // Repeating the same default value is allowed.
    Assertions.assertEquals(
        Optional.of("all"),
        JobTemplatePlaceholderUtils.parseParameters(
                shellContent(Lists.newArrayList("{{mode:-all}}", "{{mode:-all}}")))
            .get("mode"));
  }

  @Test
  public void testJsonDefaultValues() {
    Map<String, String> noConf = Collections.emptyMap();
    Assertions.assertEquals("--options {}", replace("--options {{options:-{}}}", noConf));
    Assertions.assertEquals("{\"k\":\"v\"}", replace("{{o:-{\"k\":\"v\"}}}", noConf));
    Assertions.assertEquals(
        "{\"a\":{\"b\":{\"c\":1}}}", replace("{{o:-{\"a\":{\"b\":{\"c\":1}}}}}", noConf));
    Assertions.assertEquals("[]", replace("{{o:-[]}}", noConf));
    Assertions.assertEquals(
        "[{\"a\":1},{\"b\":{\"c\":2}}]", replace("{{o:-[{\"a\":1},{\"b\":{\"c\":2}}]}}", noConf));
    // A quoted "}}" is fine as long as all the braces are balanced.
    Assertions.assertEquals("{\"a\":\"{{}}\"}", replace("{{o:-{\"a\":\"{{}}\"}}}", noConf));

    // A provided value replaces the whole JSON default value.
    Assertions.assertEquals(
        "--options {\"x\":1}",
        replace("--options {{options:-{\"k\":\"v\"}}}", ImmutableMap.of("options", "{\"x\":1}")));
  }

  @Test
  public void testDefaultValueSpanningLines() {
    Assertions.assertEquals(
        "{\n  \"k\": \"v\"\n}", replace("{{o:-{\n  \"k\": \"v\"\n}}}", Collections.emptyMap()));
    Assertions.assertEquals("a\nb", replace("{{o:-a\nb}}", Collections.emptyMap()));
  }

  @Test
  public void testMultiplePlaceholdersWithBracedDefaultValues() {
    Assertions.assertEquals(
        "--a {\"x\":1} --b v --c {}",
        replace("--a {{a:-{\"x\":1}}} --b {{b}} --c {{c:-{}}}", ImmutableMap.of("b", "v")));
    Assertions.assertEquals("{}{}", replace("{{a:-{}}}{{b:-{}}}", Collections.emptyMap()));
  }

  @Test
  public void testJsonDefaultValueIsParsedAsOneParameter() {
    JobTemplateEntity.TemplateContent content =
        shellContent(Lists.newArrayList("--options", "{{options:-{\"k\":{\"v\":1}}}}", "{{name}}"));
    Map<String, Optional<String>> parameters = JobTemplatePlaceholderUtils.parseParameters(content);
    Map<String, Optional<String>> expected = new LinkedHashMap<>();
    expected.put("options", Optional.of("{\"k\":{\"v\":1}}"));
    expected.put("name", Optional.empty());
    Assertions.assertEquals(expected, parameters);
  }

  @Test
  public void testDefaultValueWithoutBracesFollowedByBrace() {
    Map<String, String> noConf = Collections.emptyMap();
    // The "}" after the placeholder belongs to the surrounding text, and a default value without
    // braces could not have taken it, so this is not ambiguous.
    Assertions.assertEquals("{\"k\":1}", replace("{\"k\":{{v:-1}}}", noConf));
    Assertions.assertEquals("1}", replace("{{v:-1}}}", noConf));
    Assertions.assertEquals("1}", replace("{{v:-1}}}", ImmutableMap.of()));
    Assertions.assertEquals("2}", replace("{{v:-1}}}", ImmutableMap.of("v", "2")));
    // Without a default value the same text has always been accepted, and still is.
    Assertions.assertEquals("2}", replace("{{v}}}", ImmutableMap.of("v", "2")));

    // A default value that has braces is still rejected, because it could have taken the "}".
    assertMalformed("{\"k\":{{v:-{}}}}", "v");
  }

  @Test
  public void testTemplatesRelyingOnUnresolvedPlaceholdersNowFail() {
    // Before default values were supported, a placeholder with no value was passed through as
    // literal text. It is now a required parameter, so such a template has to escape it.
    Map<String, Optional<String>> parameters =
        JobTemplatePlaceholderUtils.parseParameters(
            shellContent(Lists.newArrayList("--image", "{{.Values.image}}")));
    Assertions.assertEquals(
        Lists.newArrayList(".Values.image"), Lists.newArrayList(parameters.keySet()));
    assertMalformed("{{.Values.image}}", ".Values.image");

    Assertions.assertTrue(
        JobTemplatePlaceholderUtils.parseParameters(
                shellContent(Lists.newArrayList("--image", "\\{{.Values.image}}")))
            .isEmpty());
    Assertions.assertEquals(
        "{{.Values.image}}", replace("\\{{.Values.image}}", Collections.emptyMap()));
  }

  @Test
  public void testMalformedDefaultValuesFail() {
    // Unbalanced braces in the default value.
    assertMalformed("{{c:-{}}", "c");
    assertMalformed("{{c:-{{}}", "c");
    assertMalformed("{{c:-a}b}}", "c");
    assertMalformed("{{c:-{\"k\":1}", "c");
    // A brace inside a quoted string unbalances the braces, so the default value is ambiguous.
    assertMalformed("{{c:-{\"k\":\"}\"}}}", "c");
    // The placeholder is never closed.
    assertMalformed("{{c:-abc", "c");
    assertMalformed("prefix {{c:-abc} suffix", "c");

    // Malformed placeholders are rejected when parsing, so when registering a template.
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            JobTemplatePlaceholderUtils.parseParameters(
                shellContent(Lists.newArrayList("--options", "{{options:-{}}"))));
  }

  @Test
  public void testPlaceholdersWithoutDefaultValueKeepLooseText() {
    // Without ":-", text that doesn't form a placeholder stays literal instead of failing.
    Map<String, String> conf = ImmutableMap.of("a", "1");
    Assertions.assertEquals("{{a", replace("{{a", conf));
    Assertions.assertEquals("{{a}", replace("{{a}", conf));
    Assertions.assertEquals("1}", replace("{{a}}}", conf));
    Assertions.assertEquals("{{}}", replace("{{}}", conf));
    Assertions.assertEquals("{{a b}}", replace("{{a b}}", conf));
    Assertions.assertEquals("}} 1 }}", replace("}} {{a}} }}", conf));
  }

  @Test
  public void testEscapesAreNotPlaceholders() {
    Map<String, String> conf = ImmutableMap.of("a", "1");
    Assertions.assertEquals("{{a:-{}}}", replace("\\{{a:-{}}}", conf));
    Assertions.assertEquals("{{a}} 1", replace("\\{{a}} {{a}}", conf));
    Assertions.assertTrue(
        JobTemplatePlaceholderUtils.parseParameters(
                shellContent(Lists.newArrayList("\\{{ds}}", "\\{{a:-{}}")))
            .isEmpty());
  }

  @Test
  public void testFindMissingParameters() {
    Map<String, Optional<String>> parameters =
        JobTemplatePlaceholderUtils.parseParameters(
            shellContent(Lists.newArrayList("{{b}}", "{{a}}", "{{c:-x}}", "{{d}}")));

    // A null value counts as no value, an empty string counts as a value.
    Map<String, String> conf = new HashMap<>();
    conf.put("d", "");
    conf.put("a", null);
    Assertions.assertEquals(
        Lists.newArrayList("a", "b"),
        Lists.newArrayList(JobTemplatePlaceholderUtils.findMissingParameters(parameters, conf)));

    Assertions.assertTrue(
        JobTemplatePlaceholderUtils.findMissingParameters(
                parameters, ImmutableMap.of("a", "1", "b", "2", "d", ""))
            .isEmpty());
  }

  @Test
  public void testFindUnusedKeys() {
    Map<String, Optional<String>> parameters =
        JobTemplatePlaceholderUtils.parseParameters(
            shellContent(Lists.newArrayList("{{a}}", "{{b:-}}")));
    Assertions.assertEquals(
        Lists.newArrayList("c", "z"),
        Lists.newArrayList(
            JobTemplatePlaceholderUtils.findUnusedKeys(
                parameters, ImmutableMap.of("z", "1", "a", "1", "c", "1"))));
  }

  private static JobTemplateEntity.TemplateContent shellContent(List<String> arguments) {
    return JobTemplateEntity.TemplateContent.builder()
        .withJobType(JobTemplate.JobType.SHELL)
        .withExecutable("/bin/echo")
        .withArguments(arguments)
        .withEnvironments(Collections.emptyMap())
        .withCustomFields(Collections.emptyMap())
        .withScripts(Collections.emptyList())
        .build();
  }

  private static void assertMalformed(String value, String name) {
    IllegalArgumentException e =
        Assertions.assertThrows(
            IllegalArgumentException.class, () -> replace(value, Collections.emptyMap()));
    Assertions.assertTrue(e.getMessage().contains(name), e.getMessage());
  }

  // Replaces the placeholders of a single value, whose default values are the ones written on it.
  private static String replace(String value, Map<String, String> jobConf) {
    return JobTemplatePlaceholderUtils.replacePlaceholders(value, jobConf, Collections.emptyMap());
  }
}
