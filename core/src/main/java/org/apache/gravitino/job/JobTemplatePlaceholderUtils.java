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

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.annotation.Nullable;
import org.apache.gravitino.meta.JobTemplateEntity;

/**
 * Utility methods to parse and replace the placeholders of a job template.
 *
 * <p>The syntax is:
 *
 * <ul>
 *   <li>{@code {{name}}}: a required parameter. Submitting a job without a value for it fails.
 *   <li>{@code {{name:-default}}}: an optional parameter that falls back to {@code default}, which
 *       may be empty ({@code {{name:-}}}). A default declared on one occurrence of a parameter
 *       applies to all of its occurrences in the template. The default value is used as is and may
 *       span lines, but its braces must be balanced, so the placeholder ends at the first <code>
 *       }}</code> outside of them. A JSON object is a valid default value, for example {@code
 *       {{options:-{"k":"v"}}}}.
 *   <li><code>\{{</code>: a literal <code>{{</code>, for templates that must pass through text such
 *       as another tool's {@code {{macro}}}. A value that ends with a backslash right before a
 *       placeholder therefore has to double it, as in <code>\\{{name}}</code>.
 * </ul>
 *
 * <p>A parameter takes its value from the job configuration first, where an explicit empty string
 * counts as a value, and from its default otherwise. The job configuration value is used as is and
 * is never scanned for placeholders.
 */
final class JobTemplatePlaceholderUtils {

  // The start of a placeholder: "{{", its name in group 1, and ":-" in group 2 if it has a default
  // value. The default value and the closing "}}" are found by scanning.
  private static final Pattern PLACEHOLDER_START = Pattern.compile("\\{\\{([\\w.-]+)(:-)?");

  private static final String OPEN = "{{";

  private static final String CLOSE = "}}";

  private static final String ESCAPED_OPEN = "\\{{";

  /** Receives the literal text and the placeholders of a template value, in order. */
  private interface TokenVisitor {
    void literal(String text);

    void placeholder(String name, @Nullable String defaultValue);
  }

  private JobTemplatePlaceholderUtils() {}

  /**
   * Parses the parameters referenced by a job template, together with their default values.
   *
   * @param content the job template content
   * @return the parameters in order of first appearance, each mapped to its default value, or to an
   *     empty optional if the parameter is required
   * @throws IllegalArgumentException if a placeholder is malformed, for example its default value
   *     has unbalanced braces, or a parameter declares conflicting default values
   */
  static Map<String, Optional<String>> parseParameters(JobTemplateEntity.TemplateContent content) {
    Map<String, Optional<String>> parameters = new LinkedHashMap<>();
    for (String value : templateValues(content)) {
      collectParameters(value, parameters);
    }
    return parameters;
  }

  /**
   * Finds the required parameters that have no value in the job configuration.
   *
   * @param parameters the parameters of the job template, from {@link #parseParameters}
   * @param jobConf the job configuration
   * @return the missing parameters, sorted, or an empty set if none is missing
   */
  static Set<String> findMissingParameters(
      Map<String, Optional<String>> parameters, Map<String, String> jobConf) {
    Set<String> missing = new TreeSet<>();
    parameters.forEach(
        (name, defaultValue) -> {
          if (!defaultValue.isPresent() && jobConf.get(name) == null) {
            missing.add(name);
          }
        });
    return missing;
  }

  /**
   * Finds the job configuration keys that the job template does not reference.
   *
   * @param parameters the parameters of the job template, from {@link #parseParameters}
   * @param jobConf the job configuration
   * @return the unreferenced keys, sorted
   */
  static Set<String> findUnusedKeys(
      Map<String, Optional<String>> parameters, Map<String, String> jobConf) {
    Set<String> unused = new TreeSet<>(jobConf.keySet());
    unused.removeAll(parameters.keySet());
    return unused;
  }

  /**
   * Replaces the placeholders in a template value with their values from the job configuration, or
   * with their default values.
   *
   * @param value the template value, may be null
   * @param jobConf the job configuration
   * @param parameters the parameters of the whole job template, from {@link #parseParameters},
   *     which supply the default values. A placeholder whose parameter isn't listed falls back to
   *     the default value written on it
   * @return the resolved value, or null if {@code value} is null
   * @throws IllegalArgumentException if a placeholder is malformed, or has neither a value nor a
   *     default value
   */
  @Nullable
  static String replacePlaceholders(
      @Nullable String value,
      Map<String, String> jobConf,
      Map<String, Optional<String>> parameters) {
    if (value == null) {
      return null;
    }

    StringBuilder result = new StringBuilder();
    scan(
        value,
        new TokenVisitor() {
          @Override
          public void literal(String text) {
            result.append(text);
          }

          @Override
          public void placeholder(String name, @Nullable String defaultValue) {
            String replacement = jobConf.get(name);
            if (replacement == null) {
              replacement =
                  parameters
                      .getOrDefault(name, Optional.ofNullable(defaultValue))
                      .orElseThrow(
                          () ->
                              new IllegalArgumentException(
                                  String.format(
                                      "No value is provided for the job template parameter %s",
                                      name)));
            }
            result.append(replacement);
          }
        });
    return result.toString();
  }

  private static void collectParameters(
      @Nullable String value, Map<String, Optional<String>> parameters) {
    if (value == null) {
      return;
    }

    scan(
        value,
        new TokenVisitor() {
          @Override
          public void literal(String text) {}

          @Override
          public void placeholder(String name, @Nullable String defaultValue) {
            Optional<String> existing = parameters.get(name);
            if (existing == null || !existing.isPresent()) {
              parameters.put(name, Optional.ofNullable(defaultValue));
            } else if (defaultValue != null && !existing.get().equals(defaultValue)) {
              throw new IllegalArgumentException(
                  String.format(
                      "Job template parameter %s has conflicting default values '%s' and '%s'",
                      name, existing.get(), defaultValue));
            }
          }
        });
  }

  /**
   * Splits a template value into literal text and placeholders, and passes them in order to the
   * visitor.
   *
   * <p>Text that only looks like a placeholder, for example {@code {{ name }}} or <code>{{name}
   * </code>, is literal text. A placeholder with a default value, however, must be well formed: its
   * default value must have balanced braces and the placeholder must end with <code>}}</code>.
   */
  private static void scan(String value, TokenVisitor visitor) {
    StringBuilder literal = new StringBuilder();
    Matcher start = PLACEHOLDER_START.matcher(value);
    int index = 0;
    while (index < value.length()) {
      if (value.startsWith(ESCAPED_OPEN, index)) {
        literal.append(OPEN);
        index += ESCAPED_OPEN.length();
        continue;
      }

      if (value.startsWith(OPEN, index) && start.region(index, value.length()).lookingAt()) {
        String name = start.group(1);
        boolean hasDefaultValue = start.group(2) != null;
        if (hasDefaultValue || value.startsWith(CLOSE, start.end())) {
          String defaultValue = null;
          int closeIndex = start.end();
          if (hasDefaultValue) {
            closeIndex = findDefaultValueEnd(value, start.end(), name);
            defaultValue = value.substring(start.end(), closeIndex);
          }
          flushLiteral(literal, visitor);
          visitor.placeholder(name, defaultValue);
          index = closeIndex + CLOSE.length();
          continue;
        }
      }

      literal.append(value.charAt(index));
      index++;
    }
    flushLiteral(literal, visitor);
  }

  /**
   * Returns the index of the "}}" that closes a placeholder whose default value starts at {@code
   * from}, which is the first "}}" outside of the braces of the default value.
   */
  private static int findDefaultValueEnd(String value, int from, String name) {
    int depth = 0;
    boolean hasBrace = false;
    for (int index = from; index < value.length(); index++) {
      char c = value.charAt(index);
      if (c == '{') {
        depth++;
        hasBrace = true;
      } else if (c == '}') {
        if (depth > 0) {
          // A "}" that closes a brace of the default value, not the placeholder.
          depth--;
        } else if (value.startsWith(CLOSE, index)) {
          // A default value that has braces and is followed by one more "}" was meant to take that
          // "}" too, which happens when a brace sits inside a quoted string, for example
          // {{o:-{"k":"}"}}}. Reject it rather than silently cutting the default value. A default
          // value without braces cannot be extended that way, so the "}" is literal text after the
          // placeholder, as in {"k":{{v:-1}}}.
          int next = index + CLOSE.length();
          if (hasBrace && next < value.length() && value.charAt(next) == '}') {
            throw new IllegalArgumentException(
                String.format(
                    "The default value of job template parameter %s is ambiguous because the "
                        + "placeholder is followed by '}'. Keep the braces in the default value "
                        + "balanced: %s",
                    name, value));
          }
          return index;
        } else if (index < value.length() - 1) {
          throw new IllegalArgumentException(
              String.format(
                  "The default value of job template parameter %s has an unmatched '}': %s",
                  name, value));
        }
      }
    }

    throw new IllegalArgumentException(
        String.format(
            depth > 0
                ? "The default value of job template parameter %s has an unmatched '{': %s"
                : "The placeholder of job template parameter %s is not closed with '}}': %s",
            name,
            value));
  }

  private static void flushLiteral(StringBuilder literal, TokenVisitor visitor) {
    if (literal.length() > 0) {
      visitor.literal(literal.toString());
      literal.setLength(0);
    }
  }

  private static List<String> templateValues(JobTemplateEntity.TemplateContent content) {
    List<String> values = new ArrayList<>();
    values.add(content.executable());
    addAll(values, content.arguments());
    addEntries(values, content.environments());
    addEntries(values, content.customFields());
    addAll(values, content.scripts());
    values.add(content.className());
    addAll(values, content.jars());
    addAll(values, content.files());
    addAll(values, content.archives());
    addEntries(values, content.configs());
    return values;
  }

  private static void addAll(List<String> values, @Nullable Collection<String> source) {
    if (source != null) {
      values.addAll(source);
    }
  }

  private static void addEntries(List<String> values, @Nullable Map<String, String> source) {
    if (source != null) {
      source.forEach(
          (key, value) -> {
            values.add(key);
            values.add(value);
          });
    }
  }
}
