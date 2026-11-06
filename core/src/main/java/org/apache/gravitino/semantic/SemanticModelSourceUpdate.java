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
package org.apache.gravitino.semantic;

import com.google.common.base.Preconditions;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.EqualsAndHashCode;

/** Internal write intent for source associations; never part of a public model definition. */
@EqualsAndHashCode
public final class SemanticModelSourceUpdate {
  private static final SemanticModelSourceUpdate PRESERVE =
      new SemanticModelSourceUpdate(false, List.of());
  private final boolean replace;
  private final List<SemanticModelSource> sources;

  private SemanticModelSourceUpdate(boolean replace, List<SemanticModelSource> sources) {
    this.replace = replace;
    this.sources = List.copyOf(sources);
  }

  /**
   * @return An instruction to preserve existing associations during metadata-only updates.
   */
  public static SemanticModelSourceUpdate preserve() {
    return PRESERVE;
  }

  /**
   * Creates an instruction to replace the entire association set.
   *
   * @param sources All resolved datasets of the new definition.
   * @return The immutable replacement instruction.
   * @throws IllegalArgumentException If sources is null or contains duplicate dataset names.
   */
  public static SemanticModelSourceUpdate replace(List<SemanticModelSource> sources) {
    Preconditions.checkArgument(sources != null, "sources must not be null");
    Set<String> names = new HashSet<>();
    sources.forEach(
        source ->
            Preconditions.checkArgument(
                names.add(source.getDatasetName()),
                "Duplicate dataset association: %s",
                source.getDatasetName()));
    return new SemanticModelSourceUpdate(true, sources);
  }

  /**
   * @return Whether this write replaces the associations, even for an identical definition.
   */
  public boolean replacesSources() {
    return replace;
  }

  /**
   * @return The complete immutable replacement set, or an empty list for preserve.
   */
  public List<SemanticModelSource> sources() {
    return sources;
  }

  /**
   * Checks that a replacement describes exactly the datasets being committed.
   *
   * @param definition The definition written in the same transaction.
   */
  public void validate(SemanticModelDefinition definition) {
    if (replace) {
      Set<String> datasets =
          Arrays.stream(definition.datasets()).map(Dataset::name).collect(Collectors.toSet());
      Preconditions.checkArgument(
          datasets.size() == definition.datasets().length, "Duplicate dataset names in definition");
      Set<String> associations =
          sources.stream().map(SemanticModelSource::getDatasetName).collect(Collectors.toSet());
      Preconditions.checkArgument(
          datasets.equals(associations),
          "Source associations must match the definition's dataset names");
    }
  }
}
