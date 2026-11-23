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

package org.apache.gravitino.glossary;

import com.google.common.base.Preconditions;
import java.net.URI;
import java.time.Instant;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import javax.annotation.Nullable;
import org.apache.gravitino.MetadataObjects;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.GlossaryTermEntity;
import org.apache.gravitino.meta.GlossaryTermNames;
import org.apache.gravitino.utils.PrincipalUtils;

/** Computes the final definition for a whole request before any rows are written. */
final class TermEdits {
  private TermEdits() {}

  static GlossaryTermEntity create(long id, Namespace namespace, TermSpec spec) {
    Preconditions.checkArgument(spec != null, "Term spec is required");
    String[] synonyms = spec.synonyms();
    Preconditions.checkArgument(synonyms.length <= 100, "At most 100 synonyms are allowed");
    Map<String, String> distinct = synonymsByKey(synonyms);
    return GlossaryTermEntity.builder()
        .withId(id)
        .withNamespace(namespace)
        .withName(termName(spec.name()))
        .withDefinition(spec.definition())
        .withSynonyms(distinct.values().toArray(new String[0]))
        .withStatus(spec.status().orElse(GlossaryTerm.Status.APPROVED))
        .withLinks(validateLinks(spec.links()))
        .withProperties(spec.properties())
        .withSourceRef(spec.sourceRef().orElse(null))
        .withAuditInfo(
            AuditInfo.builder()
                .withCreator(PrincipalUtils.getCurrentUserName())
                .withCreateTime(Instant.now())
                .build())
        .build();
  }

  /** Returns a copy of a not yet persisted term with the given domain names. */
  static GlossaryTermEntity withDomains(GlossaryTermEntity term, String[] domains) {
    return GlossaryTermEntity.builder()
        .withId(term.id())
        .withNamespace(term.namespace())
        .withName(term.name())
        .withDefinition(term.definition())
        .withSynonyms(term.synonyms())
        .withStatus(term.status())
        .withLinks(term.links())
        .withProperties(term.properties())
        .withSourceRef(term.sourceRef())
        .withAuditInfo(term.auditInfo())
        .withVersion(term.version())
        .withDomains(domains)
        .build();
  }

  static boolean needsHierarchyLock(TermChange[] changes) {
    return Arrays.stream(changes)
        .anyMatch(
            c ->
                c instanceof TermChange.RenameTerm
                    || c instanceof TermChange.AddSynonym
                    || c instanceof TermChange.RemoveSynonym);
  }

  static GlossaryTermEntity apply(
      GlossaryTermEntity current, TermChange[] changes, boolean relationsChanged) {
    return apply(current, changes, relationsChanged, null);
  }

  /**
   * Applies content changes. A non-null domains array is the term's new membership, which counts as
   * a change; null keeps the current domains.
   */
  static GlossaryTermEntity apply(
      GlossaryTermEntity current,
      TermChange[] changes,
      boolean relationsChanged,
      @Nullable String[] domains) {
    String name = current.name();
    String definition = current.definition();
    GlossaryTerm.Status status = current.status();
    TermLink[] links = current.links();
    String sourceRef = current.sourceRef();
    Map<String, String> properties = new LinkedHashMap<>(current.properties());
    Map<String, String> originalSynonyms = synonymsByKey(current.synonyms());
    Map<String, String> synonyms = new LinkedHashMap<>(originalSynonyms);
    for (TermChange change : changes) {
      if (change instanceof TermChange.RenameTerm) {
        name = termName(((TermChange.RenameTerm) change).getNewName());
      } else if (change instanceof TermChange.UpdateDefinition) {
        definition = ((TermChange.UpdateDefinition) change).getNewDefinition();
        GlossaryTermEntity.validateDefinition(definition);
      } else if (change instanceof TermChange.SetStatus) {
        status = ((TermChange.SetStatus) change).getNewStatus();
      } else if (change instanceof TermChange.UpdateLinks) {
        links = validateLinks(((TermChange.UpdateLinks) change).getLinks());
      } else if (change instanceof TermChange.SetSourceRef) {
        sourceRef = ((TermChange.SetSourceRef) change).getSourceRef();
        if (sourceRef != null) {
          GlossaryTermNames.validate(sourceRef);
        }
      } else if (change instanceof TermChange.AddSynonym) {
        addSynonym(synonyms, ((TermChange.AddSynonym) change).getSynonym());
      } else if (change instanceof TermChange.RemoveSynonym) {
        String synonym = ((TermChange.RemoveSynonym) change).getSynonym();
        GlossaryTermNames.validate(GlossaryTermNames.display(synonym));
        synonyms.remove(GlossaryTermNames.key(synonym));
      } else if (change instanceof TermChange.SetProperty) {
        TermChange.SetProperty property = (TermChange.SetProperty) change;
        properties.put(property.getProperty(), property.getValue());
      } else if (change instanceof TermChange.RemoveProperty) {
        properties.remove(((TermChange.RemoveProperty) change).getProperty());
      } else {
        throw new IllegalArgumentException("Unsupported glossary change: " + change);
      }
    }
    if (!relationsChanged
        && domains == null
        && name.equals(current.name())
        && Objects.equals(definition, current.definition())
        && status == current.status()
        && Arrays.equals(links, current.links())
        && Objects.equals(sourceRef, current.sourceRef())
        && properties.equals(current.properties())
        && originalSynonyms.keySet().equals(synonyms.keySet())) {
      return current;
    }
    // Removing and re-adding the same key preserves its spelling even if another field changed.
    originalSynonyms.forEach(synonyms::replace);
    return GlossaryTermEntity.builder()
        .withNamespace(current.namespace())
        .withId(current.id())
        .withName(name)
        .withDefinition(definition)
        .withSynonyms(synonyms.values().toArray(new String[0]))
        .withStatus(status)
        .withLinks(links)
        .withProperties(properties)
        .withSourceRef(sourceRef)
        .withDomains(domains == null ? current.domains() : domains)
        .withAuditInfo(
            AuditInfo.builder()
                .withCreator(current.auditInfo().creator())
                .withCreateTime(current.auditInfo().createTime())
                .withLastModifier(PrincipalUtils.getCurrentUserName())
                .withLastModifiedTime(Instant.now())
                .build())
        .withVersion(Math.addExact(current.version(), 1))
        .build();
  }

  private static String termName(String value) {
    String display = GlossaryTermNames.display(value);
    GlossaryTermNames.validate(display);
    Preconditions.checkArgument(
        !MetadataObjects.METADATA_OBJECT_RESERVED_NAME.equals(display),
        "A glossary term cannot use the reserved metadata object name '*'");
    Preconditions.checkArgument(
        !".".equals(display) && !"..".equals(display),
        "A glossary term cannot use the reserved path segment '%s'",
        display);
    return display;
  }

  private static Map<String, String> synonymsByKey(String[] values) {
    Map<String, String> synonyms = new LinkedHashMap<>();
    for (String value : values) {
      addSynonym(synonyms, value);
    }
    return synonyms;
  }

  private static void addSynonym(Map<String, String> synonyms, String value) {
    String display = GlossaryTermNames.display(value);
    GlossaryTermNames.validate(display);
    synonyms.putIfAbsent(GlossaryTermNames.key(display), display);
  }

  private static TermLink[] validateLinks(TermLink[] links) {
    for (TermLink link : links) {
      URI uri = URI.create(link.url());
      Preconditions.checkArgument(
          ("https".equalsIgnoreCase(uri.getScheme()) || "http".equalsIgnoreCase(uri.getScheme()))
              && uri.getHost() != null,
          "Documentation links require an absolute HTTP or HTTPS URL with a host");
    }
    return links;
  }
}
