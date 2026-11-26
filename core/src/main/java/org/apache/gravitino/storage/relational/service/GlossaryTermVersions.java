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

package org.apache.gravitino.storage.relational.service;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;
import javax.annotation.Nullable;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.glossary.TermPublication;
import org.apache.gravitino.glossary.TermPublication.State;
import org.apache.gravitino.json.JsonUtils;
import org.apache.gravitino.meta.GlossaryTermEntity;
import org.apache.gravitino.meta.GlossaryTermNames;
import org.apache.gravitino.storage.relational.mapper.DomainMetaMapper;
import org.apache.gravitino.storage.relational.mapper.GlossaryTermVersionMapper;
import org.apache.gravitino.storage.relational.mapper.GlossaryTermVersionMapper.Head;
import org.apache.gravitino.storage.relational.mapper.GlossaryTermVersionMapper.Version;
import org.apache.gravitino.storage.relational.po.DomainPO;
import org.apache.gravitino.storage.relational.po.GlossaryTermPO;
import org.apache.gravitino.storage.relational.utils.SessionUtils;

/** Operations on immutable business versions within the glossary manager's term transaction. */
public final class GlossaryTermVersions {
  private GlossaryTermVersions() {}

  /** Definition, audit, direct broader/related edges and domain membership at write time. */
  public record Snapshot(
      GlossaryTermPO definition,
      List<String> synonyms,
      Map<Long, String> domains,
      Set<Long> broader,
      Set<Long> related,
      Set<Long> baseRelated,
      Map<Long, String> referenceNames,
      @Nullable Long creatorUserId) {
    /** Older snapshots may omit the display names of relation targets. */
    public Snapshot {
      referenceNames = referenceNames == null ? Map.of() : Map.copyOf(referenceNames);
    }

    /**
     * Keeps the submitted identities and display names so later edits can remove retired targets.
     */
    public Snapshot withCandidateReferences(
        GlossaryTermEntity term,
        Map<Long, String> previousNames,
        Set<Long> originalRelated,
        @Nullable Long creatorId) {
      Set<Long> ids = new HashSet<>(broader);
      ids.addAll(related);
      Map<Long, String> names = new HashMap<>(previousNames);
      names.keySet().retainAll(ids);
      for (GlossaryTermEntity endpoint :
          GlossaryTermMetaService.getInstance()
              .listTermsByIds(term.namespace(), definition.getMetalakeId(), ids)) {
        names.put(endpoint.id(), endpoint.name());
      }
      for (Set<Long> members : List.of(broader, related)) {
        Set<String> distinctNames = new HashSet<>();
        for (long id : members) {
          String name = names.get(id);
          if (name != null && !distinctNames.add(GlossaryTermNames.key(name))) {
            throw new IllegalArgumentException(
                "Remove the retired relationship before adding its same-name replacement");
          }
        }
      }
      return new Snapshot(
          definition, synonyms, domains, broader, related, originalRelated, names, creatorId);
    }

    /** Frozen domain identities used to revalidate membership on publication. */
    public Set<Long> domainIds() {
      return domains.keySet();
    }

    /** Filters frozen names using access to the same domain identity at read time. */
    public String[] visibleDomainNames(Predicate<String> readable) {
      return loadDomainsById(domainIds()).stream()
          .filter(domain -> readable.test(domain.domainName()))
          .map(domain -> domains.get(domain.domainId()))
          .sorted()
          .toArray(String[]::new);
    }

    /** Materializes the definition without consulting current term or relationship rows. */
    public GlossaryTermEntity term(Namespace ns) {
      return GlossaryTermMetaService.getInstance()
          .fromPO(definition, synonyms, List.copyOf(domains.values()), ns);
    }
  }

  /** Freezes the proposed definition and its final relationships in one transaction. */
  public static Snapshot captureSnapshot(
      long lake, GlossaryTermEntity term, Set<Long> domains, Set<Long> broader, Set<Long> related) {
    Map<Long, String> names = new HashMap<>();
    for (DomainPO domain : loadDomainsById(domains)) {
      names.put(domain.domainId(), domain.domainName());
    }
    return new Snapshot(
        GlossaryTermMetaService.getInstance().toPO(term, lake),
        List.of(term.synonyms()),
        names,
        broader,
        related,
        Set.of(),
        Map.of(),
        null);
  }

  /** Decodes the exact version previously recorded. */
  public static Snapshot decodeSnapshot(Version version) {
    try {
      return JsonUtils.anyFieldMapper().readValue(version.getSnapshot(), Snapshot.class);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  /** Appends a new snapshot, superseding any outstanding candidate. Caller holds the term lock. */
  public static TermPublication recordTermVersion(
      long lake, Snapshot snapshot, long baseRevision, State state, @Nullable String reservation) {
    return SessionUtils.getWithoutCommit(
        GlossaryTermVersionMapper.class,
        mapper -> {
          long id = snapshot.definition().getTermId();
          Head head = mapper.getVersionHeadByMetalakeAndTermId(lake, id);
          if (head == null) {
            mapper.insertVersionHead(lake, id, reservation);
            head = mapper.getVersionHeadByMetalakeAndTermId(lake, id);
          }
          if (head.getCandidateVersion() != null) {
            mapper.updateVersionLifecycle(id, head.getCandidateVersion(), State.SUPERSEDED.name());
          }
          long next = Math.incrementExact(head.getLastVersion());
          Version row = new Version();
          row.setVersionId(next);
          row.setLifecycle(state.name());
          row.setBaseRevision(baseRevision);
          try {
            row.setSnapshot(JsonUtils.anyFieldMapper().writeValueAsString(snapshot));
          } catch (IOException e) {
            throw new UncheckedIOException(e);
          }
          mapper.insertTermVersion(id, row);
          head.setLastVersion(next);
          if (state == State.PUBLISHED) {
            head.setPublishedVersion(next);
            head.setCandidateVersion(null);
            head.setReservedName(null);
          } else {
            head.setCandidateVersion(next);
            head.setReservedName(reservation);
          }
          mapper.updateVersionHead(head);
          return toPublicationResult(head, row);
        });
  }

  /** Identifies the requested snapshot independently of the current pointers. */
  public static TermPublication toPublicationResult(Head head, Version row) {
    return new TermPublication(
        head.getTermId(),
        row.getVersionId(),
        State.valueOf(row.getLifecycle()),
        head.getPublishedVersion(),
        head.getCandidateVersion());
  }

  private static List<DomainPO> loadDomainsById(Set<Long> ids) {
    if (ids.isEmpty()) return List.of();
    return SessionUtils.getWithoutCommit(
        DomainMetaMapper.class, mapper -> mapper.listDomainPOsByDomainIds(List.copyOf(ids)));
  }
}
