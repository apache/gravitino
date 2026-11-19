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
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.gravitino.storage.relational.po.GlossaryTermRelPO;
import org.apache.gravitino.storage.relational.service.GlossaryTermRelService;
import org.apache.gravitino.storage.relational.service.GlossaryTermRelService.RelationType;

/** The net changes to one term's edges, calculated under the hierarchy mutex. */
final class TermRelations {
  record Edit(RelationType type, String target, boolean add) {
    @Nullable
    static Edit from(TermChange change) {
      if (change instanceof TermChange.AddBroader c) {
        return new Edit(RelationType.BROADER, c.getBroader(), true);
      }
      if (change instanceof TermChange.RemoveBroader c) {
        return new Edit(RelationType.BROADER, c.getBroader(), false);
      }
      if (change instanceof TermChange.AddRelated c) {
        return new Edit(RelationType.RELATED, c.getRelated(), true);
      }
      if (change instanceof TermChange.RemoveRelated c) {
        return new Edit(RelationType.RELATED, c.getRelated(), false);
      }
      return null;
    }
  }

  private static final GlossaryTermRelService EDGES = GlossaryTermRelService.getInstance();
  private final long lake;
  private final long root;
  private final Set<Long> oldParents;
  private final Set<Long> oldRelated;
  private final Set<Long> parents;
  private final Set<Long> related;

  TermRelations(long lake, long root, boolean creating) {
    this.lake = lake;
    this.root = root;
    oldParents = creating ? Set.of() : EDGES.listBroader(lake, root);
    oldRelated = creating ? Set.of() : EDGES.listRelated(lake, root);
    parents = new LinkedHashSet<>(oldParents);
    related = new LinkedHashSet<>(oldRelated);
  }

  void edit(Edit edit, long target) {
    Preconditions.checkArgument(root != target, "A term cannot reference itself");
    Set<Long> targets = edit.type == RelationType.BROADER ? parents : related;
    if (edit.add) {
      targets.add(target);
    } else {
      targets.remove(target);
    }
  }

  boolean changed() {
    return !parents.equals(oldParents) || !related.equals(oldRelated);
  }

  void validate(int limit) {
    if (oldParents.containsAll(parents) && oldRelated.containsAll(related)) {
      return;
    }
    // Only paths through root can become longer, and only pairs spanning its two sides
    // can acquire a new ancestor relationship. Read the final outgoing edges from the plan.
    Map<Long, Set<Long>> above = graph(true);
    Map<Long, Set<Long>> below = graph(false);
    Preconditions.checkArgument(
        depth(root, above, new HashMap<>(), limit) + depth(root, below, new HashMap<>(), limit)
            <= limit,
        "Glossary hierarchy exceeds the maximum depth of %s links",
        limit);
    for (long id : related) {
      Preconditions.checkArgument(
          !above.containsKey(id) && !below.containsKey(id),
          "Related terms cannot be ancestors or descendants");
    }
    if (!oldParents.containsAll(parents)) {
      for (GlossaryTermRelPO edge : EDGES.listRelatedEdges(lake, below.keySet())) {
        long a = edge.getFromTermId(), b = edge.getToTermId();
        if (a == root || b == root) {
          continue; // Root's final RELATED set was checked above.
        }
        Preconditions.checkArgument(
            !(below.containsKey(a) && above.containsKey(b)
                || below.containsKey(b) && above.containsKey(a)),
            "Broader change would make related terms ancestors");
      }
    }
  }

  private Map<Long, Set<Long>> graph(boolean upward) {
    Map<Long, Set<Long>> graph = new HashMap<>();
    Set<Long> frontier = Set.of(root);
    while (!frontier.isEmpty()) {
      frontier.forEach(id -> graph.put(id, new HashSet<>()));
      if (upward && frontier.contains(root)) {
        graph.get(root).addAll(parents);
      } else {
        for (GlossaryTermRelPO edge :
            upward
                ? EDGES.listBroaderEdges(lake, frontier)
                : EDGES.listNarrowerEdges(lake, frontier)) {
          long from = upward ? edge.getFromTermId() : edge.getToTermId();
          long to = upward ? edge.getToTermId() : edge.getFromTermId();
          // Incoming walks can encounter an old outgoing edge of root which this edit removed.
          if (!upward && to == root && !parents.contains(from)) {
            continue;
          }
          Preconditions.checkArgument(to != root, "Glossary hierarchy contains a cycle");
          graph.get(from).add(to);
        }
      }
      Set<Long> next = new HashSet<>();
      for (long id : frontier) {
        next.addAll(graph.get(id));
      }
      next.removeAll(graph.keySet());
      frontier = next;
    }
    return graph;
  }

  private static int depth(
      long id, Map<Long, Set<Long>> graph, Map<Long, Integer> known, int remaining) {
    Preconditions.checkArgument(remaining >= 0, "Glossary hierarchy exceeds the maximum depth");
    Integer cached = known.get(id);
    if (cached != null) {
      Preconditions.checkArgument(cached >= 0, "Glossary hierarchy contains a cycle");
      return cached;
    }
    known.put(id, -1);
    int result = 0;
    for (long next : graph.get(id)) {
      result = Math.max(result, 1 + depth(next, graph, known, remaining - 1));
    }
    known.put(id, result);
    return result;
  }

  void persist() {
    persist(RelationType.BROADER, oldParents, parents);
    persist(RelationType.RELATED, oldRelated, related);
  }

  private void persist(RelationType type, Set<Long> before, Set<Long> after) {
    for (long id : before) {
      if (!after.contains(id)) {
        EDGES.remove(lake, root, id, type);
      }
    }
    for (long id : after) {
      if (!before.contains(id)) {
        EDGES.add(lake, root, id, type);
      }
    }
  }
}
