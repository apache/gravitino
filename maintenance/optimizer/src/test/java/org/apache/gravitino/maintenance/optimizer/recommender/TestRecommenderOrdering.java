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

package org.apache.gravitino.maintenance.optimizer.recommender;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.maintenance.optimizer.api.recommender.JobExecutionContext;
import org.apache.gravitino.maintenance.optimizer.api.recommender.StrategyEvaluation;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class TestRecommenderOrdering {

  @Test
  void testRecommendForOneStrategyOrdersByScoreDescending() {
    NameIdentifier tableA = NameIdentifier.of("db", "t1");
    NameIdentifier tableB = NameIdentifier.of("db", "t2");
    NameIdentifier tableC = NameIdentifier.of("db", "t3");

    Map<NameIdentifier, Long> scores = Map.of(tableA, 10L, tableB, 50L, tableC, 30L);

    List<JobExecutionContext> jobs = orderByScore(scores);

    Assertions.assertEquals(3, jobs.size(), "All tables should produce a job context");
    Assertions.assertEquals(
        tableB, jobs.get(0).nameIdentifier(), "Highest score should come first");
    Assertions.assertEquals(tableC, jobs.get(1).nameIdentifier(), "Second highest score expected");
    Assertions.assertEquals(tableA, jobs.get(2).nameIdentifier(), "Lowest score expected last");
  }

  private static List<JobExecutionContext> orderByScore(Map<NameIdentifier, Long> scores) {
    PriorityQueue<StrategyEvaluation> scoreQueue =
        new PriorityQueue<>((a, b) -> Long.compare(b.score(), a.score()));
    scores.forEach((identifier, score) -> scoreQueue.add(createEvaluation(identifier, score)));

    List<JobExecutionContext> ordered = new ArrayList<>(scoreQueue.size());
    while (!scoreQueue.isEmpty()) {
      ordered.add(scoreQueue.poll().jobExecutionContext());
    }
    return ordered;
  }

  private static StrategyEvaluation createEvaluation(NameIdentifier identifier, long score) {
    return new StrategyEvaluation() {
      @Override
      public long score() {
        return score;
      }

      @Override
      public JobExecutionContext jobExecutionContext() {
        return new JobExecutionContext() {
          @Override
          public NameIdentifier nameIdentifier() {
            return identifier;
          }

          @Override
          public Map<String, String> jobConfig() {
            return Map.of();
          }

          @Override
          public String jobTemplateName() {
            return "template";
          }
        };
      }
    };
  }
}
