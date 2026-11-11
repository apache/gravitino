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

import org.apache.gravitino.annotation.Evolving;
import org.apache.gravitino.exceptions.NoSuchMetadataObjectException;
import org.apache.gravitino.exceptions.NoSuchTermException;

/**
 * Interface for listing and binding the glossary terms of the metrics of a semantic model. A metric
 * uses the bound terms ({@link TermBinding.Kind#USED_BY}). A semantic model itself has no terms;
 * only its metrics do.
 *
 * <p>Metrics are bound by name. A renamed metric loses its bindings, and the bindings of a removed
 * metric are removed.
 */
@Evolving
public interface SupportsMetricTerms {

  /**
   * Lists the names of the terms used by a metric that are visible to the caller.
   *
   * @param metric The name of the metric.
   * @return The names of the terms.
   * @throws NoSuchMetadataObjectException If the semantic model has no metric with this name.
   */
  String[] listMetricTerms(String metric) throws NoSuchMetadataObjectException;

  /**
   * Lists the terms used by a metric that are visible to the caller, with details.
   *
   * @param metric The name of the metric.
   * @return The terms.
   * @throws NoSuchMetadataObjectException If the semantic model has no metric with this name.
   */
  GlossaryTerm[] listMetricTermsInfo(String metric) throws NoSuchMetadataObjectException;

  /**
   * Binds terms to a metric and unbinds others, in one change. Binding a bound term, or unbinding a
   * term that is not bound, has no effect.
   *
   * @param metric The name of the metric.
   * @param termsToAdd The names of the terms to bind.
   * @param termsToRemove The names of the terms to unbind.
   * @return The names of the terms bound to the metric after the change.
   * @throws NoSuchMetadataObjectException If the semantic model has no metric with this name.
   * @throws NoSuchTermException If a term does not exist or is not visible to the caller.
   * @throws IllegalArgumentException If a term cannot be bound by the caller.
   */
  String[] associateMetricTerms(String metric, String[] termsToAdd, String[] termsToRemove)
      throws NoSuchMetadataObjectException, NoSuchTermException, IllegalArgumentException;
}
