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
package org.apache.gravitino.client;

import java.util.Map;
import javax.annotation.Nullable;
import org.apache.gravitino.Audit;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.MetadataObjects;
import org.apache.gravitino.ontology.Ontology;
import org.apache.gravitino.ontology.OntologyDefinition;
import org.apache.gravitino.policy.SupportsPolicies;
import org.apache.gravitino.tag.SupportsTags;

/** Client-side ontology with its governance operations. */
final class GenericOntology implements Ontology {
  private final Ontology ontology;
  private final SupportsTags tags;
  private final SupportsPolicies policies;

  GenericOntology(Ontology ontology, String metalake, RESTClient client) {
    this.ontology = ontology;
    MetadataObject object = MetadataObjects.of(null, ontology.name(), MetadataObject.Type.ONTOLOGY);
    this.tags = new MetadataObjectTagOperations(metalake, object, client);
    this.policies = new MetadataObjectPolicyOperations(metalake, object, client);
  }

  /** {@inheritDoc} */
  @Override
  public String name() {
    return ontology.name();
  }

  /** {@inheritDoc} */
  @Override
  @Nullable
  public String comment() {
    return ontology.comment();
  }

  /** {@inheritDoc} */
  @Override
  public OntologyDefinition definition() {
    return ontology.definition();
  }

  /** {@inheritDoc} */
  @Override
  public Map<String, String> properties() {
    return ontology.properties();
  }

  /** {@inheritDoc} */
  @Override
  public Audit auditInfo() {
    return ontology.auditInfo();
  }

  /** {@inheritDoc} */
  @Override
  public SupportsTags supportsTags() {
    return tags;
  }

  /** {@inheritDoc} */
  @Override
  public SupportsPolicies supportsPolicies() {
    return policies;
  }
}
