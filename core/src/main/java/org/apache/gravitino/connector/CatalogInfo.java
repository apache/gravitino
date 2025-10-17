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
package org.apache.gravitino.connector;

import java.util.Map;
import org.apache.gravitino.Audit;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.annotation.Evolving;

/**
 * CatalogInfo exposes all the information about a catalog to the connector interface. This class is
 * corresponding to CatalogEntity that is used internally.
 *
 * <p>This class object will be passed in through {@link CatalogOperations#initialize(Map,
 * CatalogInfo, HasPropertyMetadata)}, users can leverage this object to get the information about
 * the catalog.
 */
@Evolving
public final class CatalogInfo implements Catalog {

  private final Long id;

  private final String name;

  private final Catalog.Type type;

  private final String provider;

  private final String comment;

  private final Map<String, String> properties;

  private final Audit auditInfo;

  private final Namespace namespace;

  public CatalogInfo(
      Long id,
      String name,
      Catalog.Type type,
      String provider,
      String comment,
      Map<String, String> properties,
      Audit auditInfo,
      Namespace namespace) {
    this.id = id;
    this.name = name;
    this.type = type;
    this.provider = provider;
    this.comment = comment;
    this.properties = properties;
    this.auditInfo = auditInfo;
    this.namespace = namespace;
  }

  /**
   * @return The unique id of the catalog.
   */
  public Long id() {
    return id;
  }

  /**
   * @return The name of the catalog.
   */
  @Override
  public String name() {
    return name;
  }

  /**
   * @return The type of the catalog.
   */
  @Override
  public Catalog.Type type() {
    return type;
  }

  /**
   * @return The provider of the catalog.
   */
  @Override
  public String provider() {
    return provider;
  }

  /**
   * @return The comment or description for the catalog.
   */
  @Override
  public String comment() {
    return comment;
  }

  /**
   * @return The associated properties of the catalog.
   */
  @Override
  public Map<String, String> properties() {
    return properties;
  }

  /**
   * @return The audit details of the catalog.
   */
  @Override
  public Audit auditInfo() {
    return auditInfo;
  }

  /**
   * @return The namespace of the catalog.
   */
  public Namespace namespace() {
    return namespace;
  }
}
