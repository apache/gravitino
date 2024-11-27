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
package org.apache.gravitino.connector.authorization;

import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.authorization.Privilege;
import org.apache.gravitino.authorization.SecurableObject;

/**
 * Authorization use this provider to mapping Gravitino privilege to the underlying data source
 * privileges. We can use this it to support the different data source, such as Hive, HDFS, HBase,
 * etc.
 */
public interface AuthorizationPrivilegesMappingProvider {
  /**
   * Set the mapping Gravitino privilege name to the underlying data source privileges rule.
   *
   * @return The mapping Gravitino privilege name to the underlying data source privileges rule.
   */
  Map<Privilege.Name, Set<AuthorizationPrivilege>> privilegesMappingRule();

  /**
   * Set the owner underlying data source privileges rule.
   *
   * @return The owner underlying data source privileges rule.
   */
  Set<AuthorizationPrivilege> ownerMappingRule();

  /**
   * Allow Gravitino privilege operation defines rule.
   *
   * @return The allow Gravitino privilege operation defines rule.
   */
  Set<Privilege.Name> allowPrivilegesRule();

  /**
   * Allow Gravitino MetadataObject type defines rule.
   *
   * @return To allow Gravitino MetadataObject type defines rule.
   */
  Set<MetadataObject.Type> allowMetadataObjectTypesRule();

  /**
   * Translate the Gravitino securable object to the underlying data source securable object.
   *
   * @param securableObject The Gravitino securable object.
   * @return The underlying data source securable object list.
   */
  List<AuthorizationSecurableObject> translatePrivilege(SecurableObject securableObject);

  /**
   * Translate the Gravitino securable object to the underlying data source owner securable object.
   *
   * @param metadataObject The Gravitino metadata object.
   * @return The underlying data source owner securable object list.
   */
  List<AuthorizationSecurableObject> translateOwner(MetadataObject metadataObject);

  /**
   * Translate the Gravitino metadata object to the underlying data source metadata object.
   *
   * @param metadataObject The Gravitino metadata object.
   * @return The underlying data source metadata object.
   */
  AuthorizationMetadataObject translateMetadataObject(MetadataObject metadataObject);
}
