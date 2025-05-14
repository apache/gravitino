/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *  http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.gravitino.server.authorization;

import java.security.Principal;
import java.util.Objects;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.authorization.Privilege;

/** Mock GravitinoAuthorizer */
public class MockGravitinoAuthorizer implements GravitinoAuthorizer {

  @Override
  public void initialize() {}

  @Override
  public boolean authorize(
      Principal principal,
      String metalake,
      MetadataObject metadataObject,
      Privilege.Name privilege) {
    if (!("tester".equals(principal.getName()) && "testMetalake".equals(metalake))) {
      return false;
    }
    String name = metadataObject.name();
    MetadataObject.Type type = metadataObject.type();
    if (type == MetadataObject.Type.CATALOG
        && "testCatalog".equals(name)
        && privilege == Privilege.Name.USE_CATALOG) {
      return true;
    }
    if (type == MetadataObject.Type.SCHEMA
        && "testSchema".equals(name)
        && privilege == Privilege.Name.USE_SCHEMA) {
      return true;
    }
    return type == MetadataObject.Type.TABLE
        && "testTable".equals(name)
        && privilege == Privilege.Name.SELECT_TABLE;
  }

  @Override
  public boolean isOwner(Principal principal, String metalake, MetadataObject metadataObject) {
    if (!("tester".equals(principal.getName()) && "metalakeWithOwner".equals(metalake))) {
      return false;
    }
    return Objects.equals(metadataObject.type(), MetadataObject.Type.METALAKE)
        && Objects.equals("metalakeWithOwner", metadataObject.name());
  }

  @Override
  public void close() {}
}
