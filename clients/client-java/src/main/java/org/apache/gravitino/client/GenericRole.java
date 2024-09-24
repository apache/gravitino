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

import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import org.apache.gravitino.Audit;
import org.apache.gravitino.authorization.Role;
import org.apache.gravitino.authorization.SecurableObject;
import org.apache.gravitino.dto.authorization.RoleDTO;

/** Represents a generic role. This class is used for lazily requesting securable objects. */
class GenericRole implements Role {

  private final RoleDTO roleDTO;
  private final Supplier<List<SecurableObject>> securableObjectsSupplier;

  GenericRole(RoleDTO roleDTO, GravitinoMetalake gravitinoMetalake) {
    this.roleDTO = roleDTO;
    this.securableObjectsSupplier =
        new Supplier<List<SecurableObject>>() {
          private boolean waitForRequest = true;
          private List<SecurableObject> securableObjects;

          @Override
          public List<SecurableObject> get() {
            synchronized (this) {
              if (waitForRequest) {
                securableObjects = gravitinoMetalake.getRole(roleDTO.name()).securableObjects();
                waitForRequest = false;
              }
            }

            return securableObjects;
          }
        };
  }

  @Override
  public Audit auditInfo() {
    return roleDTO.auditInfo();
  }

  @Override
  public String name() {
    return roleDTO.name();
  }

  @Override
  public Map<String, String> properties() {
    return roleDTO.properties();
  }

  @Override
  public List<SecurableObject> securableObjects() {
    return securableObjectsSupplier.get();
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (!(obj instanceof GenericRole)) {
      return false;
    }

    GenericRole that = (GenericRole) obj;
    return roleDTO.equals(that.roleDTO);
  }

  @Override
  public int hashCode() {
    return roleDTO.hashCode();
  }
}
