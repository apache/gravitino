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
import org.apache.gravitino.Audit;
import org.apache.gravitino.authorization.SupportsRoles;
import org.apache.gravitino.dto.model.ModelDTO;
import org.apache.gravitino.model.Model;
import org.apache.gravitino.tag.SupportsTags;

/** Represents a generic model. */
class GenericModel implements Model {

  private final ModelDTO modelDTO;

  GenericModel(ModelDTO modelDTO) {
    this.modelDTO = modelDTO;
  }

  @Override
  public Audit auditInfo() {
    return modelDTO.auditInfo();
  }

  @Override
  public String name() {
    return modelDTO.name();
  }

  @Override
  public String comment() {
    return modelDTO.comment();
  }

  @Override
  public Map<String, String> properties() {
    return modelDTO.properties();
  }

  @Override
  public int latestVersion() {
    return modelDTO.latestVersion();
  }

  @Override
  public SupportsTags supportsTags() {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public SupportsRoles supportsRoles() {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof GenericModel)) {
      return false;
    }

    GenericModel that = (GenericModel) o;
    return modelDTO.equals(that.modelDTO);
  }

  @Override
  public int hashCode() {
    return modelDTO.hashCode();
  }
}
