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
package org.apache.gravitino.dto.rel;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import org.apache.gravitino.rel.Representation;
import org.apache.gravitino.rel.SQLRepresentation;

/** DTO for view representation serde. */
@JsonTypeInfo(
    use = JsonTypeInfo.Id.NAME,
    include = JsonTypeInfo.As.PROPERTY,
    property = "type",
    defaultImpl = SQLRepresentationDTO.class)
@JsonSubTypes({
  @JsonSubTypes.Type(value = SQLRepresentationDTO.class, name = Representation.TYPE_SQL)
})
public abstract class RepresentationDTO implements Representation {

  /** Constructor for Jackson. */
  protected RepresentationDTO() {}

  /**
   * Converts this DTO to a representation object.
   *
   * @return The representation object.
   */
  public abstract Representation toRepresentation();

  /**
   * Creates a representation DTO from a representation object.
   *
   * @param representation The representation object.
   * @return The representation DTO.
   */
  public static RepresentationDTO fromRepresentation(Representation representation) {
    if (representation instanceof SQLRepresentation) {
      return SQLRepresentationDTO.fromSQLRepresentation((SQLRepresentation) representation);
    }

    throw new IllegalArgumentException(
        "Unsupported representation type: " + representation.getClass().getName());
  }
}
