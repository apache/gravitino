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

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import org.apache.gravitino.rel.Representation;
import org.apache.gravitino.rel.SQLRepresentation;

/**
 * A DTO mirroring {@link org.apache.gravitino.rel.Representation}. Representation DTOs are
 * serialized with a {@code type} discriminator so Jackson can deserialize into the correct subtype.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonTypeInfo(
    use = JsonTypeInfo.Id.NAME,
    include = JsonTypeInfo.As.EXISTING_PROPERTY,
    property = "type",
    visible = true,
    defaultImpl = SQLRepresentationDTO.class)
@JsonSubTypes({
  @JsonSubTypes.Type(value = SQLRepresentationDTO.class, name = Representation.TYPE_SQL)
})
public abstract class RepresentationDTO implements Representation {

  /** Constructor for Jackson. */
  protected RepresentationDTO() {}

  /**
   * Validates the fields of this representation.
   *
   * @throws IllegalArgumentException If the representation is invalid.
   */
  public abstract void validate() throws IllegalArgumentException;

  /**
   * Converts this DTO to a {@link Representation} domain object.
   *
   * @return The representation domain object.
   */
  public abstract Representation toRepresentation();

  /**
   * Creates a {@link RepresentationDTO} from a {@link Representation} domain object.
   *
   * @param representation The representation domain object.
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
