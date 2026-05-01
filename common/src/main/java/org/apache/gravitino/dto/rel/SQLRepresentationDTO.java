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

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import org.apache.gravitino.rel.Representation;
import org.apache.gravitino.rel.SQLRepresentation;

/** DTO for SQL representation. */
@Getter
@EqualsAndHashCode(callSuper = true)
public class SQLRepresentationDTO extends RepresentationDTO {

  @JsonProperty("dialect")
  private String dialect;

  @JsonProperty("sql")
  private String sql;

  private SQLRepresentationDTO() {}

  /**
   * Creates a SQL representation DTO.
   *
   * @param dialect SQL dialect.
   * @param sql SQL body.
   */
  public SQLRepresentationDTO(String dialect, String sql) {
    this.dialect = dialect;
    this.sql = sql;
  }

  @Override
  public String type() {
    return Representation.TYPE_SQL;
  }

  @Override
  public SQLRepresentation toRepresentation() {
    return SQLRepresentation.builder().withDialect(dialect).withSql(sql).build();
  }

  /**
   * Creates SQL representation DTO from SQL representation.
   *
   * @param representation SQL representation.
   * @return SQL representation DTO.
   */
  public static SQLRepresentationDTO fromSQLRepresentation(SQLRepresentation representation) {
    return new SQLRepresentationDTO(representation.dialect(), representation.sql());
  }
}
