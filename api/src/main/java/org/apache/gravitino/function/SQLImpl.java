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
package org.apache.gravitino.function;

import com.google.common.base.Preconditions;
import java.util.Map;
import java.util.Objects;
import org.apache.commons.lang3.StringUtils;

/** SQL implementation with runtime and SQL body. */
public class SQLImpl extends FunctionImpl {
  private final String sql;

  SQLImpl(
      RuntimeType runtime,
      String sql,
      FunctionResources resources,
      Map<String, String> properties) {
    super(Language.SQL, runtime, resources, properties);
    Preconditions.checkArgument(StringUtils.isNotBlank(sql), "SQL text cannot be null or empty");
    this.sql = sql;
  }

  /**
   * @return The SQL that defines the function.
   */
  public String sql() {
    return sql;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (!(obj instanceof SQLImpl)) {
      return false;
    }
    SQLImpl that = (SQLImpl) obj;
    return Objects.equals(language(), that.language())
        && Objects.equals(runtime(), that.runtime())
        && Objects.equals(resources(), that.resources())
        && Objects.equals(properties(), that.properties())
        && Objects.equals(sql, that.sql);
  }

  @Override
  public int hashCode() {
    return Objects.hash(language(), runtime(), resources(), properties(), sql);
  }

  @Override
  public String toString() {
    return "SQLImpl{"
        + "language='"
        + language()
        + '\''
        + ", runtime='"
        + runtime()
        + '\''
        + ", sql='"
        + sql
        + '\''
        + ", resources="
        + resources()
        + ", properties="
        + properties()
        + '}';
  }
}
