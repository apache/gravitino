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
package org.apache.gravitino.storage.relational.mapper;

import java.sql.CallableStatement;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import org.apache.gravitino.storage.relational.po.auth.OperateType;
import org.apache.ibatis.type.BaseTypeHandler;
import org.apache.ibatis.type.JdbcType;
import org.apache.ibatis.type.MappedTypes;

/**
 * MyBatis type handler that maps {@link OperateType} to its stable numeric code stored in the
 * {@code entity_change_log.operate_type} column.
 */
@MappedTypes(OperateType.class)
public class OperateTypeTypeHandler extends BaseTypeHandler<OperateType> {

  @Override
  public void setNonNullParameter(
      PreparedStatement ps, int i, OperateType parameter, JdbcType jdbcType) throws SQLException {
    ps.setInt(i, parameter.getCode());
  }

  @Override
  public OperateType getNullableResult(ResultSet rs, String columnName) throws SQLException {
    int code = rs.getInt(columnName);
    return rs.wasNull() ? null : OperateType.fromCode(code);
  }

  @Override
  public OperateType getNullableResult(ResultSet rs, int columnIndex) throws SQLException {
    int code = rs.getInt(columnIndex);
    return rs.wasNull() ? null : OperateType.fromCode(code);
  }

  @Override
  public OperateType getNullableResult(CallableStatement cs, int columnIndex) throws SQLException {
    int code = cs.getInt(columnIndex);
    return cs.wasNull() ? null : OperateType.fromCode(code);
  }
}
