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
package org.apache.gravitino.storage.relational.converters;

import java.io.IOException;
import java.sql.SQLException;
import org.apache.gravitino.Entity;
import org.apache.gravitino.EntityAlreadyExistsException;

/**
 * Exception converter to Apache Gravitino exception for H2. The definition of error codes can be
 * found in the document: <a href="https://h2database.com/javadoc/org/h2/api/ErrorCode.html"></a>
 */
public class H2ExceptionConverter implements SQLExceptionConverter {
  /** It means found a duplicated primary key or unique key entry in H2. */
  private static final int DUPLICATED_ENTRY_ERROR_CODE = 23505;

  @SuppressWarnings("FormatStringAnnotation")
  @Override
  public void toGravitinoException(SQLException se, Entity.EntityType type, String name)
      throws IOException {
    switch (se.getErrorCode()) {
      case DUPLICATED_ENTRY_ERROR_CODE:
        throw new EntityAlreadyExistsException(se, se.getMessage());
      default:
        throw new IOException(se);
    }
  }
}
