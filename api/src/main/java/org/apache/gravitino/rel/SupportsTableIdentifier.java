/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// Referred from Apache's connector/catalog implementation
// sql/catalyst/src/main/java/org/apache/spark/sql/connector/catalog/TableCatalog.java

package org.apache.gravitino.rel;

import javax.annotation.Nullable;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.annotation.Evolving;
import org.apache.gravitino.exceptions.NoSuchSchemaException;
import org.apache.gravitino.exceptions.NoSuchTableException;

/**
 * Optional table operations that resolve source identity without loading columns or constraints.
 */
@Evolving
public interface SupportsTableIdentifier extends TableCatalog {
  /**
   * Checks the live source table and reads its Gravitino string identifier property.
   *
   * <p>This must query the source, distinguish tables from views, and propagate source failures. A
   * missing identifier means an existing table without a Gravitino identifier, not a missing table.
   * Callers must still import unregistered tables through the ordinary load path.
   *
   * @param ident the table identifier
   * @return the source's Gravitino string identifier, or null if the existing table has none
   * @throws NoSuchTableException if the source table does not exist or is a view
   * @throws NoSuchSchemaException if the containing source schema does not exist
   */
  @Nullable
  String loadTableIdentifier(NameIdentifier ident)
      throws NoSuchTableException, NoSuchSchemaException;
}
