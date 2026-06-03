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

package org.apache.gravitino.spark.connector.catalog;

import org.apache.spark.sql.connector.catalog.Identifier;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestBaseCatalog {

  @Test
  void builtinDataSourceFormatsShouldBeRecognized() {
    String[] formats = {"parquet", "csv", "json", "orc", "text", "avro", "binaryFile"};
    for (String format : formats) {
      Identifier ident = Identifier.of(new String[] {format}, "s3a://bucket/file");
      Assertions.assertTrue(
          BaseCatalog.isBuiltinDataSourceReference(ident),
          "Expected " + format + " to be recognized as a built-in DataSource format");
    }
  }

  @Test
  void builtinDataSourceFormatsAreCaseInsensitive() {
    Identifier upper = Identifier.of(new String[] {"PARQUET"}, "s3a://bucket/file.parquet");
    Identifier mixed = Identifier.of(new String[] {"BinaryFile"}, "s3a://bucket/folder");
    Assertions.assertTrue(BaseCatalog.isBuiltinDataSourceReference(upper));
    Assertions.assertTrue(BaseCatalog.isBuiltinDataSourceReference(mixed));
  }

  @Test
  void regularGravitinoTableNamespacesAreNotTreatedAsBuiltin() {
    // A Gravitino table reference typically has a schema-name namespace, not a format name.
    Identifier schemaIdent = Identifier.of(new String[] {"my_schema"}, "my_table");
    Assertions.assertFalse(BaseCatalog.isBuiltinDataSourceReference(schemaIdent));
  }

  @Test
  void multipartNamespaceIsNotTreatedAsBuiltin() {
    // A multipart namespace (e.g. catalog.schema) should not match — the guard targets Spark's
    // single-part "<format>" shortcut only.
    Identifier threePart = Identifier.of(new String[] {"parquet", "extra"}, "table");
    Assertions.assertFalse(BaseCatalog.isBuiltinDataSourceReference(threePart));
  }

  @Test
  void emptyNamespaceIsNotTreatedAsBuiltin() {
    Identifier noNamespace = Identifier.of(new String[] {}, "parquet");
    Assertions.assertFalse(BaseCatalog.isBuiltinDataSourceReference(noNamespace));
  }

  @Test
  void unknownFormatIsNotTreatedAsBuiltin() {
    Identifier unknown = Identifier.of(new String[] {"xml"}, "some_path");
    Assertions.assertFalse(BaseCatalog.isBuiltinDataSourceReference(unknown));
  }
}
