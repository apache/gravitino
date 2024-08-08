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
package org.apache.gravitino.authorization;

import java.util.List;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.annotation.Unstable;

/**
 * The securable object is the entity which access can be granted. Unless allowed by a grant, access
 * is denied. Apache Gravitino organizes the securable objects using tree structure. <br>
 * There are three fields in the securable object: parent, name, and type. <br>
 * The types include 6 kinds: CATALOG,SCHEMA,TABLE,FILESET,TOPIC and METALAKE. <br>
 * You can use the helper class `SecurableObjects` to create the securable object which you need.
 * <br>
 * You can use full name and type of the securable object in the RESTFUL API. <br>
 * For example, <br>
 * If you want to use a catalog named `catalog1`, you can use the code
 * `SecurableObjects.ofCatalog("catalog1")` to create the securable object, or you can use full name
 * `catalog1` and type `CATALOG` in the RESTFUL API. <br>
 * If you want to use a schema named `schema1` in the catalog named `catalog1`, you can use the code
 * `SecurableObjects.ofSchema(catalog, "schema1")` to create the securable object, or you can use
 * full name `catalog1.schema1` and type `SCHEMA` in the RESTFUL API. <br>
 * If you want to use a table named `table1` in the schema named `schema1`, you can use the code
 * `SecurableObjects.ofTable(schema, "table1")` to create the securable object, or you can use full
 * name `catalog1.schema1.table1` and type `TABLE` in the RESTFUL API. <br>
 * If you want to use a topic named `topic1` in the schema named `schema1`, you can use the code
 * `SecurableObjects.ofTopic(schema, "topic1")` to create the securable object, or you can use full
 * name `catalog1.schema1.topic1` and type `TOPIC` in the RESTFUL API. <br>
 * If you want to use a fileset named `fileset1` in the schema named `schema1`, you can use the code
 * `SecurableObjects.ofFileset(schema, "fileset1)` to create the securable object, or you can use
 * full name `catalog1.schema1.fileset1` and type `FILESET` in the RESTFUL API. <br>
 * If you want to use a metalake named `metalake1`, you can use the code
 * `SecurableObjects.ofMetalake("metalake1")` to create the securable object, or you can use full
 * name `metalake1` and type `METALAKE` in the RESTFUL API. <br>
 * If you want to use all the catalogs, you use the metalake to represent them. Likely, you can use
 * their common parent to represent all securable objects.<br>
 * For example if you want to have read table privileges of all tables of `catalog1.schema1`, " you
 * can use add `read table` privilege for `catalog1.schema1` directly
 */
@Unstable
public interface SecurableObject extends MetadataObject {

  /**
   * The privileges of the securable object. For example: If the securable object is a table, the
   * privileges could be `READ TABLE`, `WRITE TABLE`, etc. If a schema has the privilege of `LOAD
   * TABLE`. It means the role can load all tables of the schema.
   *
   * @return The privileges of the securable object.
   */
  List<Privilege> privileges();
}
