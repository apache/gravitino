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

package org.apache.gravitino.spark.connector.utils;

import static org.apache.gravitino.spark.connector.ConnectorConstants.COMMA;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestConnectorUtil {

  @Test
  void testRemoveDuplicateSparkExtensions() {
    String[] extensions = {"a", "b", "c"};
    String addedExtensions = "a,d,e";
    String result =
        ConnectorUtil.removeDuplicateSparkExtensions(extensions, addedExtensions.split(COMMA));
    Assertions.assertEquals(result, "a,b,c,d,e");

    extensions = new String[] {"a", "a", "b", "c"};
    addedExtensions = "";
    result = ConnectorUtil.removeDuplicateSparkExtensions(extensions, addedExtensions.split(COMMA));
    Assertions.assertEquals(result, "a,b,c");

    extensions = new String[] {"a", "a", "b", "c"};
    addedExtensions = "b";
    result = ConnectorUtil.removeDuplicateSparkExtensions(extensions, addedExtensions.split(COMMA));
    Assertions.assertEquals(result, "a,b,c");

    extensions = new String[] {"a", "a", "b", "c"};
    result = ConnectorUtil.removeDuplicateSparkExtensions(extensions, null);
    Assertions.assertEquals(result, "a,b,c");
  }
}
