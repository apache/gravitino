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
package org.apache.gravitino.storage.file.stats;

import java.util.Map;
import org.apache.gravitino.MetadataObject;

public class LancePartitionStatisticFileFactory implements PartitionStatisticFileFactory {
  @Override
  public PartitionStatisticFile open(
      String metalake,
      MetadataObject metadataObject,
      String location,
      Map<String, String> properties) {
    // Long metalakeId = MetalakeMetaService.getInstance().getMetalakeIdByName(metalake);
    // long tableId = new Random().nextLong();
    //    MetadataObjectService.getMetadataObjectId(
    //        metalakeId, metadataObject.fullName(), metadataObject.type());
    // String fileLocation = location + "/" + tableId + ".lance";
    return new LancePartitionStatisticFile(location, properties);
  }
}
