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
package org.apache.gravitino.storage.relational.utils;

import com.google.common.collect.Lists;
import java.util.List;
import java.util.function.Supplier;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.authorization.SecurableObject;
import org.apache.gravitino.storage.relational.po.GroupPO;
import org.apache.gravitino.storage.relational.po.RolePO;
import org.apache.gravitino.storage.relational.po.SecurableObjectPO;
import org.apache.gravitino.storage.relational.po.UserPO;
import org.apache.gravitino.storage.relational.service.MetadataObjectService;
import org.apache.gravitino.storage.relational.service.RoleMetaService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/* This class is a utilization class for creating kinds of suppliers */
public class SupplierUtils {

  private static final Logger LOG = LoggerFactory.getLogger(SupplierUtils.class);

  private SupplierUtils() {}

  public static Supplier<List<RolePO>> createRolePOsSupplier(UserPO userPO) {
    return new Supplier<List<RolePO>>() {
      private List<RolePO> rolePOs;
      private boolean waitToLoad = true;

      @Override
      public List<RolePO> get() {
        if (waitToLoad) {
          waitToLoad = false;
          rolePOs = RoleMetaService.getInstance().listRolesByUserId(userPO.getUserId());
        }

        return rolePOs;
      }
    };
  }

  public static Supplier<List<RolePO>> createRolePOsSupplier(GroupPO groupPO) {
    return new Supplier<List<RolePO>>() {
      private List<RolePO> rolePOS;
      private boolean waitToLoad = true;

      @Override
      public List<RolePO> get() {
        if (waitToLoad) {
          waitToLoad = false;
          rolePOS = RoleMetaService.getInstance().listRolesByGroupId(groupPO.getGroupId());
        }

        return rolePOS;
      }
    };
  }

  public static Supplier<List<SecurableObject>> createSecurableObjectsSupplier(RolePO rolePO) {
    return new Supplier<List<SecurableObject>>() {
      private List<SecurableObject> securableObjects;

      private boolean waitToLoad = true;

      @Override
      public List<SecurableObject> get() {
        if (waitToLoad) {
          waitToLoad = false;
          List<SecurableObjectPO> securableObjectPOs =
              RoleMetaService.getInstance().listSecurableObjectsByRoleId(rolePO.getRoleId());

          securableObjects = Lists.newArrayList();

          for (SecurableObjectPO securableObjectPO : securableObjectPOs) {
            String fullName =
                MetadataObjectService.getMetadataObjectFullName(
                    securableObjectPO.getType(), securableObjectPO.getMetadataObjectId());
            if (fullName != null) {
              securableObjects.add(
                  POConverters.fromSecurableObjectPO(
                      fullName,
                      securableObjectPO,
                      MetadataObject.Type.valueOf(securableObjectPO.getType())));
            } else {
              LOG.info(
                  "The securable object {} {} may be deleted",
                  securableObjectPO.getMetadataObjectId(),
                  securableObjectPO.getType());
            }
          }
        }

        return securableObjects;
      }
    };
  }
}
