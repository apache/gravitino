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
package org.apache.gravitino.server.web.auth.jcasbin;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import java.util.List;
import org.apache.gravitino.storage.relational.po.CatalogPO;
import org.apache.gravitino.storage.relational.po.MetalakePO;
import org.apache.gravitino.storage.relational.po.SecurableObjectPO;
import org.apache.gravitino.storage.relational.po.UserPO;
import org.apache.gravitino.storage.relational.po.UserRoleRelPO;
import org.apache.gravitino.storage.relational.service.CatalogMetaService;
import org.apache.gravitino.storage.relational.service.MetalakeMetaService;
import org.apache.gravitino.storage.relational.service.SecurableObjectService;
import org.apache.gravitino.storage.relational.service.UserMetaService;
import org.apache.gravitino.storage.relational.service.UserRoleRelService;
import org.casbin.jcasbin.model.Model;
import org.casbin.jcasbin.persist.Adapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GravitinoAdapter implements Adapter {

  private static final Logger LOG = LoggerFactory.getLogger(GravitinoAdapter.class);

  @Override
  public void loadPolicy(Model model) {
    loadSecurableObject(model);
    loadRoleUserRel(model);
    loadGroupUserRel(model);
    loadGroupRoleRel(model);
  }

  private void loadGroupRoleRel(Model model) {
    LOG.info("load group role rel {}", model);
    // TODO
  }

  private void loadGroupUserRel(Model model) {
    LOG.info("load group user rel {}", model);
    // TODO
  }

  private void loadRoleUserRel(Model model) {
    List<UserRoleRelPO> userRoleRelList = UserRoleRelService.getInstance().listAllUserRoleRel();
    for (UserRoleRelPO userRoleRelPO : userRoleRelList) {
      UserPO userPO = UserMetaService.getInstance().getUserById(userRoleRelPO.getUserId());
      Long metalakeId = userPO.getMetalakeId();
      MetalakePO metalakePO = MetalakeMetaService.getInstance().getMetalakePOById(metalakeId);
      model.addPolicy(
          "g",
          "g",
          Lists.newArrayList(
              String.valueOf(userRoleRelPO.getUserId()),
              String.valueOf(userRoleRelPO.getRoleId()),
              metalakePO.getMetalakeName()));
    }
  }

  private void loadSecurableObject(Model model) {
    List<SecurableObjectPO> securableObjectPOList = SecurableObjectService.getInstance().listAll();
    securableObjectPOList.forEach(
        securableObjectPO -> {
          switch (securableObjectPO.getType()) {
            case "CATALOG":
              loadCatalog(securableObjectPO, model);
              break;
            default:
              break;
          }
        });
  }

  private void loadCatalog(SecurableObjectPO securableObjectPO, Model model) {
    try {
      Long metadataObjectId = securableObjectPO.getMetadataObjectId();
      CatalogPO catalogPO = CatalogMetaService.getInstance().getCatalogPOById(metadataObjectId);
      String catalogName = catalogPO.getCatalogName();
      Long metalakeId = catalogPO.getMetalakeId();
      MetalakePO metalakePO = MetalakeMetaService.getInstance().getMetalakePOById(metalakeId);
      String privilegeNamesStr = securableObjectPO.getPrivilegeNames();
      ObjectMapper objectMapper = new ObjectMapper();
      List<String> privilegeNames =
          objectMapper.readValue(privilegeNamesStr, new TypeReference<List<String>>() {});
      String privilegeConditionsStr = securableObjectPO.getPrivilegeConditions();
      List<String> privilegeConditions =
          objectMapper.readValue(privilegeConditionsStr, new TypeReference<List<String>>() {});
      String roleId = String.valueOf(securableObjectPO.getRoleId());
      for (String privilegeName : privilegeNames) {
        for (String privilegeCondition : privilegeConditions) {
          model.addPolicy(
              "p",
              "p",
              Lists.newArrayList(
                  roleId,
                  metalakePO.getMetalakeName(),
                  catalogName,
                  privilegeName,
                  privilegeCondition.toLowerCase()));
        }
      }
    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
    }
  }

  @Override
  public void savePolicy(Model model) {
    throw new UnsupportedOperationException("");
  }

  @Override
  public void addPolicy(String s, String s1, List<String> list) {
    throw new UnsupportedOperationException("");
  }

  @Override
  public void removePolicy(String s, String s1, List<String> list) {
    throw new UnsupportedOperationException("");
  }

  @Override
  public void removeFilteredPolicy(String s, String s1, int i, String... strings) {
    throw new UnsupportedOperationException("");
  }
}
