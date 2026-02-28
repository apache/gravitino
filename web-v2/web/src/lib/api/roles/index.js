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

import { defHttp } from '@/lib/utils/axios'

const Apis = {
  GET: ({ metalake }) => `/api/metalakes/${encodeURIComponent(metalake)}/roles`,
  GET_DETAIL: ({ metalake, role }) =>
    `/api/metalakes/${encodeURIComponent(metalake)}/roles/${encodeURIComponent(role)}`,
  CREATE: ({ metalake }) => `/api/metalakes/${encodeURIComponent(metalake)}/roles`,
  UPDATE: ({ metalake, role }) =>
    `/api/metalakes/${encodeURIComponent(metalake)}/permissions/roles/${encodeURIComponent(role)}`,
  DELETE: ({ metalake, role }) => `/api/metalakes/${encodeURIComponent(metalake)}/roles/${encodeURIComponent(role)}`,
  GRANT_PRIVILEGE: ({ metalake, role, metadataObjectType, metadataObjectFullName }) =>
    `/api/metalakes/${encodeURIComponent(metalake)}/permissions/roles/${encodeURIComponent(role)}/${encodeURIComponent(metadataObjectType)}/${encodeURIComponent(metadataObjectFullName)}/grant`,
  REVOKE_PRIVILEGE: ({ metalake, role, metadataObjectType, metadataObjectFullName }) =>
    `/api/metalakes/${encodeURIComponent(metalake)}/permissions/roles/${encodeURIComponent(role)}/${encodeURIComponent(metadataObjectType)}/${encodeURIComponent(metadataObjectFullName)}/revoke`,
  LIST_ROLES_FOR_OBJECT: ({ metalake, metadataObjectType, metadataObjectFullName }) =>
    `/api/metalakes/${encodeURIComponent(metalake)}/objects/${encodeURIComponent(metadataObjectType)}/${encodeURIComponent(metadataObjectFullName)}/roles`
}

export const getRolesApi = ({ metalake }) => {
  return defHttp.get({
    url: Apis.GET({ metalake })
  })
}

export const createRoleApi = ({ metalake, data }) => {
  return defHttp.post({
    url: Apis.CREATE({ metalake }),
    data
  })
}

export const getRoleDetailsApi = ({ metalake, role }) => {
  return defHttp.get({
    url: Apis.GET_DETAIL({ metalake, role })
  })
}

export const updateRolePrivilegesApi = ({ metalake, role, data }) => {
  return defHttp.put({
    url: Apis.UPDATE({ metalake, role }),
    data
  })
}

export const deleteRoleApi = ({ metalake, role }) => {
  return defHttp.delete({
    url: Apis.DELETE({ metalake, role })
  })
}

export const listRolesForObjectApi = ({ metalake, metadataObjectType, metadataObjectFullName }) => {
  return defHttp.get({
    url: Apis.LIST_ROLES_FOR_OBJECT({ metalake, metadataObjectType, metadataObjectFullName })
  })
}
