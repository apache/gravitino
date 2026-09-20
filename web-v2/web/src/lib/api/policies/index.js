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
  GET: ({ metalake }) => `/api/metalakes/${encodeURIComponent(metalake)}/policies`,
  GET_DETAIL: ({ metalake, policy }) =>
    `/api/metalakes/${encodeURIComponent(metalake)}/policies/${encodeURIComponent(policy)}`,
  CREATE: ({ metalake }) => `/api/metalakes/${encodeURIComponent(metalake)}/policies`,
  DELETE: ({ metalake, policy }) =>
    `/api/metalakes/${encodeURIComponent(metalake)}/policies/${encodeURIComponent(policy)}`,
  UPDATE: ({ metalake, policy }) =>
    `/api/metalakes/${encodeURIComponent(metalake)}/policies/${encodeURIComponent(policy)}`,
  ASSOCIATE: ({ metalake, metadataObjectType, metadataObjectFullName }) =>
    `/api/metalakes/${encodeURIComponent(metalake)}/objects/${encodeURIComponent(metadataObjectType)}/${encodeURIComponent(metadataObjectFullName)}/policies`
}

export const getPoliciesApi = ({ metalake, details }) => {
  return defHttp.get({
    url: Apis.GET({ metalake }),
    params: { details }
  })
}

export const getPolicyDetailsApi = ({ metalake, policy }) => {
  return defHttp.get({
    url: Apis.GET_DETAIL({ metalake, policy })
  })
}

export const createPolicyApi = ({ metalake, data }) => {
  return defHttp.post({
    url: Apis.CREATE({ metalake }),
    data
  })
}

export const deletePolicyApi = ({ metalake, policy }) => {
  return defHttp.delete({
    url: Apis.DELETE({ metalake, policy })
  })
}

export const updatePolicyApi = ({ metalake, policy, data }) => {
  return defHttp.put({
    url: Apis.UPDATE({ metalake, policy }),
    data
  })
}

export const enableOrDisablePolicyApi = ({ metalake, policy, data }) => {
  return defHttp.patch({
    url: `${Apis.UPDATE({ metalake, policy })}`,
    data
  })
}

export const associatePolicyApi = ({ metalake, metadataObjectType, metadataObjectFullName, data }) => {
  return defHttp.post({
    url: Apis.ASSOCIATE({ metalake, metadataObjectType, metadataObjectFullName }),
    data
  })
}
