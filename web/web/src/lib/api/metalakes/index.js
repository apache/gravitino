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
  GET: '/api/metalakes',
  CREATE: '/api/metalakes',
  DELETE: '/api/metalakes',
  UPDATE: '/api/metalakes',
  UPDATEINUSE: '/api/metalakes',
  OWNER_PATH: ({ metalake, metadataObjectType, metadataObjectFullName }) =>
    `/api/metalakes/${encodeURIComponent(metalake)}/owners/${encodeURIComponent(metadataObjectType)}/${encodeURIComponent(metadataObjectFullName)}`,
  TAGS_PATH: ({ metalake, metadataObjectType, metadataObjectFullName }) =>
    `/api/metalakes/${encodeURIComponent(metalake)}/objects/${encodeURIComponent(metadataObjectType)}/${encodeURIComponent(metadataObjectFullName)}/tags`,
  POLICIES_PATH: ({ metalake, metadataObjectType, metadataObjectFullName }) =>
    `/api/metalakes/${encodeURIComponent(metalake)}/objects/${encodeURIComponent(metadataObjectType)}/${encodeURIComponent(metadataObjectFullName)}/policies`,
  METADATA_PATH_TAG: ({ metalake, tag }) =>
    `/api/metalakes/${encodeURIComponent(metalake)}/tags/${encodeURIComponent(tag)}/objects`,
  METADATA_PATH_POLICY: ({ metalake, policy }) =>
    `/api/metalakes/${encodeURIComponent(metalake)}/policies/${encodeURIComponent(policy)}/objects`
}

export const getMetalakesApi = () => {
  return defHttp.get({
    url: `${Apis.GET}`
  })
}

export const getMetalakeDetailsApi = name => {
  return defHttp.get({
    url: `${Apis.GET}/${name}`
  })
}

export const createMetalakeApi = data => {
  return defHttp.post({
    url: `${Apis.CREATE}`,
    data
  })
}

export const deleteMetalakeApi = name => {
  return defHttp.delete({
    url: `${Apis.DELETE}/${name}`
  })
}

export const updateMetalakeApi = ({ name, data }) => {
  return defHttp.put({
    url: `${Apis.UPDATE}/${name}`,
    data
  })
}

export const getMetalakeDataApi = url => {
  return defHttp.get({
    url: `/api${url}`
  })
}

export const switchInUseApi = ({ name, isInUse }) => {
  return defHttp.patch({
    url: `${Apis.UPDATEINUSE}/${name}`,
    data: { inUse: isInUse }
  })
}

export const getEntityOwnerApi = (metalake, metadataObjectType, metadataObjectFullName) => {
  return defHttp.get({
    url: Apis.OWNER_PATH({ metalake, metadataObjectType, metadataObjectFullName })
  })
}

export const setEntityOwnerApi = (metalake, metadataObjectType, metadataObjectFullName, data) => {
  return defHttp.put({
    url: Apis.OWNER_PATH({ metalake, metadataObjectType, metadataObjectFullName }),
    data
  })
}

export const getCurrentEntityTagsApi = (metalake, metadataObjectType, metadataObjectFullName, details) => {
  return defHttp.get({
    url: Apis.TAGS_PATH({ metalake, metadataObjectType, metadataObjectFullName }),
    params: { details }
  })
}

export const getCurrentEntityPoliciesApi = ({ metalake, metadataObjectType, metadataObjectFullName, details }) => {
  return defHttp.get({
    url: Apis.POLICIES_PATH({ metalake, metadataObjectType, metadataObjectFullName }),
    params: { details }
  })
}

export const getMetadataObjectsForTagApi = (metalake, tag) => {
  return defHttp.get({
    url: Apis.METADATA_PATH_TAG({ metalake, tag })
  })
}

export const getMetadataObjectsForPolicyApi = (metalake, policy) => {
  return defHttp.get({
    url: Apis.METADATA_PATH_POLICY({ metalake, policy })
  })
}
