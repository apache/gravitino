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
  GET: ({ metalake, catalog, schema }) =>
    `/api/metalakes/${encodeURIComponent(metalake)}/catalogs/${encodeURIComponent(
      catalog
    )}/schemas/${encodeURIComponent(schema)}/models`,
  GET_DETAIL: ({ metalake, catalog, schema, model }) =>
    `/api/metalakes/${encodeURIComponent(metalake)}/catalogs/${encodeURIComponent(
      catalog
    )}/schemas/${encodeURIComponent(schema)}/models/${encodeURIComponent(model)}`,
  REGISTER: ({ metalake, catalog, schema }) =>
    `/api/metalakes/${encodeURIComponent(metalake)}/catalogs/${encodeURIComponent(catalog)}/schemas/${encodeURIComponent(schema)}/models`,
  UPDATE: ({ metalake, catalog, schema, model }) =>
    `/api/metalakes/${encodeURIComponent(metalake)}/catalogs/${encodeURIComponent(catalog)}/schemas/${encodeURIComponent(schema)}/models/${encodeURIComponent(model)}`,
  DELETE: ({ metalake, catalog, schema, model }) =>
    `/api/metalakes/${encodeURIComponent(metalake)}/catalogs/${encodeURIComponent(catalog)}/schemas/${encodeURIComponent(schema)}/models/${encodeURIComponent(model)}`,
  GET_VERSIONS: ({ metalake, catalog, schema, model }) =>
    `/api/metalakes/${encodeURIComponent(metalake)}/catalogs/${encodeURIComponent(
      catalog
    )}/schemas/${encodeURIComponent(schema)}/models/${encodeURIComponent(model)}/versions`,
  GET_VERSION_DETAIL: ({ metalake, catalog, schema, model, version }) =>
    `/api/metalakes/${encodeURIComponent(metalake)}/catalogs/${encodeURIComponent(
      catalog
    )}/schemas/${encodeURIComponent(schema)}/models/${encodeURIComponent(model)}/versions/${version}`,
  LINK_VERSION: ({ metalake, catalog, schema, model }) =>
    `/api/metalakes/${encodeURIComponent(metalake)}/catalogs/${encodeURIComponent(
      catalog
    )}/schemas/${encodeURIComponent(schema)}/models/${encodeURIComponent(model)}/versions`,
  UPDATE_VERSION: ({ metalake, catalog, schema, model, version }) => {
    return `/api/metalakes/${encodeURIComponent(metalake)}/catalogs/${encodeURIComponent(
      catalog
    )}/schemas/${encodeURIComponent(schema)}/models/${encodeURIComponent(model)}/versions/${version}`
  },
  DELETE_VERSION: ({ metalake, catalog, schema, model, version }) => {
    return `/api/metalakes/${encodeURIComponent(metalake)}/catalogs/${encodeURIComponent(
      catalog
    )}/schemas/${encodeURIComponent(schema)}/models/${encodeURIComponent(model)}/versions/${version}`
  }
}

export const getModelsApi = params => {
  return defHttp.get({
    url: `${Apis.GET(params)}`
  })
}

export const getModelDetailsApi = ({ metalake, catalog, schema, model }) => {
  return defHttp.get({
    url: `${Apis.GET_DETAIL({ metalake, catalog, schema, model })}`
  })
}

export const registerModelApi = ({ metalake, catalog, schema, data }) => {
  return defHttp.post({ url: `${Apis.REGISTER({ metalake, catalog, schema })}`, data })
}

export const updateModelApi = ({ metalake, catalog, schema, model, data }) => {
  return defHttp.put({ url: `${Apis.UPDATE({ metalake, catalog, schema, model })}`, data })
}

export const deleteModelApi = ({ metalake, catalog, schema, model }) => {
  return defHttp.delete({ url: `${Apis.DELETE({ metalake, catalog, schema, model })}` })
}

export const getModelVersionsApi = params => {
  return defHttp.get({
    url: `${Apis.GET_VERSIONS(params)}`
  })
}

export const linkVersionApi = ({ metalake, catalog, schema, model, data }) => {
  return defHttp.post({ url: `${Apis.LINK_VERSION({ metalake, catalog, schema, model })}`, data })
}

export const updateVersionApi = ({ metalake, catalog, schema, model, version, data }) => {
  return defHttp.put({
    url: `${Apis.UPDATE_VERSION({ metalake, catalog, schema, model, version })}`,
    data
  })
}

export const getVersionDetailsApi = ({ metalake, catalog, schema, model, version }) => {
  return defHttp.get({
    url: `${Apis.GET_VERSION_DETAIL({ metalake, catalog, schema, model, version })}`
  })
}

export const deleteVersionApi = ({ metalake, catalog, schema, model, version }) => {
  return defHttp.delete({
    url: `${Apis.DELETE_VERSION({ metalake, catalog, schema, model, version })}`
  })
}
