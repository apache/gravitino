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
    )}/schemas/${encodeURIComponent(schema)}/tables`,
  GET_DETAIL: ({ metalake, catalog, schema, table }) =>
    `/api/metalakes/${encodeURIComponent(metalake)}/catalogs/${encodeURIComponent(
      catalog
    )}/schemas/${encodeURIComponent(schema)}/tables/${encodeURIComponent(table)}`,
  CREATE: ({ metalake, catalog, schema }) =>
    `/api/metalakes/${encodeURIComponent(metalake)}/catalogs/${encodeURIComponent(catalog)}/schemas/${encodeURIComponent(schema)}/tables`,
  UPDATE: ({ metalake, catalog, schema, table }) =>
    `/api/metalakes/${encodeURIComponent(metalake)}/catalogs/${encodeURIComponent(catalog)}/schemas/${encodeURIComponent(schema)}/tables/${encodeURIComponent(table)}`,
  DELETE: ({ metalake, catalog, schema, table }) =>
    `/api/metalakes/${encodeURIComponent(metalake)}/catalogs/${encodeURIComponent(catalog)}/schemas/${encodeURIComponent(schema)}/tables/${encodeURIComponent(table)}`
}

export const getTablesApi = params => {
  return defHttp.get({
    url: `${Apis.GET(params)}`
  })
}

export const getTableDetailsApi = ({ metalake, catalog, schema, table }) => {
  return defHttp.get({
    url: `${Apis.GET_DETAIL({ metalake, catalog, schema, table })}`
  })
}

export const createTableApi = ({ metalake, catalog, schema, data }) => {
  return defHttp.post({ url: `${Apis.CREATE({ metalake, catalog, schema })}`, data })
}

export const updateTableApi = ({ metalake, catalog, schema, table, data }) => {
  return defHttp.put({ url: `${Apis.UPDATE({ metalake, catalog, schema, table })}`, data })
}

export const deleteTableApi = ({ metalake, catalog, schema, table }) => {
  return defHttp.delete({ url: `${Apis.DELETE({ metalake, catalog, schema, table })}` })
}
