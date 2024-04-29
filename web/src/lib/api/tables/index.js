/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
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
    )}/schemas/${encodeURIComponent(schema)}/tables/${encodeURIComponent(table)}`
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
