/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

import { defHttp } from '@/lib/utils/axios'

const Apis = {
  GET: ({ metalake, catalog, schema }) => `/api/metalakes/${metalake}/catalogs/${catalog}/schemas/${schema}/tables`,
  GET_DETAIL: ({ metalake, catalog, schema, table }) =>
    `/api/metalakes/${metalake}/catalogs/${catalog}/schemas/${schema}/tables/${table}`
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
