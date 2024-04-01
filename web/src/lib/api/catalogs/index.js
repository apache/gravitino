/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

import { defHttp } from '@/lib/utils/axios'

const Apis = {
  GET: ({ metalake }) => `/api/metalakes/${metalake}/catalogs?details=true`,
  GET_DETAIL: ({ metalake, catalog }) => `/api/metalakes/${metalake}/catalogs/${catalog}`,
  CREATE: ({ metalake }) => `/api/metalakes/${metalake}/catalogs`,
  UPDATE: ({ metalake, catalog }) => `/api/metalakes/${metalake}/catalogs/${catalog}`,
  DELETE: ({ metalake, catalog }) => `/api/metalakes/${metalake}/catalogs/${catalog}`
}

export const getCatalogsApi = params => {
  return defHttp.get({
    url: `${Apis.GET(params)}`
  })
}

export const getCatalogDetailsApi = ({ metalake, catalog }) => {
  return defHttp.get({
    url: `${Apis.GET_DETAIL({ metalake, catalog })}`
  })
}

export const createCatalogApi = ({ data, metalake }) => {
  return defHttp.post({
    url: `${Apis.CREATE({ metalake })}`,
    data
  })
}

export const updateCatalogApi = ({ metalake, catalog, data }) => {
  return defHttp.put({
    url: `${Apis.UPDATE({ metalake, catalog })}`,
    data
  })
}

export const deleteCatalogApi = ({ metalake, catalog }) => {
  return defHttp.delete({
    url: `${Apis.DELETE({ metalake, catalog })}`
  })
}
