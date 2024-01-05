/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

import { defHttp } from '@/lib/utils/axios'

const Apis = {
  GET: ({ metalake }) => `/api/metalakes/${metalake}/catalogs`,
  GET_DETAIL: ({ metalake, catalog }) => `/api/metalakes/${metalake}/catalogs/${catalog}`,
  CREATE: ({ metalake }) => `/api/metalakes/${metalake}/catalogs`
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
