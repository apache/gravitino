/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

import { defHttp } from '@/lib/utils/axios'

const Apis = {
  GET: ({ metalake, catalog, schema }) => `/api/metalakes/${metalake}/catalogs/${catalog}/schemas/${schema}/topics`,
  GET_DETAIL: ({ metalake, catalog, schema, topic }) =>
    `/api/metalakes/${metalake}/catalogs/${catalog}/schemas/${schema}/topics/${topic}`
}

export const getTopicsApi = params => {
  return defHttp.get({
    url: `${Apis.GET(params)}`
  })
}

export const getTopicDetailsApi = ({ metalake, catalog, schema, topic }) => {
  return defHttp.get({
    url: `${Apis.GET_DETAIL({ metalake, catalog, schema, topic })}`
  })
}
