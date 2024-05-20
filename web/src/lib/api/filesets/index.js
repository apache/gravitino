/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

import { defHttp } from '@/lib/utils/axios'

const Apis = {
  GET: ({ metalake, catalog, schema }) =>
    `/api/metalakes/${encodeURIComponent(metalake)}/catalogs/${encodeURIComponent(
      catalog
    )}/schemas/${encodeURIComponent(schema)}/filesets`,
  GET_DETAIL: ({ metalake, catalog, schema, fileset }) =>
    `/api/metalakes/${encodeURIComponent(metalake)}/catalogs/${encodeURIComponent(
      catalog
    )}/schemas/${encodeURIComponent(schema)}/filesets/${encodeURIComponent(fileset)}`
}

export const getFilesetsApi = params => {
  return defHttp.get({
    url: `${Apis.GET(params)}`
  })
}

export const getFilesetDetailsApi = ({ metalake, catalog, schema, fileset }) => {
  return defHttp.get({
    url: `${Apis.GET_DETAIL({ metalake, catalog, schema, fileset })}`
  })
}
