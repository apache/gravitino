/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */

import axios from 'axios'

const Apis = {
  GET: ({ metalake }) => {
    return `/api/metalakes/${metalake}/catalogs`
  }
}

export const getCatalogsApi = params => {
  return axios({
    url: `${Apis.GET(params)}`,
    method: 'get',
    headers: {
      Accept: 'application/vnd.gravitino.v1+json'
    }
  })
}
