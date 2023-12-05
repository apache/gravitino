/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */

import axios from 'axios'

const Apis = {
  GET: '/api/version'
}

export const getVersionApi = () => {
  return axios({
    url: `${Apis.GET}`,
    method: 'get',
    headers: {
      Accept: 'application/vnd.gravitino.v1+json'
    }
  })
}
