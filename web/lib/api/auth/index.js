/*
 * Copyright 2023 DATASTRATO Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

import axios from 'axios'

export const getAuthConfigsApi = () => {
  return axios({
    url: `/configs`,
    method: 'get'
  })
}

export const loginApi = (url, params) => {
  return axios({
    url,
    method: 'post',
    params
  })
}
