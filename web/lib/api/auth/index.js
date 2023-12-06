/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */

import axios from 'axios'

export const loginApi = params => {
  return axios({
    url: '/oauth2/token',
    method: 'post',
    params
  })
}
