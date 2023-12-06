/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */

import defHttp from '@/lib/api'

const Apis = {
  GET: '/api/version'
}

export const getVersionApi = () => {
  return defHttp.request({
    method: 'get',
    url: `${Apis.GET}`
  })
}
