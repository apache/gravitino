/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

import { defHttp } from '@/lib/utils/axios'

const Apis = {
  GET: '/api/version'
}

export const getVersionApi = () => {
  return defHttp.get({
    url: `${Apis.GET}`
  })
}
