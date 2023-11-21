/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */

import createCache from '@emotion/cache'

export const createEmotionCache = () => {
  return createCache({ key: 'css' })
}
