/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */

'use client'

import { configureStore } from '@reduxjs/toolkit'

import { isDevEnv } from '@/lib/utils'

import version from './version'
import metalakes from './metalakes'

export const store = configureStore({
  reducer: {
    version,
    metalakes
  },
  devTools: true,
  middleware: getDefaultMiddleware =>
    getDefaultMiddleware({
      serializableCheck: isDevEnv
    })
})

export default store
