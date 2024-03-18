/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

'use client'

import { configureStore } from '@reduxjs/toolkit'

import { isDevEnv } from '@/lib/utils'

import sys from './sys'
import auth from './auth'
import metalakes from './metalakes'

export const store = configureStore({
  reducer: {
    sys,
    auth,
    metalakes
  },
  devTools: true,
  middleware: getDefaultMiddleware =>
    getDefaultMiddleware({
      serializableCheck: isDevEnv
    })
})

export default store
