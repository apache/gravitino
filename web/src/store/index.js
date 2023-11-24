/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */

import { configureStore } from '@reduxjs/toolkit'

import version from './version'
import metalakes from './metalakes'

export const store = configureStore({
  reducer: {
    version,
    metalakes
  },
  middleware: getDefaultMiddleware =>
    getDefaultMiddleware({
      serializableCheck: false
    })
})
