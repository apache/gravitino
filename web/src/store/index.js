/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */

import { configureStore } from '@reduxjs/toolkit'

import metalake from 'src/store/metalake'

export const store = configureStore({
  reducer: {
    metalake
  },
  middleware: getDefaultMiddleware =>
    getDefaultMiddleware({
      serializableCheck: false
    })
})
