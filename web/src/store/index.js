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
