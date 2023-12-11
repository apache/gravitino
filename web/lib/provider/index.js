/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

'use client'

import AuthProvider from './session'
import StoreProvider from './store'
import EmotionProvider from './emotion'
import ThemeProvider from './theme'

const Provider = ({ children }) => {
  return (
    <StoreProvider>
      <EmotionProvider>
        <AuthProvider>
          <ThemeProvider>{children}</ThemeProvider>
        </AuthProvider>
      </EmotionProvider>
    </StoreProvider>
  )
}

export default Provider
