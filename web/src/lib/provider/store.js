/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

'use client'

import { useRef } from 'react'
import { Provider } from 'react-redux'

import store from '@/lib/store'

const StoreProvider = ({ children }) => {
  const storeRef = useRef()
  if (!storeRef.current) {
    storeRef.current = store
  }

  return <Provider store={storeRef.current}>{children}</Provider>
}

export default StoreProvider
