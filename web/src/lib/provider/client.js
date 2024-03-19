/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

import { useEffect, useState, Fragment } from 'react'

const ClientOnly = ({ children, ...delegated }) => {
  const [hasMounted, setHasMounted] = useState(false)

  useEffect(() => {
    setHasMounted(true)
  }, [])

  if (!hasMounted) return null

  return <Fragment {...delegated}>{children}</Fragment>
}

export default ClientOnly
