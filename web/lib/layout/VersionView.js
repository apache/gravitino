/*
 * Copyright 2023 DATASTRATO Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
'use client'

import { Typography } from '@mui/material'

import { useAppSelector } from '@/lib/hooks/useStore'

const VersionView = () => {
  const store = useAppSelector(state => state.sys)

  return <Typography variant='subtitle2'>{store.version}</Typography>
}

export default VersionView
