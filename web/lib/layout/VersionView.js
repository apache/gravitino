/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
'use client'

import { useEffect } from 'react'

import Typography from '@mui/material/Typography'

import { useAppStore, useAppDispatch, useAppSelector } from '@/lib/hooks/useStore'
import { initialVersion, setVersion } from '@/lib/store/version'

const VersionView = () => {
  const version = useAppSelector(state => state.version)
  const dispatch = useAppDispatch()

  useEffect(() => {
    if (typeof window !== 'undefined') {
      const version = window.sessionStorage.getItem('version')

      if (!version || version === 'undefined') {
        dispatch(initialVersion())
      } else {
        dispatch(setVersion(version))
      }
    }
  }, [dispatch])

  return <Typography variant='subtitle2'>{version.version}</Typography>
}

export default VersionView
