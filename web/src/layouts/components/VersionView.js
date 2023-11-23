/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */

import { useEffect } from 'react'

import Typography from '@mui/material/Typography'

import { useDispatch, useSelector } from 'react-redux'
import { fetchVersion, setVersion } from 'src/store/version'

const VersionView = () => {
  const dispatch = useDispatch()
  const store = useSelector(state => state.version)

  useEffect(() => {
    if (typeof window !== 'undefined') {
      const version = window.sessionStorage.getItem('version')

      if (!version || version === 'undefined') {
        dispatch(fetchVersion())
      } else {
        dispatch(setVersion(version))
      }
    }
  }, [dispatch])

  return <Typography variant='subtitle2'>{store.version}</Typography>
}

export default VersionView
