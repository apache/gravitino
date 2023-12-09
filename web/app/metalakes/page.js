/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

'use client'

import { useEffect, useState } from 'react'
import { useSearchParams } from 'next/navigation'

import MetalakeView from '@/app/metalakes/MetalakeView'

const MetalakePage = () => {
  const params = useSearchParams()

  const [routeParams, setRouteParams] = useState({
    metalake: params.get('metalake'),
    catalog: params.get('catalog'),
    schema: params.get('schema'),
    table: params.get('table')
  })

  useEffect(() => {
    setRouteParams({
      metalake: params.get('metalake'),
      catalog: params.get('catalog'),
      schema: params.get('schema'),
      table: params.get('table')
    })
  }, [params])

  // console.log(params.get('metalake'))

  // console.log(params.keys().includes('metalake'))
  const getProps = () => {
    if (params.size === 1 && params.has('metalake')) {
      return {
        page: 'metalakes',
        title: 'Catalogs'
      }
    } else if (params.size === 2 && params.has('catalog')) {
      return {
        page: 'catalogs',
        title: 'Schemas'
      }
    } else if (params.size === 3 && params.has('schema')) {
      return {
        page: 'schemas',
        title: 'Tables'
      }
    } else if (params.size === 4 && params.has('table')) {
      return {
        page: 'tables',
        title: 'Columns'
      }
    } else {
      return {
        page: null,
        title: null
      }
    }
  }

  return <MetalakeView page={getProps().page} tableTitle={getProps().title} routeParams={routeParams} />
}

export default MetalakePage
