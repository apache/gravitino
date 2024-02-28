/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

'use client'

import { useEffect, useState } from 'react'

import { useSearchParams } from 'next/navigation'

import MetalakeView from '@/app/ui/metalakes/MetalakeView'

const MetalakePage = () => {
  const searchParams = useSearchParams()

  const [routeParams, setRouteParams] = useState({})

  const getParamsLens = () => {
    let lens = []
    searchParams.forEach(v => {
      lens.push(v)
    })

    return lens
  }

  const getProps = () => {
    if (getParamsLens().length === 1 && searchParams.get('metalake')) {
      return {
        page: 'metalakes',
        title: 'Catalogs'
      }
    } else if (getParamsLens().length === 2 && searchParams.has('catalog')) {
      return {
        page: 'catalogs',
        title: 'Schemas'
      }
    } else if (getParamsLens().length === 3 && searchParams.has('schema')) {
      return {
        page: 'schemas',
        title: 'Tables'
      }
    } else if (getParamsLens().length === 4 && searchParams.has('table')) {
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

  useEffect(() => {
    const takeParams = {
      metalake: searchParams.get('metalake'),
      catalog: searchParams.get('catalog'),
      schema: searchParams.get('schema'),
      table: searchParams.get('table')
    }

    setRouteParams(takeParams)
  }, [searchParams])

  return <MetalakeView page={getProps().page} tableTitle={getProps().title} routeParams={routeParams} />
}

export default MetalakePage
