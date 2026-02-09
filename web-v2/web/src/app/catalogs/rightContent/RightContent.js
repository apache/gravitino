/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

'use client'

import CatalogsPage from './entitiesContent/CatalogsPage'
import { useSearchParams } from 'next/navigation'
import { Breadcrumb } from 'antd'
import Link from 'next/link'
import useResizeObserver from 'use-resize-observer'
import dynamic from 'next/dynamic'
import Loading from '@/components/Loading'

const CatalogDetailsPage = dynamic(() => import('./entitiesContent/CatalogDetailsPage'), {
  loading: () => <Loading height={'200px'} />,
  ssr: false
})

const SchemaDetailsPage = dynamic(() => import('./entitiesContent/SchemaDetailsPage'), {
  loading: () => <Loading height={'200px'} />,
  ssr: false
})

const FilesetDetailsPage = dynamic(() => import('./entitiesContent/FilesetDetailsPage'), {
  loading: () => <Loading height={'200px'} />,
  ssr: false
})

const ModelDetailsPage = dynamic(() => import('./entitiesContent/ModelDetailsPage'), {
  loading: () => <Loading height={'200px'} />,
  ssr: false
})

const TopicDetailsPage = dynamic(() => import('./entitiesContent/TopicDetailsPage'), {
  loading: () => <Loading height={'200px'} />,
  ssr: false
})

const TableDetailsPage = dynamic(() => import('./entitiesContent/TableDetailsPage'), {
  loading: () => <Loading height={'200px'} />,
  ssr: false
})

const FunctionDetailsPage = dynamic(() => import('./entitiesContent/FunctionDetailsPage'), {
  loading: () => <Loading height={'200px'} />,
  ssr: false
})

const RightContent = () => {
  const searchParams = useSearchParams()

  const currentMetalake = searchParams.get('metalake')
  const catalogType = searchParams.get('catalogType')
  const catalog = searchParams.get('catalog')
  const schema = searchParams.get('schema')
  const functionName = searchParams.get('function')

  const entity =
    functionName ||
    searchParams.get('table') ||
    searchParams.get('fileset') ||
    searchParams.get('topic') ||
    searchParams.get('model') ||
    ''
  const paramsSize = [...searchParams.keys()].length
  const { ref, width } = useResizeObserver()

  const renderEntity = () => {
    if (paramsSize === 2 && !catalog && !schema && !entity) {
      return <CatalogsPage />
    } else if (paramsSize === 3 && catalog && !schema && !entity) {
      return <CatalogDetailsPage />
    } else if (paramsSize === 4 && catalog && schema && !entity) {
      return <SchemaDetailsPage />
    } else {
      if (functionName) {
        return <FunctionDetailsPage />
      }
      switch (catalogType) {
        case 'relational':
          return (
            <TableDetailsPage
              namespaces={{
                catalog: decodeURIComponent(catalog),
                schema: decodeURIComponent(schema),
                table: decodeURIComponent(entity)
              }}
            />
          )
        case 'fileset':
          return (
            <FilesetDetailsPage
              namespaces={{
                catalog: decodeURIComponent(catalog),
                schema: decodeURIComponent(schema),
                fileset: decodeURIComponent(entity)
              }}
            />
          )
        case 'messaging':
          return (
            <TopicDetailsPage
              namespaces={{
                catalog: decodeURIComponent(catalog),
                schema: decodeURIComponent(schema),
                topic: decodeURIComponent(entity)
              }}
            />
          )
        case 'model':
          return (
            <ModelDetailsPage
              namespaces={{
                catalog: decodeURIComponent(catalog),
                schema: decodeURIComponent(schema),
                model: decodeURIComponent(entity)
              }}
            />
          )
        default:
          return null
      }
    }
  }

  return (
    <div className='p-6' ref={ref}>
      <Breadcrumb
        className='mb-4'
        separator='>'
        items={[
          {
            title: (
              <Link data-refer='back-home-btn' href='/metalakes'>
                Metalakes
              </Link>
            )
          },
          {
            title: catalog ? (
              <Link
                data-refer='metalake-name-link'
                href={`/catalogs?metalake=${currentMetalake}&catalogType=${catalogType}`}
                title={currentMetalake}
                className='inline-block min-w-10 truncate'
                style={{ maxWidth: `calc((${width}px - 160px)/4)` }}
              >
                {decodeURIComponent(currentMetalake)}
              </Link>
            ) : (
              <span
                data-refer='metalake-name-link'
                title={currentMetalake}
                className='inline-block min-w-10 truncate'
                style={{ maxWidth: `calc((${width}px - 160px)/4)` }}
              >
                {decodeURIComponent(currentMetalake)}
              </span>
            )
          },
          ...(catalog
            ? [
                {
                  title: schema ? (
                    <Link
                      href={`/catalogs?metalake=${currentMetalake}&catalogType=${catalogType}&catalog=${decodeURIComponent(catalog)}`}
                      title={catalog}
                      className='inline-block min-w-10 truncate'
                      style={{ maxWidth: `calc((${width}px - 160px)/4)` }}
                    >
                      {decodeURIComponent(catalog)}
                    </Link>
                  ) : (
                    <span
                      title={catalog}
                      className='inline-block min-w-10 truncate'
                      style={{ maxWidth: `calc((${width}px - 160px)/4)` }}
                    >
                      {decodeURIComponent(catalog)}
                    </span>
                  )
                }
              ]
            : []),
          ...(schema
            ? [
                {
                  title: entity ? (
                    <Link
                      href={`/catalogs?metalake=${currentMetalake}&catalogType=${catalogType}&catalog=${decodeURIComponent(catalog)}&schema=${decodeURIComponent(schema)}`}
                      title={schema}
                      className='inline-block min-w-10 truncate'
                      style={{ maxWidth: `calc((${width}px - 160px)/4)` }}
                    >
                      {decodeURIComponent(schema)}
                    </Link>
                  ) : (
                    <span
                      title={schema}
                      className='inline-block min-w-10 truncate'
                      style={{ maxWidth: `calc((${width}px - 160px)/4)` }}
                    >
                      {decodeURIComponent(schema)}
                    </span>
                  )
                }
              ]
            : []),
          ...(entity
            ? [
                {
                  title: functionName ? (
                    <Link
                      href={`/catalogs?metalake=${currentMetalake}&catalogType=${catalogType}&catalog=${decodeURIComponent(catalog)}&schema=${decodeURIComponent(schema)}`}
                      title={entity}
                      className='inline-block min-w-10 truncate'
                      style={{ maxWidth: `calc((${width}px - 160px)/4)` }}
                    >
                      {decodeURIComponent(entity)}
                    </Link>
                  ) : (
                    <span
                      title={entity}
                      className='inline-block min-w-10 truncate'
                      style={{ maxWidth: `calc((${width}px - 160px)/4)` }}
                    >
                      {decodeURIComponent(entity)}
                    </span>
                  )
                }
              ]
            : [])
        ]}
      />
      {renderEntity()}
    </div>
  )
}

export default RightContent
