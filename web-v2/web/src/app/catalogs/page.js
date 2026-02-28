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

import { useEffect, createContext, useRef } from 'react'

import RightContent from './rightContent/RightContent'

import { useAppDispatch, useAppSelector } from '@/lib/hooks/useStore'
import { useSearchParams } from 'next/navigation'
import { Splitter } from 'antd'
import SectionContainer from '@/components/SectionContainer'
import { TreeComponent } from './TreeComponent'

export const TreeRefContext = createContext({})

import {
  fetchCatalogs,
  fetchSchemas,
  fetchTables,
  fetchFilesets,
  fetchTopics,
  fetchModels,
  fetchModelVersions,
  fetchFunctions,
  getFunctionDetails,
  getMetalakeDetails,
  getCatalogDetails,
  getSchemaDetails,
  getTableDetails,
  getFilesetDetails,
  getTopicDetails,
  getModelDetails,
  getVersionDetails,
  setSelectedNodes,
  getCurrentEntityTags,
  getCurrentEntityPolicies,
  resetActivatedDetails,
  setActivatedDetailsLoading
} from '@/lib/store/metalakes'

import { fetchTags } from '@/lib/store/tags'
import { fetchPolicies } from '@/lib/store/policies'

const CatalogsListPage = () => {
  const dispatch = useAppDispatch()
  const searchParams = useSearchParams()
  const paramsSize = [...searchParams.keys()].length
  const store = useAppSelector(state => state.metalakes)

  const treeRef = useRef()

  const buildNodePath = routeParams => {
    const keys = ['metalake', 'catalog', 'catalogType', 'schema', 'table', 'fileset', 'topic', 'model', 'function']

    return keys.map(key => (routeParams[key] ? `{{${routeParams[key]}}}` : '')).join('')
  }

  useEffect(() => {
    const routeParams = {
      metalake: searchParams.get('metalake'),
      catalog: searchParams.get('catalog'),
      catalogType: searchParams.get('catalogType'),
      schema: searchParams.get('schema'),
      table: searchParams.get('table'),
      fileset: searchParams.get('fileset'),
      topic: searchParams.get('topic'),
      model: searchParams.get('model'),
      function: searchParams.get('function'),
      version: searchParams.get('version')
    }
    async function fetchDependsData() {
      if ([...searchParams.keys()].length) {
        const {
          metalake,
          catalog,
          catalogType,
          schema,
          table,
          fileset,
          topic,
          model,
          function: func,
          version
        } = routeParams

        if (metalake) {
          dispatch(fetchTags({ metalake, details: true }))
          dispatch(fetchPolicies({ metalake, details: true }))
        }

        if (paramsSize === 2 && metalake) {
          dispatch(fetchCatalogs({ init: true, page: 'metalakes', metalake }))
          dispatch(getMetalakeDetails({ metalake }))
        }

        if (paramsSize === 3 && catalog) {
          if (!store.catalogs.length) {
            await dispatch(fetchCatalogs({ metalake }))
          }
          await dispatch(setActivatedDetailsLoading(true))
          await dispatch(getCatalogDetails({ init: true, metalake, catalog }))
          await dispatch(setActivatedDetailsLoading(false))
          await dispatch(fetchSchemas({ init: true, page: 'catalogs', metalake, catalog, catalogType }))
          dispatch(
            getCurrentEntityTags({
              init: true,
              metalake,
              metadataObjectType: 'catalog',
              metadataObjectFullName: catalog,
              details: true
            })
          )
          dispatch(
            getCurrentEntityPolicies({
              init: true,
              metalake,
              metadataObjectType: 'catalog',
              metadataObjectFullName: catalog,
              details: true
            })
          )
        }

        if (paramsSize === 4 && catalog && catalogType && schema) {
          if (!store.catalogs.length) {
            await dispatch(fetchCatalogs({ metalake }))
            await dispatch(fetchSchemas({ metalake, catalog, catalogType }))
          }
          dispatch(fetchFunctions({ init: true, metalake, catalog, schema }))
          switch (catalogType) {
            case 'relational':
              dispatch(fetchTables({ init: true, page: 'schemas', metalake, catalog, schema }))
              break
            case 'fileset':
              dispatch(fetchFilesets({ init: true, page: 'schemas', metalake, catalog, schema }))
              break
            case 'messaging':
              dispatch(fetchTopics({ init: true, page: 'schemas', metalake, catalog, schema }))
              break
            case 'model':
              dispatch(fetchModels({ init: true, page: 'schemas', metalake, catalog, schema }))
              break
            default:
              break
          }
          await dispatch(setActivatedDetailsLoading(true))
          await dispatch(getSchemaDetails({ init: true, metalake, catalog, schema }))
          await dispatch(setActivatedDetailsLoading(false))
          dispatch(
            getCurrentEntityTags({
              init: true,
              metalake,
              metadataObjectType: 'schema',
              metadataObjectFullName: `${catalog}.${schema}`,
              details: true
            })
          )
          dispatch(
            getCurrentEntityPolicies({
              init: true,
              metalake,
              metadataObjectType: 'schema',
              metadataObjectFullName: `${catalog}.${schema}`,
              details: true
            })
          )
        }

        if (paramsSize === 5 && catalog && catalogType && schema) {
          if (!store.catalogs.length) {
            await dispatch(fetchCatalogs({ metalake }))
            await dispatch(fetchSchemas({ metalake, catalog, catalogType }))
          }
          if (table || fileset || topic || model) {
            switch (catalogType) {
              case 'relational':
                store.tables.length === 0 &&
                  (await dispatch(fetchTables({ init: true, page: 'schemas', metalake, catalog, schema })))
                await dispatch(resetActivatedDetails())
                await dispatch(setActivatedDetailsLoading(true))
                await dispatch(getTableDetails({ init: true, metalake, catalog, schema, table }))
                await dispatch(setActivatedDetailsLoading(false))
                dispatch(
                  getCurrentEntityTags({
                    init: true,
                    metalake,
                    metadataObjectType: 'table',
                    metadataObjectFullName: `${catalog}.${schema}.${table}`
                  })
                )
                dispatch(
                  getCurrentEntityPolicies({
                    init: true,
                    metalake,
                    metadataObjectType: 'table',
                    metadataObjectFullName: `${catalog}.${schema}.${table}`,
                    details: true
                  })
                )
                break
              case 'fileset':
                store.filesets.length === 0 &&
                  (await dispatch(fetchFilesets({ init: true, page: 'schemas', metalake, catalog, schema })))
                await dispatch(setActivatedDetailsLoading(true))
                await dispatch(getFilesetDetails({ init: true, metalake, catalog, schema, fileset }))
                await dispatch(setActivatedDetailsLoading(false))
                dispatch(
                  getCurrentEntityTags({
                    init: true,
                    metalake,
                    metadataObjectType: 'fileset',
                    metadataObjectFullName: `${catalog}.${schema}.${fileset}`,
                    details: true
                  })
                )
                dispatch(
                  getCurrentEntityPolicies({
                    init: true,
                    metalake,
                    metadataObjectType: 'fileset',
                    metadataObjectFullName: `${catalog}.${schema}.${fileset}`,
                    details: true
                  })
                )
                break
              case 'messaging':
                store.topics.length === 0 &&
                  (await dispatch(fetchTopics({ init: true, page: 'schemas', metalake, catalog, schema })))
                await dispatch(setActivatedDetailsLoading(true))
                await dispatch(getTopicDetails({ init: true, metalake, catalog, schema, topic }))
                await dispatch(setActivatedDetailsLoading(false))
                dispatch(
                  getCurrentEntityTags({
                    init: true,
                    metalake,
                    metadataObjectType: 'topic',
                    metadataObjectFullName: `${catalog}.${schema}.${topic}`,
                    details: true
                  })
                )
                dispatch(
                  getCurrentEntityPolicies({
                    init: true,
                    metalake,
                    metadataObjectType: 'topic',
                    metadataObjectFullName: `${catalog}.${schema}.${topic}`,
                    details: true
                  })
                )
                break
              case 'model':
                store.models.length === 0 &&
                  (await dispatch(fetchModels({ init: true, page: 'schemas', metalake, catalog, schema })))
                await dispatch(fetchModelVersions({ init: true, metalake, catalog, schema, model }))
                await dispatch(setActivatedDetailsLoading(true))
                await dispatch(getModelDetails({ init: true, metalake, catalog, schema, model }))
                await dispatch(setActivatedDetailsLoading(false))
                dispatch(
                  getCurrentEntityTags({
                    init: true,
                    metalake,
                    metadataObjectType: 'model',
                    metadataObjectFullName: `${catalog}.${schema}.${model}`,
                    details: true
                  })
                )
                dispatch(
                  getCurrentEntityPolicies({
                    init: true,
                    metalake,
                    metadataObjectType: 'model',
                    metadataObjectFullName: `${catalog}.${schema}.${model}`,
                    details: true
                  })
                )
                break
              default:
                break
            }
          }
          if (func) {
            store.functions.length === 0 && (await dispatch(fetchFunctions({ init: true, metalake, catalog, schema })))
            await dispatch(setActivatedDetailsLoading(true))
            await dispatch(getFunctionDetails({ init: true, metalake, catalog, schema, functionName: func }))
            await dispatch(setActivatedDetailsLoading(false))
          }
        }
        if (paramsSize === 6 && version) {
          if (!store.catalogs.length) {
            await dispatch(fetchCatalogs({ metalake }))
            await dispatch(fetchSchemas({ metalake, catalog, catalogType }))
            await dispatch(fetchModels({ init: true, page: 'schemas', metalake, catalog, schema }))
          }
          await dispatch(setActivatedDetailsLoading(true))
          await dispatch(getVersionDetails({ init: true, metalake, catalog, schema, model, version }))
          await dispatch(setActivatedDetailsLoading(false))
        }
      }
    }
    fetchDependsData()

    dispatch(setSelectedNodes(routeParams.catalog ? [buildNodePath(routeParams)] : []))
  }, [searchParams, dispatch])

  return (
    <SectionContainer classProps='flex w-full'>
      <Splitter>
        <Splitter.Panel defaultSize='380' min='10%' max='50%'>
          <div className='h-full bg-white p-6'>
            <TreeComponent ref={treeRef} />
          </div>
        </Splitter.Panel>
        <Splitter.Panel>
          <div className='h-full bg-white'>
            <TreeRefContext.Provider value={treeRef}>
              <RightContent />
            </TreeRefContext.Provider>
          </div>
        </Splitter.Panel>
      </Splitter>
    </SectionContainer>
  )
}

export default CatalogsListPage
