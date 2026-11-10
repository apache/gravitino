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
import { semanticModelNodeKey } from '@/lib/utils/semanticModel'

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
  fetchViews,
  fetchSemanticModels,
  getFunctionDetails,
  getViewDetails,
  getSemanticModelDetails,
  getMetalakeDetails,
  getCatalogDetails,
  getSchemaDetails,
  getTableDetails,
  getFilesetDetails,
  getTopicDetails,
  getModelDetails,
  setSelectedNodes,
  setActiveCatalogEntity,
  getCurrentEntityTags,
  getCurrentEntityPolicies,
  resetActivatedDetails,
  setActivatedDetailsLoading,
  setLoadedNodes
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
    if (routeParams.semanticModel) {
      return semanticModelNodeKey(routeParams)
    }

    const keys = [
      'metalake',
      'catalog',
      'catalogType',
      'schema',
      'table',
      'fileset',
      'topic',
      'model',
      'function',
      'view',
      'semanticModel'
    ]

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
      view: searchParams.get('view'),
      semanticModel: searchParams.get('semanticModel'),
      version: searchParams.get('version')
    }
    let canceled = false
    const requests = new Set()

    const load = action => {
      if (canceled) return Promise.resolve({})
      const request = dispatch(action)
      if (request.abort) requests.add(request)

      return request
    }
    dispatch(setActiveCatalogEntity(routeParams))
    dispatch(setSelectedNodes(routeParams.catalog ? [buildNodePath(routeParams)] : []))
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
          view,
          semanticModel,
          version
        } = routeParams

        if (metalake) {
          dispatch(fetchTags({ metalake, details: true }))
          dispatch(fetchPolicies({ metalake, details: true }))
        }

        if (paramsSize === 2 && metalake) {
          load(fetchCatalogs({ init: true, page: 'metalakes', metalake }))
          load(getMetalakeDetails({ metalake }))
        }

        if (paramsSize === 3 && catalog) {
          if (catalogType) {
            const catalogKey = `{{${metalake}}}{{${catalog}}}{{${catalogType}}}`
            if (!store.loadedNodes.includes(catalogKey)) {
              await load(setLoadedNodes([...new Set([...store.loadedNodes, catalogKey])]))
            }
          }

          if (!store.catalogs.length) {
            await load(fetchCatalogs({ metalake }))
          }
          await load(setActivatedDetailsLoading(true))
          await load(getCatalogDetails({ init: true, metalake, catalog }))
          await load(setActivatedDetailsLoading(false))
          await load(fetchSchemas({ init: true, page: 'catalogs', metalake, catalog, catalogType }))
          load(
            getCurrentEntityTags({
              init: true,
              metalake,
              metadataObjectType: 'catalog',
              metadataObjectFullName: catalog,
              details: true
            })
          )
          load(
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
          let catalogsList = store.catalogs
          if (!catalogsList.length) {
            const { payload } = await load(fetchCatalogs({ init: true, metalake }))
            catalogsList = payload?.catalogs || []
            await load(fetchSchemas({ init: true, metalake, catalog, catalogType }))
          }
          const currentCatalog = catalogsList.find(c => c.name === catalog)

          const isIcebergJdbcCatalog =
            currentCatalog?.provider === 'lakehouse-iceberg' &&
            currentCatalog?.properties?.['catalog-backend'] === 'jdbc'
          if (isIcebergJdbcCatalog) {
            load(fetchSchemas({ metalake, catalog, catalogType, parentSchema: schema }))
          }
          load(fetchFunctions({ init: true, metalake, catalog, schema }))
          switch (catalogType) {
            case 'relational':
              load(fetchTables({ init: true, page: 'schemas', metalake, catalog, schema }))
              load(fetchViews({ init: true, metalake, catalog, schema }))
              load(fetchSemanticModels({ init: true, metalake, catalog, schema }))
              break
            case 'fileset':
              load(fetchFilesets({ init: true, page: 'schemas', metalake, catalog, schema }))
              break
            case 'messaging':
              load(fetchTopics({ init: true, page: 'schemas', metalake, catalog, schema }))
              break
            case 'model':
              load(fetchModels({ init: true, page: 'schemas', metalake, catalog, schema }))
              break
            default:
              break
          }
          await load(setActivatedDetailsLoading(true))
          await load(getSchemaDetails({ init: true, metalake, catalog, schema }))
          await load(setActivatedDetailsLoading(false))
          load(
            getCurrentEntityTags({
              init: true,
              metalake,
              metadataObjectType: 'schema',
              metadataObjectFullName: `${catalog}.${schema}`,
              details: true
            })
          )
          load(
            getCurrentEntityPolicies({
              init: true,
              metalake,
              metadataObjectType: 'schema',
              metadataObjectFullName: `${catalog}.${schema}`,
              details: true
            })
          )
        }

        if ((paramsSize === 5 || (paramsSize === 6 && model && version)) && catalog && catalogType && schema) {
          if (!store.catalogs.length) {
            await load(fetchCatalogs({ metalake }))
            await load(fetchSchemas({ metalake, catalog, catalogType }))
          }
          if (table || fileset || topic || model) {
            switch (catalogType) {
              case 'relational':
                store.tables.length === 0 &&
                  (await load(fetchTables({ init: true, page: 'schemas', metalake, catalog, schema })))
                await load(resetActivatedDetails())
                await load(setActivatedDetailsLoading(true))
                await load(getTableDetails({ init: true, metalake, catalog, schema, table }))
                await load(setActivatedDetailsLoading(false))
                load(
                  getCurrentEntityTags({
                    init: true,
                    metalake,
                    metadataObjectType: 'table',
                    metadataObjectFullName: `${catalog}.${schema}.${table}`
                  })
                )
                load(
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
                  (await load(fetchFilesets({ init: true, page: 'schemas', metalake, catalog, schema })))
                await load(resetActivatedDetails())
                await load(setActivatedDetailsLoading(true))
                await load(getFilesetDetails({ init: true, metalake, catalog, schema, fileset }))
                await load(setActivatedDetailsLoading(false))
                load(
                  getCurrentEntityTags({
                    init: true,
                    metalake,
                    metadataObjectType: 'fileset',
                    metadataObjectFullName: `${catalog}.${schema}.${fileset}`,
                    details: true
                  })
                )
                load(
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
                  (await load(fetchTopics({ init: true, page: 'schemas', metalake, catalog, schema })))
                await load(setActivatedDetailsLoading(true))
                await load(getTopicDetails({ init: true, metalake, catalog, schema, topic }))
                await load(setActivatedDetailsLoading(false))
                load(
                  getCurrentEntityTags({
                    init: true,
                    metalake,
                    metadataObjectType: 'topic',
                    metadataObjectFullName: `${catalog}.${schema}.${topic}`,
                    details: true
                  })
                )
                load(
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
                  (await load(fetchModels({ init: true, page: 'schemas', metalake, catalog, schema })))
                await load(fetchModelVersions({ init: true, metalake, catalog, schema, model }))
                await load(setActivatedDetailsLoading(true))

                // Version URLs render the parent model header and its version list.
                await load(getModelDetails({ init: true, metalake, catalog, schema, model, version }))
                await load(setActivatedDetailsLoading(false))
                load(
                  getCurrentEntityTags({
                    init: true,
                    metalake,
                    metadataObjectType: 'model',
                    metadataObjectFullName: `${catalog}.${schema}.${model}`,
                    details: true
                  })
                )
                load(
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
            store.functions.length === 0 && (await load(fetchFunctions({ init: true, metalake, catalog, schema })))
            await load(setActivatedDetailsLoading(true))
            await load(getFunctionDetails({ init: true, metalake, catalog, schema, functionName: func }))
            await load(setActivatedDetailsLoading(false))
          }
          if (view) {
            store.views.length === 0 && (await load(fetchViews({ init: true, metalake, catalog, schema })))
            await load(setActivatedDetailsLoading(true))
            await load(getViewDetails({ init: true, metalake, catalog, schema, view }))
            await load(setActivatedDetailsLoading(false))
          }
          if (semanticModel) {
            if (canceled) return
            store.semanticModels.length === 0 &&
              (await load(fetchSemanticModels({ init: true, metalake, catalog, schema })))
            if (canceled) return
            load(resetActivatedDetails())
            load(setActivatedDetailsLoading(true))

            const detailRequest = load(
              getSemanticModelDetails({ init: true, metalake, catalog, schema, semanticModel })
            )
            await detailRequest
            if (canceled) return
            load(setActivatedDetailsLoading(false))
            load(
              getCurrentEntityTags({
                init: true,
                metalake,
                metadataObjectType: 'semantic_model',
                metadataObjectFullName: `${catalog}.${schema}.${semanticModel}`,
                details: true
              })
            )
            load(
              getCurrentEntityPolicies({
                init: true,
                metalake,
                metadataObjectType: 'semantic_model',
                metadataObjectFullName: `${catalog}.${schema}.${semanticModel}`,
                details: true
              })
            )
          }
        }
      }
    }
    fetchDependsData()

    return () => {
      canceled = true
      requests.forEach(request => request.abort())
    }
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
