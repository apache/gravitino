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

import { useEffect, useState } from 'react'
import Link from 'next/link'
import { Box, Button, IconButton } from '@mui/material'
import Icon from '@/components/Icon'
import MetalakePath from './MetalakePath'
import CreateCatalogDialog from './CreateCatalogDialog'
import CreateSchemaDialog from './CreateSchemaDialog'
import CreateFilesetDialog from './CreateFilesetDialog'
import CreateTopicDialog from './CreateTopicDialog'
import CreateTableDialog from './CreateTableDialog'
import TabsContent from './tabsContent/TabsContent'
import { useSearchParams } from 'next/navigation'
import { useAppSelector } from '@/lib/hooks/useStore'

const RightContent = () => {
  const [open, setOpen] = useState(false)
  const [openSchema, setOpenSchema] = useState(false)
  const [openFileset, setOpenFileset] = useState(false)
  const [openTopic, setOpenTopic] = useState(false)
  const [openTable, setOpenTable] = useState(false)
  const searchParams = useSearchParams()
  const [isShowBtn, setBtnVisible] = useState(true)
  const [isShowSchemaBtn, setSchemaBtnVisible] = useState(false)
  const [isShowFilesetBtn, setFilesetBtnVisible] = useState(false)
  const [isShowTopicBtn, setTopicBtnVisible] = useState(false)
  const [isShowTableBtn, setTableBtnVisible] = useState(false)
  const store = useAppSelector(state => state.metalakes)

  const handleCreateCatalog = () => {
    setOpen(true)
  }

  const handleCreateSchema = () => {
    setOpenSchema(true)
  }

  const handleCreateFileset = () => {
    setOpenFileset(true)
  }

  const handleCreateTopic = () => {
    setOpenTopic(true)
  }

  const handleCreateTable = () => {
    setOpenTable(true)
  }

  useEffect(() => {
    const paramsSize = [...searchParams.keys()].length
    const isCatalogList = paramsSize == 1 && searchParams.get('metalake')
    setBtnVisible(isCatalogList)

    const isFilesetList =
      paramsSize == 4 &&
      searchParams.has('metalake') &&
      searchParams.has('catalog') &&
      searchParams.get('type') === 'fileset' &&
      searchParams.has('schema')
    setFilesetBtnVisible(isFilesetList)

    const isTopicList =
      paramsSize == 4 &&
      searchParams.has('metalake') &&
      searchParams.has('catalog') &&
      searchParams.get('type') === 'messaging' &&
      searchParams.has('schema')
    setTopicBtnVisible(isTopicList)

    if (store.catalogs.length) {
      const currentCatalog = store.catalogs.filter(ca => ca.name === searchParams.get('catalog'))[0]

      const isSchemaList =
        paramsSize == 3 &&
        searchParams.has('metalake') &&
        searchParams.has('catalog') &&
        searchParams.has('type') &&
        !['lakehouse-hudi', 'kafka'].includes(currentCatalog?.provider)
      setSchemaBtnVisible(isSchemaList)

      const isTableList =
        paramsSize == 4 &&
        searchParams.has('metalake') &&
        searchParams.has('catalog') &&
        searchParams.get('type') === 'relational' &&
        searchParams.has('schema') &&
        'lakehouse-hudi' !== currentCatalog?.provider
      setTableBtnVisible(isTableList)
    }
  }, [searchParams, store.catalogs, store.catalogs.length])

  return (
    <Box className={`twc-w-0 twc-grow twc-h-full twc-bg-customs-white twc-overflow-hidden`}>
      <Box
        className={`twc-py-3 twc-px-5 twc-flex twc-items-center twc-justify-between`}
        sx={{
          borderBottom: theme => `1px solid ${theme.palette.divider}`
        }}
      >
        <Box className={`twc-flex twc-items-center twc-flex-1 twc-overflow-hidden twc-mr-2`}>
          <Box className={`twc-flex twc-items-center twc-justify-between twc-w-full`}>
            <Box className={`twc-flex twc-items-center twc-w-full`}>
              <IconButton color='primary' component={Link} href='/metalakes' sx={{ mr: 2 }} data-refer='back-home-btn'>
                <Icon icon='mdi:arrow-left' />
              </IconButton>
              <MetalakePath />
            </Box>
          </Box>
        </Box>

        {isShowBtn && (
          <Box className={`twc-flex twc-items-center`}>
            <Button
              variant='contained'
              startIcon={<Icon icon='mdi:plus-box' />}
              onClick={handleCreateCatalog}
              sx={{ width: 200 }}
              data-refer='create-catalog-btn'
            >
              Create Catalog
            </Button>
            <CreateCatalogDialog open={open} setOpen={setOpen} />
          </Box>
        )}
        {isShowSchemaBtn && (
          <Box className={`twc-flex twc-items-center`}>
            <Button
              variant='contained'
              startIcon={<Icon icon='mdi:plus-box' />}
              onClick={handleCreateSchema}
              sx={{ width: 200 }}
              data-refer='create-schema-btn'
            >
              Create Schema
            </Button>
            <CreateSchemaDialog open={openSchema} setOpen={setOpenSchema} />
          </Box>
        )}
        {isShowFilesetBtn && (
          <Box className={`twc-flex twc-items-center`}>
            <Button
              variant='contained'
              startIcon={<Icon icon='mdi:plus-box' />}
              onClick={handleCreateFileset}
              sx={{ width: 200 }}
              data-refer='create-fileset-btn'
            >
              Create Fileset
            </Button>
            <CreateFilesetDialog open={openFileset} setOpen={setOpenFileset} />
          </Box>
        )}
        {isShowTopicBtn && (
          <Box className={`twc-flex twc-items-center`}>
            <Button
              variant='contained'
              startIcon={<Icon icon='mdi:plus-box' />}
              onClick={handleCreateTopic}
              sx={{ width: 200 }}
              data-refer='create-topic-btn'
            >
              Create Topic
            </Button>
            <CreateTopicDialog open={openTopic} setOpen={setOpenTopic} />
          </Box>
        )}
        {isShowTableBtn && (
          <Box className={`twc-flex twc-items-center`}>
            <Button
              variant='contained'
              startIcon={<Icon icon='mdi:plus-box' />}
              onClick={handleCreateTable}
              sx={{ width: 200 }}
              data-refer='create-table-btn'
            >
              Create Table
            </Button>
            <CreateTableDialog open={openTable} setOpen={setOpenTable} />
          </Box>
        )}
      </Box>

      <Box sx={{ height: 'calc(100% - 4.1rem)' }}>
        <TabsContent />
      </Box>
    </Box>
  )
}

export default RightContent
