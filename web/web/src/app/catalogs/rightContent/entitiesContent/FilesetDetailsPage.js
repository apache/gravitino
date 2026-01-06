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

import { useState, useEffect } from 'react'
import dynamic from 'next/dynamic'
import { Divider, Flex, Popover, Space, Spin, Tabs, Tooltip, Typography } from 'antd'
import useResizeObserver from 'use-resize-observer'
import AssociatedTable from '@/components/AssociatedTable'
import Tags from '@/components/CustomTags'
import Policies from '@/components/PolicyTag'
import Icons from '@/components/Icons'
import ListFiles from './ListFiles'
import { cn } from '@/lib/utils/tailwind'
import { useAppSelector, useAppDispatch } from '@/lib/hooks/useStore'
import { useSearchParams } from 'next/navigation'
import { getCatalogDetails } from '@/lib/store/metalakes'
import Loading from '@/components/Loading'
import CreateFilesetDialog from '../CreateFilesetDialog'
import { getCurrentEntityOwner } from '@/lib/store/metalakes'

const SetOwnerDialog = dynamic(() => import('@/components/SetOwnerDialog'), {
  loading: () => <Loading />,
  ssr: false
})

const { Title, Paragraph } = Typography

export default function FilesetDetailsPage({ ...props }) {
  const [open, setOpen] = useState(false)
  const [openOwner, setOpenOwner] = useState(false)
  const [metadataObjectFullName, setMetadataObjectFullName] = useState('')
  const { catalog, schema, fileset } = props.namespaces
  const { locale } = props
  const { ref, width } = useResizeObserver()
  const auth = useAppSelector(state => state.auth)
  const { anthEnable, systemConfig } = auth
  const searchParams = useSearchParams()
  const currentMetalake = searchParams.get('metalake')
  const catalogType = searchParams.get('catalogType')
  const [catalogData, setCatalogData] = useState(null)
  const dispatch = useAppDispatch()
  const store = useAppSelector(state => state.metalakes)
  const [ownerData, setOwnerData] = useState(null)

  useEffect(() => {
    if (!currentMetalake || !catalog) return

    const initLoad = async () => {
      const { payload } = await dispatch(getCatalogDetails({ metalake: currentMetalake, catalog }))
      setCatalogData(payload?.catalog)
    }
    initLoad()
  }, [currentMetalake, catalog, store.activatedDetailsLoading])

  useEffect(() => {
    const getOwnerData = async () => {
      const { payload } = await dispatch(
        getCurrentEntityOwner({
          metalake: currentMetalake,
          metadataObjectType: 'fileset',
          metadataObjectFullName: `${catalog}.${schema}.${fileset}`
        })
      )
      setOwnerData(payload?.owner)
    }
    anthEnable && getOwnerData()
  }, [anthEnable])

  const handleSetOwner = name => {
    setMetadataObjectFullName(name)
    setOpenOwner(true)
  }

  const tagContent = (
    <div>
      <Tags readOnly={true} metadataObjectType={'fileset'} metadataObjectFullName={`${catalog}.${schema}.${fileset}`} />
    </div>
  )

  const policyContent = (
    <div>
      <Policies
        readOnly={true}
        metadataObjectType={'fileset'}
        metadataObjectFullName={`${catalog}.${schema}.${fileset}`}
      />
    </div>
  )

  const properties = store.activatedDetails?.properties

  const propertyContent = () => {
    return (
      <Space.Compact className='max-h-80 overflow-auto'>
        <Space.Compact direction='vertical' className='divide-y border-gray-100'>
          <span className='min-w-24 bg-gray-100 p-1'>Key</span>
          {properties
            ? Object.keys(properties).map((key, index) => (
                <span className='p-1' key={index}>
                  {key}
                </span>
              ))
            : null}
        </Space.Compact>
        <Space.Compact direction='vertical' className='divide-y border-gray-100'>
          <span className='min-w-24 bg-gray-100 p-1'>Value</span>
          {properties
            ? Object.values(properties).map((value, index) => (
                <span className='p-1' key={index}>
                  {value || '-'}
                </span>
              ))
            : null}
        </Space.Compact>
      </Space.Compact>
    )
  }

  const storageLocations = store.activatedDetails?.storageLocations

  const storageLocationContent = () => {
    return (
      <Space.Compact className='max-h-80 overflow-auto'>
        <Space.Compact direction='vertical' className='divide-y border-gray-100'>
          <span className='bg-gray-100 p-1'>Name</span>
          {storageLocations
            ? Object.keys(storageLocations).map((name, index) => (
                <span
                  className={cn('p-1', {
                    'font-bold': name === properties?.['default-location-name']
                  })}
                  key={index}
                >
                  {name}
                </span>
              ))
            : null}
        </Space.Compact>
        <Space.Compact direction='vertical' className='divide-y border-gray-100'>
          <span className='bg-gray-100 p-1'>Location</span>
          {storageLocations
            ? Object.values(storageLocations).map((location, index) => (
                <span className='p-1' key={index}>
                  {location || '-'}
                </span>
              ))
            : null}
        </Space.Compact>
      </Space.Compact>
    )
  }

  const disableFilesystemOps = catalogData?.properties?.['disable-filesystem-ops'] === 'true'
  const [activeTab, setActiveTab] = useState(disableFilesystemOps ? '' : 'files')

  const tabOptions = [
    {
      label: disableFilesystemOps ? (
        <Tooltip title='Filesystem operations are disabled'>
          <span>Files</span>
        </Tooltip>
      ) : (
        <span>Files</span>
      ),
      key: 'files',
      disabled: disableFilesystemOps
    },
    ...(anthEnable ? [{ label: 'Associated roles', key: 'Associated roles' }] : [])
  ]

  const handleEditTable = () => {
    setOpen(true)
  }

  return (
    <>
      <Spin spinning={store.activatedDetailsLoading}>
        <Flex className='mb-2' gap='small' align='flex-start' ref={ref}>
          <div className='size-8'>
            <Icons.iconify icon='bx:file' className='my-icon-large' />
          </div>
          <div className='grow-1 relative bottom-1'>
            <Title level={3} style={{ marginBottom: '0.125rem' }}>
              <Space>
                <span
                  title={fileset}
                  className='min-w-10 truncate'
                  style={{ maxWidth: `calc(${width}px - 56px)`, display: 'inherit' }}
                >
                  {fileset}
                </span>
                <Tooltip title='Edit'>
                  <Icons.Pencil
                    className='relative size-4 hover:cursor-pointer hover:text-defaultPrimary'
                    onClick={handleEditTable}
                  />
                </Tooltip>
              </Space>
            </Title>
            <Paragraph
              type='secondary'
              className='truncate'
              title={store.activatedDetails?.comment}
              style={{ marginBottom: 0, maxWidth: `calc(${width}px - 56px)` }}
            >
              {store.activatedDetails?.comment}
            </Paragraph>
          </div>
        </Flex>
        <Space split={<Divider type='vertical' />} wrap={true} className='mb-2'>
          {anthEnable && (
            <Space size={4}>
              <Tooltip title='Owner' placement='top'>
                <Icons.User className='size-4' color='grey' />
              </Tooltip>
              <span>{ownerData?.name || '-'}</span>
              <Tooltip title='Set Owner'>
                <Icons.Pencil
                  className='relative size-3 hover:cursor-pointer hover:text-defaultPrimary'
                  onClick={() => handleSetOwner(`${catalog}.${schema}.${fileset}`)}
                />
              </Tooltip>
            </Space>
          )}
          <Space size={4}>
            <Tooltip title='Type' placement='top'>
              <Icons.Type className='size-4' color='grey' />
            </Tooltip>
            <span>{store.activatedDetails?.type?.toUpperCase()}</span>
          </Space>
          {storageLocations && Object.keys(storageLocations).length > 0 ? (
            <Space size={4}>
              <Tooltip title='Storage Location(s)' placement='top'>
                <Icons.CloudUpload className='size-4' color='grey' />
              </Tooltip>
              <Popover placement='bottom' title={<span>Storage Location(s)</span>} content={storageLocationContent}>
                <a className='text-defaultPrimary'>{Object.keys(storageLocations)?.length}</a>
              </Popover>
            </Space>
          ) : (
            <Space size={4}>
              <Tooltip title='Storage Location(s)' placement='top'>
                <Icons.CloudUpload className='size-4' color='grey' />
              </Tooltip>
              <a className='text-defaultPrimary'>0</a>
            </Space>
          )}
          <Space size={4}>
            <Tooltip title='Tags' placement='top'>
              <Icons.Tags className='size-4' color='grey' />
            </Tooltip>
            {store.currentEntityTags && store.currentEntityTags?.length > 0 ? (
              <Popover placement='bottom' title={<span>Tags</span>} content={tagContent}>
                <a className='text-defaultPrimary'>{store.currentEntityTags?.length}</a>
              </Popover>
            ) : (
              <a className='text-defaultPrimary'>0</a>
            )}
          </Space>
          <Space size={4}>
            <Tooltip title='Properties' placement='top'>
              <Icons.TableProperties className='size-4' color='grey' />
            </Tooltip>
            {properties && Object.keys(properties).length > 0 ? (
              <Popover placement='bottom' title={<span>Properties</span>} content={propertyContent}>
                <a className='text-defaultPrimary'>{Object.keys(properties)?.length}</a>
              </Popover>
            ) : (
              <a className='text-defaultPrimary'>0</a>
            )}
          </Space>
        </Space>
      </Spin>
      <Tabs items={tabOptions} activeKey={activeTab} onChange={setActiveTab} />
      {activeTab === 'files' && (
        <div data-refer='tab-files-panel'>
          <ListFiles
            metalake={currentMetalake}
            catalog={catalog}
            schema={schema}
            fileset={fileset}
            storageLocations={store.activatedDetails?.storageLocations}
            defaultLocationName={store.activatedDetails?.properties?.['default-location-name']}
          />
        </div>
      )}
      {anthEnable && activeTab === 'Associated roles' && fileset && (
        <AssociatedTable
          metalake={currentMetalake}
          metadataObjectType={'fileset'}
          metadataObjectFullName={`${catalog}.${schema}.${fileset}`}
        />
      )}
      {open && (
        <CreateFilesetDialog
          open={open}
          setOpen={setOpen}
          metalake={currentMetalake}
          catalog={catalog}
          schema={schema}
          editFileset={fileset}
          init={true}
        />
      )}
      {openOwner && (
        <SetOwnerDialog
          open={openOwner}
          setOpen={setOpenOwner}
          metalake={currentMetalake}
          metadataObjectType={'fileset'}
          metadataObjectFullName={metadataObjectFullName}
          mutateOwner={async () => {
            const { payload } = await dispatch(
              getCurrentEntityOwner({
                metalake: currentMetalake,
                metadataObjectType: 'fileset',
                metadataObjectFullName: `${catalog}.${schema}.${fileset}`
              })
            )
            setOwnerData(payload?.owner)
          }}
        />
      )}
    </>
  )
}
