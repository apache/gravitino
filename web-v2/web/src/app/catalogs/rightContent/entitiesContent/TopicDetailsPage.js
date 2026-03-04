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
import PropertiesContent from '@/components/PropertiesContent'
import { cn } from '@/lib/utils/tailwind'
import { useAppSelector, useAppDispatch } from '@/lib/hooks/useStore'
import { useSearchParams } from 'next/navigation'
import CreateTopicDialog from '../CreateTopicDialog'
import { getCurrentEntityOwner } from '@/lib/store/metalakes'
import Loading from '@/components/Loading'

const SetOwnerDialog = dynamic(() => import('@/components/SetOwnerDialog'), {
  loading: () => <Loading />,
  ssr: false
})

const { Title, Paragraph } = Typography

export default function TopicDetailsPage({ ...props }) {
  const [open, setOpen] = useState(false)
  const [openOwner, setOpenOwner] = useState(false)
  const [metadataObjectFullName, setMetadataObjectFullName] = useState('')
  const { catalog, schema, topic } = props.namespaces
  const { ref, width } = useResizeObserver()
  const auth = useAppSelector(state => state.auth)
  const { anthEnable, systemConfig } = auth
  const searchParams = useSearchParams()
  const currentMetalake = searchParams.get('metalake')
  const catalogType = searchParams.get('catalogType')
  const dispatch = useAppDispatch()
  const store = useAppSelector(state => state.metalakes)
  const [ownerData, setOwnerData] = useState(null)

  useEffect(() => {
    const getOwnerData = async () => {
      const { payload } = await dispatch(
        getCurrentEntityOwner({
          metalake: currentMetalake,
          metadataObjectType: 'topic',
          metadataObjectFullName: `${catalog}.${schema}.${topic}`
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
      <Tags readOnly={true} metadataObjectType={'topic'} metadataObjectFullName={`${catalog}.${schema}.${topic}`} />
    </div>
  )

  const policyContent = (
    <div>
      <Policies readOnly={true} metadataObjectType={'topic'} metadataObjectFullName={`${catalog}.${schema}.${topic}`} />
    </div>
  )
  const properties = store.activatedDetails?.properties

  const propertyContent = (
    <PropertiesContent properties={properties} dataReferPrefix='props' contentDataRefer='properties-popover-content' />
  )
  const tabOptions = [{ label: 'Associated Roles', key: 'Associated Roles' }]

  const handleEditTable = () => {
    setOpen(true)
  }

  return (
    <>
      <Spin spinning={store.activatedDetailsLoading}>
        <Flex className='mb-2' gap='small' align='flex-start' ref={ref}>
          <div className='size-8'>
            <Icons.iconify icon='material-symbols:topic-outline' className='my-icon-large' />
          </div>
          <div className='grow-1'>
            <Title level={3} style={{ marginBottom: '0.125rem' }}>
              <Space>
                <span
                  title={topic}
                  className='min-w-10 truncate'
                  style={{ maxWidth: `calc(${width}px - 56px)`, display: 'inherit' }}
                >
                  {topic}
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
                  onClick={() => handleSetOwner(`${catalog}.${schema}.${topic}`)}
                />
              </Tooltip>
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
                <a className='text-defaultPrimary' data-refer='properties-link'>
                  {Object.keys(properties)?.length}
                </a>
              </Popover>
            ) : (
              <a className='text-defaultPrimary' data-refer='properties-link'>
                0
              </a>
            )}
          </Space>
        </Space>
      </Spin>
      {anthEnable && <Tabs items={tabOptions} />}
      {anthEnable && topic && (
        <AssociatedTable
          metalake={currentMetalake}
          metadataObjectType={'topic'}
          metadataObjectFullName={`${catalog}.${schema}.${topic}`}
        />
      )}
      {open && (
        <CreateTopicDialog
          open={open}
          setOpen={setOpen}
          metalake={currentMetalake}
          catalog={catalog}
          schema={schema}
          editTopic={topic}
          init={true}
        />
      )}
      {openOwner && (
        <SetOwnerDialog
          open={openOwner}
          setOpen={setOpenOwner}
          metalake={currentMetalake}
          metadataObjectType={'topic'}
          metadataObjectFullName={metadataObjectFullName}
          mutateOwner={async () => {
            const { payload } = await dispatch(
              getCurrentEntityOwner({
                metalake: currentMetalake,
                metadataObjectType: 'topic',
                metadataObjectFullName: `${catalog}.${schema}.${topic}`
              })
            )
            setOwnerData(payload?.owner)
          }}
        />
      )}
    </>
  )
}
