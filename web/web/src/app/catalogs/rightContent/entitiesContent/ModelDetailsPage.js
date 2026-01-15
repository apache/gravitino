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

import { createContext, useMemo, useState, useEffect } from 'react'
import dynamic from 'next/dynamic'
import { ExclamationCircleFilled, PlusOutlined } from '@ant-design/icons'
import {
  Button,
  Divider,
  Drawer,
  Empty,
  Flex,
  Input,
  Modal,
  Popover,
  Space,
  Spin,
  Table,
  Tabs,
  Tooltip,
  Typography
} from 'antd'
import { useAntdColumnResize } from 'react-antd-column-resize'
import useResizeObserver from 'use-resize-observer'

import AssociatedTable from '@/components/AssociatedTable'
import ConfirmInput from '@/components/ConfirmInput'
import Tags from '@/components/CustomTags'
import Policies from '@/components/PolicyTag'
import Icons from '@/components/Icons'
import { cn } from '@/lib/utils/tailwind'
import { useAppSelector, useAppDispatch } from '@/lib/hooks/useStore'
import { useSearchParams } from 'next/navigation'
import Loading from '@/components/Loading'
import RegisterModelDialog from '../RegisterModelDialog'
import LinkVersionDialog from '../LinkVersionDialog'
import { getVersionDetails, deleteVersion, getCurrentEntityOwner } from '@/lib/store/metalakes'

const SetOwnerDialog = dynamic(() => import('@/components/SetOwnerDialog'), {
  loading: () => <Loading />,
  ssr: false
})

const { Title, Paragraph } = Typography
const { Search } = Input

export default function ModelDetailsPage({ ...props }) {
  const [openOwner, setOpenOwner] = useState(false)
  const [metadataObjectFullName, setMetadataObjectFullName] = useState('')
  const [open, setOpen] = useState(false)
  const { catalog, schema, model } = props.namespaces
  const [search, setSearch] = useState('')
  const [activeTab, setActiveTab] = useState('Versions')
  const [modal, contextHolder] = Modal.useModal()
  const { ref, width } = useResizeObserver()
  const [openLinkVersion, setOpenLinkVersion] = useState(false)
  const [editVersion, setEditVersion] = useState('')
  const [openVersion, setOpenVersion] = useState(false)
  const [currentVersion, setCurrentVersion] = useState()
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
          metadataObjectType: 'model',
          metadataObjectFullName: `${catalog}.${schema}.${model}`
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

  const tableData = store.tableData
    ?.filter(v => {
      if (search === '') return true

      return v.toString().includes(search)
    })
    .map(v => {
      return {
        key: v.name,
        version: v.name
      }
    })

  const tagContent = (
    <div>
      <Tags readOnly={true} metadataObjectType={'model'} metadataObjectFullName={`${catalog}.${schema}.${model}`} />
    </div>
  )

  const policyContent = (
    <div>
      <Policies readOnly={true} metadataObjectType={'model'} metadataObjectFullName={`${catalog}.${schema}.${model}`} />
    </div>
  )

  const properties = store.activatedDetails?.properties

  const propertyContent = () => {
    return (
      <Space.Compact className='max-h-80 overflow-auto'>
        <Space.Compact direction='vertical' className='divide-y border-gray-100'>
          <span className='min-w-24 bg-gray-100 p-1'>Key</span>
          {properties
            ? Object.keys(properties).map(key => (
                <span key={key} className='p-1'>
                  {key}
                </span>
              ))
            : null}
        </Space.Compact>
        <Space.Compact direction='vertical' className='divide-y border-gray-100'>
          <span className='min-w-24 bg-gray-100 p-1'>Value</span>
          {properties
            ? Object.entries(properties).map(([key, value]) => (
                <span key={key} className='p-1'>
                  {value || '-'}
                </span>
              ))
            : null}
        </Space.Compact>
      </Space.Compact>
    )
  }

  const tabOptions = [
    { label: 'Versions', key: 'Versions' },
    ...(anthEnable ? [{ label: 'Associated roles', key: 'Associated roles' }] : [])
  ]

  const onChangeTab = key => {
    setActiveTab(key)
  }

  const onSearchTable = e => {
    const { value } = e.target
    setSearch(value)
  }

  const handleLinkVeresion = () => {
    setEditVersion('')
    setOpenLinkVersion(true)
  }

  const handleEditVersion = version => {
    setEditVersion(version)
    setOpenLinkVersion(true)
  }

  const showDeleteConfirm = (NameContext, version, type) => {
    let confirmInput = ''
    let validateFn = null

    const setConfirmInput = value => {
      confirmInput = value
    }

    const registerValidate = fn => {
      validateFn = fn
    }

    modal.confirm({
      title: `Are you sure to delete the ${type} ${version}?`,
      icon: <ExclamationCircleFilled />,
      content: (
        <NameContext.Consumer>
          {name => (
            <ConfirmInput
              name={version.toString()}
              setConfirmInput={setConfirmInput}
              registerValidate={registerValidate}
            />
          )}
        </NameContext.Consumer>
      ),
      okText: 'Delete',
      okType: 'danger',
      cancelText: 'Cancel',
      onOk(close) {
        if (validateFn && !validateFn()) return

        const confirmFn = async () => {
          await dispatch(deleteVersion({ metalake: currentMetalake, catalog, catalogType, schema, model, version }))
          close()
        }
        confirmFn()
      }
    })
  }

  const handleViewVersion = async version => {
    const { payload: modelVersion } = await dispatch(
      getVersionDetails({ metalake: currentMetalake, catalog, catalogType, schema, model, version })
    )
    setCurrentVersion(modelVersion)
    setOpenVersion(true)
  }

  const handleEditModel = () => {
    setOpen(true)
  }

  const onClose = () => {
    setOpenVersion(false)
  }

  const columns = useMemo(
    () => [
      {
        title: 'Version',
        dataIndex: 'version',
        key: 'version',
        ellipsis: true,
        sorter: (a, b) => a.version.toLowerCase().localeCompare(b.version.toLowerCase()),
        render: version => <span>{version}</span>
      },
      {
        title: 'Actions',
        key: 'action',
        width: 100,
        render: (_, record) => {
          const NameContext = createContext(record.version)

          return (
            <div className='flex gap-2'>
              <NameContext.Provider value={record.version}>{contextHolder}</NameContext.Provider>
              <a>
                <Icons.Pencil className='size-4' onClick={() => handleEditVersion(record.version)} />
              </a>
              <a>
                <Icons.Eye className='size-4' onClick={() => handleViewVersion(record.version)} />
              </a>
              <a>
                <Icons.Trash2Icon
                  className='size-4'
                  onClick={() => showDeleteConfirm(NameContext, record.version, 'version')}
                />
              </a>
            </div>
          )
        }
      }
    ],
    [currentMetalake, catalog, schema, model]
  )

  const { resizableColumns, components, tableWidth } = useAntdColumnResize(() => {
    return { columns, minWidth: 100 }
  }, [columns])

  return (
    <>
      <Spin spinning={store.activatedDetailsLoading}>
        <Flex className='mb-2' gap='small' align='flex-start' ref={ref}>
          <div className='size-8'>
            <Icons.iconify icon='mdi:globe-model' className='size-8' />
          </div>
          <div className='grow-1 relative bottom-1'>
            <Title level={3} style={{ marginBottom: '0.125rem' }}>
              <Space>
                <span
                  title={model}
                  className='min-w-10 truncate'
                  style={{ maxWidth: `calc(${width}px - 56px)`, display: 'inherit' }}
                >
                  {model}
                </span>
                <Tooltip title='Edit'>
                  <Icons.Pencil
                    className='relative size-4 hover:cursor-pointer hover:text-defaultPrimary'
                    onClick={handleEditModel}
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
                  onClick={() => handleSetOwner(`${catalog}.${schema}.${model}`)}
                />
              </Tooltip>
            </Space>
          )}
          <Space size={4}>
            <Tooltip title='Tags' placement='top'>
              <Icons.Tags className='size-4' color='grey' />
            </Tooltip>
            {store.currentEntityTags && store.currentEntityTags.length > 0 ? (
              <Popover placement='bottom' title={<span>Tags</span>} content={tagContent}>
                <a className='text-defaultPrimary'>{store.currentEntityTags.length}</a>
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
      <Tabs defaultActiveKey={activeTab} onChange={onChangeTab} items={tabOptions} />
      {activeTab === 'Versions' && (
        <>
          <Flex justify='flex-end' className='mb-4'>
            <div className='flex w-1/2 gap-4'>
              <Search name='searcColumnInput' placeholder='Search...' value={search} onChange={onSearchTable} />
              <Button type='primary' icon={<PlusOutlined />} onClick={handleLinkVeresion}>
                Link Version
              </Button>
            </div>
          </Flex>
          <Spin spinning={store.tableLoading}>
            <Table
              style={{ maxHeight: 'calc(100vh - 30rem)' }}
              scroll={{ x: tableWidth, y: 'calc(100vh - 37rem)' }}
              dataSource={tableData}
              columns={resizableColumns}
              components={components}
              pagination={{ position: ['bottomCenter'], showSizeChanger: true }}
            />
          </Spin>
          {openLinkVersion && (
            <LinkVersionDialog
              open={openLinkVersion}
              setOpen={setOpenLinkVersion}
              metalake={currentMetalake}
              catalog={catalog}
              catalogType={catalogType}
              schema={schema}
              model={model}
              editVersion={editVersion}
              init={true}
            />
          )}
          {openVersion && (
            <Drawer title={`Version ${currentVersion?.version} Details`} onClose={onClose} open={openVersion}>
              <>
                {currentVersion?.uri && (
                  <div className='my-4'>
                    <div className='text-sm text-slate-400'>URI</div>
                    <span className='break-words text-base'>{currentVersion?.uri}</span>
                  </div>
                )}
                {currentVersion?.uris && (
                  <div className='my-4'>
                    <div className='mb-1 text-sm text-slate-400'>URI(s)</div>
                    <Space.Compact className='max-h-80 w-full overflow-auto'>
                      <Space.Compact direction='vertical' className='w-1/2 divide-y border-gray-100'>
                        <span className='bg-gray-100 p-1'>URI Name</span>
                        {currentVersion?.uris
                          ? Object.keys(currentVersion?.uris).map(name => (
                              <span key={name} className='truncate p-1' title={name}>
                                {name}
                              </span>
                            ))
                          : null}
                      </Space.Compact>
                      <Space.Compact direction='vertical' className='w-1/2 divide-y border-gray-100'>
                        <span className='bg-gray-100 p-1'>URI</span>
                        {currentVersion?.uris
                          ? Object.values(currentVersion?.uris).map(uri => (
                              <span key={uri} className='truncate p-1' title={uri}>
                                {uri || '-'}
                              </span>
                            ))
                          : null}
                      </Space.Compact>
                    </Space.Compact>
                  </div>
                )}
                <div className='my-4'>
                  <div className='text-sm text-slate-400'>Aliases</div>
                  <span className='break-words text-base'>
                    {currentVersion?.aliases.length === 1
                      ? currentVersion?.aliases[0]
                      : currentVersion?.aliases.length
                        ? currentVersion?.aliases.join(', ')
                        : '-'}
                  </span>
                </div>
                <div className='my-4'>
                  <div className='text-sm text-slate-400'>Comment</div>
                  <span className='break-words text-base'>{currentVersion?.comment || '-'}</span>
                </div>
                <div className='my-4'>
                  <div className='mb-1 text-sm text-slate-400'>Properties</div>
                  {currentVersion?.properties && Object.keys(currentVersion?.properties).length > 0 ? (
                    <Space.Compact className='max-h-80 w-full overflow-auto'>
                      <Space.Compact direction='vertical' className='w-1/2 divide-y border-gray-100'>
                        <span className='bg-gray-100 p-1'>Key</span>
                        {currentVersion?.properties
                          ? Object.keys(currentVersion?.properties).map(key => (
                              <span key={key} className='truncate p-1' title={key}>
                                {key}
                              </span>
                            ))
                          : null}
                      </Space.Compact>
                      <Space.Compact direction='vertical' className='w-1/2 divide-y border-gray-100'>
                        <span className='bg-gray-100 p-1'>Value</span>
                        {currentVersion?.properties
                          ? Object.values(currentVersion?.properties).map(value => (
                              <span key={value} className='truncate p-1' title={value}>
                                {value || '-'}
                              </span>
                            ))
                          : null}
                      </Space.Compact>
                    </Space.Compact>
                  ) : (
                    <Empty image={Empty.PRESENTED_IMAGE_SIMPLE} />
                  )}
                </div>
              </>
            </Drawer>
          )}
        </>
      )}
      {anthEnable && activeTab === 'Associated roles' && model && (
        <AssociatedTable
          metalake={currentMetalake}
          metadataObjectType={'model'}
          metadataObjectFullName={`${catalog}.${schema}.${model}`}
        />
      )}
      {open && (
        <RegisterModelDialog
          open={open}
          setOpen={setOpen}
          metalake={currentMetalake}
          catalog={catalog}
          schema={schema}
          editModel={model}
          init={true}
        />
      )}
      {openOwner && (
        <SetOwnerDialog
          open={openOwner}
          setOpen={setOpenOwner}
          metalake={currentMetalake}
          metadataObjectType={'model'}
          metadataObjectFullName={metadataObjectFullName}
          mutateOwner={async () => {
            const { payload } = await dispatch(
              getCurrentEntityOwner({
                metalake: currentMetalake,
                metadataObjectType: 'model',
                metadataObjectFullName: `${catalog}.${schema}.${model}`
              })
            )
            setOwnerData(payload?.owner)
          }}
        />
      )}
    </>
  )
}
