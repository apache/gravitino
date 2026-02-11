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

import { createContext, useContext, useMemo, useState, useEffect } from 'react'
import dynamic from 'next/dynamic'
import { ExclamationCircleFilled, PlusOutlined } from '@ant-design/icons'
import {
  Breadcrumb,
  Button,
  Divider,
  Dropdown,
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
import { TreeRefContext } from '../../page'
import AssociatedTable from '@/components/AssociatedTable'
import ConfirmInput from '@/components/ConfirmInput'
import Tags from '@/components/CustomTags'
import Icons from '@/components/Icons'
import Policies from '@/components/PolicyTag'
import PropertiesContent from '@/components/PropertiesContent'
import { checkCatalogIcon } from '@/config/catalog'
import Link from 'next/link'
import { cn } from '@/lib/utils/tailwind'
import { useAppSelector, useAppDispatch } from '@/lib/hooks/useStore'
import { useSearchParams } from 'next/navigation'
import { deleteSchema, getCurrentEntityOwner } from '@/lib/store/metalakes'
import Loading from '@/components/Loading'
import CreateSchemaDialog from '../CreateSchemaDialog'

const CreateCatalogDialog = dynamic(() => import('../CreateCatalogDialog'), {
  loading: () => <Loading />,
  ssr: false
})

const SetOwnerDialog = dynamic(() => import('@/components/SetOwnerDialog'), {
  loading: () => <Loading />,
  ssr: false
})

const { Title, Paragraph } = Typography
const { Search } = Input

const renderIcon = catalog => {
  const calalogIcon = checkCatalogIcon({ type: catalog?.type, provider: catalog?.provider })
  if (calalogIcon.startsWith('custom-icons-')) {
    switch (calalogIcon) {
      case 'custom-icons-hive':
        return <Icons.hive className='size-8'></Icons.hive>
      case 'custom-icons-doris':
        return <Icons.doris className='size-8'></Icons.doris>
      case 'custom-icons-paimon':
        return <Icons.paimon className='size-8'></Icons.paimon>
      case 'custom-icons-hudi':
        return <Icons.hudi className='size-8'></Icons.hudi>
      case 'custom-icons-hologres':
        return <Icons.hologres className='size-8'></Icons.hologres>
      case 'custom-icons-oceanbase':
        return <Icons.oceanbase className='size-8'></Icons.oceanbase>
      case 'custom-icons-starrocks':
        return <Icons.starrocks className='size-8'></Icons.starrocks>
      case 'custom-icons-lakehouse':
        return <Icons.lakehouse className='size-8'></Icons.lakehouse>
      case 'custom-icons-clickhouse':
        return <Icons.clickhouse className='size-8'></Icons.clickhouse>
    }
  } else {
    return <Icons.iconify icon={calalogIcon} className='size-8' />
  }
}

export default function CatalogDetailsPage() {
  const [openCatalog, setOpenCatalog] = useState(false)
  const [openSchema, setOpenSchema] = useState(false)
  const [openOwner, setOpenOwner] = useState(false)
  const [editSchema, setEditSchema] = useState('')
  const [metadataObjectType, setMetadataObjectType] = useState('')
  const [metadataObjectFullName, setMetadataObjectFullName] = useState('')
  const [modal, contextHolder] = Modal.useModal()
  const [search, setSearch] = useState('')
  const [tabKey, setTabKey] = useState('Schemas')
  const { ref, width } = useResizeObserver()
  const treeRef = useContext(TreeRefContext)
  const auth = useAppSelector(state => state.auth)
  const { anthEnable, systemConfig } = auth
  const searchParams = useSearchParams()
  const currentMetalake = searchParams.get('metalake')
  const catalogType = searchParams.get('catalogType')
  const catalog = searchParams.get('catalog')
  const store = useAppSelector(state => state.metalakes)
  const dispatch = useAppDispatch()
  const [ownerData, setOwnerData] = useState(null)

  useEffect(() => {
    const getOwnerData = async () => {
      const { payload } = await dispatch(
        getCurrentEntityOwner({
          metalake: currentMetalake,
          metadataObjectType: 'catalog',
          metadataObjectFullName: catalog
        })
      )
      setOwnerData(payload?.owner)
    }
    anthEnable && getOwnerData()
  }, [anthEnable])

  const tableData = [...store.tableData]
    .sort((a, b) => new Date(b?.audit?.lastModifiedTime).getTime() - new Date(a?.audit?.lastModifiedTime).getTime())
    .filter(c => {
      if (search === '') return true

      return c.name.includes(search)
    })
    .map(schema => {
      return {
        ...schema,
        key: schema.name,
        children: undefined
      }
    })

  const tagContent = (
    <div>
      <Tags
        readOnly={true}
        metadataObjectType={'catalog'}
        metadataObjectFullName={store.activatedDetails?.name || catalog}
        key={`catalog-${store.activatedDetails?.name || catalog}-tags`}
      />
    </div>
  )

  const policyContent = (
    <div>
      <Policies
        readOnly={true}
        metadataObjectType={'catalog'}
        metadataObjectFullName={store.activatedDetails?.name || catalog}
        key={`catalog-${store.activatedDetails?.name || catalog}-policies`}
      />
    </div>
  )

  const properties = store.activatedDetails?.properties
  const propertyContent = <PropertiesContent properties={properties} />

  const tabOptions = anthEnable
    ? [
        { label: 'Schemas', key: 'Schemas' },
        { label: 'Associated Roles', key: 'Associated Roles' }
      ]
    : [{ label: 'Schemas', key: 'Schemas' }]

  const onChangeTab = key => {
    setTabKey(key)
  }

  const onSearchTable = e => {
    const { value } = e.target
    setSearch(value)
  }

  const handleCreateScheme = () => {
    setEditSchema('')
    setOpenSchema(true)
  }

  const handleEditSchema = schema => {
    setEditSchema(schema)
    setOpenSchema(true)
  }

  const handleEditCatalog = () => {
    setOpenCatalog(true)
  }

  const handleSetOwner = (type, name) => {
    setMetadataObjectType(type)
    setMetadataObjectFullName(name)
    setOpenOwner(true)
  }

  const showDeleteConfirm = (NameContext, schema, type) => {
    let confirmInput = ''
    let validateFn = null

    const setConfirmInput = value => {
      confirmInput = value
    }

    const registerValidate = fn => {
      validateFn = fn
    }

    modal.confirm({
      title: `Are you sure to delete the ${type} ${schema}?`,
      icon: <ExclamationCircleFilled />,
      content: (
        <NameContext.Consumer>
          {name => <ConfirmInput name={schema} setConfirmInput={setConfirmInput} registerValidate={registerValidate} />}
        </NameContext.Consumer>
      ),
      okText: 'Delete',
      okType: 'danger',
      cancelText: 'Cancel',
      onOk(close) {
        if (validateFn && !validateFn()) return

        const confirmFn = async () => {
          await dispatch(deleteSchema({ metalake: currentMetalake, catalog, catalogType, schema }))
          treeRef.current.onLoadData({ key: `${catalog}`, nodeType: 'catalog', inUse: true })
          close()
        }
        confirmFn()
      }
    })
  }

  const columns = useMemo(
    () => [
      {
        title: 'Schema Name',
        dataIndex: 'name',
        key: 'name',
        ellipsis: true,
        sorter: (a, b) => a.name.toLowerCase().localeCompare(b.name.toLowerCase()),
        width: 200,
        render: name => (
          <Link
            data-refer={`schema-link-${name}`}
            href={`/catalogs?metalake=${encodeURIComponent(currentMetalake)}&catalogType=${catalogType}&catalog=${encodeURIComponent(catalog)}&schema=${encodeURIComponent(name)}`}
          >
            {name}
          </Link>
        )
      },
      {
        title: 'Tags',
        dataIndex: 'tags',
        key: 'tags',
        ellipsis: true,
        render: (_, record) =>
          record?.node === 'schema' ? (
            <Tags
              metadataObjectType={'schema'}
              metadataObjectFullName={`${record.namespace.at(-1)}.${record.name}`}
              key={`schema-${record.namespace.at(-1)}.${record.name}-tags`}
            />
          ) : null
      },
      {
        title: 'Policies',
        dataIndex: 'policies',
        key: 'policies',
        ellipsis: true,
        render: (_, record) =>
          record?.node === 'schema' ? (
            <Policies
              metadataObjectType={'schema'}
              metadataObjectFullName={`${record.namespace.at(-1)}.${record.name}`}
              key={`schema-${record.namespace.at(-1)}.${record.name}-policies`}
            />
          ) : null
      },
      ...(store.activatedDetails?.provider !== 'kafka'
        ? [
            {
              title: 'Actions',
              dataIndex: 'action',
              key: 'action',
              width: 100,
              render: (_, record) => {
                const NameContext = createContext(record.name)

                return (
                  <div className='flex gap-2'>
                    <NameContext.Provider value={record.name}>{contextHolder}</NameContext.Provider>
                    <a>
                      <Tooltip title='Edit'>
                        <Icons.Pencil className='size-4' onClick={() => handleEditSchema(record.name)} />
                      </Tooltip>
                    </a>
                    <a>
                      <Tooltip title='Delete'>
                        <Icons.Trash2Icon
                          className='size-4'
                          onClick={() => showDeleteConfirm(NameContext, record.name, 'schema')}
                        />
                      </Tooltip>
                    </a>
                    {anthEnable && (
                      <Dropdown
                        menu={{
                          items: [
                            {
                              label: 'Set Owner',
                              key: 'setOwner'
                            }
                          ],
                          onClick: ({ key }) => {
                            switch (key) {
                              case 'setOwner':
                                handleSetOwner('schema', `${catalog}.${record.name}`)
                                break
                            }
                          }
                        }}
                        trigger={['hover']}
                      >
                        <Tooltip title='Settings'>
                          <a onClick={e => e.preventDefault()}>
                            <Icons.Settings className='size-4' />
                          </a>
                        </Tooltip>
                      </Dropdown>
                    )}
                  </div>
                )
              }
            }
          ]
        : [])
    ],
    [catalogType, anthEnable]
  )

  const { resizableColumns, components, tableWidth } = useAntdColumnResize(() => {
    return { columns, minWidth: 100 }
  }, [columns])

  return (
    <div ref={ref}>
      <Spin spinning={store.activatedDetailsLoading}>
        <Flex className='mb-2' gap='small' align='flex-start'>
          <div className='size-8'>{renderIcon(store.activatedDetails)}</div>
          <div className='grow-1 relative bottom-1'>
            <Title level={3} style={{ marginBottom: '0.125rem' }}>
              <Space>
                <span
                  title={catalog}
                  className={`min-w-10 truncate`}
                  style={{ maxWidth: `calc(${width}px - 56px)`, display: 'inherit' }}
                >
                  {decodeURIComponent(catalog)}
                </span>
                <Tooltip title='Edit'>
                  <Icons.Pencil
                    className='relative size-4 hover:cursor-pointer hover:text-defaultPrimary'
                    onClick={handleEditCatalog}
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
                  onClick={() => handleSetOwner('catalog', catalog)}
                />
              </Tooltip>
            </Space>
          )}
          <Space size={4}>
            <Tooltip title='Type' placement='top'>
              <Icons.Type className='size-4' color='grey' />
            </Tooltip>
            <span>{store.activatedDetails?.type}</span>
          </Space>
          <Space size={4}>
            <Tooltip title='Provider' placement='top'>
              <Icons.CircleParkingIcon className='size-4' color='grey' />
            </Tooltip>
            <span>{store.activatedDetails?.provider}</span>
          </Space>
          <Space size={4}>
            <Tooltip title='Tags' placement='top'>
              <Icons.Tags className='size-4' color='grey' />
            </Tooltip>
            {store.currentEntityTags?.length > 0 ? (
              <Popover placement='bottom' title={<span>Tags</span>} content={tagContent}>
                <a className='text-defaultPrimary'>{store.currentEntityTags?.length}</a>
              </Popover>
            ) : (
              <a className='text-defaultPrimary'>0</a>
            )}
          </Space>
          <Space size={4}>
            <Tooltip title='Policies' placement='top'>
              <Icons.PencilRuler className='size-4' color='grey' />
            </Tooltip>
            {store.currentEntityPolicies?.length > 0 ? (
              <Popover placement='bottom' title={<span>Policies</span>} content={policyContent}>
                <a className='text-defaultPrimary'>{store.currentEntityPolicies?.length}</a>
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
      <Tabs data-refer='details-tabs' defaultActiveKey={tabKey} onChange={onChangeTab} items={tabOptions} />
      {tabKey === 'Schemas' ? (
        <>
          <Flex justify='flex-end' className='mb-4'>
            <div
              className={cn('flex gap-4', {
                'w-1/3': ['kafka', 'lakehouse-hudi'].includes(store.activatedDetails?.provider),
                'w-1/2': !['kafka', 'lakehouse-hudi'].includes(store.activatedDetails?.provider)
              })}
            >
              <Search name='searcSchemaInput' placeholder='Search...' value={search} onChange={onSearchTable} />
              {!['kafka', 'lakehouse-hudi'].includes(store.activatedDetails?.provider) && (
                <Button
                  data-refer='create-schema-btn'
                  type='primary'
                  icon={<PlusOutlined />}
                  onClick={handleCreateScheme}
                >
                  Create Schema
                </Button>
              )}
            </div>
          </Flex>
          <Spin spinning={store.tableLoading}>
            <Table
              data-refer='table-grid'
              size='small'
              style={{ maxHeight: 'calc(100vh - 30rem)' }}
              scroll={{ x: tableWidth, y: 'calc(100vh - 37rem)' }}
              dataSource={tableData}
              pagination={{ position: ['bottomCenter'], showSizeChanger: true }}
              columns={resizableColumns}
              components={components}
            />
            {tableData?.length === 1000 && (
              <span className='float-right text-xs text-slate-300'>{t('common.defaultTotalNum')}</span>
            )}
          </Spin>
          {openSchema && (
            <CreateSchemaDialog
              open={openSchema}
              setOpen={setOpenSchema}
              metalake={currentMetalake}
              catalog={catalog}
              catalogType={catalogType}
              provider={store.activatedDetails?.provider}
              locationProviders={store.activatedDetails?.properties?.['filesystem-providers']?.split(',') || []}
              catalogBackend={store.activatedDetails?.properties?.['catalog-backend']}
              editSchema={editSchema}
            />
          )}
        </>
      ) : (
        <AssociatedTable
          metalake={currentMetalake}
          metadataObjectType={'catalog'}
          metadataObjectFullName={store.activatedDetails?.name}
        />
      )}
      {openCatalog && (
        <CreateCatalogDialog
          open={openCatalog}
          setOpen={setOpenCatalog}
          metalake={currentMetalake}
          editCatalog={catalog}
          catalogType={catalogType}
          systemConfig={systemConfig}
        />
      )}
      {openOwner && (
        <SetOwnerDialog
          open={openOwner}
          setOpen={setOpenOwner}
          metalake={currentMetalake}
          metadataObjectType={metadataObjectType}
          metadataObjectFullName={metadataObjectFullName}
          mutateOwner={async () => {
            const { payload } = await dispatch(
              getCurrentEntityOwner({
                metalake: currentMetalake,
                metadataObjectType: 'catalog',
                metadataObjectFullName: catalog
              })
            )
            setOwnerData(payload?.owner)
          }}
        />
      )}
    </div>
  )
}
