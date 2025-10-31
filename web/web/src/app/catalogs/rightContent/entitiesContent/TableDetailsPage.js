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

import { useMemo, useState, useEffect } from 'react'
import dynamic from 'next/dynamic'
import { Divider, Flex, Input, Popover, Space, Spin, Table, Tabs, Tag, Tooltip, Typography } from 'antd'
import { useAntdColumnResize } from 'react-antd-column-resize'
import useResizeObserver from 'use-resize-observer'
import AssociatedTable from '@/components/AssociatedTable'
import Tags from '@/components/CustomTags'
import Policies from '@/components/PolicyTag'
import DataPreview from './DataPreview'
import Icons from '@/components/Icons'
import { ColumnTypeColorEnum } from '@/config'
import { sanitizeText } from '@/lib/utils/index'
import { cn } from '@/lib/utils/tailwind'
import { useAppSelector, useAppDispatch } from '@/lib/hooks/useStore'
import { useSearchParams } from 'next/navigation'
import { getCatalogDetails, getCurrentEntityOwner } from '@/lib/store/metalakes'
import CreateTableDialog from '../CreateTableDialog'
import Loading from '@/components/Loading'

const SetOwnerDialog = dynamic(() => import('@/components/SetOwnerDialog'), {
  loading: () => <Loading />,
  ssr: false
})

const { Title, Paragraph } = Typography
const { Search } = Input

export default function TableDetailsPage({ ...props }) {
  const [open, setOpen] = useState(false)
  const [openOwner, setOpenOwner] = useState(false)
  const [metadataObjectFullName, setMetadataObjectFullName] = useState('')
  const { catalog, schema, table } = props.namespaces
  const auth = useAppSelector(state => state.auth)
  const { anthEnable, systemConfig } = auth
  const { 'gravitino.datastrato.custom.preview.enable': previewEnable } = systemConfig || {}
  const searchParams = useSearchParams()
  const currentMetalake = searchParams.get('metalake')
  const catalogType = searchParams.get('catalogType')
  const [search, setSearch] = useState('')
  const [tabKey, setTabKey] = useState('Columns')
  const { ref, width } = useResizeObserver()
  const store = useAppSelector(state => state.metalakes)
  const dispatch = useAppDispatch()
  const [catalogData, setCatalogData] = useState(null)
  const [ownerData, setOwnerData] = useState(null)

  useEffect(() => {
    if (!currentMetalake || !catalog) return

    const initLoad = async () => {
      const { payload } = await dispatch(getCatalogDetails({ metalake: currentMetalake, catalog }))
      setCatalogData(payload?.catalog)
    }
    initLoad()
  }, [currentMetalake, catalog, dispatch])

  useEffect(() => {
    const getOwnerData = async () => {
      const { payload } = await dispatch(
        getCurrentEntityOwner({
          metalake: currentMetalake,
          metadataObjectType: 'table',
          metadataObjectFullName: `${catalog}.${schema}.${table}`
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

  const tableData = store.activatedDetails?.columns
    ?.filter(c => {
      if (search === '') return true

      return c.name.includes(search)
    })
    .map(col => {
      return {
        ...col,
        name: col.name,
        key: col.name,
        type: col.type,
        tags: []
      }
    })

  const columnTypeFitlers = store.activatedDetails?.columns
    ?.map(column => (typeof column.type === 'string' ? column.type : column.type.type))
    .filter((value, index, self) => self.indexOf(value) === index)
    .map(t => {
      return {
        text: t,
        value: t
      }
    })

  const columnTypeColor = type => {
    const formatType = typeof type === 'string' ? type.replace(/\(.*\)/, '') : 'objectType'

    return ColumnTypeColorEnum[formatType]
  }

  const tagContent = (
    <div>
      <Tags
        readOnly={true}
        metalake={currentMetalake}
        metadataObjectType={'table'}
        metadataObjectFullName={`${catalog}.${schema}.${table}`}
      />
    </div>
  )

  const policyContent = (
    <div>
      <Policies
        readOnly={true}
        metalake={currentMetalake}
        metadataObjectType={'table'}
        metadataObjectFullName={`${catalog}.${schema}.${table}`}
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
                  {sanitizeText(value) || '-'}
                </span>
              ))
            : null}
        </Space.Compact>
      </Space.Compact>
    )
  }

  const partitioning = store.activatedDetails?.partitioning?.map((i, index) => {
    let fields = i.fieldName || []
    let sub = ''
    let last = i.fieldName

    switch (i.strategy) {
      case 'bucket':
        sub = `[${i.numBuckets}]`
        fields = i.fieldNames
        last = i.fieldNames.map(v => v[0]).join(',')
        break
      case 'truncate':
        sub = `[${i.width}]`
        break
      case 'list':
        fields = i.fieldNames
        last = i.fieldNames.map(v => v[0]).join(',')
        break
      case 'function':
        sub = `[${i.funcName}]`
        fields = i.funcArgs.map(v => v.fieldName)
        last = fields.join(',')
        break
      default:
        break
    }

    return {
      key: `partitioning-${index}`,
      strategy: i.strategy,
      numBuckets: i.numBuckets,
      width: i.width,
      funcName: i.funcName,
      fields,
      text: `${i.strategy}${sub}(${last})`
    }
  })

  const sortOrders = store.activatedDetails?.sortOrders?.map((i, index) => {
    return {
      key: index,
      fields: i.sortTerm.fieldName,
      dir: i.direction,
      no: i.nullOrdering,
      text: `${i.sortTerm.fieldName[0]} ${i.direction} ${i.nullOrdering}`
    }
  })

  const sortOrdersContent = () => {
    return (
      <>
        {sortOrders &&
          sortOrders.map((s, index) => (
            <Space.Compact key={index}>
              <Space.Compact direction='vertical' className='divide-y border-gray-100'>
                <span className='p-1' data-refer='overview-tip-sortOrders-items'>
                  {s.text}
                </span>
              </Space.Compact>
            </Space.Compact>
          ))}
      </>
    )
  }

  const distribution = store.activatedDetails?.distribution?.funcArgs.map((i, index) => {
    return {
      key: `distribution-${index}`,
      fields: i.fieldName,
      number: store.activatedDetails?.distribution?.number,
      strategy: store.activatedDetails?.distribution?.strategy,
      text:
        store.activatedDetails?.distribution?.strategy === ''
          ? ``
          : `${store.activatedDetails?.distribution?.strategy}${
              store.activatedDetails?.distribution?.number === 0
                ? ''
                : `[${store.activatedDetails?.distribution?.number}]`
            }(${i.fieldName.join(',')})`
    }
  })

  const indexes = store.activatedDetails?.indexes?.map((i, index) => {
    return {
      key: `indexes-${index}`,
      fields: i.fieldNames,
      name: i.name,
      indexType: i.indexType,
      text: `${i.name}(${i.fieldNames.map(v => v.join('.')).join(',')})`
    }
  })

  const indexesContent = indexList => {
    return (
      <Space.Compact className='max-h-80 overflow-auto'>
        <Space.Compact direction='vertical' className='divide-y border-gray-100'>
          <span className='min-w-20 bg-gray-100 p-1'>Name</span>
          {indexList?.map((item, idx) => (
            <Tooltip title={item.name} key={`name-${idx}`}>
              <span className='block max-w-52 truncate p-1'>{item.name || '-'}</span>
            </Tooltip>
          ))}
        </Space.Compact>
        <Space.Compact direction='vertical' className='divide-y border-gray-100'>
          <span className='min-w-24 bg-gray-100 p-1'>Type</span>
          {indexList?.map((item, idx) => (
            <Tooltip title={item.indexType} key={`type-${idx}`}>
              <span className='block max-w-28 truncate p-1'>{item.indexType || '-'}</span>
            </Tooltip>
          ))}
        </Space.Compact>
        <Space.Compact direction='vertical' className='divide-y border-gray-100'>
          <span className='min-w-24 bg-gray-100 p-1'>Fields</span>
          {indexList?.map((item, idx) => {
            const fieldsText = item.fields?.map(v => v.join('.')).join(', ') || '-'

            return (
              <Tooltip title={fieldsText} key={`fields-${idx}`}>
                <span className='block max-w-32 truncate p-1'>{fieldsText}</span>
              </Tooltip>
            )
          })}
        </Space.Compact>
      </Space.Compact>
    )
  }

  const OtherPropertiesContent = properties => {
    return (
      <>
        {properties &&
          properties?.map(p => (
            <Space.Compact key={p.key} block={true}>
              <span className='p-1'>{p.text || p.fields}</span>
            </Space.Compact>
          ))}
      </>
    )
  }

  const tablePropertiesContent = (properties, typeKey) => {
    return (
      <>
        {properties &&
          properties.map(p => {
            // Get the first field name for data-refer attribute
            // fields can be: ["column"] or [["column"]] (nested array)
            let fieldName = Array.isArray(p.fields) ? p.fields[0] : p.fields

            // Handle nested array case
            if (Array.isArray(fieldName)) {
              fieldName = fieldName[0]
            }

            return (
              <Space.Compact key={p.key} block={true}>
                <Space.Compact direction='vertical' className='divide-y border-gray-100'>
                  <span className='p-1' data-refer={`tip-${typeKey}-item-${fieldName}`}>
                    {p.text || p.fields}
                  </span>
                </Space.Compact>
              </Space.Compact>
            )
          })}
      </>
    )
  }

  const renderColumnPopover = (title, icon, properties, name, isIndexes = false) => {
    // Check if the column name matches any field in the properties
    // fields can be: ["column"] or [["column"]] (nested array)
    const matchField = fields => {
      if (!Array.isArray(fields)) return false

      return fields.some(v => (Array.isArray(v) ? v.includes(name) : v === name))
    }

    const isFieldColumn = properties.find(i => matchField(i.fields))

    const matchedItems = properties.filter(i => matchField(i.fields))

    // Convert title to camelCase for data-refer attribute (e.g., 'SortOrders' -> 'sortOrders')
    const typeKey = title.charAt(0).toLowerCase() + title.slice(1).replace(' ', '')

    return (
      <>
        {isFieldColumn && (
          <Popover
            placement='bottom'
            title={() => <span>{title}</span>}
            content={isIndexes ? indexesContent(matchedItems) : tablePropertiesContent(matchedItems, typeKey)}
          >
            <span data-refer={`col-icon-${typeKey}-${name}`}>
              <Icons.iconify icon={icon} className='size-4' props={{ color: 'grey' }} />
            </span>
          </Popover>
        )}
      </>
    )
  }

  const tabOptions = [
    { label: 'Columns', key: 'Columns' },
    ...(anthEnable ? [{ label: 'Associated roles', key: 'Associated roles' }] : [])
  ]

  const onChangeTab = key => {
    setTabKey(key)
  }

  const onSearchTable = e => {
    const { value } = e.target
    setSearch(value)
  }

  const handleEditTable = () => {
    setOpen(true)
  }

  const columns = useMemo(
    () => [
      {
        title: 'Column Name',
        dataIndex: 'name',
        key: 'name',
        width: 300,
        ellipsis: true,
        sort: (a, b) => a.name.toLowerCase().localeCompare(b.name.toLowerCase()),
        render: name => (
          <Space>
            <span>{name}</span>
            {partitioning && renderColumnPopover('Partitioning', 'tabler:circle-letter-p', partitioning, name)}
            {sortOrders && renderColumnPopover('SortOrders', 'mdi:letter-s-circle-outline', sortOrders, name)}
            {distribution && renderColumnPopover('Distribution', 'tabler:circle-letter-d', distribution, name)}
            {indexes && renderColumnPopover('Indexes', 'mdi:letter-i-circle-outline', indexes, name, true)}
          </Space>
        )
      },
      {
        title: 'Data Type',
        dataIndex: 'type',
        key: 'type',
        width: 200,
        ellipsis: true,
        filters: columnTypeFitlers,
        onFilter: (value, record) =>
          typeof record.type === 'string' ? record.type.indexOf(value) === 0 : record.type.type.indexOf(value) === 0,
        render: type => <Tag color={columnTypeColor(type)}>{typeof type === 'string' ? type : type.type}</Tag>
      },
      {
        title: 'Tags',
        dataIndex: 'tags',
        key: 'tags',
        render: (_, record) =>
          !record?.node ? (
            <Tags
              metalake={currentMetalake}
              metadataObjectType={'column'}
              metadataObjectFullName={`${catalog}.${schema}.${table}.${record.name}`}
            />
          ) : null
      },
      {
        title: 'Policies',
        dataIndex: 'policies',
        key: 'policies',
        render: (_, record) =>
          !record?.node ? (
            <Policies
              metalake={currentMetalake}
              metadataObjectType={'column'}
              metadataObjectFullName={`${catalog}.${schema}.${table}.${record.name}`}
            />
          ) : null
      }
    ],
    [currentMetalake, catalog, schema, table, store.activatedDetails]
  )

  const { resizableColumns, components, tableWidth } = useAntdColumnResize(() => {
    return { columns, minWidth: 100 }
  }, [columns])

  return (
    <>
      <Spin spinning={store.activatedDetailsLoading}>
        <Flex className='mb-2' gap='small' align='flex-start' ref={ref}>
          <div className='size-8'>
            <Icons.iconify icon='bx:table' className='my-icon-large' />
          </div>
          <div className='grow-1 relative bottom-1'>
            <Title level={3} style={{ marginBottom: '0.125rem' }}>
              <Space>
                <span
                  title={table}
                  className='min-w-10 truncate'
                  style={{ maxWidth: `calc(${width}px - 56px)`, display: 'inherit' }}
                >
                  {table}
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
                  onClick={() => handleSetOwner(`${catalog}.${schema}.${table}`)}
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
            <Tooltip title='Policies' placement='top'>
              <Icons.PencilRuler className='size-4' color='grey' />
            </Tooltip>
            {store.currentEntityPolicies && store.currentEntityPolicies?.length > 0 ? (
              <Popover placement='bottom' title={<span>Policies</span>} content={policyContent}>
                <a className='text-defaultPrimary'>{store.currentEntityPolicies?.length}</a>
              </Popover>
            ) : (
              <a className='text-defaultPrimary'>0</a>
            )}
          </Space>
          {!['jdbc-postgresql'].includes(catalogData?.catalog?.provider) && (
            <Space size={4}>
              <Tooltip title='Properties' placement='top'>
                <Icons.TableProperties className='size-4' color='grey' />
              </Tooltip>
              {properties && Object.keys(properties).length > 0 ? (
                <Popover placement='bottom' title={<span>Properties</span>} content={propertyContent}>
                  <a className='text-defaultPrimary' data-refer='overview-properties-link'>
                    {Object.keys(properties)?.length}
                  </a>
                </Popover>
              ) : (
                <a className='text-defaultPrimary' data-refer='overview-properties-link'>
                  0
                </a>
              )}
            </Space>
          )}
          <Space size={4}>
            <Tooltip title='Partitions' placement='top'>
              <Icons.CircleParking className='size-4' color='grey' />
            </Tooltip>
            {partitioning && partitioning.length > 0 ? (
              <Popover
                placement='bottom'
                title={<span>Partitions</span>}
                content={OtherPropertiesContent(partitioning)}
              >
                <a className='text-defaultPrimary' data-refer='overview-partitions-link'>
                  {partitioning?.length}
                </a>
              </Popover>
            ) : (
              <a className='text-defaultPrimary' data-refer='overview-partitions-link'>
                0
              </a>
            )}
          </Space>
          <Space size={4}>
            <Tooltip title='Sort Orders' placement='top'>
              <Icons.ArrowUpDown className='size-4' color='grey' />
            </Tooltip>
            {sortOrders && sortOrders.length > 0 ? (
              <Popover placement='bottom' title={<span>Sort Orders</span>} content={sortOrdersContent}>
                <a className='text-defaultPrimary' data-refer='overview-sortorders-link'>
                  <span data-refer='overview-sortOrders-items'>
                    {sortOrders.length > 1 ? sortOrders.map(s => s.fields[0]).join(', ') : sortOrders[0].fields[0]}
                  </span>
                </a>
              </Popover>
            ) : (
              <a className='text-defaultPrimary' data-refer='overview-sortorders-link'>
                0
              </a>
            )}
          </Space>
          <Space size={4}>
            <Tooltip title='Distribution' placement='top'>
              <Icons.Layers className='size-4' color='grey' />
            </Tooltip>
            {distribution && distribution.length > 0 ? (
              <Popover
                placement='bottom'
                title={<span>Distribution</span>}
                content={OtherPropertiesContent(distribution)}
              >
                <a className='text-defaultPrimary' data-refer='overview-distribution-link'>
                  {distribution.length}
                </a>
              </Popover>
            ) : (
              <a className='text-defaultPrimary' data-refer='overview-distribution-link'>
                0
              </a>
            )}
          </Space>
          <Space size={4}>
            <Tooltip title='Indexes' placement='top'>
              <Icons.Boxes className='size-4' color='grey' />
            </Tooltip>
            {indexes && indexes.length > 0 ? (
              <Popover placement='bottom' title={<span>Indexes</span>} content={indexesContent(indexes)}>
                <a className='text-defaultPrimary' data-refer='overview-indexes-link'>
                  {indexes.length}
                </a>
              </Popover>
            ) : (
              <a className='text-defaultPrimary' data-refer='overview-indexes-link'>
                0
              </a>
            )}
          </Space>
        </Space>
      </Spin>
      <Tabs data-refer='details-tabs' defaultActiveKey={tabKey} onChange={onChangeTab} items={tabOptions} />
      {tabKey === 'Columns' && (
        <>
          <Flex justify='flex-end' className='mb-4'>
            <div className='flex w-1/3 gap-4'>
              <Search name='searcColumnInput' placeholder='Search...' value={search} onChange={onSearchTable} />
            </div>
          </Flex>
          <Spin spinning={store.activatedDetailsLoading}>
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
          </Spin>
        </>
      )}
      {tabKey === 'Data preview' && (
        <DataPreview
          namespaces={{
            currentMetalake,
            catalog: decodeURIComponent(catalog),
            schema: decodeURIComponent(schema),
            table: decodeURIComponent(table)
          }}
        />
      )}
      {tabKey === 'Associated roles' && anthEnable && (
        <AssociatedTable
          metalake={currentMetalake}
          metadataObjectType={'table'}
          metadataObjectFullName={`${catalog}.${schema}.${table}`}
        />
      )}
      {open && (
        <CreateTableDialog
          open={open}
          setOpen={setOpen}
          metalake={currentMetalake}
          catalog={catalog}
          catalogType={catalogType}
          provider={catalogData?.provider}
          schema={schema}
          editTable={table}
          init={true}
        />
      )}
      {openOwner && (
        <SetOwnerDialog
          open={openOwner}
          setOpen={setOpenOwner}
          metalake={currentMetalake}
          metadataObjectType={'table'}
          metadataObjectFullName={metadataObjectFullName}
          mutateOwner={async () => {
            const { payload } = await dispatch(
              getCurrentEntityOwner({
                metalake: currentMetalake,
                metadataObjectType: 'table',
                metadataObjectFullName: `${catalog}.${schema}.${table}`
              })
            )
            setOwnerData(payload?.owner)
          }}
        />
      )}
    </>
  )
}
