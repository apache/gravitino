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

import { useContext, useEffect, useMemo, useState } from 'react'
import dynamic from 'next/dynamic'
import { ExclamationCircleFilled, PlusOutlined } from '@ant-design/icons'
import {
  Breadcrumb,
  Button,
  Divider,
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
import { useAppSelector, useAppDispatch } from '@/lib/hooks/useStore'
import { useSearchParams } from 'next/navigation'
import { TreeRefContext } from '../../page'
import AssociatedTable from '@/components/AssociatedTable'
import ConfirmInput from '@/components/ConfirmInput'
import Tags from '@/components/CustomTags'
import Icons from '@/components/Icons'
import Policies from '@/components/PolicyTag'
import TableActions from '@/components/TableActions'
import PropertiesContent from '@/components/PropertiesContent'
import buildSchemaColumns from './SharedSchemaColumns'
import {
  getCatalogDetails,
  fetchSchemas,
  deleteSchema,
  deleteFileset,
  deleteModel,
  deleteTopic,
  deleteTable,
  deleteView,
  getTableDetails,
  getCurrentEntityOwner
} from '@/lib/store/metalakes'
import Link from 'next/link'
import { cn } from '@/lib/utils/tailwind'
import { to } from '@/lib/utils'
import Loading from '@/components/Loading'
import CreateSchemaDialog from '../CreateSchemaDialog'
import CreateFilesetDialog from '../CreateFilesetDialog'
import RegisterModelDialog from '../RegisterModelDialog'
import CreateTopicDialog from '../CreateTopicDialog'
import CreateTableDialog from '../CreateTableDialog'
import Functions from './Functions'

const SetOwnerDialog = dynamic(() => import('@/components/SetOwnerDialog'), {
  loading: () => <Loading />,
  ssr: false
})

const { Title, Paragraph } = Typography
const { Search } = Input

export default function SchemaDetailsPage() {
  const [openSchema, setOpenSchema] = useState(false)
  const [editSchemaName, setEditSchemaName] = useState('')
  const [editSchemaInit, setEditSchemaInit] = useState(true)
  const [openTable, setOpenTable] = useState(false)
  const [openFileset, setOpenFileset] = useState(false)
  const [openTopic, setOpenTopic] = useState(false)
  const [openOwner, setOpenOwner] = useState(false)
  const [openModel, setOpenModel] = useState(false)
  const [editTable, setEditTable] = useState('')
  const [editFileset, setEditFileset] = useState('')
  const [editTopic, setEditTopic] = useState('')
  const [editModel, setEditModel] = useState('')
  const [metadataObjectType, setMetadataObjectType] = useState('')
  const [metadataObjectFullName, setMetadataObjectFullName] = useState('')
  const auth = useAppSelector(state => state.auth)
  const { anthEnable, systemConfig } = auth
  const searchParams = useSearchParams()
  const currentMetalake = searchParams.get('metalake')
  const catalogType = searchParams.get('catalogType')
  const catalog = searchParams.get('catalog')
  const schema = searchParams.get('schema')
  const store = useAppSelector(state => state.metalakes)
  const dispatch = useAppDispatch()
  const [search, setSearch] = useState('')
  const [tabOptions, setTabOptions] = useState([])
  const [createBtn, setCreateBtn] = useState('')
  const [entityType, setEntityType] = useState('')
  const [tabKey, setTabKey] = useState('')
  const [nameCol, setNameCol] = useState('')
  const [subSchemas, setSubSchemas] = useState([])
  const { ref, width } = useResizeObserver()
  const treeRef = useContext(TreeRefContext)
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
          metadataObjectType: 'schema',
          metadataObjectFullName: `${catalog}.${schema}`
        })
      )
      setOwnerData(payload?.owner)
    }
    anthEnable && getOwnerData()
  }, [anthEnable])

  useEffect(() => {
    if (!currentMetalake || !catalog || !schema) {
      setSubSchemas([])

      return
    }

    const loadSubSchemas = async () => {
      const [err, res] = await to(
        dispatch(fetchSchemas({ metalake: currentMetalake, catalog, catalogType, parentSchema: schema }))
      )
      if (err || !res) {
        setSubSchemas([])

        return
      }

      const { schemas = [] } = res?.payload || {}

      const nextSubSchemas = schemas.map(item => {
        return {
          ...item,
          name: item.name,
          key: item.name,
          title: item.name
        }
      })

      setSubSchemas(nextSubSchemas)
    }

    loadSubSchemas()
  }, [currentMetalake, catalog, schema])

  useEffect(() => {
    const hasSubSchemas = subSchemas.length > 0
    const withSubSchemas = tabs => (hasSubSchemas ? [{ label: 'Subschemas', key: 'Subschemas' }, ...tabs] : tabs)

    let nextTabOptions = []
    let nextCreateBtn = ''
    let nextNameCol = ''
    let nextEntityType = ''

    switch (catalogType) {
      case 'relational':
        nextTabOptions = withSubSchemas(
          anthEnable
            ? [
                { label: 'Tables', key: 'Tables' },
                { label: 'Views', key: 'Views' },
                { label: 'Functions', key: 'Functions' },
                { label: 'Associated Roles', key: 'Associated Roles' }
              ]
            : [
                { label: 'Tables', key: 'Tables' },
                { label: 'Views', key: 'Views' },
                { label: 'Functions', key: 'Functions' }
              ]
        )
        nextCreateBtn = 'Create Table'
        nextNameCol = 'Table Name'
        nextEntityType = 'table'
        break
      case 'messaging':
        nextTabOptions = withSubSchemas(
          anthEnable
            ? [
                { label: 'Topics', key: 'Topics' },
                { label: 'Functions', key: 'Functions' },
                { label: 'Associated Roles', key: 'Associated Roles' }
              ]
            : [
                { label: 'Topics', key: 'Topics' },
                { label: 'Functions', key: 'Functions' }
              ]
        )
        nextCreateBtn = 'Create Topic'
        nextNameCol = 'Topic Name'
        nextEntityType = 'topic'
        break
      case 'fileset':
        nextTabOptions = withSubSchemas(
          anthEnable
            ? [
                { label: 'Filesets', key: 'Filesets' },
                { label: 'Functions', key: 'Functions' },
                { label: 'Associated Roles', key: 'Associated Roles' }
              ]
            : [
                { label: 'Filesets', key: 'Filesets' },
                { label: 'Functions', key: 'Functions' }
              ]
        )
        nextCreateBtn = 'Create Fileset'
        nextNameCol = 'Fileset Name'
        nextEntityType = 'fileset'
        break
      case 'model':
        nextTabOptions = withSubSchemas(
          anthEnable
            ? [
                { label: 'Models', key: 'Models' },
                { label: 'Functions', key: 'Functions' },
                { label: 'Associated Roles', key: 'Associated Roles' }
              ]
            : [
                { label: 'Models', key: 'Models' },
                { label: 'Functions', key: 'Functions' }
              ]
        )
        nextCreateBtn = 'Register Model'
        nextNameCol = 'Model Name'
        nextEntityType = 'model'
        break
      default:
        nextTabOptions = []
        nextCreateBtn = ''
        nextEntityType = ''
    }

    setTabOptions(nextTabOptions)
    setCreateBtn(nextCreateBtn)
    setNameCol(nextNameCol)
    setEntityType(nextEntityType)
  }, [catalogType, anthEnable, subSchemas])

  useEffect(() => {
    const hasSubSchemas = subSchemas.length > 0
    if (hasSubSchemas) {
      setTabKey('Subschemas')
    } else {
      switch (catalogType) {
        case 'relational':
          setTabKey('Tables')
          break
        case 'messaging':
          setTabKey('Topics')
          break
        case 'fileset':
          setTabKey('Filesets')
          break
        case 'model':
          setTabKey('Models')
          break
        default:
          setTabKey('')
      }
    }
  }, [subSchemas, catalogType])

  const onChangeTab = key => {
    setTabKey(key)
  }

  const showDeleteSchemaConfirm = (modal, name) => {
    modal.confirm({
      title: `Are you sure to delete the schema ${name}?`,
      icon: <ExclamationCircleFilled />,
      okText: 'Delete',
      okType: 'danger',
      cancelText: 'Cancel',
      onOk: async () => {
        await dispatch(deleteSchema({ metalake: currentMetalake, catalog, catalogType, schema: name }))

        const [err, res] = await to(
          dispatch(fetchSchemas({ metalake: currentMetalake, catalog, catalogType, parentSchema: schema }))
        )
        if (!err && res) {
          const { schemas = [] } = res.payload || {}
          const nextSubSchemas = schemas.map(item => ({ ...item, name: item.name, key: item.name, title: item.name }))
          setSubSchemas(nextSubSchemas)
        }
        treeRef.current.onLoadData({ key: catalog, nodeType: 'catalog', inUse: 'true' }, true)
      }
    })
  }

  const tableData = [...store.tableData]
    .filter(c => {
      if (search === '') return true

      return c.name.includes(search)
    })
    .map(entity => {
      return {
        ...entity,
        key: entity.name,
        children: undefined
      }
    })

  const viewData = [...(store.views || [])]
    .filter(c => {
      if (search === '') return true

      return c.name.includes(search)
    })
    .map(entity => ({
      ...entity,
      key: entity.name,
      children: undefined
    }))

  const subSchemaData = [...subSchemas]
    .filter(item => {
      if (search === '') return true

      return item.name.includes(search)
    })
    .map(item => ({
      ...item,
      key: item.name,
      children: undefined
    }))

  const tagContent = (
    <div>
      <Tags readOnly={true} metadataObjectType={'schema'} metadataObjectFullName={`${catalog}.${schema}`} />
    </div>
  )

  const policyContent = (
    <div>
      <Policies readOnly={true} metadataObjectType={'schema'} metadataObjectFullName={`${catalog}.${schema}`} />
    </div>
  )
  const properties = store.activatedDetails?.properties
  const propertyContent = <PropertiesContent properties={properties} />

  const onSearchTable = e => {
    const { value } = e.target
    setSearch(value)
  }

  const handleCreate = () => {
    switch (catalogType) {
      case 'messaging':
        setEditTopic('')
        setOpenTopic(true)
        break
      case 'fileset':
        setEditFileset('')
        setOpenFileset(true)
        break
      case 'relational':
        setEditTable('')
        setOpenTable(true)
        break
      case 'model':
        setEditModel('')
        setOpenModel(true)
        break
      default:
        return
    }
  }

  const handleEditSchema = () => {
    setEditSchemaName(schema)
    setEditSchemaInit(true)
    setOpenSchema(true)
  }

  const handleEdit = name => {
    switch (catalogType) {
      case 'messaging':
        setEditTopic(name)
        setOpenTopic(true)
        break
      case 'fileset':
        setEditFileset(name)
        setOpenFileset(true)
        break
      case 'model':
        setEditModel(name)
        setOpenModel(true)
        break
      default:
        setEditTable(name)
        setOpenTable(true)

        return
    }
  }

  const handleSetOwner = (type, name) => {
    switch (type) {
      case 'messaging':
        setMetadataObjectType('topic')
        break
      case 'fileset':
        setMetadataObjectType('fileset')
        break
      case 'relational':
        setMetadataObjectType('table')
        break
      default:
        setMetadataObjectType(type)
        break
    }
    setMetadataObjectFullName(name)
    setOpenOwner(true)
  }

  const showDeleteConfirm = async (modal, entityObj, type) => {
    const { name: entity, storageLocation, type: managedOrExtenalType } = entityObj
    let isManaged = false
    let location = ''
    if (type === 'fileset') {
      isManaged = managedOrExtenalType === 'managed'
      location = storageLocation
    } else if (type === 'table') {
      const {
        payload: { table }
      } = await dispatch(getTableDetails({ metalake: currentMetalake, catalog, schema, table: entity }))
      isManaged = table.properties?.['table-type'] === 'MANAGED_TABLE' && catalogData?.provider === 'hive'
      location = catalogData?.provider === 'hive' ? table.properties?.['location'] : ''
    }

    let confirmInput = ''
    let validateFn = null

    const setConfirmInput = value => {
      confirmInput = value
    }

    const registerValidate = fn => {
      validateFn = fn
    }

    modal.confirm({
      title: `Are you sure to delete the ${type} ${entity}?`,
      icon: <ExclamationCircleFilled />,
      content: (
        <ConfirmInput
          name={entity}
          setConfirmInput={setConfirmInput}
          isManaged={isManaged}
          location={location}
          registerValidate={registerValidate}
        />
      ),
      okText: 'Delete',
      okType: 'danger',
      cancelText: 'Cancel',
      onOk(close) {
        if (validateFn && !validateFn()) return

        const confirmFn = async () => {
          switch (catalogType) {
            case 'messaging':
              await dispatch(deleteTopic({ metalake: currentMetalake, catalog, schema, topic: entity }))
              break
            case 'fileset':
              await dispatch(
                deleteFileset({ metalake: currentMetalake, catalog, catalogType, schema, fileset: entity })
              )
              break
            case 'relational':
              if (type === 'view') {
                await dispatch(deleteView({ metalake: currentMetalake, catalog, schema, view: entity }))
              } else {
                await dispatch(deleteTable({ metalake: currentMetalake, catalog, catalogType, schema, table: entity }))
              }
              break
            case 'model':
              await dispatch(deleteModel({ metalake: currentMetalake, catalog, catalogType, schema, model: entity }))
              break
            default:
              return
          }
          treeRef.current.onLoadData({ key: `${catalog}/${schema}`, nodeType: 'schema' })
          close()
        }
        confirmFn()
      }
    })
  }

  const columns = useMemo(
    () => [
      {
        title: nameCol,
        dataIndex: 'name',
        key: 'name',
        width: 200,
        ellipsis: true,
        sorter: (a, b) => a?.name.toLowerCase().localeCompare(b?.name.toLowerCase()),
        render: name => (
          <Link
            data-refer={`${entityType}-link-${name}`}
            href={`/catalogs?metalake=${encodeURIComponent(currentMetalake)}&catalogType=${catalogType}&catalog=${encodeURIComponent(catalog)}&schema=${encodeURIComponent(schema)}&${entityType}=${encodeURIComponent(name)}`}
          >
            {name}
          </Link>
        )
      },
      {
        title: 'Tags',
        dataIndex: 'tags',
        key: 'tags',
        render: (_, record) =>
          record?.node === entityType ? (
            <Tags
              metadataObjectType={entityType}
              metadataObjectFullName={`${record.namespace.at(-2)}.${record.namespace.at(-1)}.${record.name}`}
            />
          ) : null
      },
      {
        title: 'Policies',
        dataIndex: 'policies',
        key: 'policies',
        render: (_, record) =>
          record?.node === entityType ? (
            <Policies
              metadataObjectType={entityType}
              metadataObjectFullName={`${record.namespace.at(-2)}.${record.namespace.at(-1)}.${record.name}`}
            />
          ) : null
      },
      {
        title: 'Actions',
        dataIndex: 'action',
        key: 'action',
        width: 100,
        render: (_, record) => (
          <TableActions
            name={record.name}
            catalogType={catalogType}
            provider={catalogData?.provider}
            anthEnable={anthEnable}
            handleEdit={() => handleEdit(record.name)}
            showDeleteConfirm={modal => showDeleteConfirm(modal, record, entityType)}
            handleSetOwner={() =>
              handleSetOwner(catalogType, `${record.namespace.at(-2)}.${record.namespace.at(-1)}.${record.name}`)
            }
          />
        )
      }
    ],
    [nameCol, entityType, store.tableLoading, anthEnable, catalogData?.provider]
  )

  const viewColumns = useMemo(
    () => [
      {
        title: 'View Name',
        dataIndex: 'name',
        key: 'name',
        width: 300,
        ellipsis: true,
        sorter: (a, b) => a?.name.toLowerCase().localeCompare(b?.name.toLowerCase()),
        render: name => (
          <Link
            data-refer={`view-link-${name}`}
            href={`/catalogs?metalake=${encodeURIComponent(currentMetalake)}&catalogType=${catalogType}&catalog=${encodeURIComponent(catalog)}&schema=${encodeURIComponent(schema)}&view=${encodeURIComponent(name)}`}
          >
            {name}
          </Link>
        )
      },
      {
        title: 'Actions',
        dataIndex: 'action',
        key: 'action',
        width: 100,
        render: (_, record) => (
          <a data-refer={`delete-view-${record.name}`}>
            <Tooltip title='Delete'>
              <Icons.Trash2Icon className='size-4' onClick={() => showDeleteConfirm(Modal, record, 'view')} />
            </Tooltip>
          </a>
        )
      }
    ],
    [currentMetalake, catalogType, catalog, schema, catalogData?.provider]
  )

  const subSchemaColumns = useMemo(() => {
    return buildSchemaColumns({
      currentMetalake,
      catalog,
      catalogType,
      anthEnable,
      provider: catalogData?.provider,
      handleEditSchema: name => {
        setEditSchemaName(name)
        setEditSchemaInit(false)
        setOpenSchema(true)
      },
      showDeleteConfirm: name => showDeleteSchemaConfirm(Modal, name),
      handleSetOwner
    })
  }, [currentMetalake, catalog, catalogType, anthEnable, catalogData?.provider])

  const { resizableColumns, components, tableWidth } = useAntdColumnResize(() => {
    return { columns, minWidth: 100 }
  }, [columns])

  const {
    resizableColumns: viewResizableColumns,
    components: viewComponents,
    tableWidth: viewTableWidth
  } = useAntdColumnResize(() => {
    return { columns: viewColumns, minWidth: 100 }
  }, [viewColumns])

  const {
    resizableColumns: subSchemaResizableColumns,
    components: subSchemaComponents,
    tableWidth: subSchemaTableWidth
  } = useAntdColumnResize(() => {
    return { columns: subSchemaColumns, minWidth: 100 }
  }, [subSchemaColumns])

  return (
    <div ref={ref}>
      <Spin spinning={store.activatedDetailsLoading}>
        <Flex className='mb-2' gap='small' align='flex-start'>
          <div className='size-8'>
            <Icons.Database className='size-8' />
          </div>
          <div className='grow-1 relative bottom-1'>
            <Title level={3} style={{ marginBottom: '0.125rem' }}>
              <Space>
                <span
                  title={schema}
                  className='min-w-10 truncate'
                  style={{ maxWidth: `calc(${width}px - 56px)`, display: 'inherit' }}
                >
                  {decodeURIComponent(schema)}
                </span>
                <Tooltip title='Edit'>
                  <Icons.Pencil
                    className='relative size-4 hover:cursor-pointer hover:text-defaultPrimary'
                    onClick={handleEditSchema}
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
              <Tooltip title='Owned' placement='top'>
                <Icons.User className='size-4' color='grey' />
              </Tooltip>
              <span>{ownerData?.name || '-'}</span>
              <Tooltip title='Set Owner'>
                <Icons.Pencil
                  className='relative size-3 hover:cursor-pointer hover:text-defaultPrimary'
                  onClick={() => handleSetOwner('schema', `${catalog}.${schema}`)}
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
          {!['jdbc-postgresql', 'lakehouse-paimon', 'kafka', 'jdbc-mysql'].includes(catalogData?.provider) && (
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
          )}
        </Space>
      </Spin>
      <Tabs data-refer='details-tabs' activeKey={tabKey} onChange={onChangeTab} items={tabOptions} />
      {tabKey === 'Associated Roles' ? (
        <AssociatedTable
          metalake={currentMetalake}
          metadataObjectType={'schema'}
          metadataObjectFullName={`${catalog}.${schema}`}
        />
      ) : tabKey === 'Subschemas' ? (
        <>
          <Flex justify='flex-end' className='mb-4'>
            <div className='flex w-1/3 gap-4'>
              <Search name='searchSubSchemaInput' placeholder='Search...' value={search} onChange={onSearchTable} />
            </div>
          </Flex>
          <Table
            data-refer='subschema-list-grid'
            size='small'
            style={{ maxHeight: 'calc(100vh - 30rem)' }}
            scroll={{ x: subSchemaTableWidth, y: 'calc(100vh - 37rem)' }}
            dataSource={subSchemaData}
            pagination={{ position: ['bottomCenter'], showSizeChanger: true }}
            columns={subSchemaResizableColumns}
            components={subSchemaComponents}
          />
        </>
      ) : tabKey === 'Functions' ? (
        <Functions metalake={currentMetalake} catalog={catalog} schema={schema} />
      ) : tabKey === 'Views' ? (
        <>
          <Flex justify='flex-end' className='mb-4'>
            <div className='flex w-1/3 gap-4'>
              <Search name='searchViewInput' placeholder='Search...' value={search} onChange={onSearchTable} />
            </div>
          </Flex>
          <Table
            data-refer='view-list-grid'
            size='small'
            style={{ maxHeight: 'calc(100vh - 30rem)' }}
            scroll={{ x: viewTableWidth, y: 'calc(100vh - 37rem)' }}
            dataSource={viewData}
            pagination={{ position: ['bottomCenter'], showSizeChanger: true }}
            columns={viewResizableColumns}
            components={viewComponents}
          />
        </>
      ) : (
        <>
          <Flex justify='flex-end' className='mb-4'>
            <div
              className={cn('flex gap-4', {
                'w-1/2': catalogData?.provider !== 'lakehouse-hudi',
                'w-1/3': catalogData?.provider === 'lakehouse-hudi'
              })}
            >
              <Search name='searcTableInput' placeholder='Search...' value={search} onChange={onSearchTable} />
              {catalogData?.provider !== 'lakehouse-hudi' && (
                <Button
                  data-refer={`create-${entityType}-btn`}
                  type='primary'
                  icon={<PlusOutlined />}
                  onClick={handleCreate}
                >
                  {createBtn}
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
              <span className='float-right text-xs text-slate-300'>up to 1000 data items</span>
            )}
          </Spin>
          {openTable && (
            <CreateTableDialog
              open={openTable}
              setOpen={setOpenTable}
              metalake={currentMetalake}
              catalog={catalog}
              catalogType={catalogType}
              provider={catalogData?.provider}
              schema={schema}
              editTable={editTable}
              init={false}
              catalogLocation={catalogData?.properties?.location}
              schemaLocation={store.activatedDetails?.properties?.location}
            />
          )}
          {openFileset && (
            <CreateFilesetDialog
              open={openFileset}
              setOpen={setOpenFileset}
              metalake={currentMetalake}
              catalog={catalog}
              catalogType={catalogType}
              schema={schema}
              locationProviders={catalogData?.properties?.['filesystem-providers']?.split(',') || []}
              editFileset={editFileset}
              init={false}
            />
          )}
          {openTopic && (
            <CreateTopicDialog
              open={openTopic}
              setOpen={setOpenTopic}
              metalake={currentMetalake}
              catalog={catalog}
              catalogType={catalogType}
              schema={schema}
              editTopic={editTopic}
              init={false}
            />
          )}
          {openModel && (
            <RegisterModelDialog
              open={openModel}
              setOpen={setOpenModel}
              metalake={currentMetalake}
              catalog={catalog}
              catalogType={catalogType}
              schema={schema}
              editModel={editModel}
              init={false}
            />
          )}
        </>
      )}
      {openSchema && (
        <CreateSchemaDialog
          open={openSchema}
          setOpen={setOpenSchema}
          metalake={currentMetalake}
          catalog={catalog}
          catalogType={catalogType}
          provider={catalogData?.provider}
          locationProviders={catalogData?.properties?.['filesystem-providers']?.split(',') || []}
          editSchema={editSchemaName || schema}
          init={editSchemaInit}
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
                metadataObjectType: 'schema',
                metadataObjectFullName: `${catalog}.${schema}`
              })
            )
            setOwnerData(payload?.owner)
          }}
        />
      )}
    </div>
  )
}
