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

/**
 * CreateTableDialog component
 *
 * A dialog component for creating and editing tables in a metalake catalog.
 *
 * Features:
 * - Create new tables or edit existing ones
 * - Configure table name, comment and properties
 * - Add/edit/remove table columns with name, type, nullable, and comment fields
 * - Add/edit/remove custom table properties
 * - Responsive dialog layout
 *
 * Props:
 * @param {boolean} open - Controls dialog visibility
 * @param {function} setOpen - Function to update dialog visibility
 * @param {string} type - Dialog mode: 'create' or 'edit'
 * @param {object} data - Table data for edit mode
 */

'use client'

import React, { useContext, useEffect, useRef, useState } from 'react'

import { PlusOutlined } from '@ant-design/icons'
import { Button, Flex, Form, Input, InputNumber, Modal, Pagination, Select, Spin, Switch, Tabs, Typography } from 'antd'
import { useScrolling } from 'react-use'
import { TreeRefContext } from '../page'
import Icons from '@/components/Icons'
import { useResetFormOnCloseModal } from '@/lib/hooks/use-reset'
import ColumnTypeComponent from '@/components/ColumnTypeComponent'
import RenderPropertiesFormItem from '@/components/EntityPropertiesFormItem'
import { validateMessages, mismatchName } from '@/config'
import {
  ColumnSpesicalType,
  ColumnType,
  ColumnTypeForMysql,
  ColumnTypeSupportAutoIncrement,
  ColumnWithParamType,
  UnsupportColumnType,
  autoIncrementInfoMap,
  defaultValueSupported,
  dialogContentMaxHeigth,
  distributionInfoMap,
  getPropInfo,
  indexesInfoMap,
  partitionInfoMap,
  sortOrdersInfoMap,
  transformsLimitMap
} from '@/config'
import { tableDefaultProps } from '@/config/catalog'
import { nameRegex } from '@/config/regex'
import { capitalizeFirstLetter, genUpdates } from '@/lib/utils'
import { cn } from '@/lib/utils/tailwind'
import { createTable, updateTable, getTableDetails } from '@/lib/store/metalakes'
import { useAppDispatch } from '@/lib/hooks/useStore'

const { Paragraph } = Typography
const { TextArea } = Input

export default function CreateTableDialog({ ...props }) {
  const {
    open,
    setOpen,
    metalake,
    catalog,
    catalogType,
    provider,
    schema,
    editTable,
    init,
    catalogLocation,
    schemaLocation
  } = props
  const [confirmLoading, setConfirmLoading] = useState(false)
  const [isLoading, setIsLoading] = useState(false)
  const [cacheData, setCacheData] = useState()
  const treeRef = useContext(TreeRefContext)
  const scrollRef = useRef(null)
  const loadedRef = useRef(false)
  const scrolling = useScrolling(scrollRef)
  const [bottomShadow, setBottomShadow] = useState(false)
  const [topShadow, setTopShadow] = useState(false)
  const [columnTypes, setColumnTypes] = useState([])

  const [tabOptions, setTabOptions] = useState([
    {
      label: (
        <span className='font-normal text-[rgb(0,0,0,0.88)] before:mr-0.5 before:font-["SimSun"] before:text-[#ff4d4f] before:content-["*"]'>
          Columns
        </span>
      ),
      key: 'columns'
    }
  ])
  const [tabKey, setTabKey] = useState('columns')
  const [form] = Form.useForm()
  const values = Form.useWatch([], form)

  const defaultValues = {
    name: '',
    comment: '',
    columns: [{ id: '', name: '', typeObj: { type: '' }, required: false, comment: '' }],
    properties: []
  }
  const supportProperties = getPropInfo(provider).allowAdd
  const distributionInfo = distributionInfoMap[provider]
  const partitioningInfo = partitionInfoMap[provider]
  const sortOredsInfo = sortOrdersInfoMap[provider]
  const indexesInfo = indexesInfoMap[provider]
  const autoIncrementInfo = autoIncrementInfoMap[provider]
  const isSupportDefaultValue = defaultValueSupported.includes(provider)
  const dispatch = useAppDispatch()

  useResetFormOnCloseModal({
    form,
    open
  })

  const handScroll = () => {
    if (scrollRef.current) {
      const { scrollTop, scrollHeight, clientHeight } = scrollRef.current
      if (scrollHeight > clientHeight + scrollTop) {
        setTopShadow(true)
        setBottomShadow(scrollTop > 0)
      } else if (scrollHeight === clientHeight + scrollTop) {
        setTopShadow(false)
        setBottomShadow(scrollTop > 0)
      }
    }
  }

  useEffect(() => {
    const tabs = [...tabOptions]
    if (partitioningInfo) {
      tabs.push({
        label: <span className='font-normal text-[rgb(0,0,0,0.88)]'>Partitions</span>,
        key: 'partitions'
      })
    }
    if (sortOredsInfo) {
      ;``
      tabs.push({
        label: <span className='font-normal text-[rgb(0,0,0,0.88)]'>Sort Orders</span>,
        key: 'sortOrders'
      })
    }
    if (indexesInfo) {
      tabs.push({
        label: <span className='font-normal text-[rgb(0,0,0,0.88)]'>Indexes</span>,
        key: 'indexes'
      })
    }
    if (distributionInfo) {
      tabs.push({
        label: (
          <span
            className={cn('font-normal text-[rgb(0,0,0,0.88)]', {
              'before:mr-0.5 before:font-["SimSun"] before:text-[#ff4d4f] before:content-["*"]':
                distributionInfo.required
            })}
          >
            Distribution
          </span>
        ),
        key: 'distribution'
      })
    }
    setTabOptions(tabs)
  }, [provider, partitioningInfo, sortOredsInfo, indexesInfo, distributionInfo])

  useEffect(() => {
    scrollRef.current && handScroll()
  }, [scrolling])

  useEffect(() => {
    if (provider === 'lakehouse-iceberg' && values?.distribution?.strategy === 'hash') {
      form.validateFields([['distribution', 'strategy']], { recursive: true })
    }
  }, [values?.distribution?.strategy, provider, values?.partitions])

  useEffect(() => {
    if (provider === 'lakehouse-iceberg' && values?.distribution?.strategy === 'range') {
      form.validateFields([['distribution', 'strategy']], { recursive: true })
    }
  }, [values?.distribution?.strategy, provider, values?.partitions, values?.sortOrders])

  useEffect(() => {
    values?.columns?.forEach((col, index) => {
      if (col?.autoIncrement) {
        form.validateFields([['columns', index, 'autoIncrement']], { recursive: true })
      }
    })
  }, [values?.indexes, values?.columns])

  const getElementType = typeObj => {
    if (typeof typeObj === 'object') {
      const type = typeObj.type
      switch (type) {
        case 'list':
          return {
            element: {
              type: typeof typeObj.elementType === 'string' ? typeObj.elementType : typeObj.elementType.type,
              required: !typeObj.containsNull,
              ...getElementType(typeObj.elementType)
            }
          }
        case 'map':
          return {
            keyObj: {
              type: typeof typeObj.keyType === 'string' ? typeObj.keyType : typeObj.keyType.type,
              required: true,
              ...getElementType(typeObj.keyType)
            },
            valueObj: {
              type: typeof typeObj.valueType === 'string' ? typeObj.valueType : typeObj.valueType.type,
              required: !typeObj.valueContainsNull,
              ...getElementType(typeObj.valueType)
            }
          }
        case 'struct':
          return {
            fieldColumns: typeObj.fields.map(col => {
              return {
                name: col.name,
                typeObj: {
                  type: typeof col.type === 'string' ? col.type : col.type.type,
                  ...getElementType(col.type)
                },
                required: !col.nullable,
                comment: col.comment || ''
              }
            })
          }
        case 'union':
          return {
            types: typeObj.types.map(type => {
              return {
                typeObj: {
                  type: typeof type === 'string' ? type : type.type,
                  ...getElementType(type)
                }
              }
            })
          }
        default:
          return {}
      }
    }

    return {}
  }

  useEffect(() => {
    if (open) {
      if (editTable && !loadedRef.current) {
        loadedRef.current = true

        const initLoad = async () => {
          setIsLoading(true)
          try {
            const { payload } = await dispatch(getTableDetails({ metalake, catalog, schema, table: editTable }))
            const table = payload?.table || {}
            table.columns = table.columns?.map(col => {
              return {
                ...col,
                uniqueId: col.name
              }
            })
            setCacheData(table)
            form.setFieldValue('name', table.name)
            form.setFieldValue('comment', table.comment)
            for (let key in table.columns) {
              form.setFieldValue(['columns', key, 'uniqueId'], table.columns[key].uniqueId)
              form.setFieldValue(['columns', key, 'name'], table.columns[key].name)
              form.setFieldValue(['columns', key, 'required'], !table.columns[key].nullable)
              autoIncrementInfo &&
                form.setFieldValue(['columns', key, 'autoIncrement'], table.columns[key].autoIncrement)
              form.setFieldValue(['columns', key, 'comment'], table.columns[key].comment || '')
              form.setFieldValue(['columns', key, 'isEdit'], true)
              if (typeof table.columns[key].type === 'string') {
                form.setFieldValue(['columns', key, 'typeObj', 'type'], table.columns[key].type)
              } else {
                const type = table.columns[key].type.type
                const elementType = table.columns[key].type?.elementType
                const keyType = table.columns[key].type?.keyType
                const valueType = table.columns[key].type?.valueType
                switch (type) {
                  case 'list':
                    form.setFieldValue(['columns', key, 'typeObj'], {
                      type: table.columns[key].type?.type,
                      element: {
                        type: typeof elementType === 'string' ? elementType : elementType.type,
                        required: !table.columns[key].type?.containsNull,
                        ...getElementType(table.columns[key].type?.elementType)
                      }
                    })
                    break
                  case 'map':
                    form.setFieldValue(['columns', key, 'typeObj'], {
                      type: table.columns[key].type?.type,
                      keyObj: {
                        type: typeof keyType === 'string' ? keyType : keyType.type,
                        required: true,
                        ...getElementType(table.columns[key].type?.keyType)
                      },
                      valueObj: {
                        type: typeof valueType === 'string' ? valueType : valueType.type,
                        required: !table.columns[key].type?.valueContainsNull,
                        ...getElementType(table.columns[key].type?.valueType)
                      }
                    })
                    break
                  case 'struct':
                    form.setFieldValue(['columns', key, 'typeObj'], {
                      type: table.columns[key].type?.type,
                      fieldColumns: table.columns[key].type?.fields.map(col => {
                        return {
                          id: col.name,
                          name: col.name,
                          typeObj: {
                            type: typeof col.type === 'string' ? col.type : col.type.type,
                            ...getElementType(col.type)
                          },
                          required: !col.nullable,
                          comment: col.comment || ''
                        }
                      })
                    })
                    break
                  case 'union':
                    form.setFieldValue(['columns', key, 'typeObj'], {
                      type: table.columns[key].type?.type,
                      types: table.columns[key].type?.types.map(type => {
                        return {
                          typeObj: {
                            type: typeof type === 'string' ? type : type.type,
                            ...getElementType(type)
                          }
                        }
                      })
                    })
                    break
                  default:
                    form.setFieldValue(['columns', key, 'typeObj', 'type'], table.columns[key].type)
                }
              }
              if (table.columns[key].defaultValue) {
                form.setFieldValue(['columns', key, 'defaultValue'], table.columns[key].defaultValue)
              }
            }
            if (table.distribution) {
              form.setFieldValue(['distribution', 'strategy'], table.distribution.strategy)
              form.setFieldValue(['distribution', 'number'], table.distribution.number)
              form.setFieldValue(
                ['distribution', 'field'],
                table.distribution.funcArgs.map(item => item.fieldName[0] || '')
              )
            }
            let idxPartiton = 0
            if (table.partitioning?.length) {
              table.partitioning.forEach(item => {
                const fields = item.fieldName || item.fieldNames.map(f => f[0])
                form.setFieldValue(['partitions', idxPartiton, 'strategy'], item.strategy)
                form.setFieldValue(['partitions', idxPartiton, 'fieldName'], fields)
                idxPartiton++
              })
            }
            let idxSortOrder = 0
            if (table.sortOrders?.length) {
              table.sortOrders.forEach(item => {
                const strategy = item.sortTerm?.type === 'field' ? 'field' : item.sortTerm?.funcName

                const fieldName =
                  item.sortTerm?.type === 'field'
                    ? item.sortTerm.fieldName[0]
                    : item.sortTerm?.funcArgs.find(f => f.fieldName)?.fieldName[0]
                form.setFieldValue(['sortOrders', idxSortOrder, 'strategy'], strategy)
                form.setFieldValue(['sortOrders', idxSortOrder, 'fieldName'], fieldName)
                form.setFieldValue(['sortOrders', idxSortOrder, 'direction'], item.direction)
                form.setFieldValue(['sortOrders', idxSortOrder, 'nullOrdering'], item.nullOrdering)
                idxSortOrder++
              })
            }
            let idxIndex = 0
            if (table.indexes?.length) {
              table.indexes.forEach(item => {
                const fields = item.fieldNames.map(f => f[0])
                form.setFieldValue(['indexes', idxIndex, 'name'], item.name)
                form.setFieldValue(['indexes', idxIndex, 'indexType'], item.indexType)
                form.setFieldValue(['indexes', idxIndex, 'fieldName'], fields)
                idxIndex++
              })
            }
            let idxProperty = 0
            Object.entries(table.properties || {}).forEach(([key, value]) => {
              form.setFieldValue(['properties', idxProperty, 'key'], key)
              form.setFieldValue(['properties', idxProperty, 'value'], value)
              idxProperty++
            })
            handScroll()
            setIsLoading(false)
          } catch (e) {
            console.error(e)
            setIsLoading(false)
          }
        }

        initLoad()
      } else {
        if (tableDefaultProps[provider]) {
          tableDefaultProps[provider].forEach(item => {
            form.setFieldValue(item.key, item.defaultValue)
          })
        }
      }
      if (provider) {
        let columnTypes = [...ColumnType, ...ColumnWithParamType, ...ColumnSpesicalType].filter(
          item => !UnsupportColumnType[provider]?.includes(item)
        )

        if (provider === 'jdbc-mysql') {
          columnTypes = [...columnTypes, ...ColumnTypeForMysql]
        }
        setColumnTypes(columnTypes.sort((a, b) => a.localeCompare(b)))
      }
    }

    // Reset loadedRef when dialog closes
    if (!open) {
      loadedRef.current = false
    }
  }, [open, editTable, catalog, provider, metalake, schema])

  useEffect(() => {
    if (tableDefaultProps[provider] && values?.format) {
      tableDefaultProps[provider].forEach(item => {
        if (['input-format', 'output-format', 'serde-lib'].includes(item.key)) {
          form.setFieldValue(item.key, item.defaultValueOptions[values.format])
        }
      })
    }
  }, [provider, values?.format])

  const getColumnType = typeObj => {
    const { type } = typeObj
    if (!ColumnSpesicalType.includes(type)) {
      return type
    } else {
      switch (type) {
        case 'list':
          return {
            type: 'list',
            containsNull: provider !== 'jdbc-postgresql' && !typeObj?.element?.required,
            elementType: getColumnType(typeObj?.element)
          }
        case 'map':
          return {
            type: 'map',
            valueContainsNull: !typeObj?.valueObj?.required,
            keyType: getColumnType(typeObj?.keyObj),
            valueType: getColumnType(typeObj?.valueObj)
          }
        case 'struct':
          return {
            type: 'struct',
            fields: typeObj?.fieldColumns.map(item => ({
              uniqueId: item.uniqueId || item.name,
              name: item.name,
              type: getColumnType(item.typeObj),
              nullable: !item.required,
              comment: item.comment || ''
            }))
          }
        case 'union':
          return {
            type: 'union',
            types: typeObj?.types.map(item => getColumnType(item.typeObj))
          }
        default:
          return type
      }
    }
  }

  const handleSubmit = e => {
    e.preventDefault()
    form
      .validateFields()
      .then(async () => {
        setConfirmLoading(true)

        const submitData = {
          name: values.name.trim(),
          comment: values.comment,
          tagsToAdd: values.tags,
          columns: values.columns.map(col => {
            const column = {
              uniqueId: col.uniqueId || col.name,
              name: col.name,
              type: getColumnType(col.typeObj),
              nullable: !col.required,
              comment: col.comment || ''
            }
            if (autoIncrementInfo) {
              column['autoIncrement'] = col.autoIncrement
            }
            if (col.defaultValue) {
              switch (col.defaultValue.type) {
                case 'field':
                  column['defaultValue'] = {
                    type: 'field',
                    fieldName: [col.defaultValue?.fieldName]
                  }
                  break
                case 'function':
                  column['defaultValue'] = {
                    type: 'function',
                    funcName: col.defaultValue?.funcName,
                    funcArgs: col.defaultValue?.funcArgs.map(f => {
                      const func = {}
                      if (f.type === 'literal') {
                        func['type'] = 'literal'
                        func['dataType'] = 'string'
                        func['value'] = f.value
                      } else {
                        func['type'] = 'field'
                        func['fieldName'] = [f.fieldName]
                      }

                      return func
                    })
                  }
                  break
                default:
                  column['defaultValue'] = {
                    type: 'literal',
                    dataType: 'string',
                    value: col.defaultValue?.value
                  }
              }
            }

            return column
          }),
          properties:
            values.properties &&
            values.properties.reduce((acc, item) => {
              acc[item.key] = values[item.key] || item.value

              return acc
            }, {})
        }
        if (partitioningInfo) {
          submitData['partitioning'] = values.partitions?.map(p => {
            const field = {}
            if (p.strategy === 'list') {
              field['fieldNames'] = [[p.fieldName]]
            } else if (p.strategy === 'bucket') {
              field['numBuckets'] = p.number
              field['fieldNames'] = [[p.fieldName]]
            } else if (p.strategy === 'truncate') {
              field['width'] = p.number
              field['fieldName'] = [p.fieldName]
            } else {
              field['fieldName'] = [p.fieldName]
            }

            return {
              strategy: p.strategy,
              ...field
            }
          })
        }
        if (sortOredsInfo) {
          submitData['sortOrders'] = values.sortOrders?.map(s => {
            const field = {
              sortTerm: {}
            }
            if (s.strategy !== 'field') {
              field.sortTerm['type'] = 'function'
              field.sortTerm['funcName'] = s.strategy
              field.sortTerm['funcArgs'] = []
              if (['truncate', 'bucket'].includes(s.strategy)) {
                field.sortTerm['funcArgs'] = [
                  {
                    type: 'literal',
                    dataType: 'integer',
                    value: s.number + ''
                  },
                  {
                    type: 'field',
                    fieldName: [s.fieldName]
                  }
                ]
              } else {
                field.sortTerm['funcArgs'] = [
                  {
                    type: 'field',
                    fieldName: [s.fieldName]
                  }
                ]
              }
            } else {
              field.sortTerm = {
                type: s.strategy,
                fieldName: [s.fieldName]
              }
            }
            field['direction'] = s.direction
            field['nullOrdering'] = s.nullOrdering

            return field
          })
        }
        if (indexesInfo) {
          submitData['indexes'] = values.indexes?.map(i => {
            return {
              indexType: i.indexType,
              name: i.name,
              fieldNames: i.fieldName.map(f => [f])
            }
          })
        }
        if (
          distributionInfo &&
          (values?.distribution?.strategy || values?.distribution?.number || values?.distribution?.field)
        ) {
          submitData['distribution'] = {
            strategy: values.distribution?.strategy,
            number: values.distribution?.number || 0,
            funcArgs:
              values.distribution?.field?.map(f => {
                return {
                  type: 'field',
                  fieldName: [f]
                }
              }) || []
          }
        }
        if (editTable) {
          // update table
          const reqData = {
            updates: genUpdates(cacheData, submitData, ['lakehouse-iceberg', 'lakehouse-paimon'].includes(provider))
          }
          if (reqData.updates.length) {
            await dispatch(
              updateTable({ init, metalake, catalog, catalogType, schema, table: cacheData.name, data: reqData })
            )
          }
        } else {
          submitData.columns.forEach(col => {
            delete col.uniqueId
          })
          if (tableDefaultProps[provider]) {
            tableDefaultProps[provider].forEach(item => {
              if (values[item.key]) {
                submitData.properties[item.key] = values[item.key]
              }
            })
          }
          await dispatch(createTable({ data: submitData, metalake, catalog, schema, catalogType }))
        }
        init && treeRef.current.onLoadData({ key: `${catalog}/${schema}`, nodeType: 'schema' })
        setConfirmLoading(false)
        setOpen(false)
      })
      .catch(info => {
        console.error(info)
        const formItem = info?.errorFields?.[0]?.name?.[0]
        if (
          ['columns', 'partitions', 'sortOrders', 'indexes', 'distribution'].includes(formItem) &&
          tabKey !== formItem
        ) {
          onChangeTab(formItem)
        }
        form.scrollToField(formItem)
      })
  }

  const handleCancel = () => {
    setOpen(false)
  }

  const [pageOffset, setPageOffset] = useState(1)

  const onChange = page => {
    setPageOffset(page)
  }

  const renderTableColumns = (fields, subOpt) => {
    const pageSize = 10
    const isShowPagination = fields.length > pageSize

    return (
      <div className='flex flex-col divide-y divide-solid border-b border-solid'>
        <div className={cn('grid grid-cols-10 divide-x divide-solid')}>
          <div
            className={cn('col-span-2 bg-gray-100 p-1 text-center', {
              'col-span-1': isSupportDefaultValue
            })}
          >
            Column
          </div>
          <div
            className={cn('col-span-3 bg-gray-100 p-1 text-center', {
              'col-span-2': autoIncrementInfo
            })}
          >
            Data Type
          </div>
          {isSupportDefaultValue && <div className='col-span-1 bg-gray-100 p-1 text-center'>Default Value</div>}
          <div className='bg-gray-100 p-1 text-center'>Not Null</div>
          {autoIncrementInfo && <div className='bg-gray-100 p-1 text-center'>Auto Increment</div>}
          <div className='col-span-3 bg-gray-100 p-1 text-center'>Comment</div>
          <div className='bg-gray-100 p-1 text-center'>Action</div>
        </div>
        {fields.slice((pageOffset - 1) * pageSize, pageOffset * pageSize).map(subField => {
          const subColumnType = form.getFieldValue('columns')[subField.name]?.typeObj?.type

          return (
            <div key={subField.name}>
              <div className={cn('grid grid-cols-10')}>
                <div
                  className={cn('col-span-2 px-2 py-1', { 'col-span-1': isSupportDefaultValue })}
                  data-refer={`column-name-${subField.name}`}
                >
                  <Form.Item
                    noStyle
                    name={[subField.name, 'name']}
                    label=''
                    rules={[
                      ({ getFieldValue }) => ({
                        validator(_, name) {
                          if (name) {
                            if (!nameRegex.test(name)) {
                              return Promise.reject(new Error(mismatchName))
                            }
                            if (getFieldValue('columns').length) {
                              let names = getFieldValue('columns').map(col => col?.name)
                              names.splice(subField.name, 1)
                              if (names.includes(name)) {
                                return Promise.reject(new Error('The column name already exists!'))
                              }
                            }

                            return Promise.resolve()
                          } else {
                            return Promise.reject(new Error('The column name is required!'))
                          }
                        }
                      })
                    ]}
                  >
                    <Input size='small' />
                  </Form.Item>
                  <Form.Item
                    shouldUpdate={(prevValues, currentValues) => prevValues.columns !== currentValues.columns}
                    noStyle
                  >
                    {() => {
                      const errors = form.getFieldError(['columns', subField.name, 'name'])

                      return errors.length ? <div className='text-red-500'>{errors[0]}</div> : null
                    }}
                  </Form.Item>
                </div>
                <div
                  className={cn('col-span-3 px-2 py-1', { 'col-span-2': autoIncrementInfo })}
                  data-refer={`column-type-${subField.name}`}
                >
                  <ColumnTypeComponent
                    form={form}
                    parentField={['columns']}
                    subField={subField}
                    columnNamespace={[]}
                    columnTypes={columnTypes}
                    provider={provider}
                    disabled={
                      form.getFieldValue('columns')[subField.name]?.isEdit &&
                      ['lakehouse-iceberg', 'lakehouse-paimon'].includes(provider) &&
                      ColumnSpesicalType.includes(subColumnType)
                    }
                  />
                </div>
                {isSupportDefaultValue && (
                  <div className='px-2 py-1'>
                    <Form.Item noStyle name={[subField.name, 'defaultValue', 'value']} label=''>
                      <Input size='small' />
                    </Form.Item>
                  </div>
                )}
                <div className='px-2 py-1'>
                  <Form.Item noStyle name={[subField.name, 'required']} label=''>
                    <Switch size='small' disabled={provider === 'hive'} />
                  </Form.Item>
                </div>
                {autoIncrementInfo && (
                  <div className='px-2 py-1'>
                    <Form.Item
                      noStyle
                      name={[subField.name, 'autoIncrement']}
                      label=''
                      rules={[
                        () => ({
                          validator(_, autoIncrement) {
                            if (autoIncrement) {
                              if (autoIncrementInfo.uniqueCheck) {
                                const names = form
                                  .getFieldValue('indexes')
                                  ?.filter(idx => idx?.indexType?.toLowerCase() === 'unique_key')
                                  .map(col => col?.fieldName)
                                  ?.flat()
                                if (!names?.includes(form.getFieldValue('columns')[subField.name].name)) {
                                  return Promise.reject(
                                    new Error('An auto-increment column requires simultaneously setting a unique index')
                                  )
                                }
                              }
                              if (autoIncrementInfo.notNullCheck) {
                                if (!form.getFieldValue('columns')[subField.name].required) {
                                  return Promise.reject(new Error('An auto-increment column must be not null'))
                                }
                              }
                            }

                            return Promise.resolve()
                          }
                        })
                      ]}
                    >
                      <Switch
                        size='small'
                        disabled={
                          !!editTable ||
                          !ColumnTypeSupportAutoIncrement.includes(values?.columns[subField.name]?.typeObj?.type)
                        }
                      />
                    </Form.Item>
                    <Form.Item
                      shouldUpdate={(prevValues, currentValues) => prevValues.columns !== currentValues.columns}
                      noStyle
                    >
                      {() => {
                        const errors = form.getFieldError(['columns', subField.name, 'autoIncrement'])

                        return errors.length ? <div className='text-red-500'>{errors[0]}</div> : null
                      }}
                    </Form.Item>
                  </div>
                )}
                <div className='col-span-3 px-2 py-1'>
                  <Form.Item noStyle name={[subField.name, 'comment']} label=''>
                    <Input size='small' />
                  </Form.Item>
                </div>
                <div className='px-2 py-1'>
                  <Icons.Minus
                    className={cn('size-4 cursor-pointer text-gray-400 hover:text-defaultPrimary', {
                      'text-gray-100 hover:text-gray-200 cursor-not-allowed': form.getFieldValue('columns').length === 1
                    })}
                    onClick={() => {
                      if (form.getFieldValue('columns').length === 1) return
                      subOpt.remove(subField.name)
                      if (fields.length - 1 === pageOffset * pageSize) {
                        setPageOffset(pageOffset - 1)
                      } else if (fields.length - 1 < pageOffset * pageSize) {
                        const to = Math.ceil(fields.length / pageSize)
                        setPageOffset((fields.length - 1) % pageSize === 0 ? to - 1 : to)
                      }
                    }}
                  />
                </div>
              </div>
            </div>
          )
        })}
        {isShowPagination && (
          <Pagination
            size='small'
            className='justify-end'
            current={pageOffset}
            onChange={onChange}
            pageSize={pageSize}
            total={fields.length}
          />
        )}
        {pageOffset === Math.ceil(fields.length / pageSize) && (
          <div className='text-center'>
            <Button
              type='link'
              icon={<PlusOutlined />}
              onClick={() => {
                subOpt.add()
                if (fields.length === pageOffset * pageSize) {
                  setPageOffset(pageOffset + 1)
                } else if (fields.length > pageOffset * pageSize) {
                  const to = Math.ceil(fields.length / pageSize)
                  setPageOffset(fields.length % pageSize === 0 ? to + 1 : to)
                }
              }}
            >
              Add Column
            </Button>
          </div>
        )}
      </div>
    )
  }

  const renderTablePartitions = (fields, subOpt) => {
    return (
      <div className='flex flex-col divide-y divide-solid border-b border-solid'>
        <div className='grid grid-cols-5 divide-x divide-solid'>
          <div className='col-span-2 bg-gray-100 p-1 text-center'>Field</div>
          <div className='col-span-2 bg-gray-100 p-1 text-center'>Strategy</div>
          <div className='col-span-1 bg-gray-100 p-1 text-center'>Action</div>
        </div>
        {fields.map(subField => (
          <div key={subField.name}>
            <div className='grid grid-cols-5'>
              <div className='col-span-2 px-2 py-1'>
                <Form.Item noStyle name={[subField.name, 'fieldName']} label='Field'>
                  <Select size='small' className='w-full' placeholder='Field' disabled={!!editTable}>
                    {form
                      .getFieldValue('columns')
                      ?.filter(col => col?.name)
                      ?.map(col => (
                        <Select.Option key={col?.name} value={col?.name}>
                          <Flex justify='space-between'>
                            <span>{col?.name}</span>
                            <span>{col?.typeObj?.type}</span>
                          </Flex>
                        </Select.Option>
                      ))}
                  </Select>
                </Form.Item>
              </div>
              <div className='col-span-2 flex gap-2 px-2 py-1'>
                <Form.Item noStyle name={[subField.name, 'strategy']} label='Strategy'>
                  <Select size='small' className='w-full' placeholder='Strategy' disabled={!!editTable}>
                    {partitioningInfo
                      ?.filter(s => {
                        const field = form.getFieldValue(['partitions', subField.name, 'fieldName'])
                        const column = form.getFieldValue('columns').find(c => c?.name === field)
                        const type = column?.typeObj?.type?.split('(')[0]

                        return transformsLimitMap[s]?.includes(type) || !transformsLimitMap[s]
                      })
                      .map(s => (
                        <Select.Option key={s} value={s}>
                          {capitalizeFirstLetter(s)}
                        </Select.Option>
                      ))}
                  </Select>
                </Form.Item>
                {['truncate', 'bucket'].includes(form.getFieldValue(['partitions', subField.name, 'strategy'])) && (
                  <Form.Item noStyle name={[subField.name, 'number']} label='Number' rules={[{ required: true }]}>
                    <InputNumber
                      size='small'
                      min={0}
                      placeholder={
                        form.getFieldValue(['partitions', subField.name, 'strategy']) === 'bucket' ? 'Number' : 'Width'
                      }
                      disabled={!!editTable}
                    />
                  </Form.Item>
                )}
              </div>
              <div className='px-2 py-1'>
                <Icons.Minus
                  className={cn('size-4 cursor-pointer text-gray-400 hover:text-defaultPrimary', {
                    'text-gray-100 hover:text-gray-200 cursor-not-allowed': !!editTable
                  })}
                  onClick={() => {
                    if (!!editTable) return
                    subOpt.remove(subField.name)
                  }}
                />
              </div>
            </div>
          </div>
        ))}
        <div className='text-center'>
          <Button
            type='link'
            icon={<PlusOutlined />}
            disabled={!!editTable}
            onClick={() => {
              subOpt.add()
            }}
          >
            Add Partition
          </Button>
        </div>
      </div>
    )
  }

  const renderTableSortOrders = (fields, subOpt) => {
    return (
      <div className='flex flex-col divide-y divide-solid border-b border-solid'>
        <div className='grid grid-cols-6 divide-x divide-solid'>
          <div className='col-span-1 bg-gray-100 p-1 text-center'>Field</div>
          <div className='col-span-2 bg-gray-100 p-1 text-center'>Strategy</div>
          <div className='col-span-1 bg-gray-100 p-1 text-center'>Direction</div>
          <div className='col-span-1 bg-gray-100 p-1 text-center'>Null Ordering</div>
          <div className='col-span-1 bg-gray-100 p-1 text-center'>Action</div>
        </div>
        {fields.map(subField => (
          <div key={subField.name}>
            <div className='grid grid-cols-6'>
              <div className='col-span-1 px-2 py-1'>
                <Form.Item noStyle name={[subField.name, 'fieldName']} label='Field'>
                  <Select size='small' className='w-full' placeholder='Field' disabled={!!editTable}>
                    {form
                      .getFieldValue('columns')
                      ?.filter(col => col?.name)
                      ?.map(col => (
                        <Select.Option key={col?.name} value={col?.name}>
                          <Flex justify='space-between'>
                            <span>{col?.name}</span>
                            <span>{col?.typeObj?.type}</span>
                          </Flex>
                        </Select.Option>
                      ))}
                  </Select>
                </Form.Item>
              </div>
              <div className='col-span-2 flex gap-2 px-2 py-1'>
                <Form.Item noStyle name={[subField.name, 'strategy']} label='Strategy'>
                  <Select size='small' className='w-full' placeholder='Strategy' disabled={!!editTable}>
                    {sortOredsInfo
                      ?.filter(s => {
                        const field = form.getFieldValue(['sortOrders', subField.name, 'fieldName'])
                        const column = form.getFieldValue('columns').find(c => c?.name === field)
                        const type = column?.typeObj?.type?.split('(')[0]

                        return transformsLimitMap[s]?.includes(type) || !transformsLimitMap[s]
                      })
                      .map(s => (
                        <Select.Option key={s} value={s}>
                          {capitalizeFirstLetter(s)}
                        </Select.Option>
                      ))}
                  </Select>
                </Form.Item>
                {['truncate', 'bucket'].includes(form.getFieldValue(['sortOrders', subField.name, 'strategy'])) && (
                  <Form.Item noStyle name={[subField.name, 'number']} label='Number' rules={[{ required: true }]}>
                    <InputNumber
                      size='small'
                      min={0}
                      placeholder={
                        form.getFieldValue(['sortOrders', subField.name, 'strategy']) === 'bucket' ? 'Number' : 'Width'
                      }
                      disabled={!!editTable}
                    />
                  </Form.Item>
                )}
              </div>
              <div className='col-span-1 px-2 py-1'>
                <Form.Item noStyle name={[subField.name, 'direction']} label='Direction'>
                  <Select size='small' className='w-full' placeholder='Direction' disabled={!!editTable}>
                    <Select.Option key='asc' value='asc'>
                      {t('table.asc')}
                    </Select.Option>
                    <Select.Option key='desc' value='desc'>
                      {t('table.desc')}
                    </Select.Option>
                  </Select>
                </Form.Item>
              </div>
              <div className='col-span-1 px-2 py-1'>
                <Form.Item noStyle name={[subField.name, 'nullOrdering']} label='Null Ordering'>
                  <Select size='small' className='w-full' placeholder='Null Ordering' disabled={!!editTable}>
                    <Select.Option key='nulls_first' value='nulls_first'>
                      Nulls first
                    </Select.Option>
                    <Select.Option key='nulls_last' value='nulls_last'>
                      Nulls last
                    </Select.Option>
                  </Select>
                </Form.Item>
              </div>
              <div className='px-2 py-1'>
                <Icons.Minus
                  className={cn('size-4 cursor-pointer text-gray-400 hover:text-defaultPrimary', {
                    'text-gray-100 hover:text-gray-200 cursor-not-allowed': !!editTable
                  })}
                  onClick={() => {
                    if (!!editTable) return
                    subOpt.remove(subField.name)
                  }}
                />
              </div>
            </div>
          </div>
        ))}
        <div className='text-center'>
          <Button
            type='link'
            icon={<PlusOutlined />}
            disabled={!!editTable}
            onClick={() => {
              subOpt.add()
            }}
          >
            Add Sort Order
          </Button>
        </div>
      </div>
    )
  }

  const renderTableIndexes = (fields, subOpt) => {
    return (
      <div className='flex flex-col divide-y divide-solid border-b border-solid'>
        <div className='grid grid-cols-5 divide-x divide-solid'>
          <div className='col-span-1 bg-gray-100 p-1 text-center'>Index Name</div>
          <div className='col-span-2 bg-gray-100 p-1 text-center'>Field</div>
          <div className='col-span-1 bg-gray-100 p-1 text-center'>Index Type</div>
          <div className='col-span-1 bg-gray-100 p-1 text-center'>Action</div>
        </div>
        {fields.map(subField => (
          <div key={subField.name}>
            <div className='grid grid-cols-5'>
              <div className='col-span-1 px-2 py-1'>
                <Form.Item
                  noStyle
                  name={[subField.name, 'name']}
                  label=''
                  rules={[
                    ({ getFieldValue }) => ({
                      validator(_, name) {
                        if (name) {
                          if (!nameRegex.test(name)) {
                            return Promise.reject(new Error(mismatchName))
                          }
                          if (getFieldValue('indexes').length) {
                            let names = getFieldValue('indexes').map(col => col?.name)
                            names.splice(subField.name, 1)
                            if (names.includes(name)) {
                              return Promise.reject(new Error('The index name already exists!'))
                            }
                          }

                          return Promise.resolve()
                        }
                      }
                    })
                  ]}
                >
                  <Input size='small' placeholder='Index Name' disabled={!!editTable} />
                </Form.Item>
              </div>
              <div className='col-span-2 px-2 py-1'>
                <Form.Item noStyle name={[subField.name, 'fieldName']} label='Field'>
                  <Select size='small' className='w-full' mode='multiple' placeholder='Field' disabled={!!editTable}>
                    {form
                      .getFieldValue('columns')
                      ?.filter(col => col?.name)
                      ?.map(col => (
                        <Select.Option key={col?.name} value={col?.name}>
                          {col?.name}
                        </Select.Option>
                      ))}
                  </Select>
                </Form.Item>
              </div>
              <div className='col-span-1 px-2 py-1'>
                <Form.Item noStyle name={[subField.name, 'indexType']} label='Index Type'>
                  <Select size='small' className='w-full' placeholder='Index Type' disabled={!!editTable}>
                    <Select.Option key='primary_key' value='primary_key'>
                      Primary Key
                    </Select.Option>
                    <Select.Option key='unique_key' value='unique_key'>
                      Unique Key
                    </Select.Option>
                  </Select>
                </Form.Item>
              </div>
              <div className='px-2 py-1'>
                <Icons.Minus
                  className={cn('size-4 cursor-pointer text-gray-400 hover:text-defaultPrimary', {
                    'text-gray-100 hover:text-gray-200 cursor-not-allowed': !!editTable
                  })}
                  onClick={() => {
                    if (!!editTable) return
                    subOpt.remove(subField.name)
                  }}
                />
              </div>
            </div>
          </div>
        ))}
        <div className='text-center'>
          <Button
            type='link'
            icon={<PlusOutlined />}
            disabled={!!editTable}
            onClick={() => {
              subOpt.add()
            }}
          >
            Add Index
          </Button>
        </div>
      </div>
    )
  }

  const onChangeTab = key => {
    setTabKey(key)
  }

  return (
    <>
      <Modal
        title={!editTable ? 'Create Table' : 'Edit Table'}
        open={open}
        onOk={handleSubmit}
        okText='Submit'
        okButtonProps={{ 'data-refer': 'handle-submit-table' }}
        maskClosable={false}
        keyboard={false}
        width={1000}
        confirmLoading={confirmLoading}
        onCancel={handleCancel}
      >
        <Paragraph type='secondary'>
          {!editTable ? 'Create a new table, and define columns for it.' : `Edit the table ${editTable} in ${schema}.`}
        </Paragraph>
        <div
          className={cn('relative', {
            'after:absolute after:-bottom-10 after:left-0 after:right-0 after:h-10 after:shadow-[0px_-10px_8px_-8px_rgba(5,5,5,0.1)]':
              topShadow,
            'before:absolute before:-top-10 before:left-0 before:right-0 before:h-10 before:z-10 before:shadow-[0px_10px_8px_-8px_rgba(5,5,5,0.1)]':
              bottomShadow
          })}
        >
          <div className='overflow-auto' style={{ maxHeight: `${dialogContentMaxHeigth}px` }} ref={scrollRef}>
            <Spin spinning={isLoading}>
              <Form
                form={form}
                initialValues={defaultValues}
                layout='vertical'
                name='tableForm'
                validateMessages={validateMessages}
              >
                <Form.Item
                  name='name'
                  label='Table Name'
                  rules={[{ required: true }, { type: 'string', max: 64 }, { pattern: new RegExp(nameRegex) }]}
                >
                  <Input data-refer='table-name-field' placeholder={mismatchName} disabled={!init} />
                </Form.Item>
                <Tabs activeKey={tabKey} onChange={onChangeTab} items={tabOptions} />
                <Form.Item
                  className={tabKey !== 'columns' ? 'hidden' : ''}
                  label=''
                  name='columns'
                  rules={[{ required: true }]}
                  help=''
                >
                  <Form.List name='columns'>{(fields, subOpt) => renderTableColumns(fields, subOpt)}</Form.List>
                </Form.Item>
                {partitioningInfo && (
                  <Form.Item className={tabKey !== 'partitions' ? 'hidden' : ''} label='' name='partitions'>
                    <Form.List name='partitions'>{(fields, subOpt) => renderTablePartitions(fields, subOpt)}</Form.List>
                  </Form.Item>
                )}
                {sortOredsInfo && (
                  <Form.Item className={tabKey !== 'sortOrders' ? 'hidden' : ''} label='' name='sortOrders'>
                    <Form.List name='sortOrders'>{(fields, subOpt) => renderTableSortOrders(fields, subOpt)}</Form.List>
                  </Form.Item>
                )}
                {indexesInfo && (
                  <Form.Item className={tabKey !== 'indexes' ? 'hidden' : ''} label='' name='indexes'>
                    <Form.List name='indexes'>{(fields, subOpt) => renderTableIndexes(fields, subOpt)}</Form.List>
                  </Form.Item>
                )}
                {distributionInfo && (
                  <Form.Item className={tabKey !== 'distribution' ? 'hidden' : ''} label='' help=''>
                    <div className='flex flex-col divide-y divide-solid border-b border-solid'>
                      <div className='grid grid-cols-4 divide-x divide-solid'>
                        {distributionInfo.funcArgs && (
                          <div className='col-span-2 bg-gray-100 p-1 text-center'>Field</div>
                        )}
                        <div className='col-span-2 bg-gray-100 p-1 text-center'>Strategy</div>
                      </div>
                      <div className='grid grid-cols-4'>
                        {distributionInfo.funcArgs && (
                          <div className='col-span-2 px-2 py-1'>
                            <Form.Item
                              noStyle
                              name={['distribution', 'field']}
                              label='Field'
                              rules={[{ required: values?.distribution?.strategy === 'hash' }]}
                            >
                              <Select
                                mode='multiple'
                                size='small'
                                className='w-full'
                                placeholder='Field'
                                disabled={!!editTable || values?.distribution?.strategy === 'even'}
                              >
                                {values?.columns
                                  ?.filter(col => col?.name)
                                  ?.map((col, index) => (
                                    <Select.Option key={col?.name + index} value={col?.name}>
                                      {col?.name}
                                    </Select.Option>
                                  ))}
                              </Select>
                            </Form.Item>
                            <Form.Item
                              shouldUpdate={(prevValues, currentValues) => prevValues.columns !== currentValues.columns}
                              noStyle
                            >
                              {() => {
                                const errors = form.getFieldError(['distribution', 'field'])

                                return errors.length ? <div className='text-red-500'>{errors[0]}</div> : null
                              }}
                            </Form.Item>
                          </div>
                        )}
                        <div className='col-span-2 px-2 py-1'>
                          <div className='flex gap-2'>
                            <Form.Item
                              noStyle
                              name={['distribution', 'strategy']}
                              label='Strategy'
                              rules={[
                                { required: distributionInfo.required },
                                ({ getFieldValue }) => ({
                                  validator(_, strategy) {
                                    if (strategy && provider === 'lakehouse-iceberg') {
                                      if (
                                        strategy === 'hash' &&
                                        !(getFieldValue('partitions')?.filter(p => !!p)?.length > 0)
                                      ) {
                                        return Promise.reject(
                                          new Error(
                                            "Iceberg's Distribution Mode.HASH is distributed based on partition"
                                          )
                                        )
                                      }
                                      if (
                                        strategy === 'range' &&
                                        !(getFieldValue('partitions')?.filter(p => !!p)?.length > 0) &&
                                        !(getFieldValue('sortOrders')?.filter(s => !!s)?.length > 0)
                                      ) {
                                        return Promise.reject(
                                          new Error(
                                            "Iceberg's Distribution Mode.RANGE is distributed based on sortOrder or partition"
                                          )
                                        )
                                      }
                                    }

                                    return Promise.resolve()
                                  }
                                })
                              ]}
                            >
                              <Select size='small' className='w-full' placeholder='Strategy' disabled={!!editTable}>
                                {distributionInfo.strategyOption.map(s => (
                                  <Select.Option key={s} value={s}>
                                    {capitalizeFirstLetter(s)}
                                  </Select.Option>
                                ))}
                              </Select>
                            </Form.Item>
                            {distributionInfo.funcArgs && (
                              <Form.Item
                                noStyle
                                name={['distribution', 'number']}
                                label='Number'
                                rules={[{ required: distributionInfo.required }]}
                              >
                                <InputNumber size='small' min={0} placeholder='Number' disabled={!!editTable} />
                              </Form.Item>
                            )}
                          </div>
                          <Form.Item
                            shouldUpdate={(prevValues, currentValues) => prevValues.columns !== currentValues.columns}
                            noStyle
                          >
                            {() => {
                              const errors = form.getFieldError(['distribution', 'strategy'])

                              return errors.length ? <div className='text-red-500'>{errors[0]}</div> : null
                            }}
                          </Form.Item>
                          {distributionInfo.funcArgs && (
                            <Form.Item
                              shouldUpdate={(prevValues, currentValues) => prevValues.columns !== currentValues.columns}
                              noStyle
                            >
                              {() => {
                                const errors = form.getFieldError(['distribution', 'number'])

                                return errors.length ? <div className='text-red-500'>{errors[0]}</div> : null
                              }}
                            </Form.Item>
                          )}
                        </div>
                      </div>
                    </div>
                  </Form.Item>
                )}
                <Form.Item name='comment' label='Comment'>
                  <TextArea data-refer='table-comment-field' />
                </Form.Item>
                {supportProperties && (
                  <Form.Item label='Properties'>
                    <div className='flex flex-col gap-2'>
                      {!editTable &&
                        tableDefaultProps[provider] &&
                        tableDefaultProps[provider].map((prop, idx) => {
                          const isLocationRequired =
                            prop.key === 'location' &&
                            provider === 'lakehouse-generic' &&
                            !catalogLocation &&
                            !schemaLocation

                          return (
                            <Flex gap='small' align='start' className='align-items-center mb-1' key={idx}>
                              <Input disabled value={prop.key} />
                              <Form.Item
                                className='mb-0 w-full grow'
                                name={prop.key}
                                label=''
                                rules={
                                  isLocationRequired
                                    ? [
                                        {
                                          required: true,
                                          message: 'Location is required when not set in catalog or schema'
                                        }
                                      ]
                                    : []
                                }
                              >
                                {prop.select ? (
                                  <Select className='flex-none' disabled={prop.disabled}>
                                    {prop.select?.map(item => (
                                      <Select.Option value={item} key={item}>
                                        {item}
                                      </Select.Option>
                                    ))}
                                  </Select>
                                ) : (
                                  <Input placeholder={prop.description} disabled={prop.disabled} />
                                )}
                              </Form.Item>
                              <Icons.Minus className={'cursor-not-allowed text-gray-100 hover:text-gray-200'} />
                            </Flex>
                          )
                        })}
                      <Form.List name='properties'>
                        {(fields, subOpt) => (
                          <RenderPropertiesFormItem
                            fields={fields}
                            subOpt={subOpt}
                            form={form}
                            isEdit={!!editTable}
                            isDisable={['jdbc-mysql'].includes(provider)}
                            provider={provider}
                          />
                        )}
                      </Form.List>
                    </div>
                  </Form.Item>
                )}
              </Form>
            </Spin>
          </div>
        </div>
      </Modal>
    </>
  )
}
