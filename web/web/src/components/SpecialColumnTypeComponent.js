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

import React, { useEffect, useState } from 'react'
import { PlusOutlined } from '@ant-design/icons'
import { Button, Form, Input, Pagination, Switch } from 'antd'
import Icons from '@/components/Icons'
import ColumnTypeComponent from '@/components/ColumnTypeComponent'
import { nameRegex } from '@/config/regex'
import { cn } from '@/lib/utils/tailwind'

export default function SpecialColumnTypeComponent({ ...props }) {
  const { form, parentField, subField, columnNamespace, isEdit, columnTypes, provider } = props

  const currentType = Form.useWatch(
    [...parentField, subField.name, 'typeObj', ...columnNamespace, 'type'],
    form
  )?.toLowerCase()

  const fieldColumns = Form.useWatch(
    [...parentField, subField.name, 'typeObj', ...columnNamespace, 'fieldColumns'],
    form
  )
  const unionTypes = Form.useWatch([...parentField, subField.name, 'typeObj', ...columnNamespace, 'types'], form)
  const disabled = isEdit && ['lakehouse-iceberg', 'lakehouse-paimon'].includes(provider)

  useEffect(() => {
    if (!isEdit) {
      if (currentType === 'struct' && !fieldColumns?.length) {
        form.setFieldValue(
          [...parentField, subField.name, 'typeObj', ...columnNamespace, 'fieldColumns'],
          [{ id: '', name: '', typeObj: { type: '' }, required: false, comment: '' }]
        )
      } else if (currentType === 'union' && !unionTypes?.length) {
        form.setFieldValue(
          [...parentField, subField.name, 'typeObj', ...columnNamespace, 'types'],
          [{ id: '', typeObj: { type: '' } }]
        )
      }
    }
  }, [currentType, isEdit, fieldColumns, unionTypes])

  const [pageOffset, setPageOffset] = useState(1)

  const onChange = page => {
    setPageOffset(page)
  }

  const renderFieldColumns = (fields, subOpt) => {
    const pageSize = 5
    const isShowPagination = fields.length > pageSize

    return (
      <div className='flex flex-col divide-y divide-solid border-b border-solid'>
        <div className={cn('grid divide-x divide-solid', provider !== 'hive' ? 'grid-cols-10' : 'grid-cols-7')}>
          <div className='col-span-2 bg-gray-100 p-1 text-center'>Field Name</div>
          <div className='col-span-3 bg-gray-100 p-1 text-center'>Data Type</div>
          <div className='bg-gray-100 p-1 text-center'>Not Null</div>
          {provider !== 'hive' && <div className='col-span-3 bg-gray-100 p-1 text-center'>Comment</div>}
          <div className='bg-gray-100 p-1 text-center'>Action</div>
        </div>
        {fields.slice((pageOffset - 1) * pageSize, pageOffset * pageSize).map(field => {
          return (
            <div key={field.key}>
              <div className={cn('grid', provider !== 'hive' ? 'grid-cols-10' : 'grid-cols-7')}>
                <div className='col-span-2 px-2 py-1'>
                  <Form.Item
                    noStyle
                    name={[field.name, 'name']}
                    label=''
                    rules={[
                      ({ getFieldValue }) => ({
                        validator(_, name) {
                          if (name) {
                            if (!nameRegex.test(name)) {
                              return Promise.reject(new Error(t('common.namePatternMiss')))
                            }
                            if (
                              getFieldValue([
                                ...parentField,
                                subField.name,
                                'typeObj',
                                ...columnNamespace,
                                'fieldColumns'
                              ]).length
                            ) {
                              let names = getFieldValue([
                                ...parentField,
                                subField.name,
                                'typeObj',
                                ...columnNamespace,
                                'fieldColumns'
                              ]).map(col => col?.name)
                              names.splice(field.name, 1)
                              if (names.includes(name)) {
                                return Promise.reject('The column name already exists!')
                              }
                            }

                            return Promise.resolve()
                          } else {
                            return Promise.reject('The field name is required!')
                          }
                        }
                      })
                    ]}
                  >
                    <Input size='small' disabled={disabled} />
                  </Form.Item>
                  <Form.Item
                    shouldUpdate={(prevValues, currentValues) => prevValues.fieldColumns !== currentValues.fieldColumns}
                    noStyle
                  >
                    {() => {
                      const errors = form.getFieldError([
                        ...parentField,
                        subField.name,
                        'typeObj',
                        ...columnNamespace,
                        'fieldColumns',
                        field.name,
                        'name'
                      ])

                      return errors.length ? <div className='text-red-500'>{errors[0]}</div> : null
                    }}
                  </Form.Item>
                </div>
                <div className='col-span-3 px-2 py-1'>
                  <ColumnTypeComponent
                    form={form}
                    parentField={[...parentField, subField.name, 'typeObj', ...columnNamespace, 'fieldColumns']}
                    subField={field}
                    columnNamespace={[]}
                    columnTypes={columnTypes}
                    disabled={disabled}
                  />
                </div>
                <div className='px-2 py-1'>
                  <Form.Item noStyle name={[field.name, 'required']} label=''>
                    <Switch size='small' disabled={provider === 'hive' || disabled} />
                  </Form.Item>
                </div>
                {provider !== 'hive' && (
                  <div className='col-span-3 px-2 py-1'>
                    <Form.Item noStyle name={[field.name, 'comment']} label=''>
                      <Input size='small' disabled={disabled} />
                    </Form.Item>
                  </div>
                )}
                <div className='px-2 py-1'>
                  <Icons.Minus
                    className={cn('size-4 cursor-pointer text-gray-400 hover:text-defaultPrimary', {
                      'text-gray-100 hover:text-gray-200 cursor-not-allowed':
                        form.getFieldValue([
                          ...parentField,
                          subField.name,
                          'typeObj',
                          ...columnNamespace,
                          'fieldColumns'
                        ]).length === 1 || disabled
                    })}
                    onClick={() => {
                      if (
                        form.getFieldValue([
                          ...parentField,
                          subField.name,
                          'typeObj',
                          ...columnNamespace,
                          'fieldColumns'
                        ]).length === 1 ||
                        disabled
                      )
                        return
                      subOpt.remove(field.name)
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
            className='text-right'
            current={pageOffset}
            onChange={onChange}
            pageSize={pageSize}
            total={fields.length}
          />
        )}
        <div className='text-left'>
          <Button
            type='link'
            icon={<PlusOutlined />}
            disabled={disabled}
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
            Add Field
          </Button>
        </div>
      </div>
    )
  }

  const renderTypes = (fields, subOpt) => {
    const pageSize = 5
    const isShowPagination = fields.length > pageSize

    return (
      <div className='flex flex-col divide-y divide-solid border-b border-solid'>
        <div className='grid grid-cols-3 divide-x divide-solid'>
          <div className='col-span-2 bg-gray-100 p-1 text-center'>Data Type</div>
          <div className='bg-gray-100 p-1 text-center'>Action</div>
        </div>
        {fields.slice((pageOffset - 1) * pageSize, pageOffset * pageSize).map(field => {
          return (
            <div key={field.key}>
              <div className='grid grid-cols-3'>
                <div className='col-span-2 px-2 py-1'>
                  <ColumnTypeComponent
                    form={form}
                    parentField={[...parentField, subField.name, 'typeObj', ...columnNamespace, 'types']}
                    subField={field}
                    columnNamespace={[]}
                    columnTypes={columnTypes}
                  />
                </div>
                <div className='px-2 py-1'>
                  <Icons.Minus
                    className={cn('size-4 cursor-pointer text-gray-400 hover:text-defaultPrimary', {
                      'text-gray-100 hover:text-gray-200 cursor-not-allowed':
                        form.getFieldValue([...parentField, subField.name, 'typeObj', ...columnNamespace, 'types'])
                          .length === 1
                    })}
                    onClick={() => {
                      if (
                        form.getFieldValue([...parentField, subField.name, 'typeObj', ...columnNamespace, 'types'])
                          .length === 1
                      )
                        return
                      subOpt.remove(field.name)
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
            className='text-right'
            current={pageOffset}
            onChange={onChange}
            pageSize={pageSize}
            total={fields.length}
          />
        )}
        <div className='text-left'>
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
            Types
          </Button>
        </div>
      </div>
    )
  }

  return (
    <div className='w-[600px]'>
      {currentType === 'list' && (
        <div className='size-full bg-gray-100 pl-8 pt-px shadow-[inset_1px_1px_2px_rgba(204,204,204)]'>
          <div className='grid grid-cols-4 divide-x divide-solid'>
            <div className='bg-gray-100 p-1 text-center'>Element</div>
            <div className='col-span-2 bg-gray-100 p-1 text-center'>Element Type</div>
            <div className='bg-gray-100 p-1 text-center'>Not Null</div>
          </div>
          <div className='grid grid-cols-4 bg-white'>
            <div className='px-2 py-1'></div>
            <div className='col-span-2 px-2 py-1'>
              <ColumnTypeComponent
                form={form}
                parentField={['columns']}
                subField={subField}
                columnNamespace={[...columnNamespace, 'element']}
                columnTypes={columnTypes}
                disabled={disabled}
              />
            </div>
            <div className='px-2 py-1'>
              <Form.Item noStyle name={[subField.name, 'typeObj', ...columnNamespace, 'element', 'required']} label=''>
                <Switch
                  size='small'
                  disabled={['hive', 'jdbc-postgresql'].includes(provider) || disabled}
                  defaultValue={provider === 'jdbc-postgresql'}
                />
              </Form.Item>
            </div>
          </div>
        </div>
      )}
      {currentType === 'map' && (
        <div className='size-full bg-gray-100 pl-8 pt-px shadow-[inset_1px_1px_2px_rgba(204,204,204)]'>
          <div className='grid grid-cols-4 border-b border-solid bg-white'>
            <div className='px-2 py-1'>Key</div>
            <div className='col-span-2 px-2 py-1'>
              <ColumnTypeComponent
                form={form}
                parentField={['columns']}
                subField={subField}
                columnNamespace={[...columnNamespace, 'keyObj']}
                columnTypes={columnTypes}
                disabled={disabled}
              />
            </div>
            <div className='px-2 py-1'>
              <Form.Item noStyle name={[subField.name, 'typeObj', ...columnNamespace, 'keyObj', 'required']} label=''>
                <Switch size='small' defaultValue={true} disabled={provider === 'hive' || disabled} />
              </Form.Item>
            </div>
          </div>

          <div className='grid grid-cols-4 bg-white'>
            <div className='px-2 py-1'>Value</div>
            <div className='col-span-2 px-2 py-1'>
              <ColumnTypeComponent
                form={form}
                parentField={['columns']}
                subField={subField}
                columnNamespace={[...columnNamespace, 'valueObj']}
                columnTypes={columnTypes}
                disabled={disabled}
              />
            </div>
            <div className='px-2 py-1'>
              <Form.Item noStyle name={[subField.name, 'typeObj', ...columnNamespace, 'valueObj', 'required']} label=''>
                <Switch size='small' disabled={provider === 'hive' || disabled} />
              </Form.Item>
            </div>
          </div>
        </div>
      )}
      {currentType === 'struct' && (
        <div className='size-full bg-gray-100 pl-8 pt-px shadow-[inset_1px_1px_2px_rgba(204,204,204)]'>
          <Form.Item noStyle name={[subField.name, 'typeObj', ...columnNamespace, 'fieldColumns']} label=''>
            <Form.List name={[subField.name, 'typeObj', ...columnNamespace, 'fieldColumns']}>
              {(fields, subOpt) => renderFieldColumns(fields, subOpt)}
            </Form.List>
          </Form.Item>
        </div>
      )}
      {currentType === 'union' && (
        <div className='size-full bg-gray-100 pl-8 pt-px shadow-[inset_1px_1px_2px_rgba(204,204,204)]'>
          <Form.Item noStyle name={[subField.name, 'typeObj', ...columnNamespace, 'types']} label=''>
            <Form.List name={[subField.name, 'typeObj', ...columnNamespace, 'types']}>
              {(fields, subOpt) => renderTypes(fields, subOpt)}
            </Form.List>
          </Form.Item>
        </div>
      )}
    </div>
  )
}
