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

import React, { useEffect } from 'react'

import { DownOutlined, PlusOutlined } from '@ant-design/icons'
import { Button, Dropdown, Flex, Form, Input, Select, Space } from 'antd'

import Icons from '@/components/Icons'
import { getPropInfo, mismatchForKey } from '@/config'
import { filesetLocationProviders, locationDefaultProps, rangerDefaultProps } from '@/config/catalog'
import { keyRegex } from '@/lib/utils/regex'
import { cn } from '@/lib/utils/tailwind'
import { mapValues } from 'lodash-es'
import { useAppSelector } from '@/lib/hooks/useStore'

export default function RenderPropertiesFormItem({ ...props }) {
  const { fields, subOpt, form, isEdit, isDisable, provider, selectBefore } = props
  const disableTableLevelPro = (provider && getPropInfo(provider).immutable) || []
  const reservedPro = provider ? getPropInfo(provider).reserved : []
  const auth = useAppSelector(state => state.auth)
  const { systemConfig } = auth

  const defaultPro = rangerDefaultProps.reduce((acc, p) => {
    acc[p.key] = {
      key: p.key,
      value: systemConfig?.[`gravitino.datastrato.custom.${p.key}`] || p.value,
      select: p.select,
      disabled: p.disabled,
      description: p.description ? `catalog.${p.description}` : ''
    }

    return acc
  }, {})
  const { 'gravitino.datastrato.custom.authorization.ranger.auth.type': rangerAuthType } = systemConfig || {}

  const filesystemProviders = form.getFieldValue('properties').find(p => p.key === 'filesystem-providers')
  useEffect(() => {
    if (!isEdit && provider === 'hadoop') {
      const value = filesystemProviders?.value
      const currentSelectBefore = value?.map(p => filesetLocationProviders[p]).filter(Boolean) || []
      const selectBefore = [...new Set(['file:/', 'hdfs://', ...currentSelectBefore])]
      const properties = form.getFieldValue('properties')
      properties.forEach(p => {
        if (p.key === 'location' || p.key?.startsWith('location-')) {
          p.selectBefore = selectBefore
          if (!selectBefore.includes(p.prefix)) {
            p.prefix = selectBefore[0]
          }
        }
      })
      const filteredProperties = properties.filter(p => !p?.mapForLocation || value?.includes(p.mapForLocation))
      let newProperties = []
      value?.forEach(p => {
        newProperties = [...newProperties, ...handleLocationPrefixChange(p)]
      })
      form.setFieldValue('properties', [...filteredProperties, ...newProperties])
    }
  }, [filesystemProviders, isEdit, provider])

  const items = [
    {
      label: 'Ranger Properties',
      key: 'addDefaulteProForRanger'
    }
  ]

  const handleMenuClick = (e, subOpt) => {
    if (e.key === 'addDefaulteProForRanger') {
      const existedProKeys = form.getFieldValue('properties').map(p => p?.key)
      rangerDefaultProps.forEach(p => {
        if (!existedProKeys.includes(p.key)) {
          subOpt.add(defaultPro[p.key])
        }
      })
    }
  }

  const handleLocationPrefixChange = value => {
    const locationProps = locationDefaultProps[mapValues] || []
    const existedProKeys = form.getFieldValue('properties').map(p => p?.key)
    let newProperties = []
    locationProps.forEach(p => {
      if (!existedProKeys.includes(p.key)) {
        newProperties.push({ key: p.key, value: p.defaultValue, description: p.description, mapForLocation: value })
      }
    })

    return newProperties
  }

  const handlePropertiesKey = (e, index) => {
    if (isEdit) return
    const key = e.target.value

    const locationProviders =
      form.getFieldValue(['properties']).find(p => p.key === 'filesystem-providers')?.value || []
    let filesetSelectBefore = ['file:/', 'hdfs://'] // default selectBefore values
    locationProviders.forEach(p => {
      if (filesetLocationProviders[p] && !filesetSelectBefore.includes(filesetLocationProviders[p])) {
        filesetSelectBefore.push(filesetLocationProviders[p])
      }
    })
    if ((provider === 'hadoop' || selectBefore) && (key === 'location' || key.startsWith('location-'))) {
      form.setFieldValue(['properties', index, 'selectBefore'], selectBefore || filesetSelectBefore)
      form.setFieldValue(['properties', index, 'description'], 'Hadoop catalog storage location')
      if (!filesetSelectBefore.includes(form.getFieldValue(['properties', index, 'prefix']))) {
        form.setFieldValue(['properties', index, 'prefix'], selectBefore?.[0] || filesetSelectBefore[0])
      }
    } else {
      form.setFieldValue(['properties', index, 'selectBefore'], null)
      form.setFieldValue(['properties', index, 'description'], '')
    }
  }

  return (
    <div className='flex flex-col gap-2'>
      {fields.map(subField => (
        <Form.Item label={null} className='align-items-center mb-1' key={subField.key}>
          <Flex gap='small' align='start'>
            <Form.Item
              name={[subField.name, 'key']}
              className='mb-0 w-full grow'
              label=''
              data-refer={`props-key-${subField.name}`}
              data-testid={`props-key-${subField.name}`}
              rules={[
                {
                  pattern: new RegExp(keyRegex),
                  message: mismatchForKey
                },
                ({ getFieldValue }) => ({
                  validator(_, key) {
                    if (getFieldValue('properties').length) {
                      if (key) {
                        let keys = getFieldValue('properties').map(p => p?.key)
                        keys.splice(subField.name, 1)
                        if (keys.includes(key)) {
                          return Promise.reject(new Error('The key already exists!'))
                        }
                        if (reservedPro.includes(key) && !isEdit) {
                          return Promise.reject(new Error('The key is reserved!'))
                        }
                        if (defaultPro[key]) {
                          const { value, select, disabled, description } = defaultPro[key]
                          form.setFieldValue(['properties', subField.name, 'select'], select)
                          form.setFieldValue(['properties', subField.name, 'description'], description)
                          !form.getFieldValue(['properties', subField.name, 'value']) &&
                            value &&
                            form.setFieldValue(['properties', subField.name, 'value'], value)
                          form.setFieldValue(['properties', subField.name, 'disabled'], disabled)
                        } else {
                          form.setFieldValue(['properties', subField.name, 'select'], undefined)
                          form.setFieldValue(['properties', subField.name, 'description'], '')
                          form.setFieldValue(['properties', subField.name, 'disabled'], false)
                        }

                        return Promise.resolve()
                      } else {
                        return Promise.reject(new Error('The key is required!'))
                      }
                    }
                  }
                })
              ]}
            >
              <Input
                placeholder={'Key'}
                disabled={
                  ([...reservedPro, ...disableTableLevelPro].includes(
                    form.getFieldValue(['properties', subField.name, 'key'])
                  ) &&
                    isEdit) ||
                  isDisable
                }
                onChange={e => handlePropertiesKey(e, subField.name)}
              />
            </Form.Item>
            <span className='w-full'>
              <Form.Item
                name={[subField.name, 'value']}
                className='mb-0 w-full grow'
                label=''
                data-refer={`props-value-${subField.name}`}
                data-testid={`props-value-${subField.name}`}
              >
                {form.getFieldValue(['properties', subField.name, 'select']) ? (
                  <Select
                    className='flex-none'
                    mode={form.getFieldValue(['properties', subField.name, 'multiple']) ? 'multiple' : undefined}
                    disabled={form.getFieldValue(['properties', subField.name, 'disabled'])}
                    placeholder={
                      form.getFieldValue(['properties', subField.name, 'description'])
                        ? `${form.getFieldValue(['properties', subField.name, 'description'])}`
                        : 'Value'
                    }
                  >
                    {form.getFieldValue(['properties', subField.name, 'select']).map(item => (
                      <Select.Option value={item} key={item}>
                        {item}
                      </Select.Option>
                    ))}
                  </Select>
                ) : (
                  <Input
                    placeholder={
                      form.getFieldValue(['properties', subField.name, 'description'])
                        ? `${form.getFieldValue(['properties', subField.name, 'description'])}`
                        : 'Value'
                    }
                    disabled={
                      ([...reservedPro, ...disableTableLevelPro].includes(
                        form.getFieldValue(['properties', subField.name, 'key'])
                      ) &&
                        isEdit) ||
                      isDisable ||
                      form.getFieldValue(['properties', subField.name, 'disabled'])
                    }
                    type={
                      form.getFieldValue(['properties', subField.name, 'key'])?.includes('password')
                        ? 'password'
                        : 'text'
                    }
                    addonBefore={
                      form.getFieldValue(['properties', subField.name, 'selectBefore']) ? (
                        <Select
                          value={form.getFieldValue(['properties', subField.name, 'prefix'])}
                          key={form.getFieldValue(['properties', subField.name, 'selectBefore']).join(',')}
                          onChange={value => form.setFieldValue(['properties', subField.name, 'prefix'], value)}
                        >
                          {form.getFieldValue(['properties', subField.name, 'selectBefore']).map(item => (
                            <Select.Option key={item} value={item}>
                              {item}
                            </Select.Option>
                          ))}
                        </Select>
                      ) : null
                    }
                  />
                )}
              </Form.Item>
            </span>
            <Icons.Minus
              className={cn('size-8 cursor-pointer text-gray-400 hover:text-defaultPrimary', {
                'text-gray-100 hover:text-gray-200 cursor-not-allowed':
                  ([...reservedPro, ...disableTableLevelPro].includes(
                    form.getFieldValue(['properties', subField.name, 'key'])
                  ) &&
                    isEdit) ||
                  isDisable
              })}
              onClick={() => {
                if (
                  ([...reservedPro, ...disableTableLevelPro].includes(
                    form.getFieldValue(['properties', subField.name, 'key'])
                  ) &&
                    isEdit) ||
                  isDisable
                )
                  return
                subOpt.remove(subField.name)
              }}
            />
          </Flex>
        </Form.Item>
      ))}
      {rangerAuthType && !isEdit ? (
        <Dropdown.Button
          data-refer='add-props'
          icon={<DownOutlined />}
          disabled={isDisable}
          menu={{
            items,
            onClick: e => handleMenuClick(e, subOpt)
          }}
          onClick={() => subOpt.add({ key: '', value: '' })}
        >
          <Space>
            <PlusOutlined />
            <span>{'Add Property'}</span>
          </Space>
        </Dropdown.Button>
      ) : (
        <Button
          data-refer='add-props'
          className='w-fit'
          type='dashed'
          disabled={isDisable}
          icon={<PlusOutlined />}
          onClick={() => subOpt.add({ key: '', value: '' })}
        >
          {'Add Property'}
        </Button>
      )}
    </div>
  )
}
