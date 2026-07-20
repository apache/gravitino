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

import React, { useCallback } from 'react'

import { DeleteOutlined, PlusOutlined } from '@ant-design/icons'
import { Button, DatePicker, Flex, Form, Input, InputNumber, Select } from 'antd'
import dayjs from 'dayjs'
import Icons from '@/components/Icons'
import { partitionInfoMap, transformsLimitMap } from '@/config'
import { capitalizeFirstLetter } from '@/lib/utils'
import { cn } from '@/lib/utils/tailwind'

const dateTypes = ['date', 'time', 'timestamp', 'timestamp_tz']

/** Helper: get the base column type for a partition row */
function getColumnBaseType(form, partitionIndex) {
  const fieldName = form.getFieldValue(['partitions', partitionIndex, 'fieldName'])
  const cols = form.getFieldValue('columns') || []
  const column = cols.find(c => c?.name === fieldName)

  return column?.typeObj?.type?.split('(')[0]
}

/** Helper: ensure Doris list-partition columns are set to NOT NULL */
function ensureDorisListNotNull(form, provider, partitionIndex) {
  if (provider !== 'jdbc-doris') return
  const strategy = form.getFieldValue(['partitions', partitionIndex, 'strategy'])
  if (strategy !== 'list') return
  const fieldName = form.getFieldValue(['partitions', partitionIndex, 'fieldName'])
  if (!fieldName) return
  const columns = form.getFieldValue('columns') || []
  const colIdx = columns.findIndex(c => c?.name === fieldName)
  if (colIdx >= 0 && !columns[colIdx]?.required) {
    form.setFieldValue(['columns', colIdx, 'required'], true)
  }
}

/** Helper: create a stable key for assignment items to avoid index-based keys */
let assignmentIdCounter = 0
function newAssignmentId() {
  return `assign-${++assignmentIdCounter}`
}

function BoundInput({ columnType, value, onChange, ...props }) {
  if (dateTypes.includes(columnType)) {
    return (
      <DatePicker
        size='small'
        className='w-full'
        value={value ? (dayjs.isDayjs(value) ? value : dayjs(value)) : undefined}
        onChange={(date, dateString) => {
          // dateString can be string or string[] when multiple formats are configured
          const val = Array.isArray(dateString) ? dateString[0] : dateString
          onChange?.(val || '')
        }}
        {...props}
      />
    )
  }

  return <Input size='small' value={value} onChange={e => onChange?.(e.target.value)} {...props} />
}

export default function PartitionPanel({ form, editTable, provider }) {
  const partitioningInfo = partitionInfoMap[provider]

  return (
    <Form.List name='partitions'>
      {(fields, subOpt) => (
        <div className='flex flex-col divide-y divide-solid border-b border-solid'>
          <div className='grid grid-cols-5 divide-x divide-solid'>
            <div className='col-span-2 bg-gray-100 p-1 text-center'>Field</div>
            <div className='col-span-2 bg-gray-100 p-1 text-center'>Strategy</div>
            <div className='col-span-1 bg-gray-100 p-1 text-center'>Action</div>
          </div>
          {fields.map(subField => (
            <PartitionRow
              key={subField.name}
              form={form}
              editTable={editTable}
              subField={subField}
              subOpt={subOpt}
              partitioningInfo={partitioningInfo}
              provider={provider}
            />
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
      )}
    </Form.List>
  )
}

function PartitionRow({ form, editTable, subField, subOpt, partitioningInfo, provider }) {
  // Only watch strategy to control visibility of number input and range/list assignments.
  // Do NOT watch 'columns' or 'fieldName' here — reading them via form.getFieldValue
  // inside callbacks and render avoids unnecessary re-renders that reset assignment values.
  const currentStrategy = Form.useWatch(['partitions', subField.name, 'strategy'], form)
  const idx = subField.name

  const handleFieldChange = useCallback(() => {
    // Clear bound values when field changes, since the column type may differ
    const assignments = form.getFieldValue(['partitions', idx, 'assignments'])
    if (assignments?.length > 0) {
      form.setFieldValue(
        ['partitions', idx, 'assignments'],
        assignments.map(a => ({ ...a, upper: undefined, lower: undefined }))
      )
    }

    // Also clear list assignments since column type may differ
    const listAssignments = form.getFieldValue(['partitions', idx, 'listAssignments'])
    if (listAssignments?.length > 0) {
      form.setFieldValue(['partitions', idx, 'listAssignments'], [])
    }
    ensureDorisListNotNull(form, provider, idx)
  }, [form, provider, idx])

  const handleStrategyChange = useCallback(() => {
    const newStrategy = form.getFieldValue(['partitions', idx, 'strategy'])

    // Clear assignments when strategy changes away from range/list
    if (newStrategy !== 'range') {
      form.setFieldValue(['partitions', idx, 'assignments'], [])
    }
    if (newStrategy !== 'list') {
      form.setFieldValue(['partitions', idx, 'listAssignments'], [])
    }
    ensureDorisListNotNull(form, provider, idx)
  }, [form, provider, idx])

  const columnOptions = (() => {
    const cols = form.getFieldValue('columns') || []

    return cols
      .filter(col => col?.name)
      .map(col => (
        <Select.Option key={col?.name} value={col?.name}>
          <Flex justify='space-between'>
            <span>{col?.name}</span>
            <span>{col?.typeObj?.type}</span>
          </Flex>
        </Select.Option>
      ))
  })()

  const strategyOptions = (() => {
    const fieldName = form.getFieldValue(['partitions', idx, 'fieldName'])
    const type = getColumnBaseType(form, idx)

    return partitioningInfo
      ?.filter(s => transformsLimitMap[s]?.includes(type) || !transformsLimitMap[s])
      .map(s => (
        <Select.Option key={s} value={s}>
          {capitalizeFirstLetter(s)}
        </Select.Option>
      ))
  })()

  return (
    <div>
      <div className='grid grid-cols-5'>
        <div className='col-span-2 px-2 py-1'>
          <Form.Item noStyle name={[subField.name, 'fieldName']} label='Field'>
            <Select
              size='small'
              className='w-full'
              placeholder='Field'
              disabled={!!editTable}
              onChange={handleFieldChange}
            >
              {columnOptions}
            </Select>
          </Form.Item>
        </div>
        <div className='col-span-2 flex gap-2 px-2 py-1'>
          <Form.Item noStyle name={[subField.name, 'strategy']} label='Strategy'>
            <Select
              size='small'
              className='w-full'
              placeholder='Strategy'
              disabled={!!editTable}
              onChange={handleStrategyChange}
            >
              {strategyOptions}
            </Select>
          </Form.Item>
          {['truncate', 'bucket'].includes(currentStrategy) && (
            <Form.Item noStyle name={[subField.name, 'number']} label='Number' rules={[{ required: true }]}>
              <InputNumber
                size='small'
                min={0}
                placeholder={currentStrategy === 'bucket' ? 'Number' : 'Width'}
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
      {/* Always render assignment panels but hide with CSS when not active.
          This prevents Form.List unmount/remount which loses field values. */}
      <div className={currentStrategy === 'range' ? '' : 'hidden'}>
        <RangeAssignments editTable={editTable} partitionIndex={subField.name} form={form} />
      </div>
      <div className={currentStrategy === 'list' ? '' : 'hidden'}>
        <ListAssignments editTable={editTable} partitionIndex={subField.name} form={form} />
      </div>
    </div>
  )
}

function RangeAssignments({ editTable, partitionIndex, form }) {
  // Read assignments directly from form store to avoid nested Form.List path resolution issues.
  // Nested Form.List inside another Form.List causes field path conflicts where updating
  // one field inadvertently resets siblings (name/upper/lower overwrite each other).
  const assignments = Form.useWatch(['partitions', partitionIndex, 'assignments'], form) || []

  const onFieldChange = (assignIdx, field, value) => {
    const current = form.getFieldValue(['partitions', partitionIndex, 'assignments']) || []

    const updated = current.map((item, idx) => (idx === assignIdx ? { ...item, [field]: value } : item))
    form.setFieldValue(['partitions', partitionIndex, 'assignments'], updated)
  }

  const addAssignment = () => {
    const current = form.getFieldValue(['partitions', partitionIndex, 'assignments']) || []
    form.setFieldValue(
      ['partitions', partitionIndex, 'assignments'],
      [...current, { _key: newAssignmentId(), name: '', upper: '', lower: '' }]
    )
  }

  const removeAssignment = assignIdx => {
    const current = form.getFieldValue(['partitions', partitionIndex, 'assignments']) || []
    form.setFieldValue(
      ['partitions', partitionIndex, 'assignments'],
      current.filter((_, idx) => idx !== assignIdx)
    )
  }

  const columnType = getColumnBaseType(form, partitionIndex)

  return (
    <div className='ml-4 mt-1 border-l-2 border-gray-200 p-3'>
      <div className='mb-1 text-xs text-gray-500'>Range Partitions</div>
      <div className='flex flex-col gap-1'>
        <div className='grid grid-cols-12 gap-1'>
          <div className='col-span-3 bg-gray-50 p-1 text-center text-xs'>Name</div>
          <div className='col-span-4 bg-gray-50 p-1 text-center text-xs'>Upper Bound</div>
          <div className='col-span-4 bg-gray-50 p-1 text-center text-xs'>Lower Bound</div>
          <div className='col-span-1 bg-gray-50 p-1 text-center text-xs'></div>
        </div>
        {assignments.map((assignItem, assignIdx) => (
          <div key={assignItem?._key || assignIdx} className='grid grid-cols-12 gap-1'>
            <div className='col-span-3'>
              <Input
                size='small'
                placeholder='p1'
                disabled={!!editTable}
                value={assignItem?.name ?? ''}
                onChange={e => onFieldChange(assignIdx, 'name', e.target.value)}
              />
            </div>
            <div className='col-span-4'>
              <BoundInput
                columnType={columnType}
                size='small'
                placeholder='Upper bound (empty = MAXVALUE)'
                disabled={!!editTable}
                value={assignItem?.upper ?? ''}
                onChange={value => onFieldChange(assignIdx, 'upper', value ?? '')}
              />
            </div>
            <div className='col-span-4'>
              <BoundInput
                columnType={columnType}
                size='small'
                placeholder='Lower bound (empty = MIN)'
                disabled={!!editTable}
                value={assignItem?.lower ?? ''}
                onChange={value => onFieldChange(assignIdx, 'lower', value ?? '')}
              />
            </div>
            <div className='col-span-1 flex items-center justify-center'>
              <DeleteOutlined
                className={cn('cursor-pointer text-gray-400 hover:text-red-500', {
                  'cursor-not-allowed text-gray-200 hover:text-gray-200': !!editTable
                })}
                onClick={() => {
                  if (!!editTable) return
                  removeAssignment(assignIdx)
                }}
              />
            </div>
          </div>
        ))}
        <Button type='dashed' size='small' icon={<PlusOutlined />} disabled={!!editTable} onClick={addAssignment}>
          Add Range Partition
        </Button>
      </div>
    </div>
  )
}

function ListAssignments({ editTable, partitionIndex, form }) {
  const assignments = Form.useWatch(['partitions', partitionIndex, 'listAssignments'], form) || []

  const onFieldChange = (assignIdx, field, value) => {
    const current = form.getFieldValue(['partitions', partitionIndex, 'listAssignments']) || []

    const updated = current.map((item, idx) => (idx === assignIdx ? { ...item, [field]: value } : item))
    form.setFieldValue(['partitions', partitionIndex, 'listAssignments'], updated)
  }

  const onListGroupChange = (assignIdx, groupIdx, value) => {
    const current = form.getFieldValue(['partitions', partitionIndex, 'listAssignments']) || []
    const item = current[assignIdx]
    if (!item) return
    const newGroups = [...(item.listGroups || [])]
    newGroups[groupIdx] = value

    const updated = current.map((it, idx) => (idx === assignIdx ? { ...it, listGroups: newGroups } : it))
    form.setFieldValue(['partitions', partitionIndex, 'listAssignments'], updated)
  }

  const addListGroup = assignIdx => {
    const current = form.getFieldValue(['partitions', partitionIndex, 'listAssignments']) || []
    const item = current[assignIdx]
    if (!item) return
    const newGroups = [...(item.listGroups || []), '']

    const updated = current.map((it, idx) => (idx === assignIdx ? { ...it, listGroups: newGroups } : it))
    form.setFieldValue(['partitions', partitionIndex, 'listAssignments'], updated)
  }

  const removeListGroup = (assignIdx, groupIdx) => {
    const current = form.getFieldValue(['partitions', partitionIndex, 'listAssignments']) || []
    const item = current[assignIdx]
    if (!item) return
    const newGroups = (item.listGroups || []).filter((_, i) => i !== groupIdx)

    const updated = current.map((it, idx) => (idx === assignIdx ? { ...it, listGroups: newGroups } : it))
    form.setFieldValue(['partitions', partitionIndex, 'listAssignments'], updated)
  }

  const addAssignment = () => {
    const current = form.getFieldValue(['partitions', partitionIndex, 'listAssignments']) || []
    form.setFieldValue(
      ['partitions', partitionIndex, 'listAssignments'],
      [...current, { _key: newAssignmentId(), name: '', listGroups: [''] }]
    )
  }

  const removeAssignment = assignIdx => {
    const current = form.getFieldValue(['partitions', partitionIndex, 'listAssignments']) || []
    form.setFieldValue(
      ['partitions', partitionIndex, 'listAssignments'],
      current.filter((_, idx) => idx !== assignIdx)
    )
  }

  return (
    <div className='ml-4 mt-1 border-l-2 border-gray-200 p-3'>
      <div className='mb-1 text-xs text-gray-500'>List Partitions</div>
      <div className='flex flex-col gap-2'>
        {assignments.map((assignItem, assignIdx) => (
          <div key={assignItem?._key || assignIdx} className='rounded border border-gray-200 p-2'>
            <div className='grid grid-cols-12 gap-1'>
              <div className='col-span-3'>
                <div className='mb-1 text-xs text-gray-400'>Name</div>
                <Input
                  size='small'
                  placeholder='p1'
                  disabled={!!editTable}
                  value={assignItem?.name ?? ''}
                  onChange={e => onFieldChange(assignIdx, 'name', e.target.value)}
                />
              </div>
              <div className='col-span-8'>
                <div className='mb-1 text-xs text-gray-400'>Value Groups (each group = one IN tuple)</div>
                {(assignItem?.listGroups || []).map((group, groupIdx) => (
                  <div key={groupIdx} className='mb-1 flex items-center gap-1'>
                    <Input
                      size='small'
                      className='flex-1'
                      placeholder={
                        assignItem?.listGroups?.length > 1
                          ? 'val1, val2 (multi-column values for this group)'
                          : 'val1, val2, val3'
                      }
                      disabled={!!editTable}
                      value={group ?? ''}
                      onChange={e => onListGroupChange(assignIdx, groupIdx, e.target.value)}
                    />
                    <DeleteOutlined
                      className={cn('cursor-pointer text-gray-400 hover:text-red-500', {
                        'cursor-not-allowed text-gray-200 hover:text-gray-200': !!editTable
                      })}
                      onClick={() => {
                        if (!!editTable) return
                        removeListGroup(assignIdx, groupIdx)
                      }}
                    />
                  </div>
                ))}
                <Button
                  type='dashed'
                  size='small'
                  icon={<PlusOutlined />}
                  disabled={!!editTable}
                  onClick={() => addListGroup(assignIdx)}
                >
                  Add Value Group
                </Button>
              </div>
              <div className='col-span-1 flex items-start justify-center pt-5'>
                <DeleteOutlined
                  className={cn('cursor-pointer text-gray-400 hover:text-red-500', {
                    'cursor-not-allowed text-gray-200 hover:text-gray-200': !!editTable
                  })}
                  onClick={() => {
                    if (!!editTable) return
                    removeAssignment(assignIdx)
                  }}
                />
              </div>
            </div>
          </div>
        ))}
        <Button type='dashed' size='small' icon={<PlusOutlined />} disabled={!!editTable} onClick={addAssignment}>
          Add List Partition
        </Button>
      </div>
    </div>
  )
}
