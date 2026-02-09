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

import React, { useRef, useState } from 'react'
import { Button, Dropdown, Form, FormListOperation, Input, InputNumber, Popconfirm, Select, Tooltip } from 'antd'
import { useClickAway } from 'react-use'
import SpecialColumnTypeComponent from '@/components/SpecialColumnTypeComponent'
import { ColumnSpesicalType, ColumnWithParamType } from '@/config'
import { capitalizeFirstLetter, extractNumbersInParentheses } from '@/lib/utils'
import { cn } from '@/lib/utils/tailwind'

export default function ColumnTypeComponent({ ...props }) {
  ;``
  const { form, parentField, subField, columnNamespace, columnTypes, provider, disabled } = props
  const columnTypeRef = useRef(null)
  const SpecialColumnTypeRef = useRef(null)
  const [isShowParamsInput, setIsShowParamsInput] = useState(false)
  const [errorMsg, setErrorMsg] = useState('')
  const [paramL, setParamL] = useState('')
  const [paramP, setParamP] = useState('')
  const [paramS, setParamS] = useState('')
  const [open, setOpen] = useState(false)
  const [isEditSpecialColumnType, setIsEditSpecialColumnType] = useState(false)
  const currentType = Form.useWatch([...parentField, subField.name, 'typeObj', ...columnNamespace, 'type'], form)

  const onChangeParamL = value => {
    setParamL(value)
  }

  const onChangeParamP = value => {
    setParamP(value)
  }

  const onChangeParamS = value => {
    setParamS(value)
  }

  const onFocus = e => {
    setErrorMsg('')
    setIsShowParamsInput(!isShowParamsInput)
  }

  const onChangeType = value => {
    setParamL('')
    setParamP('')
    setParamS('')
    setErrorMsg('')
    if (value && ColumnSpesicalType.includes(value)) {
      setOpen(true)
    } else {
      setOpen(false)
    }
  }

  const editSpecialColumnType = () => {
    setIsEditSpecialColumnType(true)
    setOpen(true)
  }

  const setDefaultValue = () => {
    setIsEditSpecialColumnType(true)
    setOpen(true)
  }

  useClickAway(columnTypeRef, e => {
    if (columnTypeRef.current && e?.target && !columnTypeRef.current.contains(e?.target)) {
      setIsShowParamsInput(false)
      setErrorMsg('')
      const currentTypeParam = extractNumbersInParentheses(currentType)
      const value = currentType?.replace(/\([^)]*\)/g, '')?.toLowerCase()
      if (currentTypeParam && !paramL && ColumnWithParamType.includes(value)) {
        setParamL(currentTypeParam)
      }
      const [currentTypeParam1, currentTypeParam2] = currentTypeParam.split(',')
      if (currentTypeParam && value === 'decimal' && !paramP && !paramS) {
        setParamP(currentTypeParam1)
        setParamS(currentTypeParam2)
      }
      if (ColumnWithParamType.includes(value)) {
        form.setFieldValue(
          [...parentField, subField.name, 'typeObj', ...columnNamespace, 'type'],
          capitalizeFirstLetter(value) + `(${paramL || currentTypeParam})`
        )
        if (!paramL && !currentTypeParam) {
          setErrorMsg('L is missing')
        }
      }
      if (value === 'decimal') {
        form.setFieldValue(
          [...parentField, subField.name, 'typeObj', ...columnNamespace, 'type'],
          capitalizeFirstLetter(value) + `(${paramP || currentTypeParam1 || ''},${paramS || currentTypeParam2 || ''})`
        )
        const errors = []
        if (!paramP && !currentTypeParam1) {
          errors.push('P is missing')
        }
        if (!paramS && !currentTypeParam2) {
          errors.push('S is missing')
        }
        if (paramP && +paramP > 38) {
          errors.push('P can not > 38')
        }
        if (paramS && +paramP < +paramS) {
          errors.push('P can not < S')
        }
        if (errors.length) {
          setErrorMsg(errors.join(', '))
        }
      }
    }
  })

  const confirm = () => {
    form
      .validateFields([[...parentField, subField.name, 'typeObj', ...columnNamespace]], { recursive: true })
      .then(() => setOpen(false))
  }

  const cancel = () => {
    !isEditSpecialColumnType &&
      form.setFieldValue([...parentField, subField.name, 'typeObj', ...columnNamespace], { type: '' })
    setOpen(false)
  }

  return (
    <>
      <div className='flex items-center' ref={columnTypeRef}>
        <Form.Item
          noStyle
          name={[subField.name, 'typeObj', ...columnNamespace, 'type']}
          label=''
          rules={[
            () => ({
              validator(_, type) {
                if (!type) {
                  return Promise.reject(new Error('The column type is required!'))
                }

                return Promise.resolve()
              }
            })
          ]}
        >
          <Select
            size='small'
            getPopupContainer={() => columnTypeRef.current}
            onFocus={onFocus}
            onChange={onChangeType}
            disabled={disabled}
          >
            {columnTypes.map(type => (
              <Select.Option value={type} key={type}>
                <Tooltip placement='left' title={type}>
                  <div>{capitalizeFirstLetter(type)}</div>
                </Tooltip>
              </Select.Option>
            ))}
          </Select>
        </Form.Item>
        <Popconfirm
          title=''
          icon={<></>}
          description={
            <SpecialColumnTypeComponent
              form={form}
              parentField={parentField}
              subField={subField}
              columnNamespace={columnNamespace}
              isEdit={form.getFieldValue('columns')[subField.name]?.isEdit}
              columnTypes={columnTypes}
              provider={provider}
              ref={SpecialColumnTypeRef}
            />
          }
          open={open}
          onConfirm={confirm}
          onCancel={cancel}
          okText='Ok'
          cancelText='Cancel'
        ></Popconfirm>

        {currentType && ColumnSpesicalType.includes(currentType.toLowerCase()) && (
          <Button type='link' className='ml-1 h-min p-0' onClick={editSpecialColumnType}>
            Edit
          </Button>
        )}

        {typeof currentType === 'string' &&
          ColumnWithParamType.includes(currentType?.replace(/\([^)]*\)/g, '')) &&
          isShowParamsInput && (
            <div className='ml-1 border-b focus-within:border-[color:theme(colors.defaultPrimary)]'>
              <InputNumber
                size='small'
                controls={false}
                placeholder='L'
                value={paramL}
                onChange={onChangeParamL}
                className='w-20 border-none focus-within:shadow-none'
              />
            </div>
          )}
        {typeof currentType === 'string' &&
          currentType?.replace(/\([^)]*\)/g, '').toLowerCase() === 'decimal' &&
          isShowParamsInput && (
            <>
              <div className='ml-1 border-b focus-within:border-[color:theme(colors.defaultPrimary)]'>
                <InputNumber
                  size='small'
                  controls={false}
                  placeholder='P'
                  value={paramP}
                  onChange={onChangeParamP}
                  className='w-10 border-none focus-within:shadow-none'
                />
              </div>
              <div className='ml-1 border-b focus-within:border-[color:theme(colors.defaultPrimary)]'>
                <InputNumber
                  size='small'
                  controls={false}
                  placeholder='S'
                  value={paramS}
                  onChange={onChangeParamS}
                  className='w-10 border-none focus-within:shadow-none'
                />
              </div>
            </>
          )}
      </div>
      <div className='text-red-500'>{errorMsg}</div>
      <Form.Item shouldUpdate={(prevValues, currentValues) => prevValues.type !== currentValues.type} noStyle>
        {() => {
          const errors = form.getFieldError([...parentField, subField.name, 'typeObj', ...columnNamespace, 'type'])

          return errors.length ? <div className='text-red-500'>{errors[0]}</div> : null
        }}
      </Form.Item>
    </>
  )
}
