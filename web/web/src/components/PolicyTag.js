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

import React, { useEffect, useRef, useState } from 'react'
import { useRouter } from 'next/navigation'
import { LockOutlined, PlusOutlined } from '@ant-design/icons'
import { Flex, Select, Space, Tag, Tooltip, theme } from 'antd'
import { cn } from '@/lib/utils/tailwind'
import { useAppSelector, useAppDispatch } from '@/lib/hooks/useStore'
import { associatePolicy } from '@/lib/store/policies'
import { getCurrentEntityPolicies } from '@/lib/store/metalakes'

const tagInputStyle = {
  width: 100,
  height: 22,
  marginInlineEnd: 8,
  verticalAlign: 'top'
}

export default function PolicyTag({ ...props }) {
  const { readOnly, metalake, metadataObjectType, metadataObjectFullName } = props
  const { token } = theme.useToken()
  const [inputVisible, setInputVisible] = useState(false)
  const [optionList, setOptionList] = useState([])
  const inputRef = useRef(null)
  const router = useRouter()
  const store = useAppSelector(state => state.policies)
  const dispatch = useAppDispatch()
  const [policiesForEntity, setPoliciesForEntity] = useState([])

  const getPoliciesForEntity = async () => {
    const { payload } = await dispatch(
      getCurrentEntityPolicies({ metalake, metadataObjectType, metadataObjectFullName, details: true })
    )

    return payload?.policies || []
  }

  useEffect(() => {
    const initLoad = async () => {
      const policies = await getPoliciesForEntity()
      setPoliciesForEntity(policies)
    }
    if (metalake && metadataObjectType && metadataObjectFullName) {
      initLoad()
    }
  }, [metalake, metadataObjectType, metadataObjectFullName])

  useEffect(() => {
    if (inputVisible) {
      const options =
        store.policiesData?.filter(
          policyOption => !policiesForEntity?.map(policy => policy.name)?.includes(policyOption.name)
        ) || []
      setOptionList(options)
      inputRef.current?.focus()
    }
  }, [inputVisible, policiesForEntity])

  const showInput = () => {
    setInputVisible(true)
  }

  const tagRender = props => {
    const { value, closable, onClose } = props

    const onPreventMouseDown = event => {
      event.preventDefault()
      event.stopPropagation()
    }

    return (
      <Tag
        className='text-defaultPrimary'
        onMouseDown={onPreventMouseDown}
        closable={closable}
        onClose={onClose}
        style={{ marginInlineEnd: 4 }}
      >
        {value}
      </Tag>
    )
  }

  const handleChange = async value => {
    if (value && !policiesForEntity?.map(policy => policy.name)?.includes(value)) {
      await dispatch(
        associatePolicy({ metalake, metadataObjectType, metadataObjectFullName, data: { policiesToAdd: [value] } })
      )
      const policies = await getPoliciesForEntity()
      setPoliciesForEntity(policies)
    }
    setInputVisible(false)
  }

  const handleClose = async removedPolicy => {
    await dispatch(
      associatePolicy({
        metalake,
        metadataObjectType,
        metadataObjectFullName,
        data: { policiesToRemove: [removedPolicy] }
      })
    )
    const policies = await getPoliciesForEntity()
    setPoliciesForEntity(policies)
  }

  const handleBlur = () => {
    setInputVisible(false)
  }

  const handleClick = name => () => {
    router.push(`/metadataObjectsForPolicy?policy=${name}&metalake=${metalake}`)
  }

  const tagPlusStyle = {
    height: 22,
    background: token.colorBgContainer,
    borderStyle: 'dashed'
  }

  return (
    <Flex gap='4px 0' wrap>
      {policiesForEntity?.map((policy, index) => {
        const isLongPolicy = policy.name.length > 20
        let color = policy.enabled ? token.colorPrimary : token.colorTextDisabled

        const policyElem = (
          <Tag
            key={policy.name + index}
            color={color}
            closable={!policy.inherited}
            icon={policy.inherited ? <LockOutlined /> : null}
            onClick={handleClick(policy.name)}
            className='cursor-pointer'
            style={{ userSelect: 'none' }}
            onClose={() => handleClose(policy.name)}
          >
            <span>{isLongPolicy ? `${policy.name.slice(0, 20)}...` : policy.name}</span>
          </Tag>
        )

        return isLongPolicy ? (
          <Tooltip title={policy.name} key={policy.name}>
            {policyElem}
          </Tooltip>
        ) : (
          policyElem
        )
      })}
      {policiesForEntity?.length === 0 && <span>No policies</span>}
      {inputVisible && !readOnly && (
        <Select
          size='small'
          tagRender={tagRender}
          ref={inputRef}
          style={tagInputStyle}
          onChange={handleChange}
          onBlur={handleBlur}
        >
          {optionList.map(policy => (
            <Select.Option value={policy.name} key={policy.name}>
              <Space>
                <span
                  className='tag-swatch'
                  style={{ backgroundColor: policy.enabled ? token.colorPrimary : token.colorTextDisabled }}
                ></span>
                <span>{policy.name}</span>
              </Space>
            </Select.Option>
          ))}
        </Select>
      )}
      {!inputVisible && !readOnly && (
        <Tag style={tagPlusStyle} icon={<PlusOutlined />} onClick={showInput}>
          Associate Policy
        </Tag>
      )}
    </Flex>
  )
}
