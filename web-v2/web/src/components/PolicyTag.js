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
import { useAppSelector, useAppDispatch } from '@/lib/hooks/useStore'
import { addPolicyForTag, getPoliciesForTag, removePolicyFromTag } from '@/lib/store/policies'
import { getCurrentEntityPolicies } from '@/lib/store/metalakes'
import { useSearchParams } from 'next/navigation'

const tagInputStyle = {
  width: 100,
  height: 22,
  marginInlineEnd: 8,
  verticalAlign: 'top'
}

export default function PolicyTag({ ...props }) {
  const { readOnly, metadataObjectType, metadataObjectFullName } = props
  const isTag = metadataObjectType === 'tag'
  const { token } = theme.useToken()
  const [inputVisible, setInputVisible] = useState(false)
  const [optionList, setOptionList] = useState([])
  const containerRef = useRef(null)
  const inputRef = useRef(null)
  const router = useRouter()
  const store = useAppSelector(state => state.policies)
  const dispatch = useAppDispatch()
  const [policiesForObject, setPoliciesForObject] = useState([])
  const searchParams = useSearchParams()
  const currentMetalake = searchParams.get('metalake')

  const getPolicies = async () => {
    if (isTag) {
      const result = await dispatch(getPoliciesForTag({ metalake: currentMetalake, tag: metadataObjectFullName }))

      return result.meta.requestStatus === 'fulfilled' ? result.payload || [] : undefined
    }

    const result = await dispatch(
      getCurrentEntityPolicies({ metalake: currentMetalake, metadataObjectType, metadataObjectFullName, details: true })
    )

    return result.meta.requestStatus === 'fulfilled' ? result.payload?.policies || [] : undefined
  }

  useEffect(() => {
    const initLoad = async () => {
      const policies = await getPolicies()
      if (policies) setPoliciesForObject(policies)
    }
    if (currentMetalake && metadataObjectType && metadataObjectFullName) {
      if (!isTag || typeof IntersectionObserver === 'undefined') {
        initLoad()
      } else {
        const observer = new IntersectionObserver(([entry]) => {
          if (entry.isIntersecting) {
            observer.disconnect()
            initLoad()
          }
        })
        if (containerRef.current) observer.observe(containerRef.current)

        return () => observer.disconnect()
      }
    }
  }, [currentMetalake, metadataObjectType, metadataObjectFullName, isTag])

  useEffect(() => {
    if (inputVisible) {
      const options =
        store.policiesData?.filter(
          policyOption => !policiesForObject?.map(policy => policy.name)?.includes(policyOption.name)
        ) || []
      setOptionList(options)
      inputRef.current?.focus()
    }
  }, [inputVisible, policiesForObject, store.policiesData])

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
    if (value && !policiesForObject?.map(policy => policy.name)?.includes(value)) {
      const result = await dispatch(
        addPolicyForTag({ metalake: currentMetalake, tag: metadataObjectFullName, policy: value })
      )
      if (result.meta.requestStatus === 'fulfilled') {
        const policies = await getPolicies()
        if (policies) setPoliciesForObject(policies)
      }
    }
    setInputVisible(false)
  }

  const handleClose = async (removedPolicy, event) => {
    event.preventDefault()

    const result = await dispatch(
      removePolicyFromTag({ metalake: currentMetalake, tag: metadataObjectFullName, policy: removedPolicy })
    )
    if (result.meta.requestStatus === 'fulfilled') {
      const policies = await getPolicies()
      if (policies) setPoliciesForObject(policies)
    }
  }

  const handleBlur = () => {
    setInputVisible(false)
  }

  const handleClick = name => () => {
    router.push(`/metadataObjectsForPolicy?policy=${name}&metalake=${currentMetalake}`)
  }

  const tagPlusStyle = {
    height: 22,
    background: token.colorBgContainer,
    borderStyle: 'dashed'
  }

  return (
    <Flex ref={containerRef} gap='4px 0' wrap>
      {policiesForObject?.map((policy, index) => {
        const isLongPolicy = policy.name.length > 20
        let color = policy.enabled ? token.colorPrimary : token.colorTextDisabled

        const policyElem = (
          <Tag
            key={policy.name + index}
            color={color}
            closable={isTag && !readOnly && !policy.inherited}
            icon={policy.inherited ? <LockOutlined /> : null}
            onClick={handleClick(policy.name)}
            className='cursor-pointer'
            style={{ userSelect: 'none' }}
            onClose={event => handleClose(policy.name, event)}
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
      {policiesForObject?.length === 0 && <span>No Policies</span>}
      {inputVisible && isTag && !readOnly && (
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
      {!inputVisible && isTag && !readOnly && (
        <Tag style={tagPlusStyle} icon={<PlusOutlined />} onClick={showInput}>
          Associate Policy
        </Tag>
      )}
    </Flex>
  )
}
