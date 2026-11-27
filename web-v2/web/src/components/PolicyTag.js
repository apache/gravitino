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
import { Button, Cascader, Flex, Input, Space, Tag, Tooltip, theme } from 'antd'
import { useAppSelector, useAppDispatch } from '@/lib/hooks/useStore'
import { addPolicyForTag, getPoliciesForTag, removePolicyFromTag } from '@/lib/store/policies'
import { getCurrentEntityPolicies } from '@/lib/store/metalakes'
import { useSearchParams } from 'next/navigation'

const tagInputStyle = {
  width: 130,
  height: 22,
  marginInlineEnd: 8,
  verticalAlign: 'top'
}

const tagValuePrefix = 'value:'

export default function PolicyTag({ ...props }) {
  const { readOnly, metadataObjectType, metadataObjectFullName, allowedValues, refreshVersion, onBindingsChange } =
    props
  const isTag = metadataObjectType === 'tag'
  const hasValues = allowedValues?.length !== 0
  const { token } = theme.useToken()
  const [inputVisible, setInputVisible] = useState(false)
  const [customPolicy, setCustomPolicy] = useState(null)
  const containerRef = useRef(null)
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
    let active = true
    let observer

    const initLoad = async () => {
      const policies = await getPolicies()
      if (active && policies) setPoliciesForObject(policies)
    }
    if (currentMetalake && metadataObjectType && metadataObjectFullName) {
      if (!isTag || typeof IntersectionObserver === 'undefined') {
        initLoad()
      } else {
        observer = new IntersectionObserver(([entry]) => {
          if (entry.isIntersecting) {
            observer.disconnect()
            initLoad()
          }
        })
        if (containerRef.current) observer.observe(containerRef.current)
      }
    }

    return () => {
      active = false
      observer?.disconnect()
    }
  }, [currentMetalake, metadataObjectType, metadataObjectFullName, isTag, refreshVersion])

  const refreshAfterMutation = async () => {
    onBindingsChange?.(metadataObjectFullName)
    if (containerRef.current) {
      const policies = await getPolicies()
      if (containerRef.current && policies) setPoliciesForObject(policies)
    }
  }

  const associatePolicy = async (policy, selector) => {
    setInputVisible(false)
    setCustomPolicy(null)

    const result = await dispatch(
      addPolicyForTag({ metalake: currentMetalake, tag: metadataObjectFullName, policy, selector })
    )
    if (result.meta.requestStatus === 'fulfilled') {
      await refreshAfterMutation()
    }
  }

  const handleChange = ([policy, selection]) => {
    if (selection === 'custom') {
      setInputVisible(false)
      setCustomPolicy(policy)

      return
    }

    const selector = selection?.startsWith(tagValuePrefix)
      ? { type: 'TAG_VALUE', value: selection.slice(tagValuePrefix.length) }
      : { type: 'ALL_VALUES' }
    associatePolicy(policy, selector)
  }

  const handleClose = async (removedPolicy, event) => {
    event.preventDefault()

    const result = await dispatch(
      removePolicyFromTag({ metalake: currentMetalake, tag: metadataObjectFullName, policy: removedPolicy })
    )
    if (result.meta.requestStatus === 'fulfilled') {
      await refreshAfterMutation()
    }
  }

  const handleClick = name => () => {
    router.push(`/metadataObjectsForPolicy?policy=${name}&metalake=${currentMetalake}`)
  }

  const tagPlusStyle = {
    height: 22,
    background: token.colorBgContainer,
    borderStyle: 'dashed'
  }

  const selectorOptions = hasValues
    ? [
        { label: 'All values', value: 'all' },
        ...(allowedValues == null
          ? [{ label: 'Enter a value', value: 'custom' }]
          : allowedValues.map(value => ({ label: value, value: `${tagValuePrefix}${value}` })))
      ]
    : undefined

  // The API stores one selector per tag and policy. Remove a binding before adding another.
  const availablePolicies = inputVisible
    ? store.policiesData?.filter(policy => !policiesForObject.some(boundPolicy => boundPolicy.name === policy.name))
    : []

  return (
    <Flex ref={containerRef} gap='4px 0' wrap>
      {policiesForObject?.map((policy, index) => {
        const selectorLabel = policy.selector?.type === 'TAG_VALUE' ? policy.selector.value : 'All values'
        const label = isTag && hasValues ? `${policy.name} · ${selectorLabel}` : policy.name
        const isLongPolicy = label.length > 20
        const color = policy.enabled ? token.colorPrimary : token.colorTextDisabled

        const policyElem = (
          <Tag
            key={policy.name + index}
            color={color}
            closable={isTag && !readOnly && !policy.inherited}
            icon={policy.inherited ? <LockOutlined /> : null}
            onClick={isTag ? undefined : handleClick(policy.name)}
            className={isTag ? undefined : 'cursor-pointer'}
            style={{ userSelect: 'none' }}
            onClose={event => handleClose(policy.name, event)}
          >
            <span className='inline-block max-w-[190px] truncate align-bottom'>{label}</span>
          </Tag>
        )

        return isLongPolicy ? (
          <Tooltip title={label} key={policy.name}>
            {policyElem}
          </Tooltip>
        ) : (
          policyElem
        )
      })}
      {policiesForObject?.length === 0 && <span>No Policies</span>}
      {inputVisible && isTag && !readOnly && (
        <Cascader
          size='small'
          aria-label='Associate Policy'
          placeholder='Policy'
          style={tagInputStyle}
          autoFocus
          open
          allowClear={false}
          onOpenChange={setInputVisible}
          onChange={handleChange}
          options={availablePolicies?.map(policy => ({
            value: policy.name,
            label: (
              <Space>
                <span
                  className='tag-swatch'
                  style={{ backgroundColor: policy.enabled ? token.colorPrimary : token.colorTextDisabled }}
                />
                <span>{policy.name}</span>
              </Space>
            ),
            children: selectorOptions
          }))}
        />
      )}
      {customPolicy && (
        <Space size={4} wrap>
          <Input.Search
            size='small'
            aria-label={`Tag value for ${customPolicy}`}
            placeholder='Tag value'
            enterButton='Associate'
            autoFocus
            onPressEnter={event => event.preventDefault()}
            onSearch={value => {
              const trimmed = value.trim()
              if (trimmed) associatePolicy(customPolicy, { type: 'TAG_VALUE', value: trimmed })
            }}
          />
          <Button size='small' onClick={() => setCustomPolicy(null)}>
            Cancel
          </Button>
        </Space>
      )}
      {!inputVisible && !customPolicy && isTag && !readOnly && (
        <Tag style={tagPlusStyle} icon={<PlusOutlined />} onClick={() => setInputVisible(true)}>
          Associate Policy
        </Tag>
      )}
    </Flex>
  )
}
