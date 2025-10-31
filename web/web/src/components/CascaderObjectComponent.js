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
import { Cascader, Checkbox, Form, Select, Tooltip } from 'antd'
import Icons from '@/components/Icons'
import { privilegeOptionsMap } from '@/config/security'
import { useAppDispatch } from '@/lib/hooks/useStore'
import {
  fetchCatalogs,
  fetchSchemas,
  fetchTables,
  fetchFilesets,
  fetchTopics,
  fetchModels,
  fetchModelVersions,
  getCatalogDetails
} from '@/lib/store/metalakes'
import { cn } from '@/lib/utils/tailwind'

const { Option, OptGroup } = Select

export default function CascaderObjectComponent({ ...props }) {
  const { form, subField, metalake, editRole, cacheData } = props
  const cascaderRef = useRef(null)
  const dispatch = useAppDispatch()

  const [options, setOptions] = useState(
    metalake
      ? [
          {
            value: metalake,
            label: metalake,
            key: metalake,
            entityType: 'metalake',
            catalogType: '',
            isLeaf: false
          }
        ]
      : []
  )
  const [privilegeOptions, setPrivilegeOptions] = useState([])
  const [allowPrivilegesOptions, setAllowPrivilegesOptions] = useState([])
  const [denyPrivilegesOptions, setDenyPrivilegesOptions] = useState([])
  const fullName = Form.useWatch(['securableObjects', subField.name, 'fullName'], form)
  const type = Form.useWatch(['securableObjects', subField.name, 'type'], form)
  const allowPrivileges = Form.useWatch(['securableObjects', subField.name, 'allowPrivileges'], form)
  const denyPrivileges = Form.useWatch(['securableObjects', subField.name, 'denyPrivileges'], form)

  const isShowDeny =
    Form.useWatch(['securableObjects', subField.name, 'isShowDeny'], form) || (denyPrivileges?.length > 0 && !!editRole)

  // const { trigger: getCatalogDetail } = useCatalogAsync()

  useEffect(() => {
    const initLoad = async () => {
      if (['catalog', 'schema'].includes(type)) {
        const catalog = fullName?.[1]
        if (!catalog) {
          setPrivilegeOptions([])

          return
        }
        const { payload } = await dispatch(getCatalogDetails({ metalake, catalog }))
        const key = `${type}-${payload?.catalog?.type}`
        setPrivilegeOptions(privilegeOptionsMap[key])
      } else {
        setPrivilegeOptions(privilegeOptionsMap[type])
      }
    }
    initLoad()
  }, [fullName, type])

  useEffect(() => {
    if (allowPrivileges?.length) {
      const allowOptions = privilegeOptions?.map(group => ({
        label: group.label,
        options: group.options.filter(option => !allowPrivileges.includes(option.value))
      }))
      setDenyPrivilegesOptions(allowOptions?.filter(group => group.options.length > 0))
    } else {
      setDenyPrivilegesOptions(privilegeOptions)
    }
    if (denyPrivileges?.length) {
      const denyOptions = privilegeOptions?.map(group => ({
        label: group.label,
        options: group.options.filter(option => !denyPrivileges.includes(option.value))
      }))
      setAllowPrivilegesOptions(denyOptions?.filter(group => group.options.length > 0))
    } else {
      setAllowPrivilegesOptions(privilegeOptions)
    }
  }, [privilegeOptions, allowPrivileges, denyPrivileges])

  const handleGroupSelectAll = (groupLabel, checked, item) => {
    const options = item === 'allowPrivileges' ? allowPrivilegesOptions : denyPrivilegesOptions
    const groupOptions = options.find(group => group.label === groupLabel)?.options
    const groupValues = groupOptions?.map(option => option.value)
    const selectedItems = form.getFieldValue(['securableObjects', subField.name, item])

    if (checked) {
      const values = [...new Set([...selectedItems, ...groupValues])]
      form.setFieldValue(['securableObjects', subField.name, item], values)
    } else {
      const values = selectedItems.filter(value => !groupValues.includes(value))
      form.setFieldValue(['securableObjects', subField.name, item], values)
    }
  }

  const handleCheckGroup = (groupLabel, item) => {
    const options = item === 'allowPrivileges' ? allowPrivilegesOptions : denyPrivilegesOptions
    const groupOptions = options.find(group => group.label === groupLabel)?.options
    const groupValues = groupOptions?.map(option => option.value)
    const selectedItems = form.getFieldValue(['securableObjects', subField.name, item])

    return groupValues?.every(value => selectedItems?.includes(value))
  }

  const handleIndeterminate = (groupLabel, item) => {
    const options = item === 'allowPrivileges' ? allowPrivilegesOptions : denyPrivilegesOptions
    const groupOptions = options.find(group => group.label === groupLabel)?.options
    const groupValues = groupOptions?.map(option => option.value)

    const selectedItems = form
      .getFieldValue(['securableObjects', subField.name, item])
      ?.filter(value => groupValues.includes(value))

    return selectedItems?.length && selectedItems.length < groupValues.length
  }

  const handleSettingDeny = index => {
    if (!isShowDeny) {
      form.setFieldValue(['securableObjects', index, 'isShowDeny'], true)
    } else {
      form.setFieldValue(['securableObjects', index, 'denyPrivileges'], [])
      form.setFieldValue(['securableObjects', index, 'isShowDeny'], false)
    }
  }

  const loadData = selectedOptions =>
    new Promise(async resolve => {
      if (selectedOptions?.length) {
        const targetOption = selectedOptions[selectedOptions.length - 1]
        const { entityType, catalogType, value, key } = targetOption
        let optionData = []
        switch (entityType) {
          case 'metalake':
            const { payload: catalogsRes } = await dispatch(fetchCatalogs({ metalake }))
            optionData = catalogsRes?.catalogs?.map(catalog => ({
              label: catalog.name,
              value: catalog.name,
              entityType: 'catalog',
              catalogType: catalog.type,
              key: `${catalog.name}`,
              isLeaf: false
            }))
            break
          case 'catalog':
            const { payload: schemasRes } = await dispatch(fetchSchemas({ metalake, catalog: value, catalogType }))
            optionData = schemasRes?.schemas?.map(schema => ({
              label: schema.name,
              value: schema.name,
              entityType: 'schema',
              catalogType: catalogType,
              key: `${key}/${schema.name}`,
              isLeaf: false
            }))
            break
          case 'schema':
            const [catalogName, schemaName] = key.split('/')
            switch (catalogType) {
              case 'relational':
                {
                  const { payload: tablesRes } = await dispatch(
                    fetchTables({ metalake, catalog: catalogName, schema: schemaName })
                  )
                  optionData = tablesRes?.tables?.map(table => ({
                    label: table.name,
                    value: table.name,
                    entityType: 'table',
                    catalogType: catalogType,
                    key: `${key}/${table.name}`,
                    nodeType: 'table',
                    isLeaf: true
                  }))
                }
                break
              case 'fileset':
                {
                  const { payload: filesetsRes } = await dispatch(
                    fetchFilesets({ metalake, catalog: catalogName, schema: schemaName })
                  )
                  optionData = filesetsRes?.filesets?.map(fileset => ({
                    label: fileset.name,
                    value: fileset.name,
                    entityType: 'fileset',
                    catalogType: catalogType,
                    key: `${key}/${fileset.name}`,
                    nodeType: 'fileset',
                    isLeaf: true
                  }))
                }
                break
              case 'messaging':
                const { payload: topicsRes } = await dispatch(
                  fetchTopics({ metalake, catalog: catalogName, schema: schemaName })
                )
                optionData = topicsRes?.topics?.map(topic => ({
                  label: topic.name,
                  value: topic.name,
                  entityType: 'topic',
                  catalogType: catalogType,
                  key: `${key}/${topic.name}`,
                  nodeType: 'topic',
                  isLeaf: true
                }))
                break
              case 'model':
                const { payload: modelsRes } = await dispatch(
                  fetchModels({ metalake, catalog: catalogName, schema: schemaName })
                )
                optionData = modelsRes?.models?.map(model => ({
                  label: model.name,
                  value: model.name,
                  entityType: 'model',
                  catalogType: catalogType,
                  key: `${key}/${model.name}`,
                  nodeType: 'model',
                  isLeaf: true
                }))
                break
              default:
                break
            }
            break
        }
        targetOption.children = optionData || []
        setOptions([...options])
      }
      resolve()
    })

  const handleChange = selectedOptions => {
    const fullName =
      selectedOptions && selectedOptions.length > 0
        ? selectedOptions[selectedOptions.length - 1].key.replaceAll('/', '.')
        : ''
    if (cacheData?.[fullName]) {
      const [allowPrivileges, denyPrivileges] = cacheData[fullName]
      if (allowPrivileges.length || denyPrivileges.length) {
        form.setFieldValue(['securableObjects', subField.name, 'allowPrivileges'], allowPrivileges)
        form.setFieldValue(['securableObjects', subField.name, 'isShowDeny'], denyPrivileges.length > 0)
        form.setFieldValue(['securableObjects', subField.name, 'denyPrivileges'], denyPrivileges)
      }
    } else {
      form.setFieldValue(['securableObjects', subField.name, 'allowPrivileges'], [])
      form.setFieldValue(['securableObjects', subField.name, 'denyPrivileges'], [])
    }
    if (selectedOptions?.length) {
      const entityType = selectedOptions[selectedOptions.length - 1].entityType
      const catalogType = selectedOptions[selectedOptions.length - 1].catalogType

      form.setFieldValue(['securableObjects', subField.name, 'type'], entityType)
      let options = privilegeOptionsMap[entityType]
      if (['catalog', 'schema'].includes(entityType)) {
        const key = `${entityType}-${catalogType}`
        options = privilegeOptionsMap[key]
      }
      setPrivilegeOptions(options)
    } else {
      setPrivilegeOptions([])
    }
  }

  const displayRender = labels => {
    let displayLabels = labels.map((label, index) => (
      <div
        title={label}
        key={index}
        className='absolute bottom-[-3px] inline-block overflow-hidden truncate'
        style={{ maxWidth: `${300 / (labels.length - 1)}px` }}
      >
        {label}
        <span>{index === labels.length - 1 ? '' : '/'}</span>
      </div>
    ))
    if (labels.length > 1) {
      displayLabels.splice(0, 1)
    }

    return displayLabels
  }

  return (
    <>
      <div
        ref={cascaderRef}
        className={cn(
          [
            'col-span-3 px-2 py-1',
            '[&_.ant-cascader-menu-item-content]:relative',
            '[&_.ant-cascader-menu-item]:w-full',
            '[&_.ant-cascader-menu-item]:min-w-40'
          ],
          {
            '[&_.ant-cascader-menu-item]:h-[28px]': options.length > 0
          }
        )}
      >
        <Form.Item noStyle name={[subField.name, 'fullName']}>
          <Cascader
            size='small'
            options={options}
            loadData={loadData}
            onChange={(value, selectedOptions) => handleChange(selectedOptions)}
            changeOnSelect
            displayRender={displayRender}
            getPopupContainer={() => cascaderRef.current}
            className={cn(['[&_.ant-select-selection-item>div]:relative'])}
          />
        </Form.Item>
      </div>
      <div className='col-span-4 px-2 py-1'>
        <div className='flex items-center gap-2'>
          <span>Allow</span>
          <Form.Item noStyle name={[subField.name, 'allowPrivileges']} label=''>
            <Select size='small' mode='multiple' allowClear>
              {allowPrivilegesOptions?.map(group => (
                <OptGroup
                  key={group.label}
                  label={
                    <Checkbox
                      checked={handleCheckGroup(group.label, 'allowPrivileges')}
                      indeterminate={handleIndeterminate(group.label, 'allowPrivileges')}
                      onChange={e => handleGroupSelectAll(group.label, e.target.checked, 'allowPrivileges')}
                    >
                      <span className='ant-select-item-group'>{group.label}</span>
                    </Checkbox>
                  }
                >
                  {group.options.map(option => (
                    <Option key={option.value} value={option.value}>
                      {option.label}
                    </Option>
                  ))}
                </OptGroup>
              ))}
            </Select>
          </Form.Item>
          <Tooltip title={!isShowDeny ? 'Setting deny privileges' : 'Empty deny privileges and cancel setting'}>
            <Icons.Settings className='size-4 cursor-pointer' onClick={() => handleSettingDeny(subField.name)} />
          </Tooltip>
        </div>
        {isShowDeny && (
          <div className='mt-2 flex items-center gap-2'>
            <span>Deny</span>
            <Form.Item noStyle name={[subField.name, 'denyPrivileges']} label=''>
              <Select size='small' mode='multiple' allowClear>
                {denyPrivilegesOptions?.map(group => (
                  <OptGroup
                    key={group.label}
                    label={
                      <Checkbox
                        checked={handleCheckGroup(group.label, 'denyPrivileges')}
                        indeterminate={handleIndeterminate(group.label, 'denyPrivileges')}
                        onChange={e => handleGroupSelectAll(group.label, e.target.checked, 'denyPrivileges')}
                      >
                        <span className='ant-select-item-group'>{group.label}</span>
                      </Checkbox>
                    }
                  >
                    {group.options.map(option => (
                      <Option key={option.value} value={option.value}>
                        {option.label}
                      </Option>
                    ))}
                  </OptGroup>
                ))}
              </Select>
            </Form.Item>
            <Icons.Settings className='size-4 opacity-0' />
          </div>
        )}
      </div>
    </>
  )
}
