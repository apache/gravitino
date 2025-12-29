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

import React, { useEffect, useLayoutEffect, useState, useRef } from 'react'
import { Form, Input, Select, Checkbox, Spin } from 'antd'
import { to } from '@/lib/utils'
import { useAppDispatch } from '@/lib/hooks/useStore'
import {
  fetchCatalogs,
  fetchSchemas,
  fetchTables,
  fetchFilesets,
  fetchTopics,
  fetchModels
} from '@/lib/store/metalakes'
import { fetchTags } from '@/lib/store/tags'
import { fetchPolicies } from '@/lib/store/policies'
import { fetchJobTemplates } from '@/lib/store/jobs'
import { privilegeTypes, privilegeOptions } from '@/config/security'

export default function SecurableObjectFormFields({ fieldName, fieldKey, metalake }) {
  const form = Form.useFormInstance()
  const getFieldValue = form.getFieldValue.bind(form)
  const setFieldValue = form.setFieldValue.bind(form)
  const setFieldsValue = form.setFieldsValue.bind(form)
  const { Option, OptGroup } = Select
  const currentType = getFieldValue(['securableObjects', fieldName, 'type'])
  const currentFullRaw = getFieldValue(['securableObjects', fieldName, 'fullName'])
  const currentFull = String(currentFullRaw || '')
  const displayValue = currentFull
  const dispatch = useAppDispatch()

  const parts = String(currentFull || '')
    .split('.')
    .filter(Boolean)

  // remote options are now managed by SecurableObjectFormFields
  const [searchTextMap, setSearchTextMap] = useState({})
  const [privilegeGroupsMap, setPrivilegeGroupsMap] = useState({})
  const [privilegeGroupsLoading, setPrivilegeGroupsLoading] = useState({})

  const [schemasOptions, setSchemasOptions] = useState([])
  const [schemasLoading, setSchemasLoading] = useState(false)
  const [resourcesOptions, setResourcesOptions] = useState([])
  const [resourcesLoading, setResourcesLoading] = useState(false)

  // local selections for this form row (so we can set fullName only after both chosen)
  const [localCatalogVal, setLocalCatalogVal] = useState(undefined)
  const [localSchemaVal, setLocalSchemaVal] = useState(undefined)
  const skipSyncRef = useRef(false)
  const isTypeChanging = useRef(false)
  const [localResourceVal, setLocalResourceVal] = useState(undefined)

  const [localRemoteOptions, setLocalRemoteOptions] = useState({})
  const [localRemoteLoading, setLocalRemoteLoading] = useState({})
  const catalogOptions = localRemoteOptions['catalog'] || []

  const loadOptionsForType = async type => {
    if (!metalake) return
    const t = String(type || '').toLowerCase()

    // avoid duplicate loads
    if (localRemoteOptions[t]) return
    setLocalRemoteLoading(prev => ({ ...prev, [t]: true }))
    try {
      let res
      if (t === 'catalog') {
        const [err, r] = await to(dispatch(fetchCatalogs({ metalake })))
        if (err || !r) return
        res = (r?.payload?.catalogs || []).map(c => ({ label: c.name, value: c.name, catalogType: c.type }))
      } else if (t === 'tag') {
        const [err, r] = await to(dispatch(fetchTags({ metalake, details: true })))
        if (err || !r) return
        res = (r?.payload?.tags || []).map(tg => ({ label: tg.name, value: tg.name }))
      } else if (t === 'policy') {
        const [err, r] = await to(dispatch(fetchPolicies({ metalake, details: true })))
        if (err || !r) return
        res = (r?.payload?.policies || []).map(p => ({ label: p.name, value: p.name }))
      } else if (t === 'job_template' || t === 'job template') {
        const [err, r] = await to(dispatch(fetchJobTemplates({ metalake, details: true })))
        if (err || !r) return
        res = (r?.payload?.jobTemplates || []).map(j => ({ label: j.name, value: j.name }))
      }

      if (res) setLocalRemoteOptions(prev => ({ ...prev, [t]: res }))
    } finally {
      setLocalRemoteLoading(prev => ({ ...prev, [t]: false }))
    }
  }

  const loadPrivilegeGroupsForField = async (fieldKey, type, catalogName) => {
    if (!type) return

    // Normalize type for consistent comparisons
    const t = String(type || '').toLowerCase()

    // Always compute privilege groups for the current type/catalogName
    // (do not short-circuit based on an existing cached value for this field)
    setPrivilegeGroupsLoading(prev => ({ ...prev, [fieldKey]: true }))
    try {
      const groups = await getPrivilegeGroupsForType(t, catalogName)

      // transform groups into AntD Select grouped options if they are already in that shape
      setPrivilegeGroupsMap(prev => ({ ...prev, [fieldKey]: groups }))
    } finally {
      setPrivilegeGroupsLoading(prev => ({ ...prev, [fieldKey]: false }))
    }
  }

  const getPrivilegeGroupsForType = async (type, catalogName) => {
    if (!type) return []

    if (type === 'metalake') {
      return privilegeOptions
    }

    const groups = []

    if (type === 'catalog') {
      // Always include Catalog and Schema groups if present
      const catalogGroup = privilegeOptions.find(g => g.label === 'Catalog privileges')
      const schemaGroup = privilegeOptions.find(g => g.label === 'Schema privileges')

      // For Catalog type, only include the 'Use Catalog' privilege from Catalog privileges
      if (catalogGroup) {
        const useCatalogOptions = (catalogGroup.options || []).filter(
          o => o.label === 'Use Catalog' || o.value === 'use_catalog'
        )
        if (useCatalogOptions.length > 0) groups.push({ label: catalogGroup.label, options: useCatalogOptions })
      }
      if (schemaGroup) groups.push(schemaGroup)

      // If we have a cached catalog type, append the corresponding resource group
      const catalogType = catalogName ? catalogOptions.filter(c => c.value === catalogName)[0]?.catalogType : null
      if (catalogType) {
        if (catalogType === 'relational') {
          const tableGroup = privilegeOptions.find(g => g.label === 'Table privileges')
          tableGroup && groups.push(tableGroup)
        } else if (catalogType === 'messaging') {
          const topicGroup = privilegeOptions.find(g => g.label === 'Topic privileges')
          topicGroup && groups.push(topicGroup)
        } else if (catalogType === 'fileset') {
          const filesetGroup = privilegeOptions.find(g => g.label === 'Fileset privileges')
          filesetGroup && groups.push(filesetGroup)
        } else if (catalogType === 'model') {
          const modelGroup = privilegeOptions.find(g => g.label === 'Model privileges')
          modelGroup && groups.push(modelGroup)
        }
      }

      return groups
    }

    if (type === 'schema') {
      const schemaGroup = privilegeOptions.find(g => g.label === 'Schema privileges')
      if (schemaGroup) {
        const useSchemaOptions = schemaGroup.options.filter(o => o.label === 'Use Schema' || o.value === 'use_schema')
        if (useSchemaOptions.length > 0) groups.push({ label: schemaGroup.label, options: useSchemaOptions })
      }

      const catalogType = catalogName ? catalogOptions.filter(c => c.value === catalogName)[0]?.catalogType : null
      if (catalogType) {
        if (catalogType === 'relational') {
          const tableGroup = privilegeOptions.find(g => g.label === 'Table privileges')
          tableGroup && groups.push(tableGroup)
        } else if (catalogType === 'messaging') {
          const topicGroup = privilegeOptions.find(g => g.label === 'Topic privileges')
          topicGroup && groups.push(topicGroup)
        } else if (catalogType === 'fileset') {
          const filesetGroup = privilegeOptions.find(g => g.label === 'Fileset privileges')
          filesetGroup && groups.push(filesetGroup)
        } else if (catalogType === 'model') {
          const modelGroup = privilegeOptions.find(g => g.label === 'Model privileges')
          modelGroup && groups.push(modelGroup)
        }
      }

      return groups
    }

    if (type === 'tag') {
      const tagGroup = privilegeOptions.find(g => g.label === 'Tag privileges')
      if (!tagGroup) return []
      const applyOnly = (tagGroup.options || []).filter(o => o.label === 'Apply Tag' || o.value === 'apply_tag')

      return applyOnly.length > 0 ? [{ label: tagGroup.label, options: applyOnly }] : []
    }

    if (type === 'policy') {
      const policyGroup = privilegeOptions.find(g => g.label === 'Policy privileges')
      if (!policyGroup) return []

      const applyOnly = (policyGroup.options || []).filter(
        o => o.label === 'Apply Policy' || o.value === 'apply_policy'
      )

      return applyOnly.length > 0 ? [{ label: policyGroup.label, options: applyOnly }] : []
    }

    if (type === 'job_template') {
      const jtGroup = privilegeOptions.find(g => g.label === 'Job Template privileges')
      if (!jtGroup) return []

      const useOnly = (jtGroup.options || []).filter(
        o => o.label === 'Use Job Template' || o.value === 'use_job_template'
      )

      return useOnly.length > 0 ? [{ label: jtGroup.label, options: useOnly }] : []
    }

    const keyLabelMap = {
      table: 'Table privileges',
      topic: 'Topic privileges',
      fileset: 'Fileset privileges',
      model: 'Model privileges'
    }

    const lbl = keyLabelMap[type]
    if (lbl) {
      const g = privilegeOptions.find(x => x.label === lbl)
      if (g) {
        let options = g.options
        if (type === 'table') {
          options = options.filter(o => !(o.label === 'Create Table' || o.value === 'create_table'))
        } else if (type === 'topic') {
          options = options.filter(o => !(o.label === 'Create Topic' || o.value === 'create_topic'))
        } else if (type === 'fileset') {
          options = options.filter(o => !(o.label === 'Create Fileset' || o.value === 'create_fileset'))
        } else if (type === 'model') {
          options = options.filter(o => !(o.label === 'Register Model' || o.value === 'register_model'))
        }

        return [{ label: g.label, options }]
      }

      return []
    }

    return []
  }

  const filteredCatalogOptions = catalogOptions.filter(c => {
    const catalogType = c.catalogType
    if (currentType === 'table') return catalogType === 'relational'
    if (currentType === 'fileset') return catalogType === 'fileset'
    if (currentType === 'topic') return catalogType === 'messaging'
    if (currentType === 'model') return catalogType === 'model'

    return true // for schema and other types, show all
  })

  const getPart = nFromEnd => {
    if (!parts.length) return undefined

    return parts[parts.length - nFromEnd]
  }

  const isResourceThree = ['table', 'topic', 'fileset', 'model'].includes(currentType)
  const catalogSelected = getPart(isResourceThree ? 3 : currentType === 'schema' ? 2 : 1)
  const schemaSelected = getPart(isResourceThree ? 2 : currentType === 'schema' ? 1 : 0)
  const resourceSelected = getPart(1)

  useLayoutEffect(() => {
    // If the parent form is actively initializing, skip type-change
    // side-effects until that process finishes. The parent sets the
    // `__init_in_progress` field (boolean) to indicate this state.
    const initInProgress = getFieldValue('__init_in_progress')
    if (initInProgress) return

    // Skip this effect entirely for the `metalake` type.
    if (currentType === 'metalake') return

    isTypeChanging.current = true
    setLocalSchemaVal(schemaSelected || undefined)
    setLocalResourceVal(resourceSelected || undefined)

    if (catalogSelected) loadSchemasForCatalog(catalogSelected)
    else setSchemasOptions([])

    // clear privileges when type changes
  }, [catalogSelected])

  useEffect(() => {
    if (schemaSelected && isResourceThree) {
      loadResourcesForSchema(catalogSelected, schemaSelected, currentType)
    } else {
      setResourcesOptions([])
    }
  }, [schemaSelected, currentType])

  // Handle type change effects: set fullName, clear privileges, load groups
  useLayoutEffect(() => {
    // If the parent form is actively initializing, skip type-change
    // side-effects until that process finishes. The parent sets the
    // `__init_in_progress` field (boolean) to indicate this state.
    const initInProgress = getFieldValue('__init_in_progress')
    if (initInProgress) return

    isTypeChanging.current = true
    if (currentType === 'metalake') {
      setFieldValue(['securableObjects', fieldName, 'fullName'], String(metalake || ''))
    } else {
      setFieldValue(['securableObjects', fieldName, 'fullName'], undefined)
    }

    // clear privileges when type changes
    setFieldValue(['securableObjects', fieldName, 'allowPrivileges'], [])
    setFieldValue(['securableObjects', fieldName, 'denyPrivileges'], [])

    // clear local selections when type changes
    setLocalCatalogVal(undefined)
    setLocalSchemaVal(undefined)
    setLocalResourceVal(undefined)

    // clear options when type changes
    setSchemasOptions([])
    setResourcesOptions([])
    const cf = getFieldValue(['securableObjects', fieldName, 'fullName'])
    const catalogName = Array.isArray(cf) ? cf[0] : String(cf || '').split('.')[0]
    loadPrivilegeGroupsForField(fieldName, currentType, catalogName)
    setTimeout(() => (isTypeChanging.current = false), 0)
  }, [currentType])

  // Clear privileges when fullName changes (for any type)
  useEffect(() => {
    // If parent form initialization is in progress, skip clearing privileges
    // until the parent explicitly marks initialization finished.
    const initInProgress = getFieldValue('__init_in_progress')
    if (initInProgress) {
      const entityNames = currentFull.split('.')
      if (entityNames.length >= 2) {
        setLocalCatalogVal(entityNames[0])
        setLocalSchemaVal(entityNames[1])
      }
      if (entityNames.length === 3) {
        setLocalResourceVal(entityNames[2])
      }
      loadPrivilegeGroupsForField(fieldName, currentType, entityNames[0])
    } else {
      setFieldValue(['securableObjects', fieldName, 'allowPrivileges'], [])
      setFieldValue(['securableObjects', fieldName, 'denyPrivileges'], [])
    }
  }, [currentFull])

  const loadSchemasForCatalog = async catalog => {
    if (!catalog) return
    setSchemasLoading(true)
    try {
      const { payload } = await dispatch(fetchSchemas({ metalake, catalog }))
      const schemas = payload?.schemas || []
      setSchemasOptions(schemas.map(s => ({ label: s.name, value: s.name })))
    } catch (e) {
      setSchemasOptions([])
    } finally {
      setSchemasLoading(false)
    }
  }

  const loadResourcesForSchema = async (catalog, schema, type) => {
    if (!catalog || !schema) return
    setResourcesLoading(true)
    try {
      let payload
      const t = String(type || '').toLowerCase()
      if (t === 'table') {
        const res = await dispatch(fetchTables({ metalake, catalog, schema }))
        payload = res.payload
        setResourcesOptions((payload?.tables || []).map(t => ({ label: t.name, value: t.name })))
      } else if (t === 'topic') {
        const res = await dispatch(fetchTopics({ metalake, catalog, schema }))
        payload = res.payload
        setResourcesOptions((payload?.topics || []).map(t => ({ label: t.name, value: t.name })))
      } else if (t === 'fileset') {
        const res = await dispatch(fetchFilesets({ metalake, catalog, schema }))
        payload = res.payload
        setResourcesOptions((payload?.filesets || []).map(f => ({ label: f.name, value: f.name })))
      } else if (t === 'model') {
        const res = await dispatch(fetchModels({ metalake, catalog, schema }))
        payload = res.payload
        setResourcesOptions((payload?.models || []).map(m => ({ label: m.name, value: m.name })))
      }
    } catch (e) {
      setResourcesOptions([])
    } finally {
      setResourcesLoading(false)
    }
  }

  const getGroupOptions = (groupLabel, item) => {
    const groups = item === 'allowPrivileges' ? allowFilteredGroups : denyFilteredGroups

    return groups.find(g => g.label === groupLabel)?.options || []
  }

  const handleGroupSelectAll = (groupLabel, checked, item) => {
    const groupValues = getGroupOptions(groupLabel, item).map(o => o.value)
    const selectedItems = getFieldValue(['securableObjects', fieldName, item]) || []

    if (checked) {
      const values = [...new Set([...(selectedItems || []), ...groupValues])]
      setFieldValue(['securableObjects', fieldName, item], values)
    } else {
      const values = (selectedItems || []).filter(value => !groupValues.includes(value))
      setFieldValue(['securableObjects', fieldName, item], values)
    }
  }

  const handleCheckGroup = (groupLabel, item) => {
    const groupValues = getGroupOptions(groupLabel, item).map(o => o.value)
    const selectedItems = getFieldValue(['securableObjects', fieldName, item]) || []

    return groupValues.length > 0 && groupValues.every(value => selectedItems?.includes(value))
  }

  const handleIndeterminate = (groupLabel, item) => {
    const groupValues = getGroupOptions(groupLabel, item).map(o => o.value)

    const selectedItems = (getFieldValue(['securableObjects', fieldName, item]) || []).filter(value =>
      groupValues.includes(value)
    )

    return selectedItems?.length && selectedItems.length < groupValues.length
  }

  const allowSelected = getFieldValue(['securableObjects', fieldName, 'allowPrivileges']) || []
  const denySelected = getFieldValue(['securableObjects', fieldName, 'denyPrivileges']) || []
  const groupedPrivilegeOptions = privilegeGroupsMap[fieldName] || []

  const allowFilteredGroups = groupedPrivilegeOptions
    .map(group => ({
      ...group,
      options: group.options.filter(opt => !denySelected.includes(opt.value))
    }))
    .filter(group => group.options.length > 0)

  const denyFilteredGroups = groupedPrivilegeOptions
    .map(group => ({
      ...group,
      options: group.options.filter(opt => !allowSelected.includes(opt.value))
    }))
    .filter(group => group.options.length > 0)

  return (
    <div className='flex flex-col gap-2'>
      <div>
        <Form.Item name={[fieldName, 'type']} label='Type' rules={[{ required: true }]} style={{ marginBottom: 8 }}>
          <Select placeholder='Please select type' options={privilegeTypes} />
        </Form.Item>
      </div>

      <div>
        <Form.Item name={[fieldName, 'fullName']} label='Full Name' rules={[{ required: true }]}>
          {(() => {
            if (currentType === 'metalake') {
              return <Input value={metalake} disabled />
            }

            // Schema: two linked selects (catalog -> schema)
            if (currentType === 'schema') {
              return (
                <div className='flex gap-2'>
                  <Select
                    showSearch
                    filterOption={false}
                    placeholder={'Please search or select catalog name'}
                    loading={!!localRemoteLoading['catalog']}
                    onFocus={() => loadOptionsForType('catalog')}
                    value={localCatalogVal}
                    onSearch={val => {
                      if (!filteredCatalogOptions.length) loadOptionsForType('catalog')
                    }}
                    onChange={(val, _opt) => {
                      const v = val == null ? val : String(val)

                      // update local catalog and clear schema selection (independent values)
                      setLocalCatalogVal(v || undefined)
                      setLocalSchemaVal(undefined)

                      // Only clear fullName when there was an existing fullName (edit flow)
                      // to avoid the sync effect overwriting the user's just-selected catalog.
                      if (currentFull) {
                        skipSyncRef.current = true
                        setTimeout(() => setFieldValue(['securableObjects', fieldName, 'fullName'], ''), 0)
                      }

                      // clear privileges when catalog changes
                      setTimeout(() => setFieldValue(['securableObjects', fieldName, 'allowPrivileges'], []), 0)
                      setTimeout(() => setFieldValue(['securableObjects', fieldName, 'denyPrivileges'], []), 0)

                      loadSchemasForCatalog(v)
                      loadPrivilegeGroupsForField(fieldName, currentType, v)
                    }}
                    style={{ minWidth: 180 }}
                  >
                    {filteredCatalogOptions.map((o, i) => (
                      <Option key={`${String(o.value)}-${i}`} value={String(o.value)} label={String(o.label)}>
                        {o.label}
                      </Option>
                    ))}
                  </Select>
                  <Select
                    showSearch
                    filterOption={(input, option) =>
                      String(option.label).toLowerCase().includes(String(input).toLowerCase())
                    }
                    placeholder={'Please search or select schema name'}
                    loading={schemasLoading}
                    value={localSchemaVal}
                    onSearch={() => {}}
                    onFocus={() => {
                      if (!schemasOptions.length && localCatalogVal) {
                        loadSchemasForCatalog(localCatalogVal)
                      }
                    }}
                    onChange={(val, _opt) => {
                      const v = val == null ? val : String(val)

                      // set schema locally
                      setLocalSchemaVal(v || undefined)

                      // only set fullName when both catalog and schema are present
                      const catalog = localCatalogVal
                      if (catalog && v) {
                        const newVal = [catalog, v]
                        setTimeout(() => setFieldValue(['securableObjects', fieldName, 'fullName'], newVal), 0)
                      } else {
                        setTimeout(() => setFieldValue(['securableObjects', fieldName, 'fullName'], []), 0)
                      }

                      // clear privileges when schema changes
                      setTimeout(() => setFieldValue(['securableObjects', fieldName, 'allowPrivileges'], []), 0)
                      setTimeout(() => setFieldValue(['securableObjects', fieldName, 'denyPrivileges'], []), 0)

                      loadPrivilegeGroupsForField(fieldName, currentType, localCatalogVal)
                    }}
                    style={{ minWidth: 220 }}
                  >
                    {schemasOptions.map((o, i) => (
                      <Option key={`${String(o.value)}-${i}`} value={String(o.value)} label={String(o.label)}>
                        {o.label}
                      </Option>
                    ))}
                  </Select>
                </div>
              )
            }

            // For Table/Topic/Fileset/Model types: three linked selects (catalog -> schema -> resource)
            if (['table', 'topic', 'fileset', 'model'].includes(currentType)) {
              return (
                <div className='flex gap-2'>
                  <Select
                    showSearch
                    filterOption={false}
                    placeholder={`Please search or select catalog name`}
                    loading={!!localRemoteLoading['catalog']}
                    onFocus={() => loadOptionsForType('catalog')}
                    value={localCatalogVal}
                    onChange={(val, _opt) => {
                      const v = val == null ? val : String(val)

                      // update local catalog, clear schema and resource selections
                      setLocalCatalogVal(v || undefined)
                      setLocalSchemaVal(undefined)
                      setLocalResourceVal(undefined)

                      // If there was an existing fullName (edit), skip immediate sync and clear it
                      if (currentFull) {
                        skipSyncRef.current = true
                        setTimeout(() => setFieldValue(['securableObjects', fieldName, 'fullName'], []), 0)
                      }

                      // clear privileges when catalog changes
                      setTimeout(() => setFieldValue(['securableObjects', fieldName, 'allowPrivileges'], []), 0)
                      setTimeout(() => setFieldValue(['securableObjects', fieldName, 'denyPrivileges'], []), 0)

                      loadSchemasForCatalog(v)
                      loadPrivilegeGroupsForField(fieldName, currentType, v)
                    }}
                    style={{ minWidth: 180 }}
                  >
                    {filteredCatalogOptions.map((o, i) => (
                      <Option key={`${String(o.value)}-${i}`} value={String(o.value)} label={String(o.label)}>
                        {o.label}
                      </Option>
                    ))}
                  </Select>
                  <Select
                    showSearch
                    filterOption={(input, option) =>
                      String(option.label).toLowerCase().includes(String(input).toLowerCase())
                    }
                    placeholder={`Please search or select schema name`}
                    loading={schemasLoading}
                    value={localSchemaVal}
                    onFocus={() => {
                      if (!schemasOptions.length && localCatalogVal) {
                        loadSchemasForCatalog(localCatalogVal)
                      }
                    }}
                    onChange={(val, _opt) => {
                      const v = val == null ? val : String(val)

                      // update local schema and clear resource selection
                      setLocalSchemaVal(v || undefined)
                      setLocalResourceVal(undefined)

                      // clear privileges when schema changes
                      setTimeout(() => setFieldValue(['securableObjects', fieldName, 'allowPrivileges'], []), 0)
                      setTimeout(() => setFieldValue(['securableObjects', fieldName, 'denyPrivileges'], []), 0)

                      // load resources for the selected catalog+schema
                      if (localCatalogVal && v) {
                        loadResourcesForSchema(localCatalogVal, v, currentType)
                      }

                      loadPrivilegeGroupsForField(fieldName, currentType, localCatalogVal)
                    }}
                    style={{ minWidth: 220 }}
                  >
                    {schemasOptions.map((o, i) => (
                      <Option key={`${String(o.value)}-${i}`} value={String(o.value)} label={String(o.label)}>
                        {o.label}
                      </Option>
                    ))}
                  </Select>
                  <Select
                    showSearch
                    filterOption={(input, option) =>
                      String(option.label).toLowerCase().includes(String(input).toLowerCase())
                    }
                    placeholder={`Please search or select ${currentType} name`}
                    loading={resourcesLoading}
                    value={localResourceVal}
                    onFocus={() => {
                      if (!resourcesOptions.length && localCatalogVal && localSchemaVal) {
                        loadResourcesForSchema(localCatalogVal, localSchemaVal, currentType)
                      }
                    }}
                    onChange={(val, _opt) => {
                      const v = val == null ? val : String(val)

                      // set resource locally
                      setLocalResourceVal(v || undefined)

                      // only set fullName when catalog, schema and resource are present
                      if (localCatalogVal && localSchemaVal && v) {
                        const newVal = [localCatalogVal, localSchemaVal, v]
                        setTimeout(() => setFieldValue(['securableObjects', fieldName, 'fullName'], newVal), 0)
                      } else {
                        setTimeout(() => setFieldValue(['securableObjects', fieldName, 'fullName'], []), 0)
                      }

                      setTimeout(() => setFieldValue(['securableObjects', fieldName, 'allowPrivileges'], []), 0)
                      setTimeout(() => setFieldValue(['securableObjects', fieldName, 'denyPrivileges'], []), 0)
                      loadPrivilegeGroupsForField(fieldName, currentType, localCatalogVal)
                    }}
                    style={{ minWidth: 220 }}
                  >
                    {resourcesOptions.map((o, i) => (
                      <Option key={`${String(o.value)}-${i}`} value={String(o.value)} label={String(o.label)}>
                        {o.label}
                      </Option>
                    ))}
                  </Select>
                </div>
              )
            }

            const remoteTypes = ['catalog', 'tag', 'policy', 'job_template']
            if (remoteTypes.includes(currentType)) {
              const optionsAll = localRemoteOptions[currentType] || []
              const searchText = searchTextMap[fieldName] || ''

              const displayedOptions = searchText
                ? optionsAll
                    .filter(o =>
                      String(o.label || '')
                        .toLowerCase()
                        .includes(String(searchText).toLowerCase())
                    )
                    .slice(0, 10)
                : optionsAll.slice(0, 10)

              return (
                <Select
                  showSearch
                  filterOption={false}
                  optionLabelProp='label'
                  placeholder={`Please search or select ${currentType} name`}
                  loading={!!localRemoteLoading[currentType]}
                  onFocus={() => loadOptionsForType(currentType)}
                  onSearch={val => {
                    setSearchTextMap(prev => ({ ...prev, [fieldName]: val }))
                    if (!localRemoteOptions[currentType]) {
                      loadOptionsForType(currentType)
                    }
                  }}
                  onChange={(val, _opt) => {
                    const v = val == null ? val : String(val)

                    // set the fullName in the form so the collapse title and other
                    // dependent logic reflect the user's selection
                    setFieldValue(['securableObjects', fieldName, 'fullName'], v)

                    // clear privileges when fullName changes
                    setTimeout(() => setFieldValue(['securableObjects', fieldName, 'allowPrivileges'], []), 0)
                    setTimeout(() => setFieldValue(['securableObjects', fieldName, 'denyPrivileges'], []), 0)
                    loadPrivilegeGroupsForField(fieldName, currentType, v)
                  }}
                  value={displayValue || undefined}
                  allowClear
                >
                  {displayedOptions.map((o, i) => (
                    <Option key={`${String(o.value)}-${i}`} value={String(o.value)} label={String(o.label)}>
                      {o.label}
                    </Option>
                  ))}
                </Select>
              )
            }

            return (
              <Input
                placeholder={`Please input ${currentType} name`}
                value={displayValue}
                onChange={e => {
                  const v = e.target.value

                  // clear privileges when fullName changes
                  setTimeout(() => {
                    setFieldValue(['securableObjects', fieldName, 'fullName'], [v])
                    setFieldValue(['securableObjects', fieldName, 'allowPrivileges'], [])
                    setFieldValue(['securableObjects', fieldName, 'denyPrivileges'], [])
                  }, 0)
                  loadPrivilegeGroupsForField(fieldName, currentType, v)
                }}
              />
            )
          })()}
        </Form.Item>
      </div>

      <div>
        <Form.Item name={[fieldName, 'allowPrivileges']} label='Allow Privileges'>
          <Select
            mode='tags'
            placeholder='Add allow privileges'
            style={{ width: '100%' }}
            loading={!!privilegeGroupsLoading[fieldName]}
          >
            {allowFilteredGroups.map(group => (
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
        <Form.Item name={[fieldName, 'denyPrivileges']} label='Deny Privileges'>
          <Select
            mode='tags'
            placeholder='Add deny privileges'
            style={{ width: '100%' }}
            loading={!!privilegeGroupsLoading[fieldName]}
          >
            {denyFilteredGroups.map(group => (
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
      </div>
    </div>
  )
}
