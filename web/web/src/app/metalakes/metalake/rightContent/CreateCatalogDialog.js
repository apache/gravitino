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

'use client'

import { useState, forwardRef, useEffect, Fragment } from 'react'

import {
  Box,
  Grid,
  Button,
  Dialog,
  TextField,
  Typography,
  DialogContent,
  DialogActions,
  IconButton,
  Fade,
  Select,
  MenuItem,
  InputLabel,
  FormControl,
  FormHelperText
} from '@mui/material'

import Icon from '@/components/Icon'

import { useAppDispatch } from '@/lib/hooks/useStore'
import { createCatalog, updateCatalog } from '@/lib/store/metalakes'

import * as yup from 'yup'
import { useForm, Controller } from 'react-hook-form'
import { yupResolver } from '@hookform/resolvers/yup'

import { groupBy } from 'lodash-es'
import { genUpdates } from '@/lib/utils'
import { providers, filesetProviders, messagingProviders } from '@/lib/utils/initial'
import { nameRegex, nameRegexDesc, keyRegex } from '@/lib/utils/regex'
import { useSearchParams } from 'next/navigation'

const defaultValues = {
  name: '',
  type: 'relational',
  provider: '',
  comment: '',
  propItems: []
}

const schema = yup.object().shape({
  name: yup.string().required().matches(nameRegex, nameRegexDesc),
  type: yup.mixed().oneOf(['relational', 'fileset', 'messaging', 'model']).required(),
  provider: yup.string().when('type', (type, schema) => {
    switch (type) {
      case 'relational':
        return schema.oneOf(providers.map(i => i.value)).required()
      case 'fileset':
        return schema.oneOf(filesetProviders.map(i => i.value)).required()
      case 'messaging':
        return schema.oneOf(messagingProviders.map(i => i.value)).required()
      default:
        return schema
    }
  }),
  propItems: yup.array().of(
    yup.object().shape({
      required: yup.boolean(),
      key: yup.string().required(),
      value: yup.string().when('required', {
        is: true,
        then: schema => schema.required()
      })
    })
  )
})

const Transition = forwardRef(function Transition(props, ref) {
  return <Fade ref={ref} {...props} />
})

const CreateCatalogDialog = props => {
  const { open, setOpen, type = 'create', data = {} } = props
  const searchParams = useSearchParams()
  const metalake = searchParams.get('metalake')

  const dispatch = useAppDispatch()

  const [innerProps, setInnerProps] = useState(providers[0].defaultProps)

  const [cacheData, setCacheData] = useState()

  const [providerTypes, setProviderTypes] = useState(providers)

  const {
    control,
    reset,
    watch,
    setValue,
    getValues,
    handleSubmit,
    trigger,
    formState: { errors }
  } = useForm({
    defaultValues,
    mode: 'all',
    resolver: yupResolver(schema)
  })

  const providerSelect = watch('provider')
  const typeSelect = watch('type')

  const handleFormChange = ({ index, event }) => {
    let data = [...innerProps]
    data[index][event.target.name] = event.target.value

    if (event.target.name === 'key') {
      const invalidKey = !keyRegex.test(event.target.value)
      data[index].invalid = invalidKey
    }

    const nonEmptyKeys = data.filter(item => item.key.trim() !== '')
    const grouped = groupBy(nonEmptyKeys, 'key')
    const duplicateKeys = Object.keys(grouped).some(key => grouped[key].length > 1)

    if (duplicateKeys) {
      data[index].hasDuplicateKey = duplicateKeys
    } else {
      data.forEach(it => (it.hasDuplicateKey = false))
    }

    setInnerProps(data)
    setValue('propItems', data)
  }

  const addFields = () => {
    const duplicateKeys = innerProps.some(item => item.hasDuplicateKey)

    if (duplicateKeys) {
      return
    }

    let newField = { key: '', value: '', required: false }

    setInnerProps([...innerProps, newField])
    setValue('propItems', [...innerProps, newField])
  }

  const removeFields = index => {
    let data = [...innerProps]
    data.splice(index, 1)
    setInnerProps(data)
    setValue('propItems', data)
  }

  const hideField = field => {
    if (!field) {
      return true
    }
    const parentField = innerProps.find(i => i.key === field.parentField)

    const check =
      (parentField && field.hide.includes(parentField.value)) ||
      (field.parentField === 'authentication.type' && parentField === undefined)

    return check
  }

  const handleChangeProvider = (onChange, e) => {
    onChange(e.target.value)
  }

  const resetPropsFields = (providers = [], index = -1) => {
    if (index !== -1) {
      providers[index].defaultProps.forEach((item, index) => {
        item.value = item.defaultValue || ''
      })
    }
  }

  const handleClose = () => {
    reset()
    resetPropsFields(providers, 0)
    setInnerProps(providers[0].defaultProps)
    setValue('propItems', providers[0].defaultProps)
    setOpen(false)
  }

  const handleClickSubmit = e => {
    e.preventDefault()

    return handleSubmit(onSubmit(getValues()), onError)
  }

  const onSubmit = data => {
    const hasError = innerProps.some(prop => prop.hasDuplicateKey || prop.invalid)

    if (hasError) {
      return
    }

    const { propItems, ...mainData } = data

    let nextProps = propItems

    if (
      propItems[0]?.key === 'catalog-backend' &&
      propItems[0]?.value === 'hive' &&
      ['lakehouse-iceberg', 'lakehouse-paimon'].includes(providerSelect)
    ) {
      nextProps = propItems.filter(item => !['jdbc-driver', 'jdbc-user', 'jdbc-password'].includes(item.key))
    } else if (
      propItems[0]?.key === 'catalog-backend' &&
      propItems[0]?.value === 'filesystem' &&
      providerSelect === 'lakehouse-paimon'
    ) {
      nextProps = propItems.filter(item => !['jdbc-driver', 'jdbc-user', 'jdbc-password', 'uri'].includes(item.key))
    } else if (
      propItems[0]?.key === 'catalog-backend' &&
      propItems[0]?.value === 'rest' &&
      providerSelect === 'lakehouse-iceberg'
    ) {
      nextProps = propItems.filter(
        item => !['jdbc-driver', 'jdbc-user', 'jdbc-password', 'warehouse'].includes(item.key)
      )
    }
    const parentField = nextProps.find(i => i.key === 'authentication.type')
    if (!parentField || parentField?.value === 'simple') {
      nextProps = nextProps.filter(
        item => item.key !== 'authentication.kerberos.principal' && item.key !== 'authentication.kerberos.keytab-uri'
      )
    }

    trigger()

    const validData = { propItems: nextProps, ...mainData }

    schema
      .validate(validData)
      .then(() => {
        let properties = {}

        const prevProperties = innerProps
          .filter(i => (typeSelect === 'fileset' && i.key === 'location' ? i.value.trim() !== '' : i.key.trim() !== ''))
          .reduce((acc, item) => {
            acc[item.key] = item.value

            return acc
          }, {})

        const {
          'catalog-backend': catalogBackend,
          'jdbc-driver': jdbcDriver,
          'jdbc-user': jdbcUser,
          'jdbc-password': jdbcPwd,
          uri: uri,
          'authentication.type': authType,
          'authentication.kerberos.principal': kerberosPrincipal,
          'authentication.kerberos.keytab-uri': kerberosKeytabUri,
          ...others
        } = prevProperties

        if (
          catalogBackend &&
          catalogBackend === 'hive' &&
          ['lakehouse-iceberg', 'lakehouse-paimon'].includes(providerSelect)
        ) {
          properties = {
            'catalog-backend': catalogBackend,
            uri: uri,
            ...others
          }
        } else if (catalogBackend && catalogBackend === 'filesystem' && providerSelect === 'lakehouse-paimon') {
          properties = {
            'catalog-backend': catalogBackend,
            ...others
          }
          uri && (properties['uri'] = uri)
        } else if (catalogBackend && catalogBackend === 'rest' && providerSelect === 'lakehouse-iceberg') {
          properties = {
            'catalog-backend': catalogBackend,
            uri: uri,
            ...others
          }
        } else {
          properties = {
            uri: uri,
            ...others
          }
          catalogBackend && (properties['catalog-backend'] = catalogBackend)
          jdbcDriver && (properties['jdbc-driver'] = jdbcDriver)
          jdbcUser && (properties['jdbc-user'] = jdbcUser)
          jdbcPwd && (properties['jdbc-password'] = jdbcPwd)
        }
        authType && (properties['authentication.type'] = authType)
        kerberosPrincipal && (properties['authentication.kerberos.principal'] = kerberosPrincipal)
        kerberosKeytabUri && (properties['authentication.kerberos.keytab-uri'] = kerberosKeytabUri)

        const catalogData = {
          ...mainData,
          properties
        }

        if (type === 'create') {
          dispatch(createCatalog({ data: catalogData, metalake })).then(res => {
            if (!res.payload?.err) {
              handleClose()
            }
          })
        } else {
          const reqData = { updates: genUpdates(cacheData, catalogData) }

          if (reqData.updates.length !== 0) {
            dispatch(updateCatalog({ metalake, catalog: cacheData.name, data: reqData })).then(res => {
              if (!res.payload?.err) {
                handleClose()
              }
            })
          }
        }
      })
      .catch(err => {
        console.error('valid error', err)
      })
  }

  const onError = errors => {
    console.error('fields error', errors)
  }

  useEffect(() => {
    switch (typeSelect) {
      case 'relational': {
        setProviderTypes(providers)
        setValue('provider', 'hive')
        break
      }
      case 'fileset': {
        setProviderTypes(filesetProviders)
        setValue('provider', 'hadoop')
        break
      }
      case 'messaging': {
        setProviderTypes(messagingProviders)
        setValue('provider', 'kafka')
        break
      }
      case 'model': {
        setProviderTypes([])
        setValue('provider', '')
        break
      }
    }

    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [typeSelect, open])

  useEffect(() => {
    let defaultProps = []

    const providerItemIndex = providerTypes.findIndex(i => i.value === providerSelect)

    if (providerItemIndex !== -1) {
      defaultProps = providerTypes[providerItemIndex].defaultProps

      resetPropsFields(providerTypes, providerItemIndex)
    }
    if (type === 'create') {
      setInnerProps(defaultProps)
      setValue('propItems', defaultProps)
    }

    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [providerSelect])

  useEffect(() => {
    if (open && JSON.stringify(data) !== '{}') {
      const { properties = {} } = data

      setCacheData(data)
      setValue('name', data.name)
      setValue('comment', data.comment)
      setValue('type', data.type)
      setValue('provider', data.provider)

      let providersItems = []

      switch (data.type) {
        case 'relational': {
          providersItems = providers
          break
        }
        case 'fileset': {
          providersItems = filesetProviders
          break
        }
        case 'messaging': {
          providersItems = messagingProviders
          break
        }
        case 'model': {
          providersItems = []
          break
        }
      }

      setProviderTypes(providersItems)

      const providerItem = providersItems.find(i => i.value === data.provider)
      let propsItems = providerItem ? [...providerItem.defaultProps].filter(i => i.required) : []

      propsItems = propsItems.map((it, idx) => {
        let propItem = {
          ...it,
          disabled: it.key === 'catalog-backend' && type === 'update'
        }

        const findProp = Object.keys(properties).find(i => i === it.key)

        if (findProp) {
          propItem.value = properties[findProp]
        }

        return propItem
      })

      for (let item of Object.keys(properties)) {
        const findPropIndex = propsItems.findIndex(i => i.key === item)

        if (findPropIndex === -1) {
          let propItem = {
            key: item,
            value: properties[item]
          }
          propsItems.push(propItem)
        }
      }

      setInnerProps(propsItems)
      setValue('propItems', propsItems)
    }
  }, [open, data, setValue, type])

  return (
    <Dialog fullWidth maxWidth='sm' scroll='body' TransitionComponent={Transition} open={open} onClose={handleClose}>
      <form onSubmit={e => handleClickSubmit(e)}>
        <DialogContent
          sx={{
            position: 'relative',
            pb: theme => `${theme.spacing(8)} !important`,
            px: theme => [`${theme.spacing(5)} !important`, `${theme.spacing(15)} !important`],
            pt: theme => [`${theme.spacing(8)} !important`, `${theme.spacing(12.5)} !important`]
          }}
        >
          <IconButton
            size='small'
            onClick={() => handleClose()}
            sx={{ position: 'absolute', right: '1rem', top: '1rem' }}
          >
            <Icon icon='bx:x' />
          </IconButton>
          <Box sx={{ mb: 8, textAlign: 'center' }}>
            <Typography variant='h5' sx={{ mb: 3 }}>
              {type === 'create' ? 'Create' : 'Edit'} Catalog
            </Typography>
          </Box>

          <Grid container spacing={6}>
            <Grid item xs={12}>
              <FormControl fullWidth>
                <Controller
                  name='name'
                  control={control}
                  rules={{ required: true }}
                  render={({ field: { value, onChange } }) => (
                    <TextField
                      value={value}
                      label='Name'
                      onChange={onChange}
                      placeholder=''
                      error={Boolean(errors.name)}
                      data-refer='catalog-name-field'
                    />
                  )}
                />
                {errors.name && <FormHelperText sx={{ color: 'error.main' }}>{errors.name.message}</FormHelperText>}
              </FormControl>
            </Grid>

            <Grid item xs={12}>
              <FormControl fullWidth>
                <InputLabel id='select-catalog-type' error={Boolean(errors.type)}>
                  Type
                </InputLabel>
                <Controller
                  name='type'
                  control={control}
                  rules={{ required: true }}
                  render={({ field: { value, onChange } }) => (
                    <Select
                      value={value}
                      label='Type'
                      defaultValue='relational'
                      onChange={onChange}
                      error={Boolean(errors.type)}
                      labelId='select-catalog-type'
                      disabled={type === 'update'}
                      data-refer='catalog-type-selector'
                    >
                      <MenuItem value={'relational'}>Relational</MenuItem>
                      <MenuItem value={'fileset'}>Fileset</MenuItem>
                      <MenuItem value={'messaging'}>Messaging</MenuItem>
                      <MenuItem value={'model'}>Model</MenuItem>
                    </Select>
                  )}
                />
                {errors.type && <FormHelperText sx={{ color: 'error.main' }}>{errors.type.message}</FormHelperText>}
              </FormControl>
            </Grid>

            {typeSelect !== 'model' && (
              <Grid item xs={12}>
                <FormControl fullWidth>
                  <InputLabel id='select-catalog-provider' error={Boolean(errors.provider)}>
                    Provider
                  </InputLabel>
                  <Controller
                    name='provider'
                    control={control}
                    rules={{ required: true }}
                    render={({ field: { value, onChange } }) => (
                      <Select
                        value={value}
                        label='Provider'
                        defaultValue='hive'
                        onChange={e => handleChangeProvider(onChange, e)}
                        error={Boolean(errors.provider)}
                        labelId='select-catalog-provider'
                        disabled={type === 'update'}
                        data-refer='catalog-provider-selector'
                      >
                        {providerTypes.map(item => {
                          return (
                            <MenuItem key={item.label} value={item.value}>
                              {item.label}
                            </MenuItem>
                          )
                        })}
                      </Select>
                    )}
                  />
                  {errors.provider && (
                    <FormHelperText sx={{ color: 'error.main' }}>{errors.provider.message}</FormHelperText>
                  )}
                </FormControl>
              </Grid>
            )}

            <Grid item xs={12}>
              <FormControl fullWidth>
                <Controller
                  name='comment'
                  control={control}
                  rules={{ required: false }}
                  render={({ field: { value, onChange } }) => (
                    <TextField
                      value={value}
                      label='Comment'
                      multiline
                      rows={2}
                      onChange={onChange}
                      placeholder=''
                      error={Boolean(errors.comment)}
                      data-refer='catalog-comment-field'
                    />
                  )}
                />
              </FormControl>
            </Grid>

            <Grid item xs={12} data-refer='catalog-props-layout'>
              <Typography sx={{ mb: 2 }} variant='body2'>
                Properties
              </Typography>
              {innerProps.map((item, index) => {
                return (
                  !hideField(item) && (
                    <Fragment key={index}>
                      <Grid item xs={12} sx={{ '& + &': { mt: 2 } }}>
                        <FormControl fullWidth>
                          <Box>
                            <Box
                              sx={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between' }}
                              data-refer={`catalog-props-${index}`}
                            >
                              <Box>
                                <TextField
                                  size='small'
                                  name='key'
                                  label='Key'
                                  value={item.key}
                                  disabled={
                                    item.required || item.disabled || (item.key === 'in-use' && type === 'update')
                                  }
                                  onChange={event => handleFormChange({ index, event })}
                                  error={item.hasDuplicateKey || item.invalid || !item.key.trim()}
                                  data-refer={`props-key-${index}`}
                                />
                              </Box>
                              <Box>
                                {item.select ? (
                                  <Select
                                    name='value'
                                    value={item.value}
                                    size='small'
                                    sx={{ width: 195 }}
                                    disabled={item.disabled}
                                    onChange={event => handleFormChange({ index, event })}
                                    data-refer={`props-value-${index}`}
                                    data-prev-refer={`props-${item.key}`}
                                  >
                                    {item.select.map(selectItem => (
                                      <MenuItem key={selectItem} value={selectItem}>
                                        {selectItem}
                                      </MenuItem>
                                    ))}
                                  </Select>
                                ) : (
                                  <TextField
                                    size='small'
                                    name='value'
                                    label='Value'
                                    error={item.required && item.value === ''}
                                    value={item.value}
                                    disabled={item.disabled || (item.key === 'in-use' && type === 'update')}
                                    onChange={event => handleFormChange({ index, event })}
                                    data-refer={`props-value-${index}`}
                                    data-prev-refer={`props-${item.key}`}
                                    type={item.key === 'jdbc-password' ? 'password' : 'text'}
                                  />
                                )}
                              </Box>

                              {!(item.required || item.disabled || (item.key === 'in-use' && type === 'update')) ? (
                                <Box sx={{ minWidth: 40 }}>
                                  <IconButton onClick={() => removeFields(index)}>
                                    <Icon icon='mdi:minus-circle-outline' />
                                  </IconButton>
                                </Box>
                              ) : (
                                <Box sx={{ minWidth: 40 }}></Box>
                              )}
                            </Box>
                          </Box>
                          <FormHelperText
                            sx={{
                              color: item.required && item.value === '' ? 'error.main' : 'text.main',
                              maxWidth: 'calc(100% - 40px)'
                            }}
                          >
                            {item.description}
                          </FormHelperText>
                          {item.hasDuplicateKey && (
                            <FormHelperText className={'twc-text-error-main'}>Key already exists</FormHelperText>
                          )}
                          {item.key && item.invalid && (
                            <FormHelperText className={'twc-text-error-main'}>
                              Valid key must starts with a letter/underscore, followed by alphanumeric characters,
                              underscores, hyphens, or dots.
                            </FormHelperText>
                          )}
                          {!item.key.trim() && (
                            <FormHelperText className={'twc-text-error-main'}>Key is required</FormHelperText>
                          )}
                        </FormControl>
                      </Grid>
                    </Fragment>
                  )
                )
              })}
            </Grid>

            <Grid item xs={12}>
              <Button
                size='small'
                onClick={addFields}
                variant='outlined'
                startIcon={<Icon icon='mdi:plus-circle-outline' />}
                data-refer='add-catalog-props'
              >
                Add Property
              </Button>
            </Grid>
          </Grid>
        </DialogContent>
        <DialogActions
          sx={{
            justifyContent: 'center',
            px: theme => [`${theme.spacing(5)} !important`, `${theme.spacing(15)} !important`],
            pb: theme => [`${theme.spacing(5)} !important`, `${theme.spacing(12.5)} !important`]
          }}
        >
          <Button variant='contained' sx={{ mr: 1 }} type='submit' data-refer='handle-submit-catalog'>
            {type === 'create' ? 'Create' : 'Update'}
          </Button>
          <Button variant='outlined' onClick={handleClose}>
            Cancel
          </Button>
        </DialogActions>
      </form>
    </Dialog>
  )
}

export default CreateCatalogDialog
