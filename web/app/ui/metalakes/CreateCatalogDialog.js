/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
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
import { providers } from '@/lib/utils/initial'
import { nameRegex, keyRegex } from '@/lib/utils/regex'

const defaultValues = {
  name: '',
  type: 'relational',
  provider: 'hive',
  comment: '',
  propItems: providers[0].defaultProps
}

const providerTypeValues = providers.map(i => i.value)

const schema = yup.object().shape({
  name: yup
    .string()
    .required()
    .matches(
      nameRegex,
      'This field must start with a letter or underscore, and can only contain letters, numbers, and underscores'
    ),
  type: yup.mixed().oneOf(['relational']).required(),
  provider: yup.mixed().oneOf(providerTypeValues).required(),
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
  const { open, setOpen, type = 'create', data = {}, routeParams } = props
  const { metalake } = routeParams

  const dispatch = useAppDispatch()

  const [innerProps, setInnerProps] = useState(providers[0].defaultProps)

  const [cacheData, setCacheData] = useState()

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
    const duplicateKeys = innerProps
      .filter(item => item.key.trim() !== '')
      .some(
        (item, index, filteredItems) =>
          filteredItems.findIndex(otherItem => otherItem !== item && otherItem.key.trim() === item.key.trim()) !== -1
      )

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
    const parentField = innerProps.find(i => i.key === 'catalog-backend')
    const check = parentField && parentField.value === field.hide

    return check
  }

  const handleChangeProvider = (onChange, e) => {
    onChange(e.target.value)
  }

  const resetPropsFields = (providers = [], index = -1) => {
    if (index !== -1) {
      providers[index].defaultProps.forEach((item, index) => {
        if (item.key !== 'catalog-backend') {
          item.value = ''
        }
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
    const duplicateKeys = innerProps
      .filter(item => item.key.trim() !== '')
      .some(
        (item, index, filteredItems) =>
          filteredItems.findIndex(otherItem => otherItem !== item && otherItem.key.trim() === item.key.trim()) !== -1
      )

    const invalidKeys = innerProps.some(i => i.invalid)

    if (duplicateKeys || invalidKeys) {
      return
    }

    const { propItems, ...mainData } = data

    let nextProps = []

    if (propItems[0]?.key === 'catalog-backend' && propItems[0]?.value === 'hive') {
      nextProps = propItems.slice(0, 3)
    } else {
      nextProps = propItems
    }

    trigger()

    const validData = { propItems: nextProps, ...mainData }

    schema
      .validate(validData)
      .then(() => {
        let properties = {}

        const prevProperties = innerProps
          .filter(i => i.key.trim() !== '')
          .reduce((acc, item) => {
            acc[item.key] = item.value

            return acc
          }, {})

        const {
          'catalog-backend': catalogBackend,
          'jdbc-driver': jdbcDriver,
          'jdbc-user': jdbcUser,
          'jdbc-password': jdbcPwd,
          ...others
        } = prevProperties

        if (catalogBackend && catalogBackend === 'hive') {
          properties = {
            'catalog-backend': catalogBackend,
            ...others
          }
        } else {
          properties = prevProperties
        }

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
    let defaultProps = []

    const providerItemIndex = providers.findIndex(i => i.value === providerSelect)

    if (providerItemIndex !== -1) {
      defaultProps = providers[providerItemIndex].defaultProps

      resetPropsFields(providers, providerItemIndex)

      if (type === 'create') {
        setInnerProps(defaultProps)
        setValue('propItems', providers[providerItemIndex].defaultProps)
      }
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

      const providerItem = providers.find(i => i.value === data.provider)
      let propsItems = [...providerItem.defaultProps]

      propsItems = propsItems.map((it, idx) => {
        let propItem = {
          ...it,
          disabled: true
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
  }, [open, data, setValue])

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
                    >
                      <MenuItem value={'relational'}>relational</MenuItem>
                    </Select>
                  )}
                />
                {errors.type && <FormHelperText sx={{ color: 'error.main' }}>{errors.type.message}</FormHelperText>}
              </FormControl>
            </Grid>

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
                    >
                      <MenuItem value={'hive'}>hive</MenuItem>
                      <MenuItem value={'lakehouse-iceberg'}>iceberg</MenuItem>
                      <MenuItem value={'jdbc-mysql'}>mysql</MenuItem>
                      <MenuItem value={'jdbc-postgresql'}>postgresql</MenuItem>
                    </Select>
                  )}
                />
                {errors.provider && (
                  <FormHelperText sx={{ color: 'error.main' }}>{errors.provider.message}</FormHelperText>
                )}
              </FormControl>
            </Grid>

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
                    />
                  )}
                />
              </FormControl>
            </Grid>

            <Grid item xs={12}>
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
                            <Box sx={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between' }}>
                              <Box>
                                <TextField
                                  size='small'
                                  name='key'
                                  label='Key'
                                  value={item.key}
                                  disabled={item.required}
                                  onChange={event => handleFormChange({ index, event })}
                                  error={item.hasDuplicateKey}
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
                                    disabled={item.disabled}
                                    onChange={event => handleFormChange({ index, event })}
                                  />
                                )}
                              </Box>

                              {!item.required ? (
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
                          {item.invalid && (
                            <FormHelperText className={'twc-text-error-main'}>
                              Invalid key, matches strings starting with a letter/underscore, followed by alphanumeric
                              characters, underscores, hyphens, or dots.
                            </FormHelperText>
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
          <Button variant='contained' sx={{ mr: 1 }} type='submit'>
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
