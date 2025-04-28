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
  FormHelperText,
  Tooltip,
  Switch
} from '@mui/material'

import Icon from '@/components/Icon'

import { useAppDispatch } from '@/lib/hooks/useStore'
import { createFileset, updateFileset } from '@/lib/store/metalakes'

import * as yup from 'yup'
import { useForm, Controller, useFieldArray } from 'react-hook-form'
import { yupResolver } from '@hookform/resolvers/yup'

import { groupBy } from 'lodash-es'
import { genUpdates } from '@/lib/utils'
import { nameRegex, nameRegexDesc, keyRegex } from '@/lib/utils/regex'
import { useSearchParams } from 'next/navigation'

const defaultValues = {
  name: '',
  type: 'managed',
  storageLocations: [{ name: '', location: '', defaultLocation: true }],
  comment: '',
  propItems: []
}

const schema = yup.object().shape({
  name: yup.string().required().matches(nameRegex, nameRegexDesc),
  type: yup.mixed().oneOf(['managed', 'external']).required(),
  storageLocations: yup
    .array()
    .of(
      yup.object().shape({
        name: yup.string().when('type', {
          is: 'external',
          then: schema => schema.required(),
          otherwise: schema => schema
        }),
        location: yup.string().when('name', {
          is: name => !!name,
          then: schema => schema.required(),
          otherwise: schema => schema
        })
      })
    )
    .test('unique', 'Location name must be unique', (storageLocations, ctx) => {
      const values = storageLocations?.filter(l => !!l.name).map(l => l.name)
      const duplicates = values.filter((value, index, self) => self.indexOf(value) !== index)

      if (duplicates.length > 0) {
        const duplicateIndex = values.lastIndexOf(duplicates[0])

        return ctx.createError({
          path: `storageLocations.${duplicateIndex}.name`,
          message: 'This storage location name is duplicated'
        })
      }

      return true
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

const CreateFilesetDialog = props => {
  const { open, setOpen, type = 'create', data = {} } = props
  const searchParams = useSearchParams()
  const metalake = searchParams.get('metalake')
  const catalog = searchParams.get('catalog')
  const catalogType = searchParams.get('type')
  const schemaName = searchParams.get('schema')
  const [innerProps, setInnerProps] = useState([])
  const dispatch = useAppDispatch()
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

  const defaultLocationProps = watch('propItems').filter(item => item.key === 'default-location-name')[0]
  const storageLocationsItems = watch('storageLocations')

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

  const { fields, append, remove } = useFieldArray({
    control,
    name: 'storageLocations'
  })

  const onChangeDefaultLocation = ({ index, event }) => {
    fields.forEach((item, i) => {
      if (i !== index) {
        setValue(`storageLocations.${i}.defaultLocation`, false)
      }
    })
    setValue(`storageLocations.${index}.defaultLocation`, event.target.checked)
  }

  const handleClose = () => {
    reset()
    setInnerProps([])
    setValue('propItems', [])
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

    trigger()

    schema
      .validate(data)
      .then(() => {
        const properties = innerProps.reduce((acc, item) => {
          acc[item.key] = item.value

          return acc
        }, {})

        const filesetData = {
          name: data.name,
          type: data.type,
          storageLocations: data.storageLocations.reduce((acc, item) => {
            if (item.name && item.location) {
              acc[item.name] = item.location
              if (item.defaultLocation && !properties['default-location-name']) {
                properties['default-location-name'] = item.name
              }
            }

            return acc
          }, {}),
          comment: data.comment,
          properties
        }

        if (type === 'create') {
          dispatch(createFileset({ data: filesetData, metalake, catalog, type: catalogType, schema: schemaName })).then(
            res => {
              if (!res.payload?.err) {
                handleClose()
              }
            }
          )
        } else {
          const reqData = { updates: genUpdates(cacheData, filesetData) }

          if (reqData.updates.length !== 0) {
            dispatch(
              updateFileset({
                metalake,
                catalog,
                type: catalogType,
                schema: schemaName,
                fileset: cacheData.name,
                data: reqData
              })
            ).then(res => {
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
    if (open && JSON.stringify(data) !== '{}') {
      const { properties = {} } = data

      setCacheData(data)
      setValue('name', data.name)
      setValue('type', data.type)
      setValue('comment', data.comment)

      const storageLocations = Object.entries(data.storageLocations).map(([key, value]) => {
        return {
          name: key,
          location: value,
          defaultLocation: properties['default-location-name'] === key
        }
      })

      const propsItems = Object.entries(properties).map(([key, value]) => {
        return {
          key,
          value
        }
      })

      setValue('storageLocations', storageLocations)
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
              {type === 'create' ? 'Create' : 'Edit'} Fileset
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
                      data-refer='fileset-name-field'
                    />
                  )}
                />
                {errors.name && <FormHelperText sx={{ color: 'error.main' }}>{errors.name.message}</FormHelperText>}
              </FormControl>
            </Grid>

            <Grid item xs={12}>
              <FormControl fullWidth>
                <InputLabel id='select-fileset-type' error={Boolean(errors.type)}>
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
                      defaultValue='managed'
                      onChange={onChange}
                      disabled={type === 'update'}
                      error={Boolean(errors.type)}
                      labelId='select-fileset-type'
                      data-refer='fileset-type-selector'
                    >
                      <MenuItem value={'managed'}>Managed</MenuItem>
                      <MenuItem value={'external'}>External</MenuItem>
                    </Select>
                  )}
                />
                {errors.type && <FormHelperText sx={{ color: 'error.main' }}>{errors.type.message}</FormHelperText>}
              </FormControl>
            </Grid>

            <Grid item xs={12}>
              <Typography sx={{ mb: 2 }} variant='body2'>
                Storage Locations
              </Typography>
              {fields.map((field, index) => {
                return (
                  <Grid key={index} item xs={12} sx={{ '& + &': { mt: 2 } }}>
                    <FormControl fullWidth>
                      <Box
                        key={field.id}
                        sx={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', gap: 1 }}
                        data-refer={`storageLocations-${index}`}
                      >
                        <Box>
                          <Controller
                            name={`storageLocations.${index}.name`}
                            control={control}
                            render={({ field: { value, onChange } }) => (
                              <TextField
                                {...field}
                                value={value}
                                onChange={onChange}
                                disabled={type === 'update'}
                                label={`Name ${index + 1}`}
                                data-refer={`storageLocations-name-${index}`}
                                error={!!errors.storageLocations?.[index]?.name || !!errors.storageLocations?.message}
                                helperText={
                                  errors.storageLocations?.[index]?.name?.message || errors.storageLocations?.message
                                }
                                fullWidth
                              />
                            )}
                          />
                        </Box>
                        <Box>
                          <Controller
                            name={`storageLocations.${index}.location`}
                            control={control}
                            render={({ field: { value, onChange } }) => (
                              <TextField
                                {...field}
                                value={value}
                                onChange={onChange}
                                disabled={type === 'update'}
                                label={`Location ${index + 1}`}
                                data-refer={`storageLocations-location-${index}`}
                                error={
                                  !!errors.storageLocations?.[index]?.location || !!errors.storageLocations?.message
                                }
                                helperText={
                                  errors.storageLocations?.[index]?.location?.message ||
                                  errors.storageLocations?.message
                                }
                                fullWidth
                              />
                            )}
                          />
                        </Box>
                        {!defaultLocationProps &&
                          storageLocationsItems.length > 1 &&
                          storageLocationsItems[0].name &&
                          storageLocationsItems[0].location && (
                            <Box>
                              <Controller
                                name={`storageLocations.${index}.defaultLocation`}
                                control={control}
                                render={({ field: { value, onChange } }) => (
                                  <Tooltip title='Default Location' placement='top'>
                                    <Switch
                                      checked={value}
                                      onChange={event => onChangeDefaultLocation({ index, event })}
                                      disabled={type === 'update'}
                                      size='small'
                                    />
                                  </Tooltip>
                                )}
                              />
                            </Box>
                          )}
                        <Box>
                          {index === 0 ? (
                            <Box sx={{ minWidth: 40 }}>
                              <IconButton
                                sx={{ cursor: type === 'update' ? 'not-allowed' : 'pointer' }}
                                onClick={() => {
                                  if (type === 'update') return
                                  append({ name: '', location: '', defaultLocation: false })
                                }}
                              >
                                <Icon icon='mdi:plus-circle-outline' />
                              </IconButton>
                            </Box>
                          ) : (
                            <Box sx={{ minWidth: 40 }}>
                              <IconButton
                                sx={{ cursor: type === 'update' ? 'not-allowed' : 'pointer' }}
                                onClick={() => {
                                  if (type === 'update') return
                                  remove(index)
                                }}
                              >
                                <Icon icon='mdi:minus-circle-outline' />
                              </IconButton>
                            </Box>
                          )}
                        </Box>
                      </Box>
                    </FormControl>
                  </Grid>
                )
              })}
              {errors.storageLocations ? (
                <FormHelperText sx={{ color: 'error.main' }}>{errors.storageLocations.message}</FormHelperText>
              ) : (
                <>
                  <FormHelperText sx={{ color: 'text.main' }}>
                    It is optional if the fileset is 'Managed' type and a storage location is already specified at the
                    parent catalog or schema level.
                  </FormHelperText>
                  <FormHelperText sx={{ color: 'text.main' }}>
                    It becomes mandatory if the fileset type is 'External' or no storage location is defined at the
                    parent level.
                  </FormHelperText>
                </>
              )}
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
                      data-refer='fileset-comment-field'
                    />
                  )}
                />
              </FormControl>
            </Grid>

            <Grid item xs={12} data-refer='fileset-props-layout'>
              <Typography sx={{ mb: 2 }} variant='body2'>
                Properties
              </Typography>
              {innerProps.map((item, index) => {
                return (
                  <Fragment key={index}>
                    <Grid item xs={12} sx={{ '& + &': { mt: 2 } }}>
                      <FormControl fullWidth>
                        <Box>
                          <Box
                            sx={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between' }}
                            data-refer={`fileset-props-${index}`}
                          >
                            <Box>
                              <TextField
                                size='small'
                                name='key'
                                label='Key'
                                value={item.key}
                                disabled={
                                  item.disabled ||
                                  (['location', 'default-location-name'].includes(item.key) && type === 'update')
                                }
                                onChange={event => handleFormChange({ index, event })}
                                error={item.hasDuplicateKey || item.invalid || !item.key.trim()}
                                data-refer={`props-key-${index}`}
                              />
                            </Box>
                            <Box>
                              <TextField
                                size='small'
                                name='value'
                                label='Value'
                                error={item.required && item.value === ''}
                                value={item.value}
                                disabled={
                                  item.disabled ||
                                  (['location', 'default-location-name'].includes(item.key) && type === 'update')
                                }
                                onChange={event => handleFormChange({ index, event })}
                                data-refer={`props-value-${index}`}
                                data-prev-refer={`props-${item.key}`}
                              />
                            </Box>

                            {!(
                              item.disabled ||
                              (['location', 'default-location-name'].includes(item.key) && type === 'update')
                            ) ? (
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
              })}
            </Grid>

            <Grid item xs={12}>
              <Button
                size='small'
                onClick={addFields}
                variant='outlined'
                startIcon={<Icon icon='mdi:plus-circle-outline' />}
                data-refer='add-fileset-props'
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
          <Button variant='contained' sx={{ mr: 1 }} type='submit' data-refer='handle-submit-fileset'>
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

export default CreateFilesetDialog
