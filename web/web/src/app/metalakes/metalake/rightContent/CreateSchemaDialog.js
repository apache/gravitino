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
import { createSchema, updateSchema } from '@/lib/store/metalakes'

import * as yup from 'yup'
import { useForm, Controller } from 'react-hook-form'
import { yupResolver } from '@hookform/resolvers/yup'

import { groupBy } from 'lodash-es'
import { genUpdates } from '@/lib/utils'
import { nameRegex, nameRegexDesc, keyRegex } from '@/lib/utils/regex'
import { useSearchParams } from 'next/navigation'
import { useAppSelector } from '@/lib/hooks/useStore'

const defaultValues = {
  name: '',
  comment: '',
  propItems: []
}

const schema = yup.object().shape({
  name: yup.string().required().matches(nameRegex, nameRegexDesc),
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

const CreateSchemaDialog = props => {
  const { open, setOpen, type = 'create', data = {} } = props
  const searchParams = useSearchParams()
  const metalake = searchParams.get('metalake')
  const catalog = searchParams.get('catalog')
  const catalogType = searchParams.get('type')
  const [innerProps, setInnerProps] = useState([])
  const dispatch = useAppDispatch()
  const store = useAppSelector(state => state.metalakes)
  const activatedCatalogDetail = store.activatedDetails
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

    trigger()

    schema
      .validate(data)
      .then(() => {
        const properties = innerProps.reduce((acc, item) => {
          acc[item.key] = item.value

          return acc
        }, {})

        const schemaData = {
          name: data.name,
          comment: data.comment,
          properties
        }

        if (type === 'create') {
          dispatch(createSchema({ data: schemaData, metalake, catalog, type: catalogType })).then(res => {
            if (!res.payload?.err) {
              handleClose()
            }
          })
        } else {
          const reqData = { updates: genUpdates(cacheData, schemaData) }

          if (reqData.updates.length !== 0) {
            dispatch(
              updateSchema({ metalake, catalog, type: catalogType, schema: cacheData.name, data: reqData })
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
      setValue('comment', data.comment)

      const propsItems = Object.entries(properties).map(([key, value]) => {
        return {
          key,
          value
        }
      })

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
              {type === 'create' ? 'Create' : 'Edit'} Schema
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
                      disabled={type === 'update'}
                      error={Boolean(errors.name)}
                      data-refer='schema-name-field'
                    />
                  )}
                />
                {errors.name && <FormHelperText sx={{ color: 'error.main' }}>{errors.name.message}</FormHelperText>}
              </FormControl>
            </Grid>

            {!['jdbc-mysql', 'lakehouse-paimon', 'jdbc-oceanbase'].includes(activatedCatalogDetail?.provider) && (
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
                        disabled={type === 'update'}
                        error={Boolean(errors.comment)}
                        data-refer='schema-comment-field'
                      />
                    )}
                  />
                </FormControl>
              </Grid>
            )}

            {!['jdbc-postgresql', 'lakehouse-paimon', 'kafka', 'jdbc-mysql', 'jdbc-oceanbase'].includes(
              activatedCatalogDetail?.provider
            ) && (
              <Grid item xs={12} data-refer='schema-props-layout'>
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
                              data-refer={`schema-props-${index}`}
                            >
                              <Box>
                                <TextField
                                  size='small'
                                  name='key'
                                  label='Key'
                                  value={item.key}
                                  disabled={item.disabled || (item.key === 'location' && type === 'update')}
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
                                  disabled={item.disabled || (item.key === 'location' && type === 'update')}
                                  onChange={event => handleFormChange({ index, event })}
                                  data-refer={`props-value-${index}`}
                                  data-prev-refer={`props-${item.key}`}
                                />
                              </Box>

                              {!(item.disabled || (item.key === 'location' && type === 'update')) ? (
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
                              Invalid key, matches strings starting with a letter/underscore, followed by alphanumeric
                              characters, underscores, hyphens, or dots.
                            </FormHelperText>
                          )}
                          {!item.key.trim() && (
                            <FormHelperText className={'twc-text-error-main'}>Key is required field</FormHelperText>
                          )}
                        </FormControl>
                      </Grid>
                    </Fragment>
                  )
                })}
              </Grid>
            )}

            {!['jdbc-postgresql', 'lakehouse-paimon', 'kafka', 'jdbc-mysql', 'jdbc-oceanbase'].includes(
              activatedCatalogDetail?.provider
            ) && (
              <Grid item xs={12}>
                <Button
                  size='small'
                  onClick={addFields}
                  variant='outlined'
                  startIcon={<Icon icon='mdi:plus-circle-outline' />}
                  data-refer='add-schema-props'
                >
                  Add Property
                </Button>
              </Grid>
            )}
          </Grid>
        </DialogContent>
        <DialogActions
          sx={{
            justifyContent: 'center',
            px: theme => [`${theme.spacing(5)} !important`, `${theme.spacing(15)} !important`],
            pb: theme => [`${theme.spacing(5)} !important`, `${theme.spacing(12.5)} !important`]
          }}
        >
          <Button variant='contained' sx={{ mr: 1 }} type='submit' data-refer='handle-submit-schema'>
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

export default CreateSchemaDialog
