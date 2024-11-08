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

/**
 * CreateTableDialog component
 *
 * A dialog component for creating and editing tables in a metalake catalog.
 *
 * Features:
 * - Create new tables or edit existing ones
 * - Configure table name, comment and properties
 * - Add/edit/remove table columns with name, type, nullable, and comment fields
 * - Add/edit/remove custom table properties
 * - Form validation using yup schema
 * - Responsive dialog layout
 *
 * Props:
 * @param {boolean} open - Controls dialog visibility
 * @param {function} setOpen - Function to update dialog visibility
 * @param {string} type - Dialog mode: 'create' or 'edit'
 * @param {object} data - Table data for edit mode
 */

'use client'

// Import required React hooks
import { useState, forwardRef, useEffect, Fragment } from 'react'

// Import Material UI components
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
  FormControl,
  FormHelperText,
  Switch,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
  Paper,
  Select,
  MenuItem
} from '@mui/material'

// Import custom components
import Icon from '@/components/Icon'

// Import Redux hooks and actions
import { useAppDispatch } from '@/lib/hooks/useStore'
import { createTable, updateTable } from '@/lib/store/metalakes'

// Import form validation libraries
import * as yup from 'yup'
import { useForm, Controller } from 'react-hook-form'
import { yupResolver } from '@hookform/resolvers/yup'

// Import utility functions and constants
import { groupBy } from 'lodash-es'
import { genUpdates } from '@/lib/utils'
import { nameRegex, nameRegexDesc, keyRegex } from '@/lib/utils/regex'
import { useSearchParams } from 'next/navigation'
import { relationalTypes } from '@/lib/utils/initial'

// Default form values
const defaultFormValues = {
  name: '',
  comment: '',
  columns: [],
  propItems: []
}

// Form validation schema
const schema = yup.object().shape({
  name: yup.string().required().matches(nameRegex, nameRegexDesc),
  columns: yup.array().of(
    yup.object().shape({
      name: yup.string().required(),
      type: yup.string().required(),
      nullable: yup.boolean(),
      comment: yup.string()
    })
  ),
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

// Dialog transition component
const Transition = forwardRef(function Transition(props, ref) {
  return <Fade ref={ref} {...props} />
})

/**
 * Main CreateTableDialog component
 * Handles creation and editing of tables with columns and properties
 */
const CreateTableDialog = props => {
  // Destructure props
  const { open, setOpen, type = 'create', data = {} } = props

  // Get URL parameters
  const searchParams = useSearchParams()
  const metalake = searchParams.get('metalake')
  const catalog = searchParams.get('catalog')
  const catalogType = searchParams.get('type')
  const schemaName = searchParams.get('schema')

  // Component state
  const [innerProps, setInnerProps] = useState([])
  const [tableColumns, setTableColumns] = useState([])
  const [initialTableData, setInitialTableData] = useState()
  const dispatch = useAppDispatch()

  // Initialize form with react-hook-form
  const {
    control,
    reset,
    setValue,
    getValues,
    handleSubmit,
    trigger,
    formState: { errors }
  } = useForm({
    defaultValues: defaultFormValues,
    mode: 'all',
    resolver: yupResolver(schema)
  })

  /**
   * Handle changes to property form fields
   * Validates keys and checks for duplicates
   */
  const handlePropertyChange = ({ index, event }) => {
    let updatedProps = [...innerProps]
    updatedProps[index][event.target.name] = event.target.value

    if (event.target.name === 'key') {
      const isInvalidKey = !keyRegex.test(event.target.value)
      updatedProps[index].invalid = isInvalidKey
    }

    const nonEmptyKeys = updatedProps.filter(item => item.key.trim() !== '')
    const groupedKeys = groupBy(nonEmptyKeys, 'key')
    const hasDuplicateKeys = Object.keys(groupedKeys).some(key => groupedKeys[key].length > 1)

    if (hasDuplicateKeys) {
      updatedProps[index].hasDuplicateKey = hasDuplicateKeys
    } else {
      updatedProps.forEach(item => (item.hasDuplicateKey = false))
    }

    setInnerProps(updatedProps)
    setValue('propItems', updatedProps)
  }

  /**
   * Handle changes to column fields
   */
  const handleColumnChange = ({ index, field, value }) => {
    let updatedColumns = [...tableColumns]
    updatedColumns[index][field] = value
    setTableColumns(updatedColumns)
    setValue('columns', updatedColumns)
  }

  /**
   * Add a new empty column
   */
  const addColumn = () => {
    const newColumn = { name: '', type: '', nullable: false, comment: '' }
    setTableColumns([...tableColumns, newColumn])
    setValue('columns', [...tableColumns, newColumn])
  }

  /**
   * Remove a column at specified index
   */
  const removeColumn = index => {
    let updatedColumns = [...tableColumns]
    updatedColumns.splice(index, 1)
    setTableColumns(updatedColumns)
    setValue('columns', updatedColumns)
  }

  /**
   * Add a new property field
   * Checks for duplicate keys before adding
   */
  const addProperty = () => {
    const hasDuplicateKeys = innerProps
      .filter(item => item.key.trim() !== '')
      .some(
        (item, index, filteredItems) =>
          filteredItems.findIndex(otherItem => otherItem !== item && otherItem.key.trim() === item.key.trim()) !== -1
      )

    if (hasDuplicateKeys) {
      return
    }

    const newProperty = { key: '', value: '', required: false }

    setInnerProps([...innerProps, newProperty])
    setValue('propItems', [...innerProps, newProperty])
  }

  /**
   * Remove a property field at specified index
   */
  const removeProperty = index => {
    let updatedProps = [...innerProps]
    updatedProps.splice(index, 1)
    setInnerProps(updatedProps)
    setValue('propItems', updatedProps)
  }

  /**
   * Handle dialog close
   * Resets form and clears state
   */
  const handleDialogClose = () => {
    reset()
    setInnerProps([])
    setTableColumns([])
    setValue('propItems', [])
    setValue('columns', [])
    setOpen(false)
  }

  /**
   * Handle form submission
   */
  const handleFormSubmit = e => {
    e.preventDefault()

    return handleSubmit(submitForm(getValues()), handleValidationError)
  }

  /**
   * Process form submission
   * Validates data and dispatches create/update actions
   */
  const submitForm = formData => {
    const hasDuplicateKeys = innerProps
      .filter(item => item.key.trim() !== '')
      .some(
        (item, index, filteredItems) =>
          filteredItems.findIndex(otherItem => otherItem !== item && otherItem.key.trim() === item.key.trim()) !== -1
      )

    const hasInvalidKeys = innerProps.some(prop => prop.invalid)

    if (hasDuplicateKeys || hasInvalidKeys) {
      return
    }

    trigger()

    schema
      .validate(formData)
      .then(() => {
        const properties = innerProps.reduce((acc, item) => {
          acc[item.key] = item.value

          return acc
        }, {})

        const tableData = {
          name: formData.name,
          comment: formData.comment,
          columns: formData.columns,
          properties
        }

        if (type === 'create') {
          dispatch(createTable({ data: tableData, metalake, catalog, type: catalogType, schema: schemaName })).then(
            res => {
              if (!res.payload?.err) {
                handleDialogClose()
              }
            }
          )
        } else {
          const updates = genUpdates(initialTableData, tableData)

          if (updates.length !== 0) {
            dispatch(
              updateTable({
                metalake,
                catalog,
                type: catalogType,
                schema: schemaName,
                table: initialTableData.name,
                data: { updates }
              })
            ).then(res => {
              if (!res.payload?.err) {
                handleDialogClose()
              }
            })
          }
        }
      })
      .catch(err => {
        console.error('Validation error:', err)
      })
  }

  /**
   * Handle form validation errors
   */
  const handleValidationError = errors => {
    console.error('Form validation errors:', errors)
  }

  /**
   * Effect to populate form when editing existing table
   */
  useEffect(() => {
    if (open && JSON.stringify(data) !== '{}') {
      const { properties = {}, columns = [] } = data

      setInitialTableData(data)
      setValue('name', data.name)
      setValue('comment', data.comment)
      setValue('columns', columns)
      setTableColumns(columns)

      const propertyItems = Object.entries(properties).map(([key, value]) => {
        return {
          key,
          value
        }
      })

      setInnerProps(propertyItems)
      setValue('propItems', propertyItems)
    }
  }, [open, data, setValue, type])

  return (
    <Dialog
      fullWidth
      maxWidth='md'
      scroll='body'
      TransitionComponent={Transition}
      open={open}
      onClose={handleDialogClose}
    >
      <form onSubmit={e => handleFormSubmit(e)}>
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
            onClick={() => handleDialogClose()}
            sx={{ position: 'absolute', right: '1rem', top: '1rem' }}
          >
            <Icon icon='bx:x' />
          </IconButton>
          <Box sx={{ mb: 8, textAlign: 'center' }}>
            <Typography variant='h5' sx={{ mb: 3 }}>
              {type === 'create' ? 'Create' : 'Edit'} Table
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
                      data-refer='table-name-field'
                    />
                  )}
                />
                {errors.name && <FormHelperText sx={{ color: 'error.main' }}>{errors.name.message}</FormHelperText>}
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
                      data-refer='table-comment-field'
                    />
                  )}
                />
              </FormControl>
            </Grid>

            <Grid item xs={12} data-refer='table-columns-layout'>
              <Typography sx={{ mb: 2 }} variant='body2'>
                Columns
              </Typography>
              <TableContainer component={Paper} sx={{ maxHeight: 440 }}>
                <Table stickyHeader>
                  <TableHead>
                    <TableRow>
                      <TableCell sx={{ minWidth: 100 }}>Name</TableCell>
                      <TableCell sx={{ minWidth: 100 }}>Type</TableCell>
                      <TableCell sx={{ minWidth: 100 }}>Nullable</TableCell>
                      <TableCell sx={{ minWidth: 200 }}>Comment</TableCell>
                      <TableCell sx={{ minWidth: 50 }}>Action</TableCell>
                    </TableRow>
                  </TableHead>
                  <TableBody>
                    {tableColumns.map((column, index) => (
                      <TableRow key={index}>
                        <TableCell>
                          <TextField
                            size='small'
                            fullWidth
                            value={column.name}
                            onChange={e => handleColumnChange({ index, field: 'name', value: e.target.value })}
                            error={!column.name.trim()}
                            data-refer={`column-name-${index}`}
                          />
                        </TableCell>
                        <TableCell>
                          <Select
                            size='small'
                            fullWidth
                            value={column.type}
                            onChange={e => handleColumnChange({ index, field: 'type', value: e.target.value })}
                            error={!column.type.trim()}
                            data-refer={`column-type-${index}`}
                          >
                            {relationalTypes.map(type => (
                              <MenuItem key={type.value} value={type.value}>
                                {type.label}
                              </MenuItem>
                            ))}
                          </Select>
                        </TableCell>
                        <TableCell>
                          <Switch
                            checked={column.nullable || false}
                            onChange={e => handleColumnChange({ index, field: 'nullable', value: e.target.checked })}
                            data-refer={`column-nullable-${index}`}
                          />
                        </TableCell>
                        <TableCell>
                          <TextField
                            size='small'
                            fullWidth
                            value={column.comment}
                            onChange={e => handleColumnChange({ index, field: 'comment', value: e.target.value })}
                            data-refer={`column-comment-${index}`}
                          />
                        </TableCell>
                        <TableCell>
                          <IconButton onClick={() => removeColumn(index)}>
                            <Icon icon='mdi:minus-circle-outline' />
                          </IconButton>
                        </TableCell>
                      </TableRow>
                    ))}
                  </TableBody>
                </Table>
              </TableContainer>
            </Grid>

            <Grid item xs={12}>
              <Button
                size='small'
                onClick={addColumn}
                variant='outlined'
                startIcon={<Icon icon='mdi:plus-circle-outline' />}
                data-refer='add-table-column'
              >
                Add Column
              </Button>
            </Grid>

            <Grid item xs={12} data-refer='table-props-layout'>
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
                            data-refer={`table-props-${index}`}
                          >
                            <Box>
                              <TextField
                                size='small'
                                name='key'
                                label='Key'
                                value={item.key}
                                disabled={item.disabled}
                                onChange={event => handlePropertyChange({ index, event })}
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
                                disabled={item.disabled}
                                onChange={event => handlePropertyChange({ index, event })}
                                data-refer={`props-value-${index}`}
                                data-prev-refer={`props-${item.key}`}
                              />
                            </Box>

                            {!item.disabled ? (
                              <Box sx={{ minWidth: 40 }}>
                                <IconButton onClick={() => removeProperty(index)}>
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

            <Grid item xs={12}>
              <Button
                size='small'
                onClick={addProperty}
                variant='outlined'
                startIcon={<Icon icon='mdi:plus-circle-outline' />}
                data-refer='add-table-props'
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
          <Button variant='contained' sx={{ mr: 1 }} type='submit' data-refer='handle-submit-table'>
            {type === 'create' ? 'Create' : 'Update'}
          </Button>
          <Button variant='outlined' onClick={handleDialogClose}>
            Cancel
          </Button>
        </DialogActions>
      </form>
    </Dialog>
  )
}

export default CreateTableDialog
