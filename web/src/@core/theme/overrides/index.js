// ** Overrides Imports
import MuiCard from './card'
import MuiChip from './chip'
import MuiLink from './link'
import MuiList from './list'
import MuiMenu from './menu'
import MuiTabs from './tabs'
import MuiInput from './input'
import MuiPaper from './paper'
import MuiRadio from './radio'
import MuiTable from './table'
import MuiAlerts from './alerts'
import MuiButton from './button'
import MuiDialog from './dialog'
import MuiRating from './rating'
import MuiSelect from './select'
import MuiSlider from './slider'
import MuiAvatar from './avatars'
import MuiDivider from './divider'
import MuiPopover from './popover'
import MuiTooltip from './tooltip'
import MuiBackdrop from './backdrop'
import MuiCheckbox from './checkbox'
import MuiDataGrid from './dataGrid'
import MuiProgress from './progress'
import MuiSnackbar from './snackbar'
import MuiSwitches from './switches'
import MuiTimeline from './timeline'
import MuiAccordion from './accordion'
import MuiPagination from './pagination'
import MuiTypography from './typography'
import MuiBreadcrumb from './breadcrumbs'
import MuiAutocomplete from './autocomplete'
import MuiToggleButton from './toggleButton'

const Overrides = settings => {
  const { skin } = settings
  const chip = MuiChip()
  const list = MuiList()
  const input = MuiInput()
  const radio = MuiRadio()
  const tables = MuiTable()
  const menu = MuiMenu(skin)
  const tabs = MuiTabs(skin)
  const alerts = MuiAlerts()
  const button = MuiButton()
  const rating = MuiRating()
  const slider = MuiSlider()
  const cards = MuiCard(skin)
  const avatars = MuiAvatar()
  const divider = MuiDivider()
  const tooltip = MuiTooltip()
  const dialog = MuiDialog(skin)
  const backdrop = MuiBackdrop()
  const checkbox = MuiCheckbox()
  const dataGrid = MuiDataGrid()
  const progress = MuiProgress()
  const switches = MuiSwitches()
  const timeline = MuiTimeline()
  const popover = MuiPopover(skin)
  const accordion = MuiAccordion()
  const snackbar = MuiSnackbar(skin)
  const breadcrumb = MuiBreadcrumb()
  const pagination = MuiPagination()
  const autocomplete = MuiAutocomplete(skin)

  return Object.assign(
    chip,
    list,
    menu,
    tabs,
    cards,
    input,
    radio,
    alerts,
    button,
    dialog,
    rating,
    slider,
    tables,
    avatars,
    divider,
    MuiLink,
    popover,
    tooltip,
    checkbox,
    backdrop,
    dataGrid,
    MuiPaper,
    progress,
    snackbar,
    switches,
    timeline,
    accordion,
    MuiSelect,
    breadcrumb,
    pagination,
    autocomplete,
    MuiTypography,
    MuiToggleButton
  )
}

export default Overrides
