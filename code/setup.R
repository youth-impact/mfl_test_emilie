library('cli')
library('checkmate')
library('data.table')
library('glue')
library('googledrive')
library('googlesheets4')
library('yaml')

paramsDir = 'params'
params = read_yaml(file.path(paramsDir, 'params.yaml'))

########################################

if (Sys.getenv('GOOGLE_TOKEN') == '') {
  drive_auth(email = 'youthimpactautomation@gmail.com')
} else {
  drive_auth(path = Sys.getenv('GOOGLE_TOKEN'))
}

gs4_auth(token = drive_token())

########################################

fix_list_cols = function(d) {
  assert_data_table(d)
  d = copy(d)
  cols = colnames(d)[sapply(colnames(d), \(col) is.list(d[[col]]))]
  for (col in cols) {
    val = unlist(lapply(
      d[[col]], \(v) if (is.null(v)) NA else as.character(v)))
    set(d, j = col, value = val)
  }
  d[]
}


fix_dates = function(d, date_colnames) {
  assert_data_table(d)
  assert_character(date_colnames)

  d = copy(d)
  date_zero = as.IDate('1899-12-30') # tested by trial and error in gsheets
  date_formats = c('%Y-%m-%d', '%d-%m-%Y', '%d/%m/%Y')
  date_regs = c(
    '^\\d{4}-\\d{2}-\\d{2}$', '^\\d{2}-\\d{2}-\\d{4}$', '^\\d{2}/\\d{2}/\\d{4}$')

  for (col in date_colnames) {
    if (is.character(d[[col]])) {
      d[grepl('^\\d{5}$', col),
        col := as.integer(col) + date_zero,
        env = list(col = col)]
      for (i in seq_len(length(date_formats))) {
        d[grepl(date_regs[i], col),
          col := as.IDate(col, format = date_formats[i]),
          env = list(col = col)]
      }
    }
    set(d, j = col, value = as.IDate(d[[col]]))
  }

  d[]
}


get_tables = function(
    file_id, date_colnames = NULL,
    sheets = c('groups', 'show_columns', 'sorting', 'viewers', 'data')) {

  assert_class(file_id, 'drive_id')
  assert_character(date_colnames, any.missing = FALSE, null.ok = TRUE)
  assert_character(sheets, any.missing = FALSE)

  tables = lapply(sheets, \(x) setDT(read_sheet(file_id, x)))
  names(tables) = sheets
  # if (nrow(tables$groups) > 0) setorderv(tables$groups, 'group_id')
  if (nrow(tables$show_columns) > 0) {
    tables$show_columns[is.na(column_label), column_label := column_name]
  }
  tables$viewers = unique(na.omit(tables$viewers))
  if (nrow(tables$data) > 0) {
    tables$data = tables$data[!is.na(id)]
    tables$data = fix_list_cols(tables$data)
    if (!is.null(date_colnames)) {
      tables$data = fix_dates(tables$data, date_colnames)
    }
  }
  tables
}


compare_tables = function(x, y) {
  assert_list(x, types = 'data.table', any.missing = FALSE)
  assert_list(y, types = 'data.table', any.missing = FALSE)

  meta = data.table(
    table_name = c('groups', 'show_columns', 'sorting', 'viewers', 'data'),
    ignore_row_order = c(TRUE, FALSE, FALSE, TRUE, TRUE),
    ignore_col_order = c(TRUE, FALSE, TRUE, TRUE, TRUE))

  eq = sapply(seq_len(nrow(meta)), \(i) {
    isTRUE(all.equal(
      x[[meta$table_name[i]]], y[[meta$table_name[i]]],
      check.attributes = FALSE,
      ignore.col.order = meta$ignore_col_order[i],
      ignore.row.order = meta$ignore_row_order[i]))
  })
  names(eq) = meta$table_name
  eq
}


get_sorting_validity = function(sorting, dataset) {
  assert_data_table(sorting)
  assert_data_table(dataset)

  sorting = copy(sorting)
  ans = if (!setequal(colnames(sorting), c('column_name', 'column_value'))) {
    paste('Column names of the `sorting` sheet are not',
          '"column_name" and "column_value".')
  } else if (!all(sorting$column_name %in% colnames(dataset))) {
    glue('The `column_name` column of the `sorting` sheet contains ',
         'values that are not column names of the dataset.')
  } else {
    special = c('*ascending*', '*descending*')
    n = table(sorting[column_value %in% special]$column_name)

    sort_plain = sorting[!(column_value %in% special)]
    cols = unique(sort_plain$column_name)
    data_plain = unique(melt(
      dataset[, ..cols], measure.vars = cols, variable.factor = FALSE,
      variable.name = 'column_name', value.name = 'column_value'))

    if (any(n > 1)) {
      paste('In the `sorting` sheet, "*ascending*" or "*descending*"',
            'is not the only `column_value` for a given `column_name`.')
    } else if (
      nrow(sort_plain) > 0 && nrow(fsetdiff(sort_plain, data_plain)) > 0) {
      glue('The `sorting` sheet contains combinations of `column_name` ',
           'and `column_value` not present in the dataset.')
    } else {
      0
    }
  }
  ans
}


get_tables_validity = function(x) {
  assert_list(x, types = 'data.table', any.missing = FALSE)
  cols = c('column_name', 'column_label')
  group_cols = setdiff(colnames(x$show_columns), cols)
  viewer_cols = c('viewer_name', 'viewer_email', 'group_id')

  ans = if (!identical(colnames(x$groups), c('group_id', 'file_url'))) {
    'Column names of the `groups` sheet are not "group_id" and "file_url".'
  } else if (any(apply(x$groups, 2, uniqueN) != nrow(x$groups))) {
    'At least one column of the `groups` sheet contains duplicated values.'
  } else if (!identical(
    as_id(x$groups$file_url), drive_get(x$groups$file_url)$id)) {
    # might just throw an error
    paste('At least one row of the `file_url` column of the `groups`',
          'sheet does not correspond to a valid spreadsheet file.')
  } else if (!identical(colnames(x$show_columns)[1:2], cols)) {
    paste('The first two columns of the `show_columns` sheet',
          'are not named "column_name" and "column_label".')
  } else if (!setequal(x$groups$group_id, group_cols)) {
    paste('Values of `group_id` of the `groups` sheet do not match the',
          'column names (from C column onward) of the `show_columns` sheet.')
  } else if (anyDuplicated(x$show_columns$column_name) != 0) {
    paste('The `column_name` column of the `show_columns`',
          'sheet contains duplicated values.')
  } else if (!all(x$show_columns$column_name %in% colnames(x$data))) {
    glue('The `column_name` column of the `show_columns` sheet contains',
         ' values not present in the column names of the dataset.')
  } else if (anyDuplicated(x$show_columns$column_label) != 0) {
    paste('The `column_label` column of the `show_columns`',
          'sheet contains duplicated values.')
  } else if (anyNA(x$show_columns[, ..group_cols])) {
    paste('The columns for the groups in the `show_columns` sheet',
          'contain missing values.')
  } else if (!all(as.matrix(x$show_columns[, ..group_cols]) %in% 0:1)) {
    paste('The columns for the groups in the `show_columns` sheet',
          'contain values other than 0 and 1.')
  } else if (!setequal(colnames(x$viewers), viewer_cols)) {
    paste('Column names of the `viewers` sheet are not',
          '"viewer_name", "viewer_email", and "group_id".')
  } else if (!all(x$viewers$group_id %in% group_cols)) {
    paste('Values of `group_id` of the `viewers` sheet',
          'do not match those of the `groups` sheet.')
  } else {
    get_sorting_validity(x$sorting, x$data)
  }
  if (ans != 0) ans = paste('Error:', ans)
  ans
}

########################################

drive_share_get = function(file_id) {
  assert_class(file_id, 'drive_id')
  perms = drive_get(file_id)$drive_resource[[1L]]$permissions
  shares = data.table(
    email = sapply(perms, `[[`, 'emailAddress'),
    user_id = sapply(perms, `[[`, 'id'),
    role = sapply(perms, `[[`, 'role'))
}


drive_share_add = function(file_id, emails, role = 'reader') {
  assert_class(file_id, 'drive_id')
  assert_character(emails, any.missing = FALSE)
  assert_string(role)

  gfile = drive_get(file_id)
  cli_alert_success('Adding permissions for "{gfile$name}".')
  result = lapply(unique(emails), \(email) {
    tryCatch(
      drive_share(
        file_id, role = role, type = 'user', emailAddress = email,
        sendNotificationEmail = FALSE),
      # below has to be print. cat can't handle a purrr indexed error.
      # message and warning will trigger the tryCatch in update_views.R.
      error = \(e) print(e))
  })
  ans = if (any(sapply(result, inherits, 'error'))) 1 else 0
  invisible(ans)
}


drive_share_remove = function(file_id, user_ids) {
  assert_class(file_id, 'drive_id')
  gfile = drive_get(file_id)
  cli_alert_success('Removing permissions for "{gfile$name}".')
  # https://developers.google.com/drive/api/v3/reference/permissions/delete
  for (user_id in unique(user_ids)) {
    request = gargle::request_build(
      path = 'drive/v3/files/{fileId}/permissions/{permissionId}',
      method = 'DELETE',
      params = list(fileId = file_id, permissionId = user_id),
      token = drive_token())
    result = googledrive::request_make(request)
  }
  invisible(gfile)
}

########################################

drive_get_background = function(file_id, sheet, range, nonwhite = TRUE) {
  assert_class(file_id, 'drive_id')
  assert_string(sheet)
  assert_flag(nonwhite)

  bg = setDT(range_read_cells(file_id, sheet, range, cell_data = 'full'))
  for (color in c('red', 'green', 'blue')) {
    value = lapply(bg$cell, \(z) z$effectiveFormat$backgroundColor[[color]]) |>
      sapply(\(z) if (is.null(z)) 0 else z)
    set(bg, j = color, value = value)
  }
  bg[, cell := NULL][]
  if (nonwhite) bg = bg[!(red == 1 & green == 1 & blue == 1)]
  bg
}


drive_set_background = function(file_id, background) {
  assert_class(file_id, 'drive_id')
  assert_data_table(background)

  # only for setting one color per entire column
  gfile = drive_get(file_id)
  cli_alert_success('Setting background colors for "{gfile$name}".')

  bod_base = '{
  "repeatCell": {
    "range": {
      "startColumnIndex": (start_col),
      "endColumnIndex": (start_col + 1)
    },
    "cell": {
      "userEnteredFormat": {
        "backgroundColor": {
          "red": (red),
          "green": (green),
          "blue": (blue)
        }
      }
    },
    "fields": "userEnteredFormat.backgroundColor"
    }
  }'

  background = copy(background)
  background[, bod := glue(bod_base, .envir = .SD, .open = '(', .close = ')')]
  bod = sprintf('{"requests": [%s]}', paste(background$bod, collapse = ',\n'))

  request = googlesheets4::request_generate(
    'sheets.spreadsheets.batchUpdate', list(spreadsheetId = file_id))
  request$body = bod
  result = googlesheets4::request_make(request)
  invisible(result)
}

########################################

sort_dataset = function(dataset, sorting) {
  assert_data_table(dataset)
  assert_data_table(sorting)
  dataset = copy(dataset)

  special = c('*ascending*', '*descending*')
  cols_tmp = unique(sorting[!(column_value %in% special)]$column_name)
  for (col in cols_tmp) {
    levs_tmp = sorting[column_name == col]$column_value
    levs = c(levs_tmp, setdiff(unique(dataset[[col]]), levs_tmp))
    dataset[, y := factor(y, levs), env = list(y = col)]
  }

  ordering = sorting[, .(
    ord = 1 - 2 * any(column_value == '*descending*')),
    by = column_name]
  setorderv(dataset, cols = ordering$column_name, order = ordering$ord)
  dataset
}


get_view_prefix = function(file_id) {
  gfile = drive_get(file_id)$name
  gsub('main$', 'view', gfile)
}


set_views = function(x, bg, prefix, sheet_name) {
  assert_list(x, types = 'data.table', any.missing = FALSE)
  assert_data_table(bg)
  assert_string(prefix)
  assert_string(sheet_name)

  dataset = sort_dataset(x$data, x$sorting)
  bg = copy(bg)[, column_name := x$show_columns$column_name[row - 1L]]

  result = sapply(1:nrow(x$groups), \(i) {
    file_id = as_id(x$groups$file_url[i])
    group_id_now = x$groups$group_id[i]

    # rename file
    drive_rename(file_id, glue('{prefix}_{group_id_now}'), overwrite = TRUE)

    # update contents
    idx = x$show_columns[[group_id_now]] == 1
    cols_now = x$show_columns$column_name[idx]
    dataset_now = dataset[, ..cols_now]
    setnames(dataset_now, cols_now, x$show_columns$column_label[idx])

    write_sheet(dataset_now, file_id, sheet = sheet_name)
    range_autofit(file_id, sheet = sheet_name)

    # update formatting
    bg_now = bg[column_name %in% cols_now]
    bg_now[, start_col := match(column_name, cols_now) - 1L]
    drive_set_background(file_id, bg_now)

    # update permissions
    viewers_now = x$viewers[group_id == group_id_now]
    viewers_old = drive_share_get(file_id)[role == 'reader']
    viewers_add = viewers_now[!viewers_old, on = c('viewer_email' = 'email')]
    viewers_del = viewers_old[!viewers_now, on = c('email' = 'viewer_email')]

    drive_share_remove(file_id, viewers_del$user_id)
    drive_share_add(file_id, viewers_add$viewer_email) # returns 0 or 1
  })

  ans = max(result) # 0 or 1 from drive_share_add
  invisible(ans)
}

########################################

update_views = function(params) {
  assert_list(params)

  main_id = as_id(params$main_file_url)
  mirror_id = as_id(params$mirror_file_url)
  cli_alert_success('Created file ids from file urls.')

  # get previous and current versions of tables
  tables_old = get_tables(mirror_id, params$date_colnames)
  cli_alert_success('Fetched old tables from mirror file.')
  tables_new = get_tables(main_id, params$date_colnames)
  cli_alert_success('Fetched new tables from main file.')

  # check validity of tables
  msg = get_tables_validity(tables_new)
  cli_alert_success('Checked validity of new tables.')
  if (msg != 0) {
    cli_alert_danger(msg)
    return(msg)
  }

  # check whether anything has changed (ignores formatting)
  tables_eq = compare_tables(tables_new, tables_old)
  cli_alert_success('Compared old and new tables.')

  # update the views
  bg = drive_get_background(main_id, 'show_columns', 'A2:A')
  cli_alert_success('Got background colors.')
  view_prefix = get_view_prefix(main_id)
  cli_alert_success('Got prefix for view files.')
  msg = set_views(tables_new, bg, view_prefix, params$view_sheet_name)
  cli_alert_success('Wrote new tables to view files.')

  # update the mirror file
  lapply(names(tables_new)[!tables_eq], \(tbl_name) {
    write_sheet(tables_new[[tbl_name]], mirror_id, tbl_name)})
  cli_alert_success('Wrote new tables to mirror file.')

  # make final message
  if (msg != 0) {
    msg = paste(
      "Updated views, albeit with issues. Please check the GitHub Actions",
      "workflow log and the Google Sheet's version history.")
    cli_alert_warning(msg)
  } else if (all(tables_eq)) {
    msg = 'Successfully updated views, although no changes detected.'
    cli_alert_success(msg)
  } else {
    msg_end = paste(names(tables_eq)[!tables_eq], collapse = ', ')
    msg = glue('Successfully updated views based on changes to {msg_end}.')
    cli_alert_success(msg)
  }
  msg
}


get_env_output = function(
    msg, file_url, sheet = 'maintainers', colname = 'email',
    env = 'GITHUB_ENV') {
  maintainers = read_sheet(file_url, sheet)
  emails = paste(maintainers[[colname]], collapse = ', ')
  env_out = glue('MESSAGE={msg}\nFILE_URL={file_url}\nEMAIL_TO={emails}')
  if (Sys.getenv(env) != '') {
    cat(env_out, file = Sys.getenv(env), sep = '\n', append = TRUE)
  }
  env_out
}
