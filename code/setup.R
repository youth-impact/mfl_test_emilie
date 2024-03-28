library('cli')
library('checkmate')
library('data.table')
library('glue')
library('googledrive')
library('googlesheets4')
library('yaml')

########################################

if (Sys.getenv('GOOGLE_TOKEN') == '') { # not on GitHub
  drive_auth(email = 'youthimpactautomation@gmail.com')
} else { # on GitHub Actions runner
  drive_auth(path = Sys.getenv('GOOGLE_TOKEN'))
}
gs4_auth(token = drive_token())

########################################

# used by [update_views()]
get_params = function() {
  params_raw = read_yaml(file.path('params', 'params.yaml'))
  envir = Sys.getenv('ENVIRONMENT')
  if (envir != 'production') envir = 'testing'
  params = params_raw[[envir]]
}

########################################

#' Convert list columns to character columns
#'
#' This function is useful for dealing with data.tables derived from Google
#' Sheets, and takes care to convert `NULL` values in a list to `NA` in the
#' resulting character vector.
#'
#' @param d A `data.table`.
#'
#' @return A `data.table`.
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

#' Standardize date formats in a data.table
#'
#' This function deals with the cornucopia of date formats coming from
#' SurveyCTO via Google Sheets, where a given column in a `data.table` could
#' have values in multiple different formats.
#'
#' @param d A `data.table`.
#' @param date_colnames A character vector of column names in `d` that contain
#'   dates.
#' @param preferred_date_format A string, see [strptime()].
#'
#' @return A `data.table` in which the given columns have been converted to
#'   class `IDate` using [data.table::as.IDate()].
fix_dates = function(d, date_colnames, preferred_date_format) {
  if (is.null(date_colnames)) return(d)
  assert_data_table(d)
  assert_character(date_colnames)

  d = copy(d)
  date_zero = as.IDate('1899-12-30') # tested by trial and error in gsheets
  date_formats = c('%Y-%m-%d', '%d-%m-%Y', '%d/%m/%Y', '%d-%b-%Y')
  date_regs = c(
    '^\\d{4}-\\d{2}-\\d{2}$', '^\\d{2}-\\d{2}-\\d{4}$',
    '^\\d{2}/\\d{2}/\\d{4}$', '^\\d{2}-[a-zA-Z]{3}-\\d{4}$')

  for (col in date_colnames) {
    if (is.character(d[[col]])) {
      d[col == '-', col := NA, env = list(col = col)]
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
    # converts to charater, but this ensures proper display in the google sheets
    # and valid comparison of old and new tables
    set(d, j = col, value = format(d[[col]], format = preferred_date_format))
  }

  d[]
}

#' Read multiple worksheets from a Google Sheet
#'
#' @param file_id A `drive_id` corresponding to a Google Sheet.
#' @param date_colnames A character vector of column names in the `data`
#'   worksheet that contains dates.
#' @param preferred_date_format A string, see [strptime()].
#' @param sheets A character vector of names of worksheets in the Google Sheet
#'   to be read in as `data.table`s.
#'
#' @return A named list of `data.table`s derived from
#'   [googlesheets4::read_sheet()].
get_tables = function(
    file_id, preferred_date_format = NULL,
    sheets = c(
      'groups', 'show_columns', 'hide_rows', 'sorting', 'viewers',
      'date_columns', 'data')) {

  assert_class(file_id, 'drive_id')
  assert_character(sheets, any.missing = FALSE)

  tables = lapply(sheets, \(x) setDT(read_sheet(file_id, x)))
  names(tables) = sheets

  if (nrow(tables$show_columns) > 0) {
    # if no column label specified, just use the column name
    tables$show_columns[is.na(column_label), column_label := column_name]
  }
  # omit rows with missing values, which typically correspond to group headers
  tables$viewers = unique(na.omit(tables$viewers))

  # using this table before checking its validity
  if ('column_name' %in% colnames(tables$date_columns)) {
    tables$date_columns[, column_name := as.character(column_name)]
  }

  if (nrow(tables$data) > 0) {
    # omit rows lacking an id, typically for former facilitators
    tables$data = tables$data[!is.na(id)]
    # deal with messy dates
    tables$data = fix_list_cols(tables$data)
    tables$data = fix_dates(
      tables$data, tables$date_columns$column_name, preferred_date_format)
  }
  tables
}

#' Check for equality between data.tables within two lists
#'
#' This function makes deliberate use of [data.table::all.equal()].
#'
#' @param x A list of `data.tables`.
#' @param y A list of `data.tables`.
#'
#' @return A named logical vector.
compare_tables = function(x, y) {
  assert_list(x, types = 'data.table', any.missing = FALSE)
  assert_list(y, types = 'data.table', any.missing = FALSE)

  meta = data.table(
    table_name = c(
      'groups', 'show_columns', 'hide_rows', 'sorting', 'viewers',
      'date_columns', 'data'),
    ignore_row_order = c(TRUE, FALSE, FALSE, FALSE, TRUE, TRUE, TRUE),
    ignore_col_order = c(TRUE, FALSE, FALSE, TRUE, TRUE, TRUE, TRUE))

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

#' Determine whether the groups table is valid
#'
#' @param groups A `data.table` from the `groups` worksheet of the main Google
#'   Sheet.
#'
#' @return 0 if valid, a string otherwise.
get_validity_groups = function(groups) {
  ans = if (!identical(colnames(groups), c('group_id', 'file_url'))) {
    'Column names of the `groups` sheet are not "group_id" and "file_url".'
  } else if (any(apply(groups, 2, uniqueN) != nrow(groups))) {
    'At least one column of the `groups` sheet contains duplicated values.'
  # } else if (!all(RCurl::url.exists(groups$file_url))) {
  #   paste('At least one row of the `file_url` column of the `groups`',
  #         'sheet does not correspond to a valid URL.')
  } else {
    ans_tmp = tryCatch(
      anyNA(sapply(groups$file_url, as_sheets_id)), error = \(e) e)
    if (inherits(ans_tmp, 'error') || isTRUE(ans_tmp)) {
      paste('At least one row of the `file_url` column of the `groups`',
            'sheet does not correspond to a valid spreadsheet file.')
    } else {
      0
    }
  }
  ans
}

#' Determine whether the show_columns table is valid
#'
#' @param show_columns A `data.table` from the `show_columns` worksheet of the
#'   main Google Sheet.
#' @param group_ids A vector of group ids.
#' @param data_cols A vector of columns in the `data` table in the main Google
#'   Sheet.
#'
#' @return 0 if valid, a string otherwise.
get_validity_show_columns = function(show_columns, group_ids, data_cols) {
  ans = if (!identical(
    colnames(show_columns)[1:2], c('column_name', 'column_label'))) {
    paste('The first two columns of the `show_columns` sheet',
          'are not named "column_name" and "column_label".')
  } else if (anyDuplicated(show_columns$column_name) != 0) {
    paste('The `column_name` column of the `show_columns`',
          'sheet contains duplicated values.')
  } else if (!all(show_columns$column_name %in% data_cols)) {
    paste('The `column_name` column of the `show_columns` sheet contains',
          'values not present in the column names of the dataset.')
  } else if (anyDuplicated(show_columns$column_label) != 0) {
    paste('The `column_label` column of the `show_columns`',
          'sheet contains duplicated values.')
  } else if (!setequal(colnames(show_columns)[-(1:2)], group_ids)) {
    paste('Column names (from C column onward) of the `show_columns` sheet',
          'do not match values of `group_id` in the `groups` sheet.')
  } else if (anyNA(show_columns[, ..group_ids])) {
    paste('The columns for the groups in the `show_columns` sheet',
          'contain missing values.')
  } else if (!all(as.matrix(show_columns[, ..group_ids]) %in% 0:1)) {
    paste('The columns for the groups in the `show_columns` sheet',
          'contain values other than 0 and 1.')
  } else {
    0
  }
  ans
}

#' Determine whether the hide_rows table is valid
#'
#' @param hide_rows A `data.table` from the `hide_rows` worksheet of the
#'   main Google Sheet.
#' @param group_ids A vector of group ids.
#' @param dataset A `data.table` from the `data` worksheet of the main Google
#'   Sheet.
#'
#' @return 0 if valid, a string otherwise.
get_validity_hide_rows = function(hide_rows, group_ids, dataset) {
  cols = c('column_name', 'column_value')
  ans = if (!identical(colnames(hide_rows)[1:2], cols)) {
    paste('The first two columns of the `hide_rows` sheet',
          'are not named "column_name" and "column_value".')
  } else if (!setequal(colnames(hide_rows)[-(1:2)], group_ids)) {
    paste('Column names (from C column onward) of the `hide_rows` sheet',
          'do not match values of `group_id` in the `groups` sheet.')
  } else if (anyDuplicated(hide_rows[, .(column_name, unlist(column_value))])) {
    paste('The `column_name` and `column_value` columns of',
          'the `hide_rows` sheet contain duplicated values.')
  } else if (anyNA(hide_rows[, ..group_ids])) {
    paste('The columns for the groups in the `hide_rows` sheet',
          'contain missing values.')
  } else if (!all(as.matrix(hide_rows[, ..group_ids]) %in% c('show', 'hide'))) {
    paste('The columns for the groups in the `hide_rows` sheet',
          'contain values other than "show" and "hide".')
  } else if (!all(hide_rows$column_name %in% colnames(dataset))) {
    # this block and the next could be adapted for sorting validity,
    # but if it ain't broke
    paste('The `hide_rows` sheet contains values for',
          '`column_name` not present in the dataset.')
  } else {
    ans = 0
    # for (i in seq_len(nrow(hide_rows))) {
    #   now = hide_rows[i]
    #   if (!(now$column_value %in% dataset[[now$column_name]])) {
    #     ans = glue(
    #       'The `hide_rows` sheet contains `column_name` "{now$column_name}"',
    #       ' and `column_value` "{now$column_value}", but this combination is',
    #       ' not present in the dataset.')
    #     break
    #   }
    # }
    ans
  }
  ans
}

#' Determine whether the viewers table is valid
#'
#' @param viewers A `data.table` from the `viewers` worksheet of the
#'   main Google Sheet.
#' @param group_ids A vector of group ids.
#'
#' @return 0 if valid, a string otherwise.
get_validity_viewers = function(viewers, group_ids) {
  ans = if (!setequal(
    colnames(viewers), c('viewer_name', 'viewer_email', 'group_id'))) {
    paste('Column names of the `viewers` sheet are not',
          '"viewer_name", "viewer_email", and "group_id".')
  } else if (!all(viewers$group_id %in% group_ids)) {
    paste('Values of `group_id` of the `viewers` sheet',
          'do not match those of the `groups` sheet.')
  } else {
    0
  }
  ans
}

#' Determine whether the sorting table is valid
#'
#' @param sorting A `data.table` from the `sorting` worksheet of the main Google
#'   Sheet.
#' @param group_ids A vector of group ids.
#'
#' @return 0 if valid, a string otherwise.
get_validity_sorting = function(sorting, dataset) {
  assert_data_table(sorting)
  assert_data_table(dataset)

  sorting = copy(sorting)
  ans = if (!setequal(colnames(sorting), c('column_name', 'column_value'))) {
    paste('Column names of the `sorting` sheet are not',
          '"column_name" and "column_value".')
  } else if (!all(sorting$column_name %in% colnames(dataset))) {
    paste('The `column_name` column of the `sorting` sheet contains',
          'values that are not column names of the dataset.')
  } else {
    special = c('*ascending*', '*descending*')
    n = table(sorting[column_value %in% special]$column_name)

    sort_plain = sorting[!(column_value %in% special)]
    cols = unique(sort_plain$column_name)
    # this might break for non-character columns
    data_plain = unique(melt(
      dataset[, ..cols], measure.vars = cols, variable.factor = FALSE,
      variable.name = 'column_name', value.name = 'column_value'))

    if (any(n > 1)) {
      paste('In the `sorting` sheet, "*ascending*" or "*descending*"',
            'occurs more than once for a given `column_name`.')
    # } else if (
    #   nrow(sort_plain) > 0 && nrow(fsetdiff(sort_plain, data_plain)) > 0) {
    #   paste('The `sorting` sheet contains combinations of `column_name`',
    #         'and `column_value` not present in the dataset.')
    } else {
      0
    }
  }
  ans
}

#' Determine whether the date_columns table is valid
#'
#' @param sorting A `data.table` from the `date_columns` worksheet of the main
#'   Google Sheet.
#'
#' @return 0 if valid, a string otherwise.
get_validity_date_columns = function(date_columns) {
  assert_data_table(date_columns)
  ans = if (!setequal(colnames(date_columns), 'column_name')) {
    paste('The `date_columns` sheet does not have',
          'only one column, named "column_name".')
  } else {
    0
  }
  ans
}

#' Determine whether the tables from a Google Sheet are valid
#'
#' This function individually checks the validity of multiple tables.
#'
#' @param x A named list of `data.table`s.
#'
#' @return 0 if valid, a string otherwise.
get_validity_tables = function(x) {
  assert_list(x, types = 'data.table', any.missing = FALSE)

  ans = get_validity_groups(x$groups)
  if (ans != 0) return(ans)

  group_ids = x$groups$group_id
  data_cols = colnames(x$data)
  ans = get_validity_show_columns(x$show_columns, group_ids, data_cols)
  if (ans != 0) return(ans)

  ans = get_validity_hide_rows(x$hide_rows, group_ids, x$data)
  if (ans != 0) return(ans)

  ans = get_validity_viewers(x$viewers, group_ids)
  if (ans != 0) return(ans)

  ans = get_validity_sorting(x$sorting, x$data)
  if (ans != 0) return(ans)

  ans = get_validity_date_columns(x$date_columns)
}

########################################

#' Get current share permissions for a Drive file
#'
#' This function extracts relevant information using [googledrive::drive_get()].
#'
#' @param file_id A `drive_id` corresponding to a Drive file.
#'
#' @return A `data.table`.
drive_share_get = function(file_id) {
  assert_class(file_id, 'drive_id')
  perms = drive_get(file_id)$drive_resource[[1L]]$permissions
  shares = data.table(
    email = sapply(perms, `[[`, 'emailAddress'),
    user_id = sapply(perms, `[[`, 'id'),
    role = sapply(perms, `[[`, 'role'))
}

#' Add share permissions to a Drive file
#'
#' This function uses [googledrive::drive_share()] to grant access.
#'
#' @param file_id A `drive_id` corresponding to a Drive file.
#' @param emails A character vector of email addresses.
#' @param role The role to give the email addresses.
#'
#' @return 0 if no errors encountered, 1 otherwise, invisibly.
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

#' Remove share permissions from a Drive file
#'
#' This function uses [googledrive::request_make()] to send a DELETE request.
#'
#' @param file_id A `drive_id` corresponding to a Drive file.
#' @param user_ids A vector of user ids for which to revoke access.
#'
#' @return A `dribble` for the Drive file, invisibly.
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

#' Get background colors of a range in a Google Sheet
#'
#' @param file_id A `drive_id` corresponding to a Drive file.
#' @param sheet Passed to [googlesheets4::range_read_cells()].
#' @param range Passed to [googlesheets4::range_read_cells()].
#' @param nonwhite Logical indicating whether to only include cells having
#'   non-white background colors.
#'
#' @return A `data.table` having one row per cell.
drive_get_background = function(file_id, sheet, range, nonwhite = TRUE) {
  assert_class(file_id, 'drive_id')
  assert_string(sheet)
  assert_flag(nonwhite)

  bg = setDT(range_read_cells(file_id, sheet, range, cell_data = 'full'))
  for (color in c('red', 'green', 'blue')) {
    # sometimes one of the RGB values is missing
    value = lapply(bg$cell, \(z) z$effectiveFormat$backgroundColor[[color]]) |>
      sapply(\(z) if (is.null(z)) 0 else z)
    set(bg, j = color, value = value)
  }
  bg[, cell := NULL][]
  if (nonwhite) bg = bg[!(red == 1 & green == 1 & blue == 1)]
  bg
}

#' Set background colors in columns of a Google Sheet
#'
#' This function constructs a JSON string, then makes and sends a Google Sheets
#' API request.
#'
#' @param file_id A `drive_id` corresponding to a Drive file.
#' @param background A `data.table` having columns `start_col`, `red`, `green`,
#'   and `blue`.
#' @param sheet A string indicating the worksheet name in the Google Sheet.
#'
#' @return The result of [googlesheets4::request_make()], invisibly.
drive_set_background = function(file_id, background, sheet) {
  assert_class(file_id, 'drive_id')
  assert_data_table(background)

  # only for setting one color per entire column
  gfile = gs4_get(file_id)
  cli_alert_success('Setting background colors for "{gfile$name}".')

  bod_base = '{
  "repeatCell": {
    "range": {
      "sheetId": (sheet_id),
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
  background[, sheet_id := gfile$sheets$id[gfile$sheets$name == sheet]]
  background[, bod := glue(bod_base, .envir = .SD, .open = '(', .close = ')')]
  bod = sprintf('{"requests": [%s]}', paste(background$bod, collapse = ',\n'))

  request = googlesheets4::request_generate(
    'sheets.spreadsheets.batchUpdate', list(spreadsheetId = file_id))
  request$body = bod
  result = googlesheets4::request_make(request)
  invisible(result)
}

########################################

#' Sort rows of a dataset according to a sorting table
#'
#' @param dataset A `data.table`.
#' @param sorting A `data.table` derived from the `sorting` worksheet of the
#'   Google Sheet.
#'
#' @return The `dataset`, now sorted.
sort_dataset = function(dataset, sorting) {
  assert_data_table(dataset)
  assert_data_table(sorting)
  dataset = copy(dataset)

  # for specified sorting orders, create factors
  special = c('*ascending*', '*descending*')
  cols_tmp = unique(sorting[!(column_value %in% special)]$column_name)
  for (col in cols_tmp) {
    levs_tmp = sorting[column_name == col]$column_value
    n = length(levs_tmp)
    levs_main = sort(
      setdiff(unique(dataset[[col]]), levs_tmp),
      decreasing = '*descending*' %in% levs_tmp)
    idx = which(levs_tmp %in% special)
    levs = if (length(idx) > 0L) {
      levs_before = if (idx == 1L) NULL else levs_tmp[1:(idx - 1)]
      levs_after = if (idx == n) NULL else levs_tmp[(idx + 1):n]
      c(levs_before, levs_main, levs_after)
    } else {
      c(levs_tmp, setdiff(unique(dataset[[col]]), levs_tmp))
    }
    dataset[, y := factor(y, levs), env = list(y = col)]
  }

  # otherwise use ascending or descending
  ordering = sorting[, .(
    ord = 1 - 2 * any(column_value == '*descending*')),
    by = column_name]
  setorderv(dataset, cols = ordering$column_name, order = ordering$ord)
  dataset
}

#' Get the prefix for the names of the views Google Sheets
#'
#' The prefix is based on the name of the main Google Sheet.
#'
#' @param file_id A `drive_id` corresponding to a Drive file.
#'
#' @return A string.
get_view_prefix = function(file_id) {
  gfile = drive_get(file_id)$name
  gsub('main$', 'view', gfile)
}

#' Filter rows of a dataset based on the hide_rows table
#'
#' @param dataset A `data.table`.
#' @param hide_rows A `data.table` derived from the `hide_rows` worksheet of
#'   the Google Sheet.
#' @param group_id The column in `hide_rows` based on which to filter rows.
#'
#' @return The `dataset`, now with some rows possibly removed.
get_filtered_dataset = function(dataset, hide_rows, group_id) {
  dataset_new = copy(dataset)
  for (i in seq_len(nrow(hide_rows))) {
    if (hide_rows[[group_id]][i] == 'hide') {
      env = list(col_name = hide_rows$column_name[i])
      dataset_new = dataset_new[
        col_name != hide_rows$column_value[[i]], env = env]
    }
  }
  dataset_new
}

#' Set the views Google Sheets
#'
#' @param x A named list of `data.table`s.
#' @param bg A `data.table` of background colors
#' @param prefix A string indicating the prefix for the file names.
#' @param sheet A string indicating the worksheet name in the Google Sheet.
#'
#' @return 0 if no errors encountered in [drive_share_add()], 1 otherwise.
set_views = function(x, bg, prefix, sheet) {
  assert_list(x, types = 'data.table', any.missing = FALSE)
  assert_data_table(bg)
  assert_string(prefix)
  assert_string(sheet)

  dataset = sort_dataset(x$data, x$sorting)
  bg = copy(bg)[, column_name := x$show_columns$column_name[row - 1L]]

  result = sapply(seq_len(nrow(x$groups)), \(i) {
    file_id = as_id(x$groups$file_url[i])
    group_id_now = x$groups$group_id[i]

    # rename file
    drive_rename(file_id, glue('{prefix}_{group_id_now}'), overwrite = TRUE)

    # update contents
    dataset_now = get_filtered_dataset(dataset, x$hide_rows, group_id_now)
    idx = x$show_columns[[group_id_now]] == 1
    cols_now = x$show_columns$column_name[idx]
    dataset_now = dataset_now[, ..cols_now]
    setnames(dataset_now, cols_now, x$show_columns$column_label[idx])

    write_sheet(dataset_now, file_id, sheet = sheet)
    range_autofit(file_id, sheet = sheet)

    # update formatting
    bg_now = bg[column_name %in% cols_now]
    bg_now[, start_col := match(column_name, cols_now) - 1L]
    drive_set_background(file_id, bg_now, sheet)

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

#' Update the Google Sheets comprising the facilitator database
#'
#' This is the main function called in GitHub Actions.
#'
#' @param params A named list of parameters.
#'
#' @return A string indicating success or failure.
update_views = function(params) {
  assert_list(params)

  main_id = as_id(params$main_file_url)
  mirror_id = as_id(params$mirror_file_url)
  cli_alert_success('Created file ids from file urls.')

  # get previous and current versions of tables
  tables_old = get_tables(mirror_id, params$preferred_date_format)
  cli_alert_success('Fetched old tables from mirror file.')
  tables_new = get_tables(main_id, params$preferred_date_format)
  cli_alert_success('Fetched new tables from main file.')

  # check validity of tables
  msg = get_validity_tables(tables_new)
  cli_alert_success('Checked validity of new tables.')
  if (msg != 0) {
    msg = paste('Error:', msg)
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

#' Send environmental variables to a file for GitHub Actions
#'
#' @param msg A string containing a message to be assigned to `MESSAGE`.
#' @param file_url A string indicating the URL of a Google Sheet, to be assigned
#'   to `FILE_URL`.
#' @param sheet A string indicating a worksheet in the Google Sheet.
#' @param colname A string indicating the column name that contains email
#'   addresses in the given worksheet of the Google Sheet, which will all be
#'   written to `EMAIL_TO`.
#' @param env A string indicating the environmental variable pointing to the
#'   file to which to write the environmental variables.
#'
#' @return A `glue` string, invisibly.
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
