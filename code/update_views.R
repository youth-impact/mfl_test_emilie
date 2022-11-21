source(file.path('code', 'setup.R'))

msg = tryCatch(
  update_views(params),
  error = function(e) trimws(as.character(e)))

get_env_output(msg, params$main_file_url)
