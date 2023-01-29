source(file.path('code', 'setup.R'))

msg = tryCatch(
  update_views(params), error = function(e) {
    warning(e)
    'Error encountered. Please check the workflow logs.'
  })

env_output = get_env_output(msg, params$main_file_url)
