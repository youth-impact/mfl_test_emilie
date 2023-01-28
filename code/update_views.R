source(file.path('code', 'setup.R'))

msg = tryCatch(
  update_views(params), error = function(e) {
    print(e)
    'Error encountered. Please check the workflow logs.'
  })

print(msg)
get_env_output(msg, params$main_file_url)
