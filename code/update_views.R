source(file.path('code', 'setup.R'))

msg = tryCatch({
  params = get_params()
  update_views(params)
  },
  error = \(e) {
    message(as.character(e))
    paste("Error encountered. Please check the GitHub Actions",
          "workflow log and the Google Sheet's version history.")
  })

env_output = get_env_output(msg, params$main_file_url)
