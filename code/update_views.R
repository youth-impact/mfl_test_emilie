source(file.path('code', 'setup.R'))

# TODO: facilitator_database_main gsheet triggers gh actions in prod

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
