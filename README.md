# facilitator_database

See the main Google Sheet for a link to the documentation.

## Contents

* .github/workflows/ci.yaml: GitHub Actions workflow that runs the code and sends emails conveying the outcome.
* DESCRIPTION: Used by ci.yaml to determine which R packages to install to run the code.
* code/setup.R: Loads packages, parameters, and functions used by update_views.R.
* code/update_views.R: Script run by ci.yaml to update the views.
* params/params.yaml: Parameters used by the code to update the views.
