function dispatch(repo_url, pat_url, client_payload = {}) {
  var url = repo_url.replace('github\.com', 'api.github.com/repos') + '/dispatches';
  var pat_file_id = pat_url.match(/[-\w]{25,}/);
  var pat_file = DriveApp.getFileById(pat_file_id);
  var auth = 'Bearer ' + pat_file.getBlob().getDataAsString();

  var headers = {
    'Accept': 'application/vnd.github+json',
    'Authorization': auth
  };
  var data = {
    'event_type': 'webhook',
    'client_payload': client_payload
  };
  var options = {
    'method': 'post',
    'contentType': 'application/json',
    'headers': headers,
    'payload': JSON.stringify(data)
  };
  UrlFetchApp.fetch(url, options);
}

function get_params(ss, sheet_name, key_idx = 0, val_idx = 1) {
  var sheet = ss.getSheetByName(sheet_name);
  var arr = sheet.getRange(1, 1, 2, sheet.getLastColumn()).getValues();
  var p = {};
  arr[key_idx].forEach((key, i) => p[key] = arr[val_idx][i]);
  return p;
}

function at_edit(e) {
  var ss = SpreadsheetApp.getActiveSpreadsheet();
  var sheets = ss.getRange('triggers!A2:A').getValues().filter(String).flat();
  var sheet_now = e.range.getSheet().getName();
  var gh = get_params(ss, 'github');

  if (gh['enabled'] == 1 && sheets.includes(sheet_now)) {
    dispatch(gh['repo_url'], gh['pat_url']);
  }
}

function at_change(e) {
  user_now = e.user.getEmail();
  var ss = SpreadsheetApp.getActiveSpreadsheet();
  var gh = get_params(ss, 'github');

  // should correspond to changes made by surveycto and not via github actions
  if (gh['enabled'] == 1 && user_now == '' && e.changeType == 'INSERT_ROW') {
    dispatch(gh['repo_url'], gh['pat_url']);
  }
}
