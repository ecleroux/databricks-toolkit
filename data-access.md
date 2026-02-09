# Data Access

## Databricks job access to ADLG2 storage using Azure SPN

In your cluster configuration add

fs.azure.account.auth.type.{{storage-account-name}}.dfs.core.windows.net OAuth
fs.azure.account.oauth.provider.type.{{storage-account-name}}.dfs.core.windows.net org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider
fs.azure.account.oauth2.client.endpoint.{{storage-account-name}}.dfs.core.windows.net https://login.microsoftonline.com/{{your-tenent-id}}/oauth2/token
fs.azure.account.oauth2.client.id.{{storage-account-name}}.dfs.core.windows.net {{your-client-id}}
fs.azure.account.oauth2.client.secret.{{storage-account-name}}.dfs.core.windows.net {{your-client-secret}}