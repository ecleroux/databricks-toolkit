#Add mssql-init.sh as an init script in your cluster configuration to use this code snippet

server = "<server> or <server>:<port>"
database = "<your-database>"
username = "<username>"
password = "<password>"
dbtable = "<schema>.<table-name>"


jdbc_connection_string = f"jdbc:sqlserver://{server};databaseName={database};user={username};password={password};encrypt=true;trustServerCertificate=true;"

df = spark.read.format('jdbc').option('driver', 'com.microsoft.sqlserver.jdbc.SQLServerDriver').option(
                    'url', jdbc_connection_string).option('dbtable', dbtable).load()

df.show()