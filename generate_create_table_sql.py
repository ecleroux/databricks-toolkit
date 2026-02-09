def generate_create_table_sql(
    parquet_path,
    table_name,
    partition_cols=None
):
    if partition_cols is None:
        partition_cols = []
    df = spark.read.parquet(parquet_path)
    def spark_type_to_sql(spark_type):
        t = str(spark_type)
        if t.startswith("StringType"):
            return "STRING"
        elif t.startswith("IntegerType"):
            return "INT"
        elif t.startswith("LongType"):
            return "BIGINT"
        elif t.startswith("DoubleType"):
            return "DOUBLE"
        elif t.startswith("FloatType"):
            return "FLOAT"
        elif t.startswith("DecimalType"):
            import re
            m = re.match(r"DecimalType\((\d+), (\d+)\)", t)
            if m:
                return f"DECIMAL({m.group(1)},{m.group(2)})"
            else:
                return "DECIMAL"
        elif t.startswith("DateType"):
            return "DATE"
        elif t.startswith("TimestampType"):
            return "TIMESTAMP"
        elif t.startswith("BooleanType"):
            return "BOOLEAN"
        else:
            return t.upper()
    columns = []
    for field in df.schema.fields:
        col_def = f"{field.name} {spark_type_to_sql(field.dataType)}"
        columns.append(col_def)
    non_partition_columns = [c for c in columns if c.split()[0] not in partition_cols]
    partition_columns = [c for c in columns if c.split()[0] in partition_cols]
    create_stmt = f"CREATE TABLE {table_name} (\n  " + ",\n  ".join(non_partition_columns + partition_columns) + "\n)"
    create_stmt += f"\nUSING PARQUET\nLOCATION '{parquet_path}'"
    if partition_columns:
        create_stmt += f"\nPARTITIONED BY ({', '.join(partition_cols)})"
    create_stmt += f"\nTBLPROPERTIES ('partitionMetadataEnabled' = 'false');"
    return create_stmt

# Example usage:
parquet_path = "s3://eu-west-1-dev-cib-data-pot-cif/technical_curated/cdg_cust_market_consent/"
table_name = "cib_workspace.cif.tbl_cdg_cust_market_consent"
partition_cols = ["year", "month", "day", "dataproductlogid"]
sql_ddl = generate_create_table_sql(parquet_path, table_name, partition_cols)
print(sql_ddl)