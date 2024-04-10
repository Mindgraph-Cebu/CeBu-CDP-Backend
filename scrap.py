if user_env == 's3':
    duckdb.execute("INSTALL httpfs;")
    duckdb.execute("LOAD httpfs;")

    duckdb.execute("INSTALL aws;")
    duckdb.execute("LOAD aws;")

    duckdb.execute("CREATE SECRET (TYPE S3,PROVIDER CREDENTIAL_CHAIN,REGION 'ap-southeast-1',ENDPOINT 's3.amazonaws.com',URL_STYLE 'vhost');")
