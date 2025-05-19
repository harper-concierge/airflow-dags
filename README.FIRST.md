# Datalake

## Astro Docs are here

[Here](Astro.md)

## Running locally

The first time you setup your local docker,
create an aitflow_settings.yaml file

```yaml
airflow:
    connections:
        - conn_id: ""
          conn_type: ""
          conn_host: ""
          conn_schema: ""
          conn_login: ""
          conn_password: ""
          conn_port: 0
          conn_uri: ""
          conn_extra:
            example_extra_field: example-value
    pools: ## pool_name and pool_slot are required
      - pool_name: sql_single_thread_pool
        pool_slot: 1
        pool_description: "Used to enforce one query ata  time to speed up heavy queries"
      - pool_name: shopify_import_pool
        pool_slot: 5
        pool_description: "Set the concurrency of shopify calls for partner data processing"
      - pool_name: mongo_default_pool
        pool_slot: 5
        pool_description: "Set the concurrency of calls to production mongoDB"
    variables:
        - variable_name: REBUILD_MONGO_DATA
          variable_value: "False"
        - variable_name: REFRESH_CONCURRENTLY
          variable_value: "False"
        - variable_name: SHOPIFY_START_DATE
          variable_value: "2024-08-01T00:00:00.000Z"
        - variable_name: ZETTLE_START_DAYS_AGO
          variable_value: "60"
        - variable_name: MONGO_START_DATE
          variable_value: "2024-08-01T00:00:00.000Z"
```

If you wish to wipe your local docker - e.g. when an airflow DB upgrade is needed you can issue the following command

```shell
astro dev kill
```

If you are running for the first time, or have just issued a kill command you need to do this once:-

```shell
astro login
astro config set -g disable_env_objects false
astro workspace list
# NAME         ID
# DataLake     clr9qwhbn033u01qzg6shab5j

# you only need to do this once
astro dev start --workspace-id clr9qwhbn033u01qzg6shab5j

Then if you need to
astro dev restart

```

Now in admin->Connections edit the 'mongo_db_conn_id' connection and change its type from Generic to MongoDB and save.

Now in admin->Pools Check the following pools

* sql_single_thread_pool - slots=1
* shopify_import_pool - slots=5
* mongo_default_pool - slots=5
* mongo_rebuild_pool - slots=2

## restarting your local environment to pickup code changes outside of your plugins/operators directory

``` shell
astro dev restart
```

## References

### .env

cp .env.example .env
Set your correct AIRFLOW_VAR_DEVICE_NAME

### Data Types used

Currently these are the data type mappings used to map from our schemas to NumPy datatypes, which are then mapped to SQL field types. <https://numpy.org/doc/stable/user/basics.types.html>

The subset we use are:-

``` python
bson_pandas_numpy_mapping = {
          "double": "Float64",  # BSON double maps to a 64-bit floating point
          "string": "string",  # BSON string maps to pandas object (for string data)
          "object": "object",  # BSON object maps to pandas object (for mixed types)
          "array": "object",  # BSON array maps to pandas object (for mixed types)
          "binData": "object",  # BSON binary data maps to pandas object (for arbitrary binary data)
          "objectId": "string",  # BSON ObjectId maps to pandas object (for unique object identifiers)
          "bool": "bool",  # BSON boolean maps to pandas/numpy boolean
          # "date": "datetime64[ns]",  # BSON date maps to pandas datetime64[ns]
          "int": "Int32",  # BSON 32-bit integer maps to pandas/numpy int32
          "timestamp": "datetime64[ns]",  # BSON timestamp maps to pandas datetime64[ns] (with note on precision)
          "long": "Int64",  # BSON 64-bit integer maps to pandas/numpy int64
          "decimal": "Float64",  # BSON Decimal128 maps to pandas/numpy float64 (considerations for precision apply)
      }
```

### Shopify Import Pool in Airflow

Our Airflow deployment utilizes a custom 'pool' called shopify_import_pool to manage the concurrency of tasks that import data from Shopify. This pool has a maximum capacity of 5 slots.

Here's where it is used in the 15_get_shopify_data_dag.py:

for partner in partners:
    task_id = f"get_{partner}_shopify_data_task"
    shopify_task = ImportShopifyPartnerDataOperator(
        task_id=task_id,
        postgres_conn_id="postgres_datalake_conn_id",
        schema="public",
        destination_schema="transient_data",
        destination_table=destination_table,
        partner_ref=partner,
        dag=dag,
        pool="shopify_import_pool",
    )

Add this to the 'pools' tab in your local environment.
