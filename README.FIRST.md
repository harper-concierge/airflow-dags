# Datalake

## Astro Docs are here

[Here](Astro.md)

## Running locally

Do this once, the first time you setup your local docker, or after every astro dev kill

```shell
touch airflow_settings.yaml
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

Now in admin->connections edit the 'mongo_db_conn_id' connection and change its type from Generic to MongoDB and save.

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
