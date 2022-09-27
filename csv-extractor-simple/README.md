CSV Extractor
=============

This is a simplified version of the csv-extractor sample that reads CSV files and uploads the content to a configurable
database and table in CDF RAW. This extractor can be deployed as a Cognite Function. 
This simple version differ from the csv-extractor sample by
1. No state store
2. No Prometheus metrics
3. Added extraction pipeline in config
4. The config file path can be passed into extractor.main() so it can be retrieved from the data JSON when running as a Cognite Function

## Running

First, you will need `poetry` installed. You should then be able to run

```
poetry run csv_extractor_simple
```

from the command line.

To run the extractor with the provided example config, start by setting the
following environment variables:

 * `COGNITE_PROJECT`
 * `COGNITE_TOKEN_URL`
 * `COGNITE_CLIENT_ID`
 * `COGNITE_CLIENT_SECRET`
 * `COGNITE_BASE_URL` (can be omitted if your project is hosted at
   `https://api.cognitedata.com`)
 * `EXTRACTION_PIPELINE_EXT_ID`

This can be done by creating a .env file in the `csv-extractor-simple` directory with the needed environment variables.

To add more files to upload, add items to the `files` list in
`example_config.yaml`, as such:

``` yaml
files:
  - path: example.csv
    key-column: Title
    destination:
        database: my_database
        table: movies

  - path: path/to/myfile.csv
    key-column: key_column
    destination:
        database: my_database
        table: another_table

  ...
```
