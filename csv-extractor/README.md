CSV Extractor
=============

This sample extractor reads CSV files and uploads the content to a configurable
database and table in CDF RAW.


## Running

First, you will need `poetry` installed. You should then be able to run

```
poetry run csv_extractor <config file>
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
