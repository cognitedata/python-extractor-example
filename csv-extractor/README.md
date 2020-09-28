CSV Extractor
=============

This sample extractor reads CSV files and uploads the content to a configurable
database and table in CDF RAW.


## Running locally

First, you will ned the Cognite Extracor Utilities installed. They are available
as `cognite-extractor-utils` from PyPI. To install them globally, run

```
pip install [--user] cognite-extractor-utils
```

from the command line (include the `--user` flag to install for active user
only, which is sometimes preferable).

To run the extractor with the provided example config, start by setting the
following environment variables:

 * `COGNITE_PROJECT`
 * `COGNITE_API_KEY`
 * `COGNITE_BASE_URL` (can be omitted if your project is hosted at
   `https://api.cognitedata.com`)

Then run the extractor with the config as argument:

```
python main.py example_config.yaml
```

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
