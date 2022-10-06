Weather Extractor
=================

This sample extractor queries the [Ice Cream Factory API](https://ice-cream-factory.inso-internal.cognite.ai/docs#)
for timeseries datapoints on a configured set of sites. The datapoints are extracted directly to CDF clean. 
If timeseries don't exist in CDF, they will be created in the dataset provided in config. 

## Running locally
First, you will need `poetry` installed. You should then be able to run

```
poetry install
poetry run rest_extractor
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
