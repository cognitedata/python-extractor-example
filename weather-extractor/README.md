Weather Extractor
=================

This sample extractor queries the publicly available Frost API from The
Norwegian Meteorological Institute for observational data on e.g. temperature,
air pressure or wind speed on a configured set of observational stations.

To use the Frost API, you need to get credentials for it. It a very straight
foreward processs, follow the instructions
[here](https://frost.met.no/auth/requestCredentials.html). You will only need
the client ID for this extractor.

The data from the Frost APIs comes from **The Norwegian Meteorological
Institute**, and is licensed under Norwegian license for public data (NLOD) and
[Creative Commons 4.0](http://creativecommons.org/licenses/by/4.0/).


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
 * `FROST_CLIENT_ID`

Then run the extractor with the config as argument:

```
python main.py example_config.yaml
```
