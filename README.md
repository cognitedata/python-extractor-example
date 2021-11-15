Sample Extractors using the Python Extractor Util Library
=========================================================

This repository contains two example extractors using the [Cognite Python Extractor Utils
libary](https://github.com/cognitedata/python-extractor-utils):

 * A [CSV extractor](./csv-extractor) reading files on the CSV format and uploading the content to
   CDF RAW
 * A [weather data extractor](./weather-extractor) reading observational data from the The Norwegian
   Meteorological Institute and uploading the data as time series in CDF
 * A sample [Azure IOT Hub extractor](./azure-iot-hub-extractor) polling from Azure IOT Hub and pushing datapoints to CDF

See the READMEs in the subfolders for more details on each extractor.
