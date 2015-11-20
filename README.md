GalacticusIngest
===============

This code uses the DBIngestor library (https://github.com/aipescience/DBIngestor) to ingest 
Galacticus catalogues into a database.

These catalogues come in the HDF5 format, so the hdf5-c++ libraries are used for reading 
the data and need to be installed. 

At the moment, the path to the data is very hard-coded, as well as some specific things.
Nonetheless it should be fairly straightforward to adjust it to other HDF5 data files as well.

For any questions, please contact me at
Kristin Riebe, kriebe@aip.de


Data files
-----------
The HDF5 data file structured as follows:
...


Features
---------
Byteswapping is automatically taken care of by the HDF5-library.
You can provide a map-file to map fields from the data file to database columns. 
The format is:

(see readMappingFile function in SchemaMapper.cpp)

Installation
--------------
see INSTALL


Example
--------
An example will be given at some point ...


TODO
-----
... - 


