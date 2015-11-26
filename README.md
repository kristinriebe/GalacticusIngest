GalacticusIngest
===============

**NOTE: Still alpha/beta version, there's no guarantee at all that it works!**

This code uses the DBIngestor library (https://github.com/aipescience/DBIngestor) to ingest Galacticus catalogues into a database.

These catalogues come in the HDF5 format, so the hdf5-c++ libraries are used for reading the data and need to have been installed. 

At the moment, the path to the data (inside the file) is very hard-coded, as well as some other specific things. Nonetheless it should be fairly straightforward to adjust it to other HDF5 data files as well.

For any questions, please contact me at
Kristin Riebe, kriebe@aip.de


Data files
-----------
The HDF5 data files are structured as follows:  
...  
[see README in data directory on erebos]


Features
---------
Byteswapping is automatically taken care of by the HDF5-library.
Provide a map-file to map fields from the data file to database columns. 
The format is:  
name_in_file  datatype_in_file  name_in_DB  datatype_in_DB

(see readMappingFile function in SchemaMapper.cpp)

Installation
--------------
see INSTALL


Example
--------
An example will be given at some point ...


TODO
-----
* Make mapping file optional, use internal names and datatypes as default
* Read constant values as well (for phkeys)
* Allow calculations on the fly (ix, iy, iz)
* Maybe use same format as structure files of AsciiIngest
* Optimize getDataItem (avoid loop through all data names each time?)
* Use snapnum as argument to ingest only data for a certain snapshot number (output number)
* Make data path for HDF5-file variable (user input?)
* Allow usage of startRow, maxRows to start reading at an arbitrary row
* Use asserters


