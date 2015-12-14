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
There are a number of results file, each one of them is in HDF5-format and is structured as follows:  

* `Outputs` group:  
    - contains metadata like scale and time for each subgroup  
    - contains subgroups for each snapshot in time called Output1, Output2 etc.  
* `Outputs/Output*` subgroups: contain the data in `nodeData`  
* `Outputs/Output*/nodeData`: contains the actual data in form of arrays for each column. The column names roughly correspond to the names in the database table for most columns. Some columns are ignored for the database, though. 


Features
---------
Byteswapping is automatically taken care of by the HDF5-library.
Provide a map-file to map fields from the data file to database columns. 
The format is:  
`name_in_file`  `datatype_in_file`  `name_in_DB`  `datatype_in_DB`

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
* Make data path for HDF5-file variable (user input?)
* Allow usage of startRow, maxRows to start reading at an arbitrary row
* Use asserters


