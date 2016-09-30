GalacticusIngest
================

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
The *Example* directory contains:

* *create_galacticus_test_mysql.sql*: example create table statement  
* *galacticus_test.fieldmap*: example map file for mapping data file fields to database fields  
* *galacticus_test_results.hdf5*, an example data file, extracted from a Galacticus HDF5 output. It contains only data for output numbers 70 - 79, corresponding to snapshot numbers 116 - 125  

First a database and table must be created on your server (in the example, I use MySQL, adjust to your own needs). Then you can ingest the example data into the Galacticus_test table with a command line like this: 

```
build/GalacticusIngest.x  -s mysql -D TestDB -T Galacticus_test -U myusername -P mypassword -H 127.0.0.1 -O 3306 -f Example/galacticus_test.fieldmap  --fileNum=0 Example/galacticus_test_results.hdf5 --snapnums 116 117
```

Replace *myusername* and *mypassword* with your own credentials for your own database. 

The important new options are:   

`-f`: filename for field map  
`--fileNum`: an integer as file number, for easier check if data was uploaded from all files and number of rows are correct  
`--snapnums` [optional]: a list of snapshot numbers, for which data is to be inserted. the list is separated by whitespace, so please do not put it before the data file (positional argument), but rather at the end, as given in the example above. Note that the mapping between snapshot numbers and output numbers is still hard-coded for now.  


TODO
-----
* Read mapping between snapnums and output-numbers from file (remove hard-coded mapping)
* Make mapping file optional, use internal names and datatypes as default
* Read constant values as well (for phkeys)
* Allow calculations on the fly (ix, iy, iz)
* Maybe use same format as structure files of AsciiIngest
* Make data path for HDF5-file variable (user input?)
* Allow usage of startRow, maxRows to start reading at an arbitrary row
* Use asserters


