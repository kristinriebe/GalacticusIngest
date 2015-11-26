/*  
 *  Copyright (c) 2015, Kristin Riebe <kriebe@aip.de>,
 *                      eScience team AIP Potsdam
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  See the NOTICE file distributed with this work for additional
 *  information regarding copyright ownership. You may obtain a copy
 *  of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

#include <iostream>
#include <stdio.h>
#include <stdlib.h>
#include <math.h>   // sqrt, pow
#include "galacticusingest_error.h"
#include <list>
//#include <boost/filesystem.hpp>
//#include <boost/serialization/string.hpp> // needed on erebos for conversion from boost-path to string()
#include <boost/regex.hpp> // for string regex match/replace to remove redshift from dataSetNames

#include "Galacticus_Reader.h"

//using namespace boost::filesystem;

//#include <boost/chrono.hpp>
#include <cmath>


namespace Galacticus {
    GalacticusReader::GalacticusReader() {
        fp = NULL;

        //counter = 0;
        currRow = 0;
    }
    
    GalacticusReader::GalacticusReader(std::string newFileName, int newJobNum, int newFileNum, int newSnapnum, int newStartRow, int newMaxRows) {          
        // this->box = box;     
        snapnum = newSnapnum;
        startRow = newStartRow;
        maxRows = newMaxRows;

        fileName = newFileName;
        
        // strip path from file name
        //boost::filesystem::path p(fileName);
        //dataFileBaseName = p.filename().string(); // or use stem() for omitting extension
        
        // get job number and file number from file name?
        // for this dataset, files are named like this: job0/the_trees_0_1000000_0_results.hdf5
        // and job1/the_trees_1000000_2000000_9_results.hdf5
        // => jobNum: strip "the_trees_", then everything beyond "_"; 
        // => fileNum: strip _results.hdf5, everthing before "_"
        // Welll ... just let the user provide these numbers ...

        jobNum = newJobNum;
        fileNum = newFileNum;

        currRow = 0;
        
        //numBytesPerRow = 2*sizeof(int)+28*sizeof(float)+6*sizeof(int)+2*sizeof(int); // 36 values in total; + two fortran-binary-specific intermediate numbers
        
        fp = NULL;

        counter = 0;
        countInBlock = 0;   // counts values in each datablock (output)

        //const H5std_string FILE_NAME( "SDS.h5" );
        openFile(newFileName);
        //offsetFileStream();

        // read expansion factors
        expansionFactors = getOutputsMeta(numOutputs);

/*        cout << "numOutputs: " << numOutputs << endl;
        cout << "Scale factors read from 'outputExpansionFactor' attribute for each Output*-group: " << endl;
        for (int i=1; i<=numOutputs; i++) {
            cout << "i = " << i << ", a = " << expansionFactors[i] << endl;
        }
        exit(0);
*/        
    }
    
    
    GalacticusReader::~GalacticusReader() {
        closeFile();
        //delete expansionFactors();
        // delete data sets? i.e. call DataBlock::deleteData?
    }
    
    void GalacticusReader::openFile(string newFileName) {
        // open file as hdf5-file
        H5std_string h5fileName;
        h5fileName = (H5std_string) newFileName;

        if (fp) {
            fp->close();
            delete fp;
        }

        fp = new H5File(h5fileName, H5F_ACC_RDONLY); // allocates properly
        
        if (!fp) { 
            GalacticusIngest_error("GalacticusReader: Error in opening file.\n");
        }
        
    }
    
    void GalacticusReader::closeFile() {
        if (fp) {
            fp->close();
            delete fp;
        }
        fp = NULL;
    }

    double* GalacticusReader::getOutputsMeta(long &numOutputs) {
        char line[1000];
        double aexp;

        std::string s("Outputs");


        Group group(fp->openGroup(s));
        hsize_t size = group.getNumObjs();

        // should close the group now. How to do this?? just fp->closeGroup()?? or group.close()??
        group.close();

        //cout << "Number of outputs stored in this file is " << size << endl;
        double *expansionFactors = new double[size+1];

        for (int i=1; i<=size; i++) {
            sprintf(line, "Outputs/Output%d", i);
            Group group(fp->openGroup(line));
            Attribute att = group.openAttribute("outputExpansionFactor");

            // check type
            H5T_class_t type_class = att.getTypeClass();
            if (type_class != H5T_FLOAT) {
                cout << "Float attribute does not have correct type!" << endl;
                abort();
            }
            // check byte order
            FloatType intype = att.getFloatType();
            H5std_string order_string;
            H5T_order_t order = intype.getOrder(order_string);
            // only print order_string, if not little endian
            //if (order != 0) {
            //    cout << order_string << endl;
            //}

            // check again data sizes
            if (sizeof(double) != intype.getSize()) {
                cout << "Mismatch of double data type." << endl;
                abort();
            }

            // read the attribute
            att.read(intype, &aexp);

            // close the group
            group.close();

            // add to array
            expansionFactors[i] = aexp;
        }

        numOutputs = size;

        return expansionFactors;

    }

    long GalacticusReader::getNumRowsInDataSet(std::string s) {
        // get number of rows (data entries) in given dataset
        // just check with the one given dataset and assume that all datasets
        // have the same size!
        //sprintf(outputname, "Outputs/Output%d/nodeData", ioutput);
        long nvalues;

        DataSet *d = new DataSet(fp->openDataSet(s));
        DataSpace dataspace = d->getSpace();

        hsize_t dims_out[1];
        int ndims = dataspace.getSimpleExtentDims(dims_out, NULL);
        //cout << "dimension " << (unsigned long)(dims_out[0]) << endl;
        nvalues = dims_out[0];

        delete d;

        return nvalues;

    }
    
/*    void GalacticusReader::offsetFileStream() {
        int irowda        streamoff ipos;
        
        assert(fileStream.is_open());
        
        // position pointer at beginning of the row where ingestion should start
        //numBytesPerRow = 2*sizeof(int)+28*sizeof(float)+6*sizeof(int)+2*sizeof(int); // 36 values in total;  
        ipos = (streamoff) ( (long) startRow * (long) numBytesPerRow );
        fileStream.seekg(ipos, ios::beg);
        
        // update currow
        currRow = startRow;
        
        cout<<"offset currRow: "<<currRow<<endl;
    }
*/            

    vector<string> GalacticusReader::getDataSetNames() {
        return dataSetNames;

    }

    int GalacticusReader::getNextRow() {
        //assert(fileStream.is_open());

        DataBlock b;
        string name;

        // get one line from already read datasets (using readNextBlock)

        // cout << endl << "row count in this block " << countInBlock << ":" << endl;
        //cout << endl << "nvalues " << nvalues << endl;

        if (counter == 0) {
            ioutput = 1;
            nvalues = readNextBlock(ioutput);
            countInBlock = 0;
        } else if (countInBlock == nvalues-1) {
            // end of data block/start of new one is reached!
            // => read next datablocks (for next output number)
            //cout << "Read next datablock!" << endl; 
            ioutput = ioutput + 1;
            nvalues = readNextBlock(ioutput);
            countInBlock = 0;
        } else {
            countInBlock++;
        }

        // STOP WHEN TESTING
        /*if (ioutput == 5) {
            cout << "Stopping for now." << endl;
            abort();
        }*/

        currRow++;
        counter++;

        // stop reading/ingesting, if number of particles (lkl) gets smaller than 20
        
        // stop after reading maxRows, but only if it is not -1
        // Note: Could this be accelerated? It's unnecessary most of the time,
        // but now it is evaluated for each row ...
//        if (maxRows != -1) {
//            if (currRow-startRow > maxRows) {
//                printf("Maximum number of rows to be ingested is reached (%d).\n", maxRows);
//                return 0;
//            }
//        }
    
        return 1;       
    }

    int GalacticusReader::readNextBlock(int ioutput) {
        // read one complete Output* block from Galacticus HDF5-file
        // should fit into memory ... if not, need to adjust this
        // and provide the number of values to be read each time

        long nvalues;
        char outputname[1000];

        //char ds[1000];

        // first get names of all DataSets in nodeData group and their item size

        if (ioutput >= numOutputs) {
            cout << "ioutput too large!" << endl;
            abort(); 
        }

        sprintf(outputname, "Outputs/Output%d/nodeData", ioutput);
        cout << "ouputname: " << outputname<< endl;

        //sprintf(ds, "%s/basicMass", outputname);
        //nvalues = getNumRowsInDataSet(ds);
        //cout << "nvalues: " << nvalues << endl;

        Group group(fp->openGroup(outputname));

        hsize_t len = group.getNumObjs();
        //cout << "number of output groups: " << len << endl; // works!!


        //cout << "Iterating over Datasets in the group ... " << endl;
        //H5L_iterate_t
        //vector<string> dsnames;
        dataSetNames.clear(); // actually, the names should be exactly the same as for the group before!! --> check this???
        int idx2  = H5Literate(group.getId(), H5_INDEX_NAME, H5_ITER_INC, NULL, file_info, &dataSetNames);
        
        // cleanup the dataSetNames
        // i.e. remove redshift, if it is included in the name, 
        // e.g. spheroidLuminositiesStellar:SDSS_g:observed:z6.0000
        // or spheroidLuminositiesStellar:SDSS_g:observed:z6.0000:dustAtlas
        //size_t found = str.find(str2);
        //if (found!=std::string::npos)
        //std::cout << "first 'needle' found at: " << found << '\n';
        //char *piece = NULL;
        //char namechar[1024] = "";
        
        /*dataSetMatchNames.clear();
        dataSetMatchNames = dataSetNames; 
        for (int m=0; m<dataSetNames.size(); m++) {
            //redshift = 
            //size_t found = dataSetNames[m].find(":z");
            // if (found!=std::string::npos) cout << found << endl;

            string newtext = "";
            //boost::regex re(":z[:digit:]+\.[:digit:]*");
            boost::regex re(":z[0-9.]*");
            //cout << "before: " << dataSetNames[m] << endl;

            string result = boost::regex_replace(dataSetNames[m], re, newtext);
            dataSetMatchNames[m] = result;
            // should store it as key-value for easy access to datasets by match name!
            //cout << "after:  " << dataSetMatchNames[m] << endl;
        }
        */

        // print some of the field names:
        //for (int m=0; m<dataSetNames.size(); m++) {
        //    cout << dataSetNames[m] << endl;
        //}
        //delete group;


        // read each desired data set, use corresponding read routine for different types
        string dsname;

        //dataSetNames.push_back("blackHoleCount");
        //dataSetNames.push_back("inclination");

        string s;

        int numDataSets = dataSetNames.size();
        //cout << "numDataSets: " << numDataSets << endl;
        
        // clear datablocks from previous block, before reading new ones:
        datablocks.clear();

        for (int k=0; k<numDataSets; k++) {

            //cout << "k, numDataSets: " << k << ", " << numDataSets << endl;

            dsname = dataSetNames[k];
            s = string(outputname) + string("/") + dsname;
            //cout << "dsname: " << dsname << endl;

            DataSet *dptr = new DataSet(fp->openDataSet(s));
            DataSet dataset = *dptr; // for convenience

            // check class type
            H5T_class_t type_class = dataset.getTypeClass();
            if (type_class == H5T_INTEGER) {
                //cout << "DataSet has long type!" << endl;
                long *data = readLongDataSet(s, nvalues);
            } else if (type_class == H5T_FLOAT) {
                //cout << "DataSet has double type!" << endl;
                double *data2 = readDoubleDataSet(s, nvalues);
            }
            //cout << nvalues << " values read." << endl;
        }
        // How to proceed from here onwards??
        // Could read all data into data[0] to data[104] or so,
        // but I need to keep the information which is which!
        // Alternatively create one big structure to hold it all?

        // => use a small class that contains
        // 1) name of dataset
        // 2) array of values, number of values
        // use vector<newclass> to create a vector of these datasets.
        // maybe can use datasets themselves, so no need to define own class?
        // => assigning to the new class has already happened now inside the read-class.

        return nvalues; //assume that nvalies os the same for each dataset (datablock) inside one Output-group (same redshift)

    }


    long* GalacticusReader::readLongDataSet(const std::string s, long &nvalues) {
        // read a long-type dataset
        //std::string s2("Outputs/Output79/nodeData/blackHoleCount");
        // DataSet dataset = fp->openDataSet(s);
        // rather need pointer to dataset in order to delete it later on:

        //cout << "Reading DataSet '" << s << "'" << endl;

        DataSet *dptr = new DataSet(fp->openDataSet(s)); // need pointer because of "new ..."
        DataSet dataset = *dptr; // for convenience

        // check class type
        H5T_class_t type_class = dataset.getTypeClass();
        if (type_class != H5T_INTEGER) {
            cout << "Data does not have long type!" << endl;
            abort();
        }
        // check byte order
        IntType intype = dataset.getIntType();
        H5std_string order_string;
        H5T_order_t order = intype.getOrder(order_string);
        //cout << order_string << endl;

        // check again data sizes
        if (sizeof(long) != intype.getSize()) {
            cout << "Mismatch of long data type." << endl;
            abort();
        }

        size_t dsize = intype.getSize();
        //cout << "Data size is " << dsize << endl;


        // get dataspace of the dataset (the array length or so)
        DataSpace dataspace = dataset.getSpace();
        ////hid_t dataspace = H5Dget_space(dataset); --> this does not work!! At least not with dataset defined as above!

        // get number of dimensions in dataspace
        int rank = dataspace.getSimpleExtentNdims();
        //cout << "Dataspace rank is " << rank << endl;
        // I expect this to be 1 for all Galacticus datasets!
        // There are no 2 (or more) dimensional arrays stored in one dataset, are there?
        if (rank > 1) {
            cout << "ERROR: Cannot cope with multi-dimensional datasets!" << endl;
            abort();
        }

        hsize_t dims_out[1];
        int ndims = dataspace.getSimpleExtentDims(dims_out, NULL);
        //cout << "dimension " << (unsigned long)(dims_out[0]) << endl;
        nvalues = dims_out[0];

        // alternative way of determining data size (needed for buffer memory allocation!)
        //size_t size = dataset.getInMemDataSize();
        //cout << size << endl;
        //int nvalues = size/sizeof(long);

        // read data
        long *buffer = new long[nvalues]; // = same as malloc
        dataset.read(buffer,PredType::NATIVE_LONG);

        // the data is stored in buffer now, so we can delete the dataset;
        // to do this, call delete on the pointer to the dataset
        dataset.close();
        // delete dataset is not necessary, if it is a variable on the heap.
        // Then it is removed automatically when the function ends.
        delete dptr;

        //std::vector<int> data_out(NX);
        //H5Dread(dataset, H5T_NATIVE_INT, H5S_ALL, H5S_ALL, H5P_DEFAULT, &data_out[0]);
        // --> this did not work, do not know why.
        //cout << "status: " << status << endl;
        //int data_out2[dims_out[0]];
        //dataset.read(data_out, PredType::NATIVE_LONG, memspace, filespace);
        // --> this caused problems with incompatible memspace and filespace etc.

        /*cout << "First values: ";
        for (int j = 0; j < 10; j++) {
            cout << buffer[j] << " ";
        }
        cout << endl;
        */

        DataBlock b;
        b.nvalues = nvalues;
        b.longval = buffer;
        b.name = s;
        datablocks.push_back(b);
        // b is added to datablocks-vector now


        return buffer;
    }


    double* GalacticusReader::readDoubleDataSet(const std::string s, long &nvalues) {
        // read a double-type dataset
        //std::string s2("Outputs/Output79/nodeData/blackHoleCount");
        // DataSet dataset = fp->openDataSet(s);
        // rather need pointer to dataset in order to delete it later on:

        //cout << "Reading DataSet '" << s << "'" << endl;

        DataSet *dptr = new DataSet(fp->openDataSet(s));
        DataSet dataset = *dptr; // for convenience

        // check class type
        H5T_class_t type_class = dataset.getTypeClass();
        if (type_class != H5T_FLOAT) {
            cout << "Data does not have double type!" << endl;
            abort();
        }
        // check byte order
        FloatType intype = dataset.getFloatType();
        H5std_string order_string;
        H5T_order_t order = intype.getOrder(order_string);
        //cout << order_string << endl;

        // check again data sizes
        if (sizeof(double) != intype.getSize()) {
            cout << "Mismatch of double data type." << endl;
            abort();
        }

        size_t dsize = intype.getSize();
        //cout << "Data size is " << dsize << endl;

        // get dataspace of the dataset (the array length or so)
        DataSpace dataspace = dataset.getSpace();
        //hid_t dataspace = H5Dget_space(dataset); --> this does not work!! At least not with dataset defined as above!

        // get number of dimensions in dataspace
        int rank = dataspace.getSimpleExtentNdims();
        //cout << "Dataspace rank is " << rank << endl;
        // I expect this to be 1 for all Galacticus datasets!
        // There are no 2 (or more) dimensional arrays stored in one dataset, are there?
        if (rank > 1) {
            cout << "ERROR: Cannot cope with multi-dimensional datasets!" << endl;
            abort();
        }

        hsize_t dims_out[1];
        int ndims = dataspace.getSimpleExtentDims(dims_out, NULL);
        //cout << "dimension " << (unsigned long)(dims_out[0]) << endl;
        nvalues = dims_out[0];

        // read data
        double *buffer = new double[nvalues];
        dataset.read(buffer,PredType::NATIVE_DOUBLE);

        // the data is stored in buffer now, so we can delete the dataset;
        // to do this, call delete on the pointer to the dataset
        dataset.close();
        delete dptr;

        /*cout << "First values: ";
        for (int j = 0; j < 10; j++) {
            cout << buffer[j] << " ";
        }
        cout << endl;
        */

        DataBlock b;
        b.nvalues = nvalues;
        b.doubleval = buffer;
        b.name = s;
        datablocks.push_back(b);


        return buffer;
    }

    bool GalacticusReader::getItemInRow(DBDataSchema::DataObjDesc * thisItem, bool applyAsserters, bool applyConverters, void* result) {
        //reroute constant items:
        if(thisItem->getIsConstItem() == true) {
            getConstItem(thisItem, result);
        } else if (thisItem->getIsHeaderItem() == true) {
            printf("We never told you to read headers...\n");
            exit(EXIT_FAILURE);
        } else {
            getDataItem(thisItem, result);
        }
        //cout << " again ioutput: " << ioutput << endl;
        //check assertions
        //checkAssertions(thisItem, result);
        
        //apply conversion
        //applyConversions(thisItem, result);
        
        /*cout << "Name, value: " << thisItem->getDataObjName() << ": " << *(double*) result << ", long: " << *(long*) result << endl;
        if (thisItem->getDataObjName() == "dataFile") {
            cout << "Name, value: " << thisItem->getDataObjName() << ": " << *(char**) result << endl;
            //abort();
        } */   

        return 0;
    }

    bool GalacticusReader::getDataItem(DBDataSchema::DataObjDesc * thisItem, void* result) {

        //check if this is "Col1" etc. and if yes, assign corresponding value
        //the variables are declared already in Galacticus_Reader.h 
        //and the values were read in getNextRow()
        bool isNull;
        //cout << " again2 ioutput: " << ioutput << endl;
        //NInFile = currRow-1;	// start counting rows with 0       
        
        // go through all data items and assign the correct column numbers for the DB columns, 
        // maybe depending on a read schema-file:
        
        //cout << "dataobjname: " << thisItem->getDataObjName() << endl;

        isNull = false;

        DataBlock b;
        string name;
        
        string newtext = "";
        boost::regex re(":z[0-9.]*");
            
        // loop through all fields for one row
        for (int j=0; j<datablocks.size(); j++) {
            
            b = datablocks[j]; // block for current column (or field), it's all of the same type
            if (countInBlock >= b.nvalues) {
                cout << "countInBlock is too large! (" << countInBlock << " >= " << b.nvalues << ")" << endl;
                abort();
            }

            //cout << "sizes in b: " << b.longval.size() << ", " << b.doubleval.size() << endl;
            // strip output etc. from name, i.e. use only 
            // last part of the name-string, after last '/'
            //boost::chrono::system_clock::time_point start = boost::chrono::system_clock::now();
            
            size_t found = b.name.find_last_of("/");
            name = b.name.substr(found+1);
            
            //boost::chrono::duration<double> sec = boost::chrono::system_clock::now() - start;
            
            // use name with redshift removed:            
            string matchname = boost::regex_replace(name, re, newtext);
            name = matchname;
            //name = dataSetMatchNames[j];

            //boost::chrono::duration<double> sec2 = boost::chrono::system_clock::now() - start;
            
            //cout << "1 took " << sec.count() << " seconds\n";
            //cout << "1+2 took " << sec2.count() << " seconds\n";
    

            if (thisItem->getDataObjName().compare(name) == 0) {
                if (b.longval) {

                    //if (thisItem->getColumnDBType() == DBT_SMALLINT) {
                    //    cout << "Found a small int!!!" << endl;
                    //    abort();
                    //}

                    *(long*)(result) = b.longval[countInBlock];
                    return isNull;
                } else if (b.doubleval) {
                     *(double*)(result) = b.doubleval[countInBlock];
                    return isNull;
                } else {
                    cout << "No corresponding data found!" << endl;
                    abort();
                }
            }
        }
            
        // get snapshot number and expansion factor from current Output number 
        //cout << "ioutput: " << ioutput << endl;
        if (thisItem->getDataObjName().compare("snapnum") == 0) {
            //cout << "ioutput: " << ioutput << endl;
            *(int*)(result) = ioutput;
            return isNull;
        }

        if (thisItem->getDataObjName().compare("scale") == 0) {
            *(double*)(result) = expansionFactors[ioutput];
            return isNull;
        }

        if (thisItem->getDataObjName().compare("NInFileSnapnum") == 0) {
            *(long*)(result) = countInBlock;
            //result = (void *) countInBlock;
            return isNull;
        }
        
        if (thisItem->getDataObjName().compare("dataFile") == 0) {

            //*(char**)(result) = dataFileBaseName; 
            // If I do it this way (with string to char conversion already done in constructor), 
            // then the free() at some later step fails (in DBIngestor, Converter.cpp, free(result)?).
            // Thus allocate memory here and copy (as done in HelloWorld-example):
            
            //printf("datafile!\n");
            char *charArray = (char*) malloc(dataFileBaseName.size()+1);
            strcpy(charArray, dataFileBaseName.c_str());
            //*(char**)(result) = charArray;

            // still have problems with segmentation faults!

            return isNull;
        }

        if (thisItem->getDataObjName().compare("jobNum") == 0) {
            *(int*) result = jobNum;
            return isNull;
        }

        if (thisItem->getDataObjName().compare("fileNum") == 0) {
            *(int*) result = fileNum;
            return isNull;
        }

        
        /* if (thisItem->getDataObjName().compare("ix") == 0) {
            *(long*)(result) = countInBlock;
            return isNull;
        } */ // --> do it on the database side; 
            // or: put current x, y, z in global reader variables, 
            // could calculate ix, iy, iz here, but can only do this AFTER x,y,z 
            // were assigned!  => i.e. would need to check in generated schema 
            // that it is in correct order




        // if we still did not return ...
        fflush(stdout);
        fflush(stderr);
        printf("\nERROR: Something went wrong... (no dataItem for schemaItem %s found)\n", thisItem->getDataObjName().c_str());
        exit(EXIT_FAILURE);
        
        return isNull;
    }

    void GalacticusReader::getConstItem(DBDataSchema::DataObjDesc * thisItem, void* result) {
        memcpy(result, thisItem->getConstData(), DBDataSchema::getByteLenOfDType(thisItem->getDataObjDType()));
    }

    void GalacticusReader::setCurrRow(long n) {
        currRow = n;
        return;
    }

    long GalacticusReader::getCurrRow() {
        return currRow;
    }

    long GalacticusReader::getNumOutputs() {
        return numOutputs;
    }


    DataBlock::DataBlock() {
        nvalues = 0;
        name = "";
        idx = -1;
        doubleval = NULL;
        longval = NULL;
        type = "unknown";
    };

    /* // copy constructor, probably needed for vectors? -- works better without, got strange error messages when using this and trying to use push_back
    DataBlock::DataBlock(DataBlock &source) {
        nvalues = source.nvalues;
        name = source.name;
        idx = source.idx;
        doubleval = source.doubleval;
        longval = source.longval;
        type = source.type;
    }
    */

    void DataBlock::deleteData() {
        if (longval) {
            delete longval;
            nvalues = 0;
        }
        if (doubleval) {
            delete doubleval;
            nvalues = 0;
        }
    }

}


// Here comes a call back function, thus it lives outside of the Galacticus-Reader class
// operator function, must reside outside of Galacticus-class, because it's extern C?
herr_t file_info(hid_t loc_id, const char *name, const H5L_info_t *linfo, void *opdata) {
    // display name of group/dataset
    //cout << "Name : " << name << endl;
    //cout << name << endl;

    // store name in string vector, pointer to this is provided as parameter
    std::vector<string>* n = (std::vector<string>*) opdata;
    n->push_back(name);

    return 0;
}
