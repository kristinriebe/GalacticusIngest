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
//#include <cmath>
#include <boost/date_time/posix_time/posix_time.hpp>


namespace Galacticus {
    GalacticusReader::GalacticusReader() {
        fp = NULL;

        currRow = 0;
    }
    
    GalacticusReader::GalacticusReader(std::string newFileName, int newJobNum, int newFileNum, int newSnapnum, int newStartRow, int newMaxRows) {          
        user_snapnum = newSnapnum;
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

        
        //numBytesPerRow = 2*sizeof(int)+28*sizeof(float)+6*sizeof(int)+2*sizeof(int); // 36 values in total; + two fortran-binary-specific intermediate numbers
        
        fp = NULL;

        currRow = 0;
        countInBlock = 0;   // counts values in each datablock (output)

        //const H5std_string FILE_NAME( "SDS.h5" );
        openFile(newFileName);
        //offsetFileStream();

        // read expansion factors, output names etc.
        getOutputsMeta(numOutputs);

        // initialize iterator for meta data (output names)
        it_outputmap = outputMetaMap.begin();

    }
    
    
    GalacticusReader::~GalacticusReader() {
        closeFile();
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


    int GalacticusReader::getSnapnum(long ioutput) {
        // map output-index from the file to the snapshot numbers used in the underlying Rockstar-catalogues
        // 
        int snapnum;

        if (ioutput == 1) {
            snapnum = 26;
        } else if (ioutput == 2) {
            snapnum = 31;
        } else if (ioutput == 3) {
            snapnum = 37;
        } else if (ioutput == 4) {
            snapnum = 39;
        } else if (ioutput >= 5 && ioutput <= 79) {
            snapnum = ioutput + 46;
        } else {
            printf("ERROR: No matching snapshot number for output %ld available.\n", ioutput);
            exit(0);
        }

        return snapnum;

    }

    void GalacticusReader::getOutputsMeta(long &numOutputs) {
        char line[1000];
        double aexp;
        int snapnum;

        string s("Outputs");
        Group group(fp->openGroup(s));
        hsize_t size = group.getNumObjs();
        cout << "Number of outputs stored in this file is " << size << endl;

        outputNames.clear();
        int idx2  = H5Literate(group.getId(), H5_INDEX_NAME, H5_ITER_INC, NULL, file_info, &outputNames);

        // should close the group now
        group.close();

        OutputMeta o;
        for (int i=0; i<size; i++) {
            string sg = s + string("/") + outputNames[i];
            Group group(fp->openGroup(sg));
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

            // check again data sizes
            if (sizeof(double) != intype.getSize()) {
                cout << "Mismatch of double data type." << endl;
                abort();
            }

            // read the attribute
            att.read(intype, &aexp);

            // assign values
            o.outputName = s + string("/") + outputNames[i] + string("/") + string("nodeData"); // complete name to access the data block
            o.outputExpansionFactor = (float) aexp;

            // get snapshot number = number at the end of "Output%d"
            string prefix = "Output"; // Is this always the case??? Could also search for a number using boost regex
            string numstr = outputNames[i].substr(prefix.length(),outputNames[i].length());
            o.ioutput = (int) atoi(numstr.c_str());
            snapnum = getSnapnum(o.ioutput);
            o.snapnum = snapnum;

            // store in map
            outputMetaMap[snapnum] = o;

            // close the group
            group.close();

        }

        numOutputs = size;
        return;
    }

    long GalacticusReader::getNumRowsInDataSet(string s) {
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


    vector<string> GalacticusReader::getDataSetNames() {
        return dataSetNames;
    }


    int GalacticusReader::getNextRow() {
        //assert(fileStream.is_open());

        DataBlock b;
        string name;
        int current_snapnum;
        string outputName;

        // get one line from already read datasets (using readNextBlock)
        // use readNextBlock to read the next block of datasets if necessary
        if (currRow == 0) {
            // we are at the very beginning
            // read block for given snapnum or start reading from 1. block
            if (user_snapnum != -1) {
                current_snapnum = user_snapnum;
                numOutputs = 1;
            } else {
                it_outputmap = outputMetaMap.begin(); // just to be on the save side, actually already done at constructor
                current_snapnum = it_outputmap->first;
            }

            // get corresponding output name
            outputName = outputMetaMap[current_snapnum].outputName;

            nvalues = readNextBlock(outputName);
            countInBlock = 0;

        } else if (countInBlock == nvalues-1) {
            // end of data block/start of new one is reached!
            // => read next datablocks (for next output number)
            //cout << "Read next datablock!" << endl;

            if (numOutputs == 1) {
                // we are done already
                return 0;
            }

            // increase iteratoru over output-metadata (names)
            it_outputmap++;

            // check if we are at the end
            if (it_outputmap == outputMetaMap.end()) {
                return 0;
            }

            // if we end up here, we should read the next block
            current_snapnum = it_outputmap->first;
            outputName = (it_outputmap->second).outputName;
            nvalues = readNextBlock(outputName);
            countInBlock = 0;

        } else {
            countInBlock++;
        }

        currRow++; // counts all rows

        // stop reading/ingesting, if mass is lower than threshold?
        
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

    int GalacticusReader::readNextBlock(string outputName) {
        // read one complete Output* block from Galacticus HDF5-file
        // should fit into memory ... if not, need to adjust this
        // and provide the number of values to be read each time

        long nvalues;
        //char outputname[1000];

        //performance output stuff
        boost::posix_time::ptime startTime;
        boost::posix_time::ptime endTime;
        string newtext = "";
        boost::regex re(":z[0-9.]*");
        

        startTime = boost::posix_time::microsec_clock::universal_time();

        // first get names of all DataSets in nodeData group and their item size
        cout << "outputName: " << outputName<< endl;

        Group group(fp->openGroup(outputName));
        // maybe check here that it worked?

        hsize_t len = group.getNumObjs();

        //cout << "Iterating over Datasets in the group ... " << endl;
        //H5L_iterate_t
        //vector<string> dsnames;
        dataSetNames.clear(); // actually, the names should be exactly the same as for the group before!! --> check this???
        int idx2  = H5Literate(group.getId(), H5_INDEX_NAME, H5_ITER_INC, NULL, file_info, &dataSetNames);

        string s;
        string dsname;
        string matchname;
        int numDataSets = dataSetNames.size();
        //cout << "numDataSets: " << numDataSets << endl;
        
        // create a key-value map for the dataset names, do it from scratch for each block,
        // and remove redshifts from the dataset names (where necessary)    
        dataSetMap.clear();
        for (int k=0; k<numDataSets; k++) {
            dsname = dataSetNames[k];
            // convert to matchname, i.e. remove possibly given redshift from the name:
            string matchname = boost::regex_replace(dsname, re, newtext);
            dataSetMap[matchname] = k;
            //dataSetMatchNames.push_back(matchname);
        }

        // clear datablocks from previous block, before reading new ones:
        datablocks.clear();

        // read each desired data set, use corresponding read routine for different types
        for (int k=0; k<numDataSets; k++) {

            dsname = dataSetNames[k];
            s = string(outputName) + string("/") + dsname;

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

        endTime = boost::posix_time::microsec_clock::universal_time();
        printf("Time for reading output %s (%ld rows): %lld ms\n", outputName.c_str(), nvalues, (long long int) (endTime-startTime).total_milliseconds());
        fflush(stdout);
            
        return nvalues; //assume that nvalues is the same for each dataset (datablock) inside one Output-group (same redshift)
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

        return 0;
    }

    bool GalacticusReader::getDataItem(DBDataSchema::DataObjDesc * thisItem, void* result) {

        // check which DB column is requested and assign the corresponding data value,
        // variables are declared already in Galacticus_Reader.h
        // and the values were read in getNextRow()
        bool isNull;
        //cout << " again2 ioutput: " << ioutput << endl;
        //NInFile = currRow-1;	// start counting rows with 0       
        
        // go through all data items and assign the correct column numbers for the DB columns, 
        // maybe depending on a read schema-file:
        
        //cout << "dataobjname: " << thisItem->getDataObjName() << endl;

        isNull = false;

        DataBlock b;
        string name;

        // quickly access the correct data block by name (should have redshift removed already),
        // but make sure that key really exists in the map
        map<string,int>::iterator it = dataSetMap.find(thisItem->getDataObjName());
        if (it != dataSetMap.end()) {
            b = datablocks[it->second];
            if (b.longval) {
                *(long*)(result) = b.longval[countInBlock];
                return isNull;
            } else if (b.doubleval) {
                *(double*)(result) = b.doubleval[countInBlock];
                return isNull;
            } else {
                cout << "Error: No corresponding data found!" << " (" << thisItem->getDataObjName() << ")" << endl;
                abort();
            }
        }
            
        // get snapshot number and expansion factor from already read metadata 
        // for this output
        if (thisItem->getDataObjName().compare("snapnum") == 0) {
            *(int*)(result) = current_snapnum;
            return isNull;
        }

        if (thisItem->getDataObjName().compare("scale") == 0) {
            *(double*)(result) = outputMetaMap[current_snapnum].outputExpansionFactor;
            return isNull;
        }

        if (thisItem->getDataObjName().compare("NInFileSnapnum") == 0) {
            *(long*)(result) = countInBlock;
            //result = (void *) countInBlock;
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

        if (thisItem->getDataObjName().compare("forestId") == 0) {
            *(long*) result = 0;
            isNull = true;
            return isNull;
        }

        if (thisItem->getDataObjName().compare("ix") == 0) {
            *(int*) result = 0;
            isNull = true;
            return isNull;
        }

        if (thisItem->getDataObjName().compare("iy") == 0) {
            *(int*) result = 0;
            isNull = true;
            return isNull;
        }

        if (thisItem->getDataObjName().compare("iz") == 0) {
            *(int*) result = 0;
            isNull = true;
            return isNull;
        }

        if (thisItem->getDataObjName().compare("phkey") == 0) {
            *(long*) result = 0;
            isNull = true;
            return isNull;
        }

        // --> do it on the database side;
        // or: put current x, y, z in global reader variables,
        // could calculate ix, iy, iz here, but can only do this AFTER x,y,z
        // were assigned!  => i.e. would need to check in generated schema
        // that it is in correct order!

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

    OutputMeta::OutputMeta() {
        ioutput = 0;
        outputExpansionFactor = 0;
        outputTime = 0;
    };

    /* OutputMeta::deleteData() {
    }; */
}


// Here comes a call back function, thus it lives outside of the Galacticus-Reader class
// operator function, must reside outside of Galacticus-class, because it's extern C?
herr_t file_info(hid_t loc_id, const char *name, const H5L_info_t *linfo, void *opdata) {
    // display name of group/dataset
    //cout << "Name : " << name << endl;
    //cout << name << endl;

    // store name in string vector, pointer to this is provided as parameter
    vector<string>* n = (vector<string>*) opdata;
    n->push_back(name);

    return 0;
}
