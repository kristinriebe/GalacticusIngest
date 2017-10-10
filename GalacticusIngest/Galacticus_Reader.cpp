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

    GalacticusReader::GalacticusReader(string newFileName, int newFileNum, vector<int> newSnapnums, float newHubble_h) {

        user_snapnums = newSnapnums;
        fileName = newFileName;

        // strip path from file name
        //boost::filesystem::path p(fileName);
        //dataFileBaseName = p.filename().string(); // or use stem() for omitting extension
        // get directory number and file number from file name?
        // no, just let the user provide a file number and take care of mapping
        // the (arbitrary) file/directory names to the number; mainly for internal use
        fileNum = newFileNum;
        hubble_h = newHubble_h;

        fp = NULL;

        currRow = 0;
        countInBlock = 0;   // counts values in each datablock (output)
        countSnap = 0;

        // factors for constructing dbId, could/should be read from user input, actually
        snapnumfactor = 1000;
        rowfactor = 1000000;

        //const H5std_string FILE_NAME( "SDS.h5" );
        openFile(newFileName);
        //offsetFileStream();

        // read expansion factors, output names etc.
        getOutputsMeta(numOutputs);

        // set numOutputs, if snapnums are given
        if (user_snapnums.size() > numOutputs) {
            printf("ERROR: desired number of snapshot numbers is larger than available number of output! (%ld > %ld)\n", user_snapnums.size(), numOutputs);
            exit(0);
        }

        if (user_snapnums.size() > 0) {
            numOutputs = user_snapnums.size();
            printf("Number of outputs set to %ld because of user options\n", numOutputs);
        } else {
            // initialize iterator for meta data (output names), (actually only needed, if not using user_snapnums)
            it_outputmap = outputMetaMap.begin();
        }

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

        // TODO: catch error, if file does not exist or not accessible? before using H5 lib?
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
        // map output-index from the file to the snapshot numbers used in the underlying Rockstar-catalogues;
        // very specific for this dataSet, should be read from file
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
            printf("ERROR in GalacticusReader: No matching snapshot number for output %ld available.\n", ioutput);
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
        string outputName;

        // get one line from already read datasets (using readNextBlock)
        // use readNextBlock to read the next block of datasets if necessary
        if (currRow == 0) {
            // we are at the very beginning
            // read block for given snapnum or start reading from 1. block

            if (user_snapnums.size() > 0) {
                current_snapnum = user_snapnums[countSnap];
                // check, if this snapnum really exists in outputMetaMap

                it_outputmap = outputMetaMap.find(current_snapnum);
                while (it_outputmap == outputMetaMap.end() && countSnap < numOutputs) {
                    cout << "Skipping snapnum " << current_snapnum << " because no corresponding output-group was found." << endl;
                    countSnap++;
                    current_snapnum = user_snapnums[countSnap];
                    it_outputmap = outputMetaMap.find(current_snapnum);
                }
                if (it_outputmap == outputMetaMap.end()) {
                    return 0;
                }
                outputName = (it_outputmap->second).outputName;
            } else {
                current_snapnum = it_outputmap->first;
                outputName = (it_outputmap->second).outputName;
            }

            // get corresponding output name
            //outputName = outputMetaMap[current_snapnum].outputName;

            nvalues = readNextBlock(outputName);
            countInBlock = 0;

        } else if (countInBlock == nvalues-1) {
            // end of data block/start of new one is reached!
            // => read next datablocks (for next output number)
            //cout << "Read next datablock!" << endl;

            // check if we are at the end
            //if (it_outputmap == outputMetaMap.end() || countSnap == user_snapnums.size()) {
            //    return 0;
            //}
            if (countSnap == numOutputs-1) {
                return 0;
            }

            // if we end up here, we should read the next block
            if (user_snapnums.size() > 0) {
                countSnap++;
                current_snapnum = user_snapnums[countSnap];
                // check, if this snapnum really exists in outputMetaMap

                it_outputmap = outputMetaMap.find(current_snapnum);
                while (it_outputmap == outputMetaMap.end() && countSnap < numOutputs) {
                    cout << "Skipping snapnum " << current_snapnum << " because no corresponding output-group was found." << endl;
                    countSnap++;
                    current_snapnum = user_snapnums[countSnap];
                    it_outputmap = outputMetaMap.find(current_snapnum);
                }
                if (it_outputmap == outputMetaMap.end()) {
                    return 0;
                }
                outputName = (it_outputmap->second).outputName;
            } else {
                it_outputmap++;

                // check, if we haven't reached the end yet
                if (it_outputmap == outputMetaMap.end()) {
                    cout << "End of outputs group is reached." << endl;
                    return 0;
                }

                current_snapnum = it_outputmap->first;
                outputName = (it_outputmap->second).outputName;
            }

            // get corresponding output name -- already done
            // outputName = outputMetaMap[current_snapnum].outputName;

            nvalues = readNextBlock(outputName);
            countInBlock = 0;

        } else {
            countInBlock++;
        }

        currRow++; // counts all rows

        // stop reading/ingesting, if mass is lower than threshold?
        // stop after reading maxRows?

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
        //cout << "outputName: " << outputName<< endl;

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
        // using the already read schema-map for mapping database and file fields;
        // variables are declared already in Galacticus_Reader.h
        // and the values were read in getNextRow()
        // also apply any necessary unit conversion etc. here!
        bool isNull;
        //cout << " again2 ioutput: " << ioutput << endl;
        //NInFile = currRow-1;	// start counting rows with 0

        // go through all data items and assign the correct value from DB columns, 
        // depending on a read schema-file:

        //cout << "dataobjname: " << thisItem->getDataObjName() << endl;

        isNull = false;

        DataBlock b;
        string name;
        //cout << "Get data item .... " << thisItem->getDataObjName() << endl;

        // Here I should apply any necessary unit conversion.
        // But this will make the upload slower ...
        double scale;
        scale = outputMetaMap[current_snapnum].outputExpansionFactor;
        long satelliteNodeIndex;
        long nodeIndex;
        long parentIndex;
        int satelliteStatus;
        long rockstarId;
        long MainHaloId;
        double HaloMass;
        double basicMass;
        double satelliteBoundMass;
        double SFRdisk;
        double SFRspheroid;
        map<string,int>::iterator it;
        double x,y,z;

        // deal with the dataItems which need special treatment or are not stored in map

        // get snapshot number and expansion factor from already read metadata 
        // for this output
        if (thisItem->getDataObjName().compare("snapnum") == 0) {
            *(int*)(result) = current_snapnum;
            return isNull;
        }

        if (thisItem->getDataObjName().compare("scale") == 0) {
            *(double*)(result) = scale;
            return isNull;
        }

        if (thisItem->getDataObjName().compare("redshift") == 0) {
            *(double*)(result) = 1./scale - 1.;
            return isNull;
        }

        if (thisItem->getDataObjName().compare("NInFileSnapnum") == 0) {
            *(long*)(result) = countInBlock;
            return isNull;
        }

        if (thisItem->getDataObjName().compare("fileNum") == 0) {
            *(int*) result = fileNum;
            return isNull;
        }

        if (thisItem->getDataObjName().compare("dbId") == 0) {
            *(long*)(result) = (fileNum * snapnumfactor + current_snapnum) * rowfactor + countInBlock;
            return isNull;
        }

        if (thisItem->getDataObjName().compare("forestId") == 0) {
            *(long*) result = 0;
            isNull = true;
            return isNull;
        }

        if (thisItem->getDataObjName().compare("depthFirstId") == 0) {
            *(long*) result = 0;
            isNull = true;
            return isNull;
        }

        if (thisItem->getDataObjName().compare("rockstarId") == 0) {
            // first get satelliteNodeIndex, nodeIndex and satelliteStatus, 
            // then assign correctly
            it = dataSetMap.find("satelliteNodeIndex");
            if (it != dataSetMap.end()) {
                b = datablocks[it->second];
                if (b.longval) {
                    satelliteNodeIndex = b.longval[countInBlock];
                }
            } else {
                cout << "Error: No corresponding data found!" << " (satelliteNodeIndex)" << endl;
                abort();
            }

            it = dataSetMap.find("satelliteStatus");
            if (it != dataSetMap.end()) {
                b = datablocks[it->second];
                if (b.longval) {
                    satelliteStatus = b.longval[countInBlock];
                }
            } else {
                cout << "Error: No corresponding data found!" << " (satelliteStatus)" << endl;
                abort();
            }

            it = dataSetMap.find("nodeIndex");
            if (it != dataSetMap.end()) {
                b = datablocks[it->second];
                if (b.longval) {
                    nodeIndex = b.longval[countInBlock];
                }
            } else {
                cout << "Error: No corresponding data found!" << " (nodeIndex)" << endl;
                abort();
            }

            if (satelliteStatus == 0) {
                rockstarId = nodeIndex;
            } else {
                rockstarId = satelliteNodeIndex;
            }

            *(long*) result = rockstarId;
            return isNull;
        }

        if (thisItem->getDataObjName().compare("HostHaloId") == 0) {
            // first get satelliteNodeIndex, nodeIndex and satelliteStatus, 
            // then assign correctly
            it = dataSetMap.find("satelliteNodeIndex");
            if (it != dataSetMap.end()) {
                b = datablocks[it->second];
                if (b.longval) {
                    satelliteNodeIndex = b.longval[countInBlock];
                }
            } else {
                cout << "Error: No corresponding data found!" << " (satelliteNodeIndex)" << endl;
                abort();
            }

            it = dataSetMap.find("satelliteStatus");
            if (it != dataSetMap.end()) {
                b = datablocks[it->second];
                if (b.longval) {
                    satelliteStatus = b.longval[countInBlock];
                }
            } else {
                cout << "Error: No corresponding data found!" << " (satelliteStatus)" << endl;
                abort();
            }

            it = dataSetMap.find("nodeIndex");
            if (it != dataSetMap.end()) {
                b = datablocks[it->second];
                if (b.longval) {
                    nodeIndex = b.longval[countInBlock];
                }
            } else {
                cout << "Error: No corresponding data found!" << " (nodeIndex)" << endl;
                abort();
            }

            if (satelliteStatus == 0) {
                rockstarId = nodeIndex;
            } else {
                rockstarId = satelliteNodeIndex;
            }

            *(long*) result = rockstarId;
            return isNull;
        }

        if (thisItem->getDataObjName().compare("MainHaloId") == 0) {
            // first get satelliteNodeIndex, nodeIndex and satelliteStatus, 
            // then assign correctly
            it = dataSetMap.find("parentIndex");
            if (it != dataSetMap.end()) {
                b = datablocks[it->second];
                if (b.longval) {
                    parentIndex = b.longval[countInBlock];
                }
            } else {
                cout << "Error: No corresponding data found!" << " (parentIndex)" << endl;
                abort();
            }

            it = dataSetMap.find("satelliteStatus");
            if (it != dataSetMap.end()) {
                b = datablocks[it->second];
                if (b.longval) {
                    satelliteStatus = b.longval[countInBlock];
                }
            } else {
                cout << "Error: No corresponding data found!" << " (satelliteStatus)" << endl;
                abort();
            }

            it = dataSetMap.find("nodeIndex");
            if (it != dataSetMap.end()) {
                b = datablocks[it->second];
                if (b.longval) {
                    nodeIndex = b.longval[countInBlock];
                }
            } else {
                cout << "Error: No corresponding data found!" << " (nodeIndex)" << endl;
                abort();
            }

            if (satelliteStatus == 0) {
                MainHaloId = nodeIndex;
            } else {
                MainHaloId = parentIndex;
            }

            *(long*) result = MainHaloId;
            return isNull;
        }

        if (thisItem->getDataObjName().compare("HaloMass") == 0) {
            // first get satelliteBoundMass, basicMass,
            // then assign correctly: if sat.Mass == 0, then use basicMass, otherwise sat.Mass

            it = dataSetMap.find("basicMass");
            if (it != dataSetMap.end()) {
                b = datablocks[it->second];
                if (b.doubleval) {
                    basicMass = b.doubleval[countInBlock];
                }
            } else {
                cout << "Error: No corresponding data found!" << " (basicMass)" << endl;
                abort();
            }

            it = dataSetMap.find("satelliteBoundMass");
            if (it != dataSetMap.end()) {
                b = datablocks[it->second];
                if (b.doubleval) {
                    satelliteBoundMass = b.doubleval[countInBlock];
                }
            } else {
                cout << "Error: No corresponding data found!" << " (satelliteBoundMass)" << endl;
                abort();
            }

            if (satelliteBoundMass != 0) {
                HaloMass = satelliteBoundMass;
            } else {
                HaloMass = basicMass;
            }

            *(double*) result = HaloMass*hubble_h;
            return isNull;
        }

        if (thisItem->getDataObjName().compare("SFR") == 0) {
            // first get disk- and spheroid SFR, then add

            it = dataSetMap.find("spheroidStarFormationRate");
            if (it != dataSetMap.end()) {
                b = datablocks[it->second];
                if (b.doubleval) {
                    SFRspheroid = b.doubleval[countInBlock];
                }
            } else {
                cout << "Error: No corresponding data found!" << " (spheroidStarFormationRate)" << endl;
                abort();
            }
            it = dataSetMap.find("diskStarFormationRate");
            if (it != dataSetMap.end()) {
                b = datablocks[it->second];
                if (b.doubleval) {
                    SFRdisk = b.doubleval[countInBlock];
                }
            } else {
                cout << "Error: No corresponding data found!" << " (diskStarFormationRate)" << endl;
                abort();
            }

            *(double*) result = (SFRdisk + SFRspheroid) * hubble_h;
            return isNull;
        }

        /* Multiply Abundance* columns with h, since it is not the mass fraction, but masses */
        if (thisItem->getDataObjName().compare("MZgasDisk") == 0) {
            it = dataSetMap.find("diskAbundancesGasMetals");
            if (it != dataSetMap.end()) {
                b = datablocks[it->second];
                if (b.doubleval) {
                    abundance = b.doubleval[countInBlock];
                }
            } else {
                cout << "Error: No corresponding data found!" << " (diskAbundancesGasMetals)" << endl;
                abort();
            }
            *(double*) result = abundance * hubble_h;
            return isNull;
        }

        if (thisItem->getDataObjName().compare("MZstarDisk") == 0) {
            it = dataSetMap.find("diskAbundancesStellarMetals");
            if (it != dataSetMap.end()) {
                b = datablocks[it->second];
                if (b.doubleval) {
                    abundance = b.doubleval[countInBlock];
                }
            } else {
                cout << "Error: No corresponding data found!" << " (diskAbundancesStellarMetals)" << endl;
                abort();
            }
            *(double*) result = abundance * hubble_h;
            return isNull;
        }

        if (thisItem->getDataObjName().compare("MZhotHalo") == 0) {
            it = dataSetMap.find("hotHaloAbundancesMetals");
            if (it != dataSetMap.end()) {
                b = datablocks[it->second];
                if (b.doubleval) {
                    abundance = b.doubleval[countInBlock];
                }
            } else {
                cout << "Error: No corresponding data found!" << " (hotHaloAbundancesMetals)" << endl;
                abort();
            }
            *(double*) result = abundance * hubble_h;
            return isNull;
        }

        if (thisItem->getDataObjName().compare("MZgasSpheroid") == 0) {
            it = dataSetMap.find("spheroidAbundancesGasMetals");
            if (it != dataSetMap.end()) {
                b = datablocks[it->second];
                if (b.doubleval) {
                    abundance = b.doubleval[countInBlock];
                }
            } else {
                cout << "Error: No corresponding data found!" << " (spheroidAbundancesGasMetals)" << endl;
                abort();
            }
            *(double*) result = abundance * hubble_h;
            return isNull;
        }

        if (thisItem->getDataObjName().compare("MZstarSpheroid") == 0) {
            it = dataSetMap.find("spheroidAbundancesStellarMetals");
            if (it != dataSetMap.end()) {
                b = datablocks[it->second];
                if (b.doubleval) {
                    abundance = b.doubleval[countInBlock];
                }
            } else {
                cout << "Error: No corresponding data found!" << " (spheroidAbundancesStellarMetals)" << endl;
                abort();
            }
            *(double*) result = abundance * hubble_h;
            return isNull;
        }


        if (thisItem->getDataObjName().compare("ix") == 0) {
            it = dataSetMap.find("positionPositionX");
            if (it != dataSetMap.end()) {
                b = datablocks[it->second];
                if (b.doubleval) {
                    x = b.doubleval[countInBlock];
                }
            } else {
                cout << "Error: No corresponding data found!" << " (positionPositionX)" << endl;
                abort();
            }

            *(int*) result = (int) (x*hubble_h/scale * (1024/1000.) );
            isNull = true;
            return isNull;
        }

        if (thisItem->getDataObjName().compare("iy") == 0) {
            it = dataSetMap.find("positionPositionY");
            if (it != dataSetMap.end()) {
                b = datablocks[it->second];
                if (b.doubleval) {
                    y = b.doubleval[countInBlock];
                }
            } else {
                cout << "Error: No corresponding data found!" << " (positionPositionY)" << endl;
                abort();
            }

            *(int*) result = (int) (y*hubble_h/scale * (1024/1000.) );
            isNull = true;
            return isNull;
        }

        if (thisItem->getDataObjName().compare("iz") == 0) {
            it = dataSetMap.find("positionPositionZ");
            if (it != dataSetMap.end()) {
                b = datablocks[it->second];
                if (b.doubleval) {
                    z = b.doubleval[countInBlock];
                }
            } else {
                cout << "Error: No corresponding data found!" << " (positionPositionZ)" << endl;
                abort();
            }

            *(int*) result = (int) (z*hubble_h/scale * (1024/1000.) );
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


        // quickly access the correct data block by name (should have redshift removed already),
        // but make sure that key really exists in the map
        // should do this at the end => only for those datasets that got no special treatment!
        isNull = false;
        it = dataSetMap.find(thisItem->getDataObjName());
        if (it != dataSetMap.end()) {
            b = datablocks[it->second];
            if (b.longval) {
                *(long*)(result) = b.longval[countInBlock];
                return isNull;
            } else if (b.doubleval) {
                *(double*)(result) = b.doubleval[countInBlock];

                // apply unit conversion for the necessary parts:
                if (thisItem->getDataObjName().compare("blackHoleMass") == 0
                    || thisItem->getDataObjName().compare("basicMass") == 0
                    || thisItem->getDataObjName().compare("diskMassGas") == 0
                    || thisItem->getDataObjName().compare("diskMassStellar") == 0
                    || thisItem->getDataObjName().compare("diskStarFormationRate") == 0
                    || thisItem->getDataObjName().compare("hotHaloMass") == 0
                   // || thisItem->getDataObjName().compare("satelliteBoundMass") == 0 => already covered above at HaloMass
                    || thisItem->getDataObjName().compare("spheroidMassGas") == 0
                    || thisItem->getDataObjName().compare("spheroidMassStellar") == 0
                    || thisItem->getDataObjName().compare("spheroidStarFormationRate") == 0
                   ) {
                    *(double*)(result) = b.doubleval[countInBlock] * hubble_h;
                }
                if (thisItem->getDataObjName().compare("diskRadius") == 0
                    || thisItem->getDataObjName().compare("hotHaloOuterRadius") == 0
                    || thisItem->getDataObjName().compare("positionPositionX") == 0
                    || thisItem->getDataObjName().compare("positionPositionY") == 0
                    || thisItem->getDataObjName().compare("positionPositionZ") == 0
                    || thisItem->getDataObjName().compare("spheroidRadius") == 0
                   ) {
                    *(double*)(result) = b.doubleval[countInBlock] * hubble_h/scale;
                }
                return isNull;

            } else {
                cout << "Error: No corresponding data found!" << " (" << thisItem->getDataObjName() << ")" << endl;
                abort();
            }
        }

        // if we still did not return until now ...
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
