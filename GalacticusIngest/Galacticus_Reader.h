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

#include <Reader.h>
#include <string>
#include <fstream>
#include <stdio.h>
#include <assert.h>
#include <list>
#include <sstream>
#include <map>

#ifndef Galacticus_Galacticus_Reader_h
#define Galacticus_Galacticus_Reader_h

using namespace DBReader;
using namespace DBDataSchema;
using namespace std;
//using cout;
//using endl;
#include "H5Cpp.h"
using namespace H5;

extern "C" herr_t file_info(hid_t loc_id, const char *name, const H5L_info_t *linfo,
                                    void *opdata);

namespace Galacticus {
    
    class OutputMeta {
        public:
            int ioutput;
            int snapnum; // usually the same as ioutput, but we never know ...
            string outputName;
            float outputExpansionFactor; // scale
            float outputTime;

            OutputMeta();
    };


    class DataBlock {
        public:
            long nvalues;   // number of values in the block
            string name;
            long idx;
            double *doubleval;
            long *longval;
            string type;

            DataBlock();
            //DataBlock(DataBlock &source);

            void deleteData();
    };
    // This own DataBlock-class is in principle the same as the dataset class!!
    // The only difference may be that the DataBlock shall not contain the whole
    // DataSet, only a part, but this is what hyperslabs are for!!
    // Hmmm ... does DataSet contain all the data or just a handle to these data???

    
    class GalacticusReader : public Reader {
    private:
        string fileName;
        string mapFile;

        ifstream fileStream;

        H5File* fp;//holds the opened hdf5 file
        long ioutput; // number of current output
        long numOutputs; // total number of outputs (one for reach redshift)
        long numDataSets; // number of DataSets (= row fields, = columns) in each output
        long nvalues; // values in one dataset (assume the same number for each dataset of the same output group (redshift))

        vector<string> dataSetNames;
        map<string,int> dataSetMap;

        vector<string> outputNames;
        map<int, OutputMeta> outputMetaMap;
        map<int, OutputMeta>::iterator it_outputmap;

        // improve performance by defining it here (instead if inside getItemInRow)
        string tmpStr;

        long currRow;
        long countInBlock;
       
        //fields to be generated/converted/...
        int current_snapnum;
        int user_snapnum;
        long NInFileSnapnum;
        double scale;

        int jobNum;
        int fileNum;

        int ix;
        int iy;
        int iz;
        long phkey;
        
        // to be passed on from main
        int startRow;   // at which row should we start ingesting
        int maxRows;    // max. number of rows to ingest
          
        // define something to hold all datasets from one read block 
        // (one complete Output* block or a part of it)
        vector<DataBlock> datablocks;

    public:
        GalacticusReader();
        GalacticusReader(string newFileName, int jobNum, int fileNum, int newSnapnum, int newStartRow, int newMaxRows);
        ~GalacticusReader();

        void openFile(string newFileName);

        void closeFile();

        void offsetFileStream();

        void getOutputsMeta(long &numOutputs);


        int getNextRow();
        int readNextBlock(string outputName); //possibly add startRow (numRow?), numRows? --> but these are global anyway
        long* readLongDataSet(const string s, long &nvalues);
        double* readDoubleDataSet(const string s, long &nvalues);

        long getNumRowsInDataSet(string s);

        vector<string> getDataSetNames();

        void setCurrRow(long n);
        long getCurrRow();
        long getNumOutputs();

        int getSnapnum(long ioutput);
        
        bool getItemInRow(DBDataSchema::DataObjDesc * thisItem, bool applyAsserters, bool applyConverters, void* result);

        bool getDataItem(DBDataSchema::DataObjDesc * thisItem, void* result);

        void getConstItem(DBDataSchema::DataObjDesc * thisItem, void* result);
    };
    
}

#endif
