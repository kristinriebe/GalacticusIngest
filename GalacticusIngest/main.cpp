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

/* Reader for Galacticus output in HDF5-format
 */

#include <iostream>
#include "Galacticus_Reader.h"
#include "Galacticus_SchemaMapper.h"
#include "galacticusingest_error.h"
#include <Schema.h>
#include <DBIngestor.h>
#include <DBAdaptorsFactory.h>
#include <AsserterFactory.h>
#include <ConverterFactory.h>
#include <boost/program_options.hpp>

#include <sstream>
#include <vector>

using namespace Galacticus;
using namespace std;
namespace po = boost::program_options;


int main (int argc, const char * argv[])
{
    string dataFile;
    string mapFile;
    int snapnum;
    int user_snapnum;
    int ngrid;
    int jobNum;
    int fileNum;
    
    // allow to use only some part of the data file, 
    // i.e. specify offset and maximum number of rows:
    int startRow;
    int maxRows;

    string dbase;
    string table;
    string system;
    string socket;
    string user;
    string pwd;
    string port;
    string host;
    string path;
    uint32_t bufferSize;
    uint32_t outputFreq;

//    bool greedyDelim;
    bool isDryRun = false;
    bool resumeMode;
    bool askUserToValidateRead = true; // can be overwritten by options below
    
    DBServer::DBAbstractor * dbServer;
    DBIngest::DBIngestor * galacticusIngestor;
    DBServer::DBAdaptorsFactory adaptorFac;


    //build database string
    string dbSystemDesc = "database system to use (";
    
#ifdef DB_SQLITE3
    dbSystemDesc.append("sqlite3, ");
#endif
    
#ifdef DB_MYSQL
    dbSystemDesc.append("mysql, ");
#endif
    
#ifdef DB_ODBC
    dbSystemDesc.append("unix_sqlsrv_odbc, ");
    dbSystemDesc.append("sqlsrv_odbc, ");
    dbSystemDesc.append("sqlsrv_odbc_bulk, ");
    dbSystemDesc.append("cust_odbc, ");
    dbSystemDesc.append("cust_odbc_bulk, ");
#endif
    
    dbSystemDesc.append(") - [default: mysql]");
    
    
    po::options_description progDesc("GalacticusIngest - Ingest binary HDF5 Galacticus files into databases\n\nGalacticusIngest [OPTIONS] [dataFile]\n\nCommand line options:");
        
    progDesc.add_options()
                ("help,?", "output help")
                ("data,d", po::value<string>(&dataFile), "datafile to ingest")
                ("system,s", po::value<string>(&system)->default_value("mysql"), dbSystemDesc.c_str())
                ("bufferSize,B", po::value<uint32_t>(&bufferSize)->default_value(128), "ingest buffer size (will be reduced to sytem maximum if needed) [default: 128]")
                ("outputFreq,F", po::value<uint32_t>(&outputFreq)->default_value(100000), "number of rows after which a performance measurement is output [default: 100000]")
                ("dbase,D", po::value<string>(&dbase)->default_value(""), "name of the database where the data is added to (where applicable)")
                ("table,T", po::value<string>(&table)->default_value(""), "name of the table where the data is added to")
                ("socket,S", po::value<string>(&socket)->default_value(""), "socket to use for database access (where applicable)")
                ("user,U", po::value<string>(&user)->default_value(""), "user name (where applicable")
                ("pwd,P", po::value<string>(&pwd)->default_value(""), "password (where applicable")
                ("port,O", po::value<string>(&port)->default_value("3306"), "port to use for database access (where applicable) [default: 3306 (mysql)]")
                ("host,H", po::value<string>(&host)->default_value("localhost"), "host to use for database access (where applicable) [default: localhost]")
                ("path,p", po::value<string>(&path)->default_value(""), "path to a database file (mainly for sqlite3, where applicable)")
                ("mapFile,f", po::value<string>(&mapFile)->default_value(""), "path to the mapping file")
//                ("ngrid,g", po::value<int32_t>(&ngrid)->default_value(1024), "number of cells for positional grid)")
                ("isDryRun", po::value<bool>(&isDryRun)->default_value(0), "should this run be carried out as a dry run (no data added to database)? [default: 0]")
                ("jobNum", po::value<int>(&jobNum)->default_value(0), "number of the job (containing fileNum data files) [default: 0]")
                ("fileNum", po::value<int>(&fileNum)->default_value(0), "number of the data file for given job number")
//                ("startRow,i", po::value<int32_t>(&startRow)->default_value(0), "start reading at this initial row number (default 0)")
//                ("maxRows,m", po::value<int32_t>(&maxRows)->default_value(-1), "max. number of rows to be read (default -1 for all rows)")
                ("snapnum", po::value<int32_t>(&user_snapnum)->default_value(-1), "only read data for given snaphot number? [default: -1]")
                ("resumeMode,R", po::value<bool>(&resumeMode)->default_value(0), "try to resume ingest on failed connection (turns off transactions)? [default: 0]")
                ("validateSchema,v", po::value<bool>(&askUserToValidateRead)->default_value(1), "ask user to validate the schema mapping [default: 1]")
                ;
    // Attention: many of these options actually are required; boost version 1.42 and above support ->required() (instead of default()), but not older versions;
    // Unfortunately our servers only have boost 1.41 installed, so it would not work there.
    // required options: dbase, table, mapFile, jobNum, fileNum

    po::positional_options_description posDesc;
    posDesc.add("data", 1);

    //read out the options
    po::variables_map varMap;
    //po::store(po::command_line_parser(argc, argv).options(progDesc).positional(posDesc).run(), varMap);
    po::store(po::command_line_parser(argc, (char **) argv).options(progDesc).positional(posDesc).run(), varMap);
    // --> only compiles at erebos if I include the (char **) cast
    po::notify(varMap);
    
    if(varMap.count("help") || varMap.count("?") || dataFile.length() == 0) {
        cout << progDesc;
        return EXIT_SUCCESS;
    }
    
    cout << "You have entered the following parameters:" << endl;
    cout << "Data file: " << dataFile << endl;
    cout << "DB system: " << system << endl;
    cout << "Buffer size: " << bufferSize << endl;
    cout << "Performance output frequency: " << outputFreq << endl;
    cout << "Database name: " << dbase << endl;
    cout << "Table name: " << table << endl;
    cout << "Socket: " << socket << endl;
    cout << "User: " << user << endl;
    if(pwd.compare("") == 0) {
        cout << "Password not given" << endl;
    } else {
        cout << "Password given" << endl;
    }
    cout << "Port: " << port << endl;
    cout << "Host: " << host << endl;
    cout << "Path: " << path << endl << endl;
   
    DBAsserter::AsserterFactory * assertFac = new DBAsserter::AsserterFactory;
    DBConverter::ConverterFactory * convFac = new DBConverter::ConverterFactory;

    //now setup the file reader
    GalacticusReader *thisReader = new GalacticusReader(dataFile, jobNum, fileNum, user_snapnum, startRow, maxRows);
    dbServer = adaptorFac.getDBAdaptors(system);

    //vector<string> dataSetNames;
    //dataSetNames = thisReader->getDataSetNames(); // problem: they are not yet defined here, because they are read for each block again
    //cout << dataSetNames.size() << endl;;

    GalacticusSchemaMapper * thisSchemaMapper = new GalacticusSchemaMapper(assertFac, convFac);     //registering the converter and asserter factories
    cout << "Mapping file: " << mapFile << endl;
    thisSchemaMapper->readMappingFile(mapFile);

    DBDataSchema::Schema * thisSchema;
    thisSchema = thisSchemaMapper->generateSchema(dbase, table);

    /*cout << "complete schema: " << endl;
    for (int j=0; j<thisSchema->getArrSchemaItems().size(); j++) {
        cout << "col name: " << thisSchema->getArrSchemaItems().at(j)->getColumnName() << endl;
    }
    */
    
    galacticusIngestor = new DBIngest::DBIngestor(thisSchema, thisReader, dbServer);
    galacticusIngestor->setUsrName(user);
    galacticusIngestor->setPasswd(pwd);

    //settings for different DBs (copy&paste from AsciiIngest)
    if(system.compare("mysql") == 0) {
        galacticusIngestor->setSocket(socket);
        galacticusIngestor->setPort(port);
        galacticusIngestor->setHost(host);
    } else if (system.compare("sqlite3") == 0) {
        galacticusIngestor->setHost(path);
    } else if (system.compare("unix_sqlsrv_odbc") == 0) {
        galacticusIngestor->setSocket("DRIVER=FreeTDS;TDS_Version=7.0;");
        //galacticusIngestor->setSocket("DRIVER=SQL Server Native Client 10.0;");
        galacticusIngestor->setPort(port);
        galacticusIngestor->setHost(host);
    } else if (system.compare("sqlsrv_odbc") == 0) {
        galacticusIngestor->setSocket("DRIVER=SQL Server Native Client 10.0;");
        galacticusIngestor->setPort(port);
        galacticusIngestor->setHost(host);
    } else if (system.compare("sqlsrv_odbc_bulk") == 0) {
        //TESTS ON SQL SERVER SHOWED THIS IS VERY SLOW. BUT NO CLUE WHY, DID NOT BOTHER TO LOOK AT PROFILER YET
        galacticusIngestor->setSocket("DRIVER=SQL Server Native Client 10.0;");
        galacticusIngestor->setPort(port);
        galacticusIngestor->setHost(host);
    }  else if (system.compare("cust_odbc") == 0) {
        galacticusIngestor->setSocket(socket);
        galacticusIngestor->setPort(port);
        galacticusIngestor->setHost(host);
    } else if (system.compare("cust_odbc_bulk") == 0) {
        //TESTS ON SQL SERVER SHOWED THIS IS VERY SLOW. BUT NO CLUE WHY, DID NOT BOTHER TO LOOK AT PROFILER YET
        galacticusIngestor->setSocket(socket);
        galacticusIngestor->setPort(port);
        galacticusIngestor->setHost(host);
    }
    
    // setup resume option, if desired
    galacticusIngestor->setResumeMode(resumeMode); 
    galacticusIngestor->setIsDryRun(isDryRun); 
    galacticusIngestor->setAskUserToValidateRead(askUserToValidateRead); 
   
    cout << "now everything ready to ingest ..." << endl;
   
    //now ingest data after setup
    galacticusIngestor->setPerformanceMeter(outputFreq);	// after how many lines should I print the status?
    cout << "Go now!" << endl;
    galacticusIngestor->ingestData(bufferSize);  		// buffer size (in bytes??)
    
    delete thisSchemaMapper;
    delete thisSchema;
    //delete assertFac;
    //delete convFac;

    return 0;
}

