#include <cstdlib>
#include <iostream>

#include "arrow/io/interfaces.h"
#include "parquet/api/reader.h"
#include "arrow/io/hdfs.h" // for HdfsConnectionConfig
#include "arrow/status.h" // for arrow::Status


int main(int argc, char** argv) {
    // Instantiate the HdfsConnectionConfig struct
    std::string h = argv[1];
    int p = std::stoi(argv[2]);
    std::string u = ""; // set to empty for now
    std::string kTicket = "";
    arrow::io::HdfsDriver dr = arrow::io::HdfsDriver::LIBHDFS;

    const std::string path = argv[3]; // Complete file path on HDFS

    // Set up all the variables in HdfsConnectionConfig 
    arrow::io::HdfsConnectionConfig* HDFSconf = new arrow::io::HdfsConnectionConfig();
    HDFSconf -> host = h; // localhost checking?
    HDFSconf -> port = p; // default port number check?
    HDFSconf -> user = u;
    HDFSconf -> kerb_ticket = kTicket;
    HDFSconf -> driver = dr;

    // Open up a connection to HDFS
    // LibHdfsShim* driver_shim; We won't use the LibHdfsShim* for now, as we will be controlling the connection to HDFS
    // and also have the JARs for HDFS and JAVA correctly linked during compile time. 
    std::shared_ptr<arrow::io::HdfsClient> clientPtr = nullptr;
    bool loadedDriver = false;
    arrow::Status s;
/*
    if (HDFSconf.driver == HdfsDriver::LIBHDFS) {
        s = ConnectLibHdfs(&driver_shim);
        if (!s.ok()) {
            std::cout << "Loading libhdfs failed, skipping tests gracefully" << std::endl;
            return;
        }
    } else {
        std::cout << "We currently do not support libhdfs3" << std::endl;
        return;
    }
*/
    // driver is loaded at this point
    loadedDriver = true;
    s = arrow::io::HdfsClient::Connect(HDFSconf, &clientPtr);
    if (!s.ok()) {
        std::cout << "Connection to the HDFSClient failed" << std::endl;
        return -1;
    }
    // Open up a file, pass it into arrow's Open() API
    std::cout << "Established a connection to HDFS" << std::endl;
    // Test that it can correctly read from it.


    // parquet's ParquetFileReader APIs defined in file/reader.h
    // requires a ::arrow::io::ReadableFileInterface source.
    // ReadableFileInterface is a RandomAccessFile, which inherits from
    // FileInterface and Readable classes, which are all defined in arrow/io/interfaces.h
    
    // Now, HdfsReadableFile defined in arrow/io/hdfs.h inherits from RandomAccessFile
    // Question here is, would ParquetFileReader's Open API call handle the connection to HDFS?

    std::shared_ptr<arrow::io::HdfsReadableFile> filePtr = nullptr;
    s = clientPtr -> OpenReadable(&path, &filePtr); // Now the filePtr points to an HdfsReadableFile that has been correctly instantiated.
    std::unique_ptr<parquet::ParquetFileReader> pfr = parquet::ParquetFileReader::Open(std::move(filePtr));
    std::shared_ptr<parquet::FileMetaData> metadata = pfr -> metadata();
    const parquet::SchemaDescriptor* schema = metadata -> schema();
    int numCols = schema -> num_columns();
    printf("Number of Columns in this Parquet File are: %u \n", numCols);

}
