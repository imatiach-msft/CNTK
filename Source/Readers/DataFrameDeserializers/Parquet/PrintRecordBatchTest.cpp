#include <cstdlib>
#include <iostream>
/*
#include "arrow/io/interfaces.h"
#include "parquet/api/reader.h"
#include "arrow/io/hdfs.h" // for HdfsConnectionConfig
#include "arrow/status.h" // for arrow::Status
*/
#include "ParquetReader.h"
#include "HDFSArrowReader.h"

using namespace Microsoft::MSR::CNTK::hdfs;

    int main(int argc, char** argv) {
        // Instantiate the HdfsConnectionConfig struct
        std::string h = argv[1];
        int p = std::stoi(argv[2]);
        const std::string path = argv[3]; // Complete file path on HDFS

        HDFSArrowReader* hdfsReader = new HDFSArrowReader(h, path, p);
        hdfsReader -> Connect();

        std::shared_ptr<arrow::io::HdfsReadableFile> filePtr = nullptr;
        hdfsReader -> OpenReadable(&filePtr); // Now the filePtr points to an HdfsReadableFile that has been correctly instantiated.

        ParquetReader* pReader = new ParquetReader(filePtr);
        arrow::RecordBatch rb = pReader -> ReadBatch(0); // Hardcoded in 0 for now
        std::stringstream ss;
        pReader -> PrintRecordBatch(rb, 4, &ss);
        std::cout << ss.str() << std::endl;
        return 0;
    }
