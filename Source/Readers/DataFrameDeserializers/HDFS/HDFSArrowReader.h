//
// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE.md file in the project root for full license information.
//

#pragma once
#include "arrow/io/interfaces.h"
#include "arrow/io/hdfs.h"
#include "arrow/status.h"
#include "Constants.h"

namespace Microsoft { namespace MSR { namespace CNTK { namespace hdfs
{
    class HDFSArrowReader 
    {

    public:
        HDFSArrowReader(
            const std::string host,
            const std::string path,
            const int port,
            const std::string user = "",
            const std::string kerbTicket = "",
            arrow::io::HdfsDriver driver = arrow::io::HdfsDriver::LIBHDFS);
        ~HDFSArrowReader();

        void HDFSArrowReader::Connect(); 
        void HDFSArrowReader::OpenReadable();

    private:
        arrow::io::HdfsConnectionConfig* m_HDFSconf;
        arrow::io::HdfsClient* m_HDFSClient;
        arrow::Status m_status;
        std::shared_ptr<arrow::io::HdfsReadableFile> m_filePtr;
        std::string m_filePath;
    }

} // hdfs
}}}