//
// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE.md file in the project root for full license information.
//

#pragma once
#include <string>
#include <memory> // for shared_ptr
#include "hdfs.h"
#include "parquet/util/memory.h" // for RandomAccessSource

namespace Microsoft { namespace MSR { namespace CNTK { namespace hdfs
{

    using Buffer = ::arrow::Buffer;

    class HDFSFileSystem;
    class HDFSFile;
    class HDFSFileInfo;

    typedef std::shared_ptr<HDFSFileSystem> HDFSFileSystemPtr;
    typedef std::shared_ptr<HDFSFile> HDFSFilePtr;
    typedef std::shared_ptr<HDFSFileInfo> HDFSFileInfoPtr;

    class HDFSFileSystem
    {
    public:
        HDFSFileSystem(const std::string& nameNode, int port = 9000);
        operator hdfsFS();
        HDFSFileInfoPtr ListDir(const std::string& dirName);
        HDFSFileInfoPtr GetPathInfo(const std::string& pathName);
        bool Exists(const std::string& pathName);
        void MkDir(const std::string& pathName);
        ~HDFSFileSystem();
    private:
        hdfsFS m_FS;
    };


    enum HDFSKind {HDFS_KIND_FILE = 0, HDFS_KIND_DIRECTORY};

    enum HDFSFileMode {HDFS_MODE_READ = 0, HDFS_MODE_WRITE};

    class HDFSFileInfo
    {
    public:
        HDFSFileInfo(hdfsFileInfo* info, int numEntries);
        int NumEntries();
        HDFSKind Kind(int entry = 0);    
        std::string Name(int entry = 0);
        time_t LastMod(int entry = 0);
        ssize_t Size(int entry = 0);
        short Replication(int entry = 0);
        ssize_t BlockSize(int entry = 0);
        std::string Owner(int entry = 0);
        std::string Group(int entry = 0);
        short Permissions(int entry = 0);
        time_t LastAccess(int entry = 0);
        virtual ~HDFSFileInfo();
    private:
        hdfsFileInfo* m_Info;
        int m_NumEntries;    
    };

    // This class inherits from Apache Parquet-cpp's RandomAccessSource, which is
    // defined in parquet/util/memory.h 
    class HDFSFile : public parquet::RandomAccessSource
    {
    public:
        HDFSFile(HDFSFileSystemPtr fs, const std::string& fileName, HDFSFileMode mode);
        ~HDFSFile();

        operator     hdfsFile();
        void         Seek(int64_t offset);
        int64_t      Tell();
        int64_t      Read(void* buffer, int64_t bufferSize);
        int64_t      Write(const void* buffer, int64_t bufferSize);
        void         Flush();
        HDFSFileMode Mode();

        // Overriden functions from Parquet's RandomAccessSource
        int64_t      Size();
        // Returns bytes read
        int64_t      Read(int64_t nbytes, uint8_t* out);
        std::shared_ptr<Buffer> Read(int64_t nbytes);

        // Returns bytes read
        int64_t      ReadAt(int64_t position, int64_t nbytes, uint8_t* out);
        std::shared_ptr<Buffer> ReadAt(int64_t position, int64_t nbytes);

    private:
        HDFSFileSystemPtr     m_FS;
        hdfsFile              m_File;    
        HDFSFileMode          m_Mode;
        int64_t               m_Size;
        std::vector<uint8_t>  m_Buffer;
    };

}}}}
