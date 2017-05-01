//
// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE.md file in the project root for full license information.
//

#include <cassert>
#include "hdfs.h"
#include "HDFSFileObjects.h"

// This class inherits from Apache Parquet-cpp's RandomAccessSource, which is
// defined in util/memory.h
// All these functions assume that it has an open connection to HDFS and has
// the file open on HDFS.

namespace Microsoft { namespace MSR { namespace CNTK { namespace hdfs
{

    HDFSFileSystem::HDFSFileSystem(const std::string& nameNode, int port)
    {
        m_FS = hdfsConnect(nameNode.c_str(), port);
        if (m_FS == nullptr)
        {
            throw std::runtime_error("Could not connect to name node");
        }
        assert(m_FS);
    }


    HDFSFileSystem::~HDFSFileSystem()
    {
        if (m_FS)
        {
            hdfsDisconnect(m_FS);
            m_FS = nullptr;
        }
    }

    HDFSFileInfoPtr HDFSFileSystem::ListDir(const std::string& dirName)
    {
        int numEntries;
        hdfsFileInfo* fInfo = hdfsListDirectory(m_FS, dirName.c_str(), &numEntries);
        return std::make_shared<HDFSFileInfo>(fInfo, numEntries);
    }

    HDFSFileInfoPtr HDFSFileSystem::GetPathInfo(const std::string& pathName)
    {
        hdfsFileInfo* fInfo = hdfsGetPathInfo(m_FS, pathName.c_str());
        return std::make_shared<HDFSFileInfo>(fInfo, 1);
    }

    bool HDFSFileSystem::Exists(const std::string& pathName)
    {    
        return !hdfsExists(m_FS, pathName.c_str());
    }

    HDFSFileSystem::operator hdfsFS()
    {
        return m_FS;
    }

    void HDFSFileSystem::MkDir(const std::string& pathName)
    {
        if (hdfsCreateDirectory(m_FS, pathName.c_str()) != 0)
        {
            throw std::runtime_error("Unable to create directory.");
        }
    }

    HDFSFileInfo::HDFSFileInfo(hdfsFileInfo* info, int numEntries) : m_Info(info), m_NumEntries(numEntries)
    {

    }

    HDFSFileInfo::~HDFSFileInfo()
    {
        if (m_Info)
        {
            hdfsFreeFileInfo(m_Info, m_NumEntries);
        }
        m_Info = nullptr;
    }


    int HDFSFileInfo::NumEntries()
    {
        return m_NumEntries;
    }

    HDFSKind HDFSFileInfo::Kind(int entry /*= 0*/)
    {
        return m_Info[entry].mKind == 'F' ? HDFS_KIND_FILE : HDFS_KIND_DIRECTORY;
    }

    std::string HDFSFileInfo::Name(int entry /*= 0*/)
    {
        return m_Info[entry].mName;
    }

    time_t HDFSFileInfo::LastMod(int entry /*= 0*/)
    {
        return m_Info[entry].mLastMod;
    }

    short HDFSFileInfo::Replication(int entry /*= 0*/)
    {
        return m_Info[entry].mReplication;
    }

    ssize_t HDFSFileInfo::BlockSize(int entry /*= 0*/)
    {
        return m_Info[entry].mBlockSize;
    }

    std::string HDFSFileInfo::Owner(int entry /*= 0*/)
    {
        return m_Info[entry].mOwner;
    }

    std::string HDFSFileInfo::Group(int entry /*= 0*/)
    {
        return m_Info[entry].mGroup;
    }

    short HDFSFileInfo::Permissions(int entry /*= 0*/)
    {
        return m_Info[entry].mPermissions;
    }

    time_t HDFSFileInfo::LastAccess(int entry /*= 0*/)
    {
        return m_Info[entry].mLastAccess;
    }

    ssize_t HDFSFileInfo::Size( int entry /*= 0*/ )
    {
        return m_Info[entry].mSize;
    }

    HDFSFile::HDFSFile(HDFSFileSystemPtr fs, const std::string& fileName, HDFSFileMode mode) : m_FS(fs), m_Mode(mode)
    {
        if (mode == HDFS_MODE_READ && !fs->Exists(fileName))
        {
            throw std::runtime_error("File not found.");
        }

        int flags = mode == HDFS_MODE_READ ? O_RDONLY : O_WRONLY;
        m_File = hdfsOpenFile(*m_FS, fileName.c_str(), flags, 0, 0, 0);
        assert(m_File);
        m_Size = m_FS->GetPathInfo(fileName)->Size();
    }

    HDFSFile::~HDFSFile()
    {
        if (m_File)
        {
            hdfsCloseFile(*m_FS, m_File);
        }
        m_File = nullptr;
    }

    void HDFSFile::Close() 
    {
        if (m_File)
        {
            hdfsCloseFile(*m_FS, m_File);
        }
        m_File = nullptr;
    }

    // Functions inherited from Parquet-cpp's RandomAccessSource class
    int64_t HDFSFile::Size() const
    {
        return m_Size;
    }

    void HDFSFile::Seek(int64_t offset)
    {
        if (hdfsSeek(*m_FS, m_File, static_cast<tOffset>(offset)) != 0)
        {
            throw std::runtime_error("Seek failed.");
        }
    }

    int64_t HDFSFile::Tell()
    {
        auto offset = hdfsTell(*m_FS, m_File);
        if (offset < 0)
        {
            throw std::runtime_error("Tell failed.");
        }
        return static_cast<int64_t> (offset);  
    }

    // Return bytes read
    int64_t HDFSFile::Read(int64_t nbytes, uint8_t* out)
    {
        // clear out the vector before reading data
        int64_t totalBytesRead = 0;
        char* charBuffer = reinterpret_cast<char*> (out);
        int64_t availBuffLen = nbytes;
        int64_t bytesRead = 0;
        while (bytesRead < availBuffLen) {
            // increment totalBytesRead by the number of bytes read
            totalBytesRead += bytesRead;
            // move the current position in the buffer
            charBuffer += bytesRead;
            // decrement availBuffLen by bytesRead
            availBuffLen -= bytesRead;
            bytesRead = hdfsRead(*m_FS, m_File, charBuffer, availBuffLen);
            if (bytesRead < 0) {
                throw std::runtime_error("Read failed.");
            }
            if (bytesRead == 0) break; // Reached EOF
        }
        return totalBytesRead;
    }

    // Reads a number of bytes from an HDFSFile and returns a shared_ptr<::arrow::Buffer>
    std::shared_ptr<Buffer> HDFSFile::Read(int64_t nbytes)
    {
        uint8_t* buffer = new uint8_t[nbytes];
        Read(nbytes, buffer);
        return std::shared_ptr<Buffer>(new Buffer(buffer, nbytes), [&](Buffer* b) { printf("Returning shared_ptr<Buffer> from Read"); delete[] buffer; delete b; }); // TODO: Maybe return nbytes?
    }

    int64_t HDFSFile::ReadAt(int64_t position, int64_t nbytes, uint8_t* out)
    {
        // First, seek to the given position within the HDFS File
        Seek(position);
        // Then, read nbytes and return the total number of bytes read.
        return Read(nbytes, out);
    }

    std::shared_ptr<Buffer> HDFSFile::ReadAt(int64_t position, int64_t nbytes) {
        Seek(position);
        return Read(nbytes);
    }

    int64_t HDFSFile::Read(void* buffer, int64_t bufferSize)
    {
        tSize bytesRead = 0;
        char* charBuffer = reinterpret_cast<char*> (buffer);
        tSize buffLength = static_cast<tSize> (bufferSize);
        while(bytesRead < buffLength)
        {
            // Increment the buffer
            charBuffer += bytesRead;
            buffLength -= bytesRead;
            bytesRead   = hdfsRead(*m_FS, m_File, charBuffer, buffLength);
            if (bytesRead < 0)
            {
                throw std::runtime_error("Read failed.");
            }
        }
        return bufferSize;
    }

    HDFSFileMode HDFSFile::Mode()
    {
        return m_Mode;
    }

    int64_t HDFSFile::Write(const void* buffer, int64_t bufferSize)
    {
        auto bytesWritten = hdfsWrite(*m_FS, m_File, buffer, static_cast<tSize>(bufferSize));
        if (bytesWritten < 0)
        {
            throw std::runtime_error("Write failed.");
        }
        return static_cast<int64_t> (bytesWritten);
    }

    void HDFSFile::Flush()
    {
        if (hdfsFlush(*m_FS, m_File) != 0)
        {
            throw std::runtime_error("Flush failed.");
        }
    }
}}}}
