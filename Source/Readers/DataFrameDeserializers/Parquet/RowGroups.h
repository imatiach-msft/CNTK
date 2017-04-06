//
// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE.md file in the project root for full license information.
//
#pragma once

#include <string>
#include <vector>
#include <parquet/api/reader.h>

namespace Microsoft { namespace MSR { namespace CNTK {

// This class adds an abstraction layer on top of the Parquet Reader APIs
// It is used by the HDFSDeserializer when reading Parquet Row Groups

class RowGroupReader
{
public:

    
private:
    // Each Row group is a batch of rows in a columnar layout
    // Get the RowGroup Reader "row_group_reader = parquet_reader -> RowGroup(i)" 

    // Note:
    // Only one instance of the FileMetaData is ever created
    // and the RowGroupReader is owned by the FileReader 
    // ParquetFileReader calls RowGroup(i) to get the RowGroupReader
    // RowGroupReader calls Column(i) to get the ColumnReader
    // When RowGroup(i) is called, it returns a std::unique_ptr<RowGroupMetaData>
    // RowGroup(i) checks that the RowGroup exists, then returns contents_->GetRowGroup(i)
    // struct Contents reside in file/reader.h and the private variable contents_ reside in file/reader.h
    // GetRowGroup(i) function is defined in reader-internal.cc
    // GetRowGroup instantiates a new SerializedRowGroup and then returns a shared_ptr<RowGroupReader>
    
    // TODO:
    // When the ParquetFileReader is instantiated, this RowGroupReader class should save the pointer
    // to the RowGroupReader, which then should save the pointers to the ColumnReader. (Is this necessary? maybe save only the num_columns? Better yet, save the physical type of the columns)?
    
};

}}}
