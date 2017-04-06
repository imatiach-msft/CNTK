//
// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE.md file in the project root for full license information.
//
// DataReader.cpp : Defines the exported functions for the DLL application.
//

#include "stdafx.h"
#include "Basics.h"

#define DATAREADER_EXPORTS
#include "DataReader.h"
#include "Config.h"
#include "ReaderShim.h"
#include "HTKMLFReader.h"
#include "HeapMemoryProvider.h"
#include "HTKDataDeserializer.h"
#include "MLFDataDeserializer.h"
#include "StringUtil.h"

// hdfs-specific
#include "Constants.h"


namespace Microsoft { namespace MSR { namespace CNTK { namespace hdfs {

// TODO: Not safe from the ABI perspective. Will be uglified to make the interface ABI.
extern "C" DATAREADER_API bool CreateDeserializer(IDataDeserializer** deserializer, const std::wstring& type, const ConfigParameters& deserializerConfig, CorpusDescriptorPtr corpus,  bool primary)
{
    if (type == CLASS_TYPE_NAME)
    {
        *deserializer = new HDFSDeserializer(corpus, deserializerConfig, primary);
    }
    else
    {
        // Unknown type.
        return false;
    }

    // Deserializer created.
    return true;
}

}}}}
