//
// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE.md file in the project root for full license information.
//

#pragma once

#include "DataDeserializerBase.h"
#include "Config.h"

#include "DataFrameConfigHelper.h"

namespace Microsoft { namespace MSR { namespace CNTK { namespace DF {

// Retry policy helper for idempotent reads, based on original util
class RetryPolicy
{
    int maxAttempts;
    int currentSleep;
    bool backoff;

public:
    RetryPolicy(int numAttempts = 5, int initialSleepMillis = 1000, bool exponentialBackoff = false)
    {
        maxAttempts = numAttempts;
        currentSleep = initialSleepMillis;
        backoff = exponentialBackoff;
    }

    template <typename F>
    void attempt(const F& body)
    {
        for (int attempt = 1;; ++attempt)
        {
            try
            {
                body();
                if (attempt > 1)
                    fprintf(stderr, "attempt: success after %d retries\n", attempt);
                break;
            }
            catch (const std::exception &e)
            {
                if (attempt >= maxAttempts)
                {
                    fprintf(stderr, "attempt: %s, giving up, attempt %d > %d...\n", e.what(), attempt, maxAttempts);
                    throw; // failed N times --give up and rethrow the error
                }
                fprintf(stderr, "attempt: %s, retrying %d-th time out of %d...\n", e.what(), attempt + 1, maxAttempts);
                ::Sleep(currentSleep); // wait a little, then try again
                if (backoff)
                {
                    currentSleep *= 2;
                }
            }
        }
    }
}

// Wrapper view for a chunk of data in memory. Given up to the randomizer.
// It is up to the randomizer to decide when to release a particular chunk, so this
// view is similar to a shared_ptr::deleter with an extra method to query for sequences
// inside the chunk.
class TabularChunk : public Chunk
{
public:
    TabularChunk(std::shared_ptr<DataFrameDeserializer> parent, ChunkIdType chunkId) :
        m_parent(parent), m_chunkId(chunkId)
    {
        auto& chunkDescription = m_parent->m_chunks[chunkId];
        // TODO: Provide knobs for retry policy
        RetryPolicy policy;
        policy.attempt([&]()
        {
            chunkDescription.RequireData(m_parent->m_featureKind, m_parent->m_ioFeatureDimension, m_parent->m_samplePeriod, m_parent->m_verbosity);
        });
    }

    // Gets data for the sequence.
    virtual void GetSequence(SequenceIdType sequenceId, vector<SequenceDataPtr>& result) override
    {
        m_parent->GetSequenceById(m_chunkId, sequenceId, result);
    }

    // Unloads the data from memory.
    ~TabularChunk()
    {
        auto& chunkDescription = m_parent->m_chunks[m_chunkId];
        chunkDescription.ReleaseData(m_parent->m_verbosity);
    }

private:
    DISABLE_COPY_AND_MOVE(TabularChunk);
    std::shared_ptr<DataFrameDeserializer> m_parent;
    ChunkIdType m_chunkId;
};

}}}}
