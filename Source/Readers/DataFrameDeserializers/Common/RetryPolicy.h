//
// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE.md file in the project root for full license information.
//

#pragma once

namespace CNTK { namespace DF {

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
                    LOG("attempt: success after %d retries\n", attempt);
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
};

}}
