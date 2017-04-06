//
// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE.md file in the project root for full license information.
//
#pragma once

#include <utility>
#include <string>
#include <vector>

#include "MemoryProvider.h"

namespace CNTK {

template <typename T>
final class cntk_alloc
{
    
public: 
    typedef T                   value_type;
    typedef value_type *        pointer;
    typedef const value_type *  const_pointer;
    typedef value_type &        reference;
    typedef const value_type &  const_reference;
    typedef std::size_t         size_type;
    typedef std::ptrdiff_t      difference_type;

    
    // The whole point of this exercise: custom allocator
    // What is the plan? I see MemAllocator, MemoryProvider - what should we do?
    // Currently using MemoryProvider as it seemed intended for readers
    std::shared_ptr<MemoryProvider> customAllocator;

    // rebind allocation types (used in, for example, std::list,
    // where Nodes, rather than the list type, are allocated)
    template <typename Other>
    struct rebind
    {
        typedef cntk_alloc<Other> other;
    };

    explicit cntk_alloc()                                   : customAllocator(nullptr)     {}
    cntk_alloc(std::shared_ptr<MemoryProvider> pAllocator)  : customAllocator(pAllocator)  {}
    
    // Rule of 5 - with shared_ptr, mostly useless, but logger?
    cntk_alloc(const cntk_alloc & other)    : customAllocator(other.customAllocator) {}
    cntk_alloc(cntk_alloc && other)     : customAllocator(other.customAllocator) {}
    
    cntk_alloc& operator=(const cntk_alloc& other) {}
    cntk_alloc& operator=(cntk_alloc&& other) {}

    // Rebinding constructor
    template <typename Other>
    cntk_alloc(const cntk_alloc<Other> & other) : customAllocator(other.customAllocator) {}

    ~cntk_alloc() {}

    // Required methods to pull pointers from references
    pointer         address(reference       r) { return &r; }
    const_pointer   address(const_reference r) { return &r; }
 
    // Needed this in SSPA but maybe we can do it cleanly in CNTK
    // void setCustomAllocator(std::shared_ptr<MemoryProvider> pAllocator)  { customAllocator = pAllocator; };
    // std::shared_ptr<MemoryProvider> getCustomAllocator() const           { return customAllocator; };

    // Actual allocation routines - here's where our wrapper logic comes in
    // Second, unnamed argument is the hint for location. Let's ignore for now
    pointer allocate(size_type count, typename std::allocator<void>::const_pointer = 0)
    {
        if ( customAllocator == nullptr )
        {
            return reinterpret_cast<pointer>(::operator new(count * sizeof(T)));
        }
        return reinterpret_cast<pointer>(customAllocator->Alloc(sizeof(T), count));
    }

    void deallocate(pointer p, size_type n = 0)
    { 
        if ( customAllocator == nullptr )
        {
            return ::operator delete(p);
        }
        customAllocator->Free(p);
    }
    
    // Limit number of allocations available
    size_type max_size() const
    {
        // std::numeric_limits<size _type>::max() / sizeof(T)
        // but windows may have #defined max() macro, so
        // parentheses around the function name to cut off macro expansion
        return (std::numeric_limits<size_type>::max)() / sizeof(T);
    }

    // T construction/destruction in allocated memory
    void construct  (pointer p, const T & t) { new(p) T(t); }
    void destroy    (pointer p)              { p->~T(); }

    // Allocation object equality - by the rules of the standard for single allocator
    bool operator==(const cntk_alloc & o)   { return customAllocator == o.customAllocator; }
    bool operator!=(const cntk_alloc & a)   { return !operator==(a); }
};

}
