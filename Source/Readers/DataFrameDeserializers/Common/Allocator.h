//
// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE.md file in the project root for full license information.
//
#pragma once

#include <utility>
#include <string>
#include <vector>

#include "MemoryProvider.h"

namespace Microsoft { namespace MSR { namespace CNTK { namespace DF {

template <typename T>
class DFAlloc
{
    
public: 
    typedef T					value_type;
    typedef value_type *		pointer;
    typedef const value_type *	const_pointer;
    typedef value_type &		reference;
    typedef const value_type &	const_reference;
    typedef std::size_t			size_type;
    typedef std::ptrdiff_t		difference_type;

	
	// The whole point of this exercise: custom allocator
    // What is the plan? I see MemAllocator, MemoryProvider - what should we do?
    // Currently using MemoryProvider as it seemed intended for readers
	std::shared_ptr<MemoryProvider> customAllocator;

    // rebind allocation types (used in, for example, std::list,
	// where Nodes, rather than the list type, are allocated)
    template <typename Other>
    struct rebind
	{
        typedef DFAlloc<Other> other;
    };
    /*

    // Allocates contiguous storage for specified number of elements of provided size.
    virtual void* Alloc(size_t elementSize, size_t numberOfElements) = 0;

    // Frees contiguous storage.
    virtual void Free(void* ptr) = 0;
    */

	// Rule of 3 (move ctor/assign needed now?)
	// Explicit constructors - deny compiler implicit conversion
	explicit DFAlloc()									: customAllocator(nullptr)	   {}
    DFAlloc(const DFAlloc & other)					    : customAllocator(other.customAllocator) {}
    DFAlloc(std::shared_ptr<MemoryProvider> pAllocator) : customAllocator(pAllocator)	   {}
	// Rebinding constructor
    template <typename Other>
    DFAlloc(const DFAlloc<Other> & other) : customAllocator(other.customAllocator) {}
	// Put the dtor in there even though it's auto generated. DO NOT free the allocator, it's COM ref counted.
    ~DFAlloc() {}

	// Required methods to pull pointers from references
    pointer			address(reference		r) { return &r; }
    const_pointer	address(const_reference r) { return &r; }
 
	// Set/Get the custom allocator
	void setCustomAllocator(std::shared_ptr<MemoryProvider> pAllocator)	{ customAllocator = pAllocator; };
	std::shared_ptr<MemoryProvider> getCustomAllocator() const			{ return customAllocator; };

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
		// The syntax should be:
		// std::numeric_limits<size _type>::max() / sizeof(T)
		// but windows may have #defined max() macro, which will cause
		// a compiler error in some situations, so as a simple fix I've
		// explicitly put parentheses around the function name to cut off the
		// macro expansion
        return (std::numeric_limits<size_type>::max)() / sizeof(T);
	}

	// T construction/destruction in allocated memory
    void construct	(pointer p, const T & t) { new(p) T(t); }
    void destroy	(pointer p)				 { p->~T(); }

	// Allocation object equality - by the rules of the standard for single allocator
	bool operator==(const DFAlloc & o)	{ return customAllocator == o.customAllocator; }
    bool operator!=(const DFAlloc & a)	{ return !operator==(a); }
};

}}}}
