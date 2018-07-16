/**
Copyright: Copyright (c) 2018, Joakim Brännström. All rights reserved.
License: $(LINK2 http://www.boost.org/LICENSE_1_0.txt, Boost Software License 1.0)
Author: Joakim Brännström (joakim.brannstrom@gmx.com)

Range API for various data structures.
*/
module datacat.range;

/// Range API for an `Iterator`.
struct IteratorRange(T) {
    private {
        T* iter;
        bool lastChange = true;
    }

    bool front() {
        assert(!empty, "Can't get front of an empty range");
        return lastChange;
    }

    void popFront() {
        assert(!empty, "Can't pop front of an empty range");
        lastChange = iter.changed;
    }

    bool empty() {
        return !lastChange;
    }
}
