/**
Copyright: Copyright (c) 2018 Frank McSherry
License: MIT
Author: Joakim BrännströmJoakim Brännström (joakim.brannstrom@gmx.com)

Port of DataFrog to D.

A lightweight Datalog engine in Rust

The intended design is that one has static `Relation` types that are sets of
tuples, and `Variable` types that represent monotonically increasing sets of
tuples.

The types are mostly wrappers around `Vec<Tuple>` indicating sorted-ness, and
the intent is that this code can be dropped in the middle of an otherwise
normal Rust program, run to completion, and then the results extracted as
vectors again.
*/
module app;

version (unittest) {
} else {
    int main(string[] args) {
        return 0;
    }
}
