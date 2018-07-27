/**
Copyright: Copyright (c) 2018 Frank McSherry
License: MIT
Author: Joakim BrännströmJoakim Brännström (joakim.brannstrom@gmx.com)

Port of DataFrog to D.

Functionality for mapping a function on a variable.
*/
module datacat.map;

import std.traits : hasMember;

version (unittest) {
    import unit_threaded;
    import datacat : Relation, ThreadStrategy;
}

// TODO: add constraint on Fn that the param is T1 returning T2.
/**
 *
 * The task pool from `output` is used if the `ThreadStrategy` is parallel.
 *
 * Params:
 *  output = the result of the join
 */
private void mapInto(alias logicFn, InputT, OutputT)(InputT input, OutputT output) {
    import std.array : appender;

    auto results = appender!(OutputT.TT[])();

    foreach (v; input.recent)
        results.put(logicFn(v));

    output.insert(results.data);
}

/** Adds tuples that result from mapping `input`.
 */
template fromMap(Args...) if (Args.length == 1) {
    auto fromMap(Self, I1)(Self self, I1 i1) if (is(Self == class)) {
        import std.functional : unaryFun;

        alias fn_ = unaryFun!(Args[0]);
        return mapInto!(fn_)(i1, self);
    }
}

/**
 * This example starts a collection with the pairs (x, x) for x in 0 .. 10. It then
 * repeatedly adds any pairs (x, z) for (x, y) in the collection, where z is the Collatz
 * step for y: it is y/2 if y is even, and 3*y + 1 if y is odd. This produces all of the
 * pairs (x, y) where x visits y as part of its Collatz journey.
 */
@("shall be the tuples that result from applying a function on the input")
@safe unittest {
    import std.algorithm : map, filter;
    import std.range : iota;
    import datacat : Iteration, kvTuple;

    // arrange
    Iteration iter;
    auto variable = iter.variable!(int, int)("source");
    variable.insert(iota(10).map!(x => kvTuple(x, x)));

    // act
    while (iter.changed) {
        variable.fromMap!((a) => a.value % 2 == 0 ? kvTuple(a.key, a.value / 2)
                : kvTuple(a.key, 3 * a.value + 1))(variable);
    }

    auto result = variable.complete;

    // assert
    result.length.should == 74;
}
