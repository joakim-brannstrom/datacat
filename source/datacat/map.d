/**
Copyright: Copyright (c) 2018 Frank McSherry
License: MIT
Author: Joakim BrännströmJoakim Brännström (joakim.brannstrom@gmx.com)

Port of DataFrog to D.

Functionality for mapping a function on a variable.
*/
module datacat.map;

import std.traits : hasMember;

import datacat : Relation, ThreadStrategy;

// TODO: add constraint on Fn that the param is T1 returning T2.
/**
 *
 * The task pool from `output` is used if the `ThreadStrategy` is parallel.
 *
 * Params:
 *  output = the result of the join
 */
void mapInto(alias logicFn, ThreadStrategy TS, InputT, OutputT)(InputT input, OutputT output) {
    import std.array : appender;

    auto results = appender!(OutputT.TT[])();

    foreach (v; input.recent)
        results.put(logicFn(v));

    Relation!(OutputT.TT) rel;
    static if (hasMember!(OutputT, "taskPool"))
        rel.__ctor!(TS)(results.data, output.taskPool);
    else
        static assert(0, "output (" ~ OutputT.stringof ~ ") has no member taskPool");
    output.insert(rel);
}
