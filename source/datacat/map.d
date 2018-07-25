/**
Copyright: Copyright (c) 2018 Frank McSherry
License: MIT
Author: Joakim BrännströmJoakim Brännström (joakim.brannstrom@gmx.com)

Port of DataFrog to D.

Functionality for mapping a function on a variable.
*/
module datacat.map;

import datacat : Variable, Relation, ThreadStrategy;

// TODO: add constraint on Fn that the param is T1 returning T2.
void mapInto(alias logicFn, ThreadStrategy TS, InputT, OutputT)(InputT input, OutputT output) {
    import std.array : appender;

    auto results = appender!(OutputT.TT[])();

    foreach (v; input.recent)
        results.put(logicFn(v));

    Relation!(OutputT.TT) rel;
    rel.__ctor!(TS)(results.data);
    output.insert(rel);
}
