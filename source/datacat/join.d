/**
Copyright: Copyright (c) 2018 Frank McSherry
License: MIT
Author: Joakim BrännströmJoakim Brännström (joakim.brannstrom@gmx.com)

Port of DataFrog to D.

Functionality for joining Variables.
*/
module datacat.join;

import logger = std.experimental.logger;
import std.traits;

import datacat : Variable, Relation, hasKeyField, hasValueField,
    hasKeyValueFields;

// TODO: change Input1T and Input2T to KeyT, Val1T, Val2T.
// Add condition that logicFn(ref Key, ref Val1, ref Val2)->Result
// Add condition that OutputT!Result, the Result is the same as the return type of logicFn.
/** TODO: add description
 * Params:
 *  output = the result of the cross product between input1 and input2 by applying logicFn
 */
void join(alias logicFn, Input1T, Input2T, OutputT)(Input1T input1, Input2T input2, OutputT output) {
    import std.array : appender;

    auto results = appender!(Input1T.TT[]);

    alias fn = (k, v1, v2) { return results.put(logicFn(k, v1, v2)); };

    auto recent1 = input1.recent;
    foreach (batch; input2.stable)
        joinHelper!fn(recent1, batch);

    auto recent2 = input2.recent;
    foreach (batch; input1.stable)
        joinHelper!fn(batch, recent2);

    joinHelper!fn(recent1, recent2);

    output.insert(results.data.Relation!(Input1T.TT));
}

/// Moves all recent tuples from `input1` that are not present in `input2` into `output`.
void antiJoin(alias logicFn, Input1T, Input2T, OutputT)(Input1T input1,
        Input2T input2, OutputT output) {
    import std.array : appender, empty;

    auto results = appender!(Input1T.TT[]);
    auto tuples2 = input2[];

    foreach (kv; input1.recent) {
        tuples2 = tuples2.gallop!(k => k.key < kv.key);
        if (!tuples2.empty && tuples2[0].key != kv.key)
            results.put(logicFn(kv.key, kv.value));
    }

    output.insert(results.data.Relation!(Input1T.TT));
}

// TODO: add constraint for CmpT, Fn(&T)->bool
SliceT gallop(alias cmp, SliceT)(SliceT slice) {
    // if empty slice, or already >= element, return
    if (slice.length > 0 && cmp(slice[0])) {
        auto step = 1;
        while (step < slice.length && cmp(slice[step])) {
            slice = slice[step .. $];
            // TODO: add check so this doesn't overflow.
            step = step << 1;
        }

        step = step >> 1;
        while (step > 0) {
            if (step < slice.length && cmp(slice[step])) {
                slice = slice[step .. $];
            }
            // TODO: add check so this doesn't overflow.
            step = step >> 1;
        }

        slice = slice[1 .. $]; // advance one, as we always stayed < value
    }

    return slice;
}

private:

/**
 * Params:
 *  logicFn = call repeatedly with key, value1, value2.
 */
void joinHelper(alias logicFn, Slice1T, Slice2T)(ref Slice1T slice1, ref Slice2T slice2) {
    import std.algorithm : count, until;

    while (!slice1.empty && !slice2.empty) {
        auto e1 = slice1[0];
        auto e2 = slice2[0];

        if (e1.key < e2.key) {
            slice1 = slice1.gallop!((x) { return x.key < e2.key; });
        } else if (e1.key > e2.key) {
            slice2 = slice2.gallop!((x) { return x.key < e1.key; });
        } else {
            // Determine the number of matching keys in each slice.
            const cnt1 = slice1.until!(x => x.key != e1.key).count;
            const cnt2 = slice2.until!(x => x.key != e2.key).count;

            // Produce results from the cross-product of matches.
            foreach (index1; 0 .. cnt1) {
                foreach (index2; 0 .. cnt2) {
                    logicFn(e1.key, slice1[index1].value, slice2[index2].value);
                }
            }

            // Advance slices past this key.
            slice1 = slice1[cnt1 .. $];
            slice2 = slice2[cnt2 .. $];
        }
    }
}
