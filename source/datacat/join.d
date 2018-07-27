/**
Copyright: Copyright (c) 2018 Frank McSherry
License: MIT
Author: Joakim BrännströmJoakim Brännström (joakim.brannstrom@gmx.com)

Port of DataFrog to D.

Functionality for joining Variables.
*/
module datacat.join;

import logger = std.experimental.logger;
import std.traits : hasMember;

version (unittest) {
    import unit_threaded;
    import datacat : Relation, Iteration, kvTuple, relation;
}

// TODO: change Input1T and Input2T to KeyT, Val1T, Val2T.
// Add condition that logicFn(ref Key, ref Val1, ref Val2)->Result
// Add condition that OutputT!Result, the Result is the same as the return type of logicFn.
/** Perform a cross-product between `input1` and `input2` by applying `logicFn`.
 *
 * The task pool from `output` is used if the `ThreadStrategy` is parallel.
 *
 * Params:
 *  output = the result of the join
 */
private void join(alias logicFn, Input1T, Input2T, OutputT)(Input1T input1,
        Input2T input2, OutputT output) {
    import std.array : appender;

    auto results = appender!(OutputT.TT[]);

    alias fn = (k, v1, v2) { return results.put(logicFn(k, v1, v2)); };

    auto recent1 = input1.recent;
    foreach (batch; input2.stable)
        joinHelper!fn(recent1, batch);

    auto recent2 = input2.recent;
    foreach (batch; input1.stable)
        joinHelper!fn(batch, recent2);

    joinHelper!fn(recent1, recent2);

    output.insert(results.data);
}

/** Adds tuples that result from joining `input1` and `input2`.
 */
template fromJoin(Args...) if (Args.length == 1) {
    auto fromJoin(Self, I1, I2)(Self self, I1 i1, I2 i2) {
        import std.functional : unaryFun;

        alias fn_ = unaryFun!(Args[0]);
        return join!(fn_)(i1, i2, self);
    }
}

/**
 * This example starts a collection with the pairs (x, x+1) and (x+1, x) for x in 0 .. 10.
 * It then adds pairs (y, z) for which (x, y) and (x, z) are present. Because the initial
 * pairs are symmetric, this should result in all pairs (x, y) for x and y in 0 .. 11.
 */
@("shall join two variables to produce all pairs (x,y) in the provided range")
@safe unittest {
    import std.algorithm : map;
    import std.range : iota;

    // arrange
    Iteration iter;
    auto variable = iter.variable!(int, int)("source");
    variable.insert(iota(3).map!(x => kvTuple(x, x + 1)));
    // [[0,1],[1,2],[2,3],]
    variable.insert(iota(3).map!(x => kvTuple(x + 1, x)));
    // [[1,0],[2,1],[3,2],]

    // act
    while (iter.changed) {
        variable.fromJoin!((k, v1, v2) => kvTuple(v1, v2))(variable, variable);
    }

    auto result = variable.complete;

    // assert
    result.should == [[0, 0], [0, 1], [0, 2], [0, 3], [1, 0], [1, 1], [1, 2],
        [1, 3], [2, 0], [2, 1], [2, 2], [2, 3], [3, 0], [3, 1], [3, 2], [3, 3]].map!(
            a => kvTuple(a[0], a[1]));
}

/** Moves all recent tuples from `input1` that are not present in `input2` into `output`.
 *
 * The task pool from `output` is used if the `ThreadStrategy` is parallel.
 *
 * Params:
 *  output = the result of the join
 */
private void antiJoin(alias logicFn, Input1T, Input2T, OutputT)(Input1T input1,
        Input2T input2, OutputT output) {
    import std.array : appender, empty;

    auto results = appender!(OutputT.TT[]);
    auto tuples2 = input2[];

    foreach (kv; input1.recent) {
        tuples2 = tuples2.gallop!(k => k.key < kv.key);
        if (!tuples2.empty && tuples2[0].key != kv.key)
            results.put(logicFn(kv.key, kv.value));
    }

    output.insert(results.data);
}

/** Adds tuples from `input1` whose key is not present in `input2`.
 */
template fromAntiJoin(Args...) if (Args.length == 1) {
    auto fromAntiJoin(Self, I1, I2)(Self self, I1 i1, I2 i2) {
        import std.functional : unaryFun;

        alias fn_ = unaryFun!(Args[0]);
        return antiJoin!(fn_)(i1, i2, self);
    }
}

/**
 * This example starts a collection with the pairs (x, x+1) for x in 0 .. 10. It then
 * adds any pairs (x+1,x) for which x is not a multiple of three. That excludes four
 * pairs (for 0, 3, 6, and 9) which should leave us with 16 total pairs.
 */
@("shall anti-join two variables to produce only those pairs that are not multiples of 3")
@safe unittest {
    import std.algorithm : map, filter;
    import std.range : iota;

    // arrange
    Iteration iter;
    auto variable = iter.variable!(int, int)("source");
    variable.insert(iota(10).map!(x => kvTuple(x, x + 1)));
    auto relation_ = relation!(int).from(iota(10).filter!(x => x % 3 == 0)
            .map!kvTuple);

    // act
    while (iter.changed) {
        variable.fromAntiJoin!((k, v) => kvTuple(v, k))(variable, relation_);
    }

    auto result = variable.complete;

    // assert
    result.should == relation!(int, int).from([[0, 1], [1, 2], [2, 1], [2, 3],
            [3, 2], [3, 4], [4, 5], [5, 4], [5, 6], [6, 5], [6, 7], [7, 8], [8,
            7], [8, 9], [9, 8], [9, 10],]);
    //.map!(a => kvTuple(a[0], a[1]));
    result.length.should == 16;
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
