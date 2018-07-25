/**
Copyright: Copyright (c) 2018, Joakim Brännström. All rights reserved.
License: $(LINK2 http://www.boost.org/LICENSE_1_0.txt, Boost Software License 1.0)
Author: Joakim Brännström (joakim.brannstrom@gmx.com)

This file contains benchmarks of the datacat implementation.
*/
module datacat_test.benchmark;

import core.time;
import logger = std.experimental.logger;
import std.algorithm : map, filter;
import std.array : array;
import std.range : iota;
import std.traits : ReturnType;

import datacat;
import datacat_test.common;

void perf_join() {
    auto bench() {
        // arrange
        Iteration iter;
        auto variable = iter.variable!(int, int)("source");
        variable.insert(relation!(int, int).from(iota(100).map!(x => tuple(x, x + 1))));
        // [[0,1],[1,2],[2,3],]
        variable.insert(relation!(int, int).from(iota(100).map!(x => tuple(x + 1, x))));
        // [[1,0],[2,1],[3,2],]

        // act
        while (iter.changed) {
            static auto helper(T0, T1, T2)(T0 k, T1 v1, T2 v2) {
                return kvTuple(v1, v2);
            }

            variable.fromJoin!(helper)(variable, variable);
        }

        return variable.complete;
    }

    auto r = benchmark!(bench)(10, "join");
    logger.infof("%s %s: %s", __FUNCTION__, __LINE__, r);
}

void perf_parallel_join() {
    // requires 100 variables to achieve any notable speedup

    auto bench(ThreadStrategy Kind)() {
        // arrange
        auto iter = makeIteration!Kind;

        Variable!(KVTuple!(int, int), Kind)[] vars;

        foreach (i; 0 .. 10) {
            vars ~= iter.variable!(int, int)("source");
            vars[$-1].insert(relation!(int, int).from(iota(100).map!(x => tuple(x, x + 1))));
            vars[$-1].insert(relation!(int, int).from(iota(100).map!(x => tuple(x + 1, x))));
        }

        // act
        while (iter.changed) {
            static auto helper(T0, T1, T2)(T0 k, T1 v1, T2 v2) {
                return kvTuple(v1, v2);
            }

            foreach (ref v; vars)
                v.fromJoin!(helper)(v, v);
        }

        return vars.map!"a.complete".array;
    }

    auto r0 = benchmark!(bench!(ThreadStrategy.single))(10, "parallel_join_single");
    logger.infof("%s %s: %s", __FUNCTION__, __LINE__, r0);

    auto r1 = benchmark!(bench!(ThreadStrategy.parallel))(10, "parallel_join_parallel");
    logger.infof("%s %s: %s", __FUNCTION__, __LINE__, r1);
}

void perf_antijoin() {
    auto bench() {
        // arrange
        Iteration iter;
        auto variable = iter.variable!(int, int)("source");
        variable.insert(relation!(int, int).from(iota(100).map!(x => kvTuple(x, x + 1))));
        auto relation_ = relation!(int).from(iota(100).filter!(x => x % 3 == 0)
                .map!kvTuple);

        // act
        while (iter.changed) {
            static auto helper(T0, T1)(T0 k, T1 v) {
                return kvTuple(v, k);
            }

            variable.fromAntiJoin!(helper)(variable, relation_);
        }

        return variable.complete;
    }

    auto r = benchmark!(bench)(10, "antijoin");
    logger.infof("%s %s: %s", __FUNCTION__, __LINE__, r);
}

void perf_map() {
    auto bench() {
        // arrange
        Iteration iter;
        auto variable = iter.variable!(int, int)("source");
        variable.insert(relation!(int, int).from(iota(100).map!(x => kvTuple(x, x))));

        // act
        while (iter.changed) {
            static auto helper(KV)(KV a) {
                if (a.value % 2 == 0)
                    return kvTuple(a.key, a.value / 2);
                else
                    return kvTuple(a.key, 3 * a.value + 1);
            }

            variable.fromMap!(helper)(variable);
        }

        return variable.complete;
    }

    auto r = benchmark!(bench)(10, "map");
    logger.infof("%s %s: %s", __FUNCTION__, __LINE__, r);
}
