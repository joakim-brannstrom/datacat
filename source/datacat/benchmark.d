/**
Copyright: Copyright (c) 2018, Joakim Brännström. All rights reserved.
License: $(LINK2 http://www.boost.org/LICENSE_1_0.txt, Boost Software License 1.0)
Author: Joakim Brännström (joakim.brannstrom@gmx.com)

This file contains benchmarks of the datacat implementation.

To run a test in this module do:
---
dub test -- -s -d datacat.benchmark.perf_join datacat.benchmark.perf_antijoin datacat.benchmark.perf_map
*/
module datacat.benchmark;

version (unittest) {
} else {
    static assert(0, "This file should only be part of the target `unittest`");
}

import core.time;
import logger = std.experimental.logger;
import std.algorithm : map, filter;
import std.range : iota;

import unit_threaded;

import datacat;

struct BenchResult {
    Duration total;
    Duration lowest = Duration.max;
    string toString() {
        import std.format : format;

        return format("Benchmark: total:(%s) lowest:(%s)", total, lowest);
    }
}

BenchResult benchmark(alias fn)(int times) {
    import std.datetime.stopwatch : StopWatch;
    import std.typecons : Yes;
    import std.stdio;

    BenchResult res;
    foreach (const i; 0 .. times) {
        auto sw = StopWatch(Yes.autoStart);
        auto fnres = fn();
        sw.stop;
        if (sw.peek < res.lowest)
            res.lowest = sw.peek;
        res.total += sw.peek;
    }

    return res;
}

@("perf_join")
@HiddenTest unittest {
    auto bench() {
        // arrange
        Iteration!(int, int) iter;
        auto variable = iter.variable("source");
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

    auto r = benchmark!(bench)(10);
    logger.infof("%s %s: %s", __FUNCTION__, __LINE__, r);
}

@("perf_antijoin")
@HiddenTest unittest {
    auto bench() {
        // arrange
        Iteration!(int, int) iter;
        auto variable = iter.variable("source");
        variable.insert(relation!(int, int).from(iota(100).map!(x => kvTuple(x, x + 1))));
        auto relation_ = relation!(int).from(iota(100).filter!(x => x % 3 == 0).map!kvTuple);

        // act
        while (iter.changed) {
            static auto helper(T0, T1)(T0 k, T1 v) {
                return kvTuple(v, k);
            }

            variable.fromAntiJoin!(helper)(variable, relation_);
        }

        return variable.complete;
    }

    auto r = benchmark!(bench)(1000);
    logger.infof("%s %s: %s", __FUNCTION__, __LINE__, r);
}

@("perf_map")
@HiddenTest unittest {
    auto bench() {
        // arrange
        Iteration!(int, int) iter;
        auto variable = iter.variable("source");
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

    auto r = benchmark!(bench)(100);
    logger.infof("%s %s: %s", __FUNCTION__, __LINE__, r);
}
