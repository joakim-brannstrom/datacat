/**
Copyright: Copyright (c) 2018, Joakim Brännström. All rights reserved.
License: $(LINK2 http://www.boost.org/LICENSE_1_0.txt, Boost Software License 1.0)
Author: Joakim Brännström (joakim.brannstrom@gmx.com)

This file contains benchmarks of the datacat implementation.
*/
module benchmark;

import core.time;
import logger = std.experimental.logger;
import std.algorithm : map, filter;
import std.range : iota;

import unit_threaded;

import datacat;

immutable ResultFileExt = ".dat";

struct BenchResult {
    string name;
    Duration total;
    Duration lowest = Duration.max;

    this(string name) {
        this.name = name;
    }

    ~this() {
        import std.file : exists;
        import std.stdio : File;

        auto fname = name ~ ResultFileExt;

        try {
            if (!exists(fname))
                File(fname, "w").writeln(`"lowest(usec)","total(usec)"`);
            File(fname, "a").writefln(`"%s","%s"`, lowest.total!"usecs", total.total!"usecs");
        } catch (Exception e) {
            logger.error(e.msg);
        }
    }

    string toString() {
        import std.format : format;

        return format(`lowest(%s) total(%s)`, lowest, total);
    }
}

auto benchmark(alias fn)(int times, string func = __FUNCTION__) {
    import std.datetime.stopwatch : StopWatch;
    import std.typecons : Yes, RefCounted;
    import std.stdio;

    auto res = RefCounted!BenchResult(func);
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
unittest {
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

    auto r = benchmark!(bench)(10);
    logger.infof("%s %s: %s", __FUNCTION__, __LINE__, r);
}

@("perf_antijoin")
unittest {
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

    auto r = benchmark!(bench)(1000);
    logger.infof("%s %s: %s", __FUNCTION__, __LINE__, r);
}

@("perf_map")
unittest {
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

    auto r = benchmark!(bench)(100);
    logger.infof("%s %s: %s", __FUNCTION__, __LINE__, r);
}
