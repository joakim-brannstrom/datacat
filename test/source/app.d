/**
Copyright: Copyright (c) 2018, Joakim Brännström. All rights reserved.
License: $(LINK2 http://www.boost.org/LICENSE_1_0.txt, Boost Software License 1.0)
Author: Joakim Brännström (joakim.brannstrom@gmx.com)
*/
module app;

enum Name {
    perf_join,
    perf_antijoin,
    perf_map,
}

int main(string[] args) {
    import std.algorithm : map;
    import std.array : array;
    import std.conv : to;
    import std.traits : EnumMembers;
    import std.stdio : writeln, writefln;

    const run_bench = () {
        if (args.length == 1)
            return [EnumMembers!Name];
        try {
            return args[1 .. $].map!(a => a.to!Name).array;
        } catch (Exception e) {
            writeln(e.msg);
            writefln("Valid benchmarks are: %s", [EnumMembers!Name]);
        }

        return null;
    }();

    alias Fn = void function();
    Fn[Name] bns;

    static import datacat_test.benchmark;

    bns[Name.perf_join] = &datacat_test.benchmark.perf_join;
    bns[Name.perf_antijoin] = &datacat_test.benchmark.perf_antijoin;
    bns[Name.perf_map] = &datacat_test.benchmark.perf_map;

    foreach (b; run_bench) {
        writeln("Running ", b);
        if (auto f = b in bns)
            (*f)();
        else
            writefln("No benchmark registered");
    }

    return 0;
}
