/**
Copyright: Copyright (c) 2018, Joakim Brännström. All rights reserved.
License: $(LINK2 http://www.boost.org/LICENSE_1_0.txt, Boost Software License 1.0)
Author: Joakim Brännström (joakim.brannstrom@gmx.com)

Port of the rust implementation in graspan1.
The dataset is downloaded from [graspan dataset](https://drive.google.com/drive/folders/0B8bQanV_QfNkbDJsOWc2WWk4SkE).

The dataformat is: [EDGE SOURCE] [EDGE DESTINATION] [EDGE VALUE].
An edge-list format.

To run this test the file http_df has to be put in the build directory.
Then execute the test with: dub test -- -s -d "shall calculate the dataflow from http_df"
*/
module datacat_test.graspan1;

import std.algorithm : filter, map, splitter;
import std.array : appender, empty, array;
import std.ascii : isWhite;
import std.conv : to;
import std.datetime.stopwatch : StopWatch;
import std.file : thisExePath;
import std.format : format;
import std.path : buildPath, baseName;
import std.range : takeExactly;
import std.stdio : writeln, writefln, File;
import std.traits : EnumMembers;
import std.typecons : Yes;

import datacat;

enum TestType {
    all,
    single,
    parallel,
}

int main(string[] args) {
    TestType test;
    if (auto ecode = parseCli(args, test))
        return ecode;

    const datafile = args[1];

    writeln("Processing ", datafile);
    auto timer = StopWatch(Yes.autoStart);

    // Make space for input data.
    auto nodes = appender!(KVTuple!(uint, uint)[])();
    auto edges = appender!(KVTuple!(uint, uint)[])();

    loadData(timer, datafile, nodes, edges);

    void runTest(TestType test) {
        final switch (test) with (TestType) {
        case all:
            runTest(TestType.single);
            runTest(TestType.parallel);
            break;
        case single:
            writeln("Single threaded");
            auto t = timer;
            t.start;
            Iteration iter;
            calculate(t, iter, nodes, edges);
            break;
        case parallel:
            writeln("Multi threaded");
            auto t = timer;
            t.start;
            auto iter = makeIteration!(ThreadStrategy.parallel);
            calculate(t, iter, nodes, edges);
            break;
        }
    }

    timer.stop;
    runTest(test);

    return 0;
}

auto myPopFront(RT)(ref RT r) {
    auto v = r.front;
    r.popFront;
    return v;
}

void loadData(T)(ref StopWatch timer, const string datafile, ref T nodes, ref T edges) {
    scope (exit)
        writefln("%s: Data loaded", timer.peek);

    // Read input data from a handy file.
    foreach (line; File(buildPath(datafile)).byLine.filter!(a => !a.empty && a[0] != '#')) {
        auto elts = line.splitter.filter!"!a.empty".takeExactly(3);
        auto src = elts.myPopFront.to!uint;
        auto dst = elts.myPopFront.to!uint;
        auto ty = elts.myPopFront;
        switch (ty) {
        case "n":
            nodes.put(kvTuple(dst, src));
            break;
        case "e":
            edges.put(kvTuple(src, dst));
            break;
        default:
            assert(0, "should not happen. Unknown type: " ~ ty);
        }
    }
}

void calculate(IterT, T)(ref StopWatch timer, ref IterT iter, T nodes, T edges) {
    auto partial = StopWatch(Yes.autoStart);

    // .. some variables, ..
    auto variable1 = iter.variable!(uint, uint)("nodes");
    auto variable2 = iter.variable!(uint, uint)("edges");

    // .. load them with some initial values, ..
    variable1.insert(nodes.data);
    variable2.insert(edges.data);

    writefln("elapsed(%s) partial(%s): Initial Variable.insert of data", timer.peek, partial.peek);

    // .. and then start iterating rules!
    while (iter.changed) {
        // N(a,c) <-  N(a,b), E(b,c)
        static auto helper(T0, T1, T2)(T0 b, T1 a, T2 c) {
            return kvTuple(c, a);
        }

        variable1.fromJoin!helper(variable1, variable2);
    }

    auto reachable = variable1.complete;
    timer.stop;
    partial.stop;
    writefln("elapsed(%s) partial(%s): Computation complete (nodes_final: %s)",
            timer.peek, partial.peek, reachable.length);
}

int parseCli(ref string[] args, ref TestType test) {
    import std.getopt;

    auto help_info = getopt(args, config.passThrough, config.keepEndOfOptions, "test",
            format("kind fo test to perform %s", [EnumMembers!TestType]), &test,);

    if (help_info.helpWanted || args.length < 2) {
        defaultGetoptPrinter(format("usage: %s [options] DATA_FILE\n",
                args[0].baseName), help_info.options);
        return 1;
    }

    return 0;
}
