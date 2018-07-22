/**
Copyright: Copyright (c) 2018, Joakim Brännström. All rights reserved.
License: $(LINK2 http://www.boost.org/LICENSE_1_0.txt, Boost Software License 1.0)
Author: Joakim Brännström (joakim.brannstrom@gmx.com)
*/
module datacat_test.common;

import core.time : Duration;
import logger = std.experimental.logger;

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
