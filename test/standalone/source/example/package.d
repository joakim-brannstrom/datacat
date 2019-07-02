/**
Copyright: Copyright (c) 2018, Joakim Brännström. All rights reserved.
License: $(LINK2 http://www.boost.org/LICENSE_1_0.txt, Boost Software License 1.0)
Author: Joakim Brännström (joakim.brannstrom@gmx.com)

This file contains some datalog examples and how they are implemented in datacat.
*/
module datacat_example;

import std.algorithm;
import std.range;
import std.stdio;

import datacat;

void main(string[] args) {
    static foreach (memberName; __traits(allMembers, datacat_example).only.filter!(
            a => a.startsWith("example"))) {
        writeln("Running: ", memberName);
        __traits(getMember, datacat_example, memberName)();
    }
}

private:

/**
 *
 * Schemes:
 *   SK(A,B)
 * Facts:
 *   SK('a','c').
 *   SK('b','c').
 *   SK('b','b').
 *   SK('b','c').
 * Rules:
 *   DoNothing(Z) :- Stuff(Z).
 * Queries:
 *   SK(A,'c')?
 *   SK('b','c')?
 *   SK(X,X)?
 *   SK(A,B)?
 *
 * Output:
 * SK(A,'c')? Yes(2)
 *  A='a'
 *  A='b'
 * SK('b','c')? Yes(1)
 * SK(X,X)? Yes(1)
 *  X='b'
 * SK(A,B)? Yes(3)
 *  A='a', B='c'
 *  A='b', B='b'
 *  A='b', B='c'
 */
void example1() {
    Iteration iter;
    // schema
    auto sk = iter.variable!(string, string)("SK(A,B)");
    // facts
    sk.insert([["a", "c"], ["b", "c"], ["b", "b"], ["b", "c"]]);

    // process
    while (iter.changed) {
        // rule
        static auto rule(T)(T kv) {
            return kv;
        }

        sk.fromMap!rule(sk);
    }
    auto result = sk.complete;

    // query
    writeln(`SK(A, "c")? `, result.filter!`a.value == "c"`
            .tee!writeln
            .count);
    writeln(`SK("b","c")? `, result.filter!`a.key == "b" && a.value == "c"`.count);
    writeln(`SK(X,X)? `, result.filter!"a.key == a.value"
            .tee!writeln
            .count);
    writeln(result);
    writeln(`SK(A,B)? `, result.length);
}
