/**
Copyright: Copyright (c) 2018 Frank McSherry
License: MIT
Author: Joakim BrännströmJoakim Brännström (joakim.brannstrom@gmx.com)

Port of DataFrog to D.

A lightweight Datalog engine in Rust

The intended design is that one has static `Relation` types that are sets of
tuples, and `Variable` types that represent monotonically increasing sets of
tuples.

The types are mostly wrappers around `Vec<Tuple>` indicating sorted-ness, and
the intent is that this code can be dropped in the middle of an otherwise
normal Rust program, run to completion, and then the results extracted as
vectors again.
*/
module datacat;

import logger = std.experimental.logger;
import std.traits : hasMember;
import std.typecons : Tuple;
import std.parallelism : TaskPool, taskPool;

public import datacat.range;

version (unittest) {
    import std.typecons : tuple;
    import unit_threaded;
}

alias KVTuple(K, V) = Tuple!(K, "key", V, "value");
alias KVTuple(K) = Tuple!(K, "key");

/// Convenient function for creating a key/value tuple.
auto kvTuple(K, V)(auto ref K k, auto ref V v) {
    return KVTuple!(K, V)(k, v);
}

/// ditto
auto kvTuple(K)(auto ref K k) {
    return KVTuple!(K)(k);
}

/// A static, ordered list of key-value pairs.
///
/// A relation represents a fixed set of key-value pairs. In many places in a
/// Datalog computation we want to be sure that certain relations are not able
/// to vary (for example, in antijoins).
struct Relation(TupleT) {
    import std.range : isInputRange, ElementType;

    /// Convenient alias to retrieve the tuple type.
    alias TT = TupleT;

    /// Sorted list of distinct tuples.
    TupleT[] elements;
    alias elements this;

    /** Convenient function to create a `Relation` from an array containing
     * values with the length 2.
     *
     * Returns: a `Relation`.
     */
    static auto from(T, ThreadStrategy TS = ThreadStrategy.single)(T values) {
        import std.algorithm : map;

        static if (hasKeyValueFields!TupleT) {
            return Relation!(TupleT)(values.map!(a => TupleT(a[0], a[1])));
        } else static if (hasKeyField!TupleT) {
            return Relation!(TupleT)(values.map!(a => TupleT(a)));
        } else {
            static assert(0,
                    "Mismatch between Relations (" ~ TupleT.stringof
                    ~ ") and provided type " ~ T.stringof);
        }
    }

    /** Create an instance.
     *
     * If the input is sorted no further sorting is done.
     *
     * Params:
     *  other = the source to pull elements from.
     */
    this(ThreadStrategy TS = ThreadStrategy.single, T, ARGS...)(T other, auto ref ARGS args)
            if (isInputRange!T && is(ElementType!T == TupleT)
                && (TS == ThreadStrategy.parallel && ARGS.length == 1
                && is(ARGS[0] == TaskPool) || TS == ThreadStrategy.single)) {
        import std.algorithm : copy, sort, until, uniq;
        import std.array : appender;
        import std.range : SortedRange, hasLength;

        auto app = appender!(TupleT[])();

        static if (hasLength!T)
            app.reserve(other.length);

        other.copy(app);

        static if (is(T : SortedRange!U, U)) {
            // do nothing
        } else static if (TS == ThreadStrategy.parallel) {
            // code copied from std.parallelism
            static void parallelSort(T)(T[] data, TaskPool pool) {
                import std.parallelism : task;
                import std.algorithm : swap, partition;

                // Sort small subarrays serially.
                if (data.length < 100) {
                    static import std.algorithm;

                    std.algorithm.sort(data);
                    return;
                }

                // Partition the array.
                swap(data[$ / 2], data[$ - 1]);
                auto pivot = data[$ - 1];
                bool lessThanPivot(T elem) {
                    return elem < pivot;
                }

                auto greaterEqual = partition!lessThanPivot(data[0 .. $ - 1]);
                swap(data[$ - greaterEqual.length - 1], data[$ - 1]);

                auto less = data[0 .. $ - greaterEqual.length - 1];
                greaterEqual = data[$ - greaterEqual.length .. $];

                // Execute both recursion branches in parallel.
                auto recurseTask = task!parallelSort(greaterEqual, pool);
                pool.put(recurseTask);
                parallelSort(less, pool);
                recurseTask.yieldForce;
            }

            TaskPool pool = args[0];
            parallelSort(app.data, pool);
        } else {
            sort(app.data);
        }

        elements.length = app.data.length;
        elements.length -= app.data.uniq.copy(elements).length;
    }

    /// Merges two relations into their union.
    auto merge(T)(T other)
            if (hasMember!(T, "elements") && is(ElementType!(typeof(other.elements)) == TupleT)) {
        import std.algorithm : until;
        import std.array : appender, empty, popFront, front;

        // If one of the element lists is zero-length, we don't need to do any work
        if (elements.length == 0) {
            elements = other.elements;
            return this;
        } else if (other.elements.length == 0) {
            return this;
        }

        auto elements2 = other.elements;

        // Make sure that elements starts with the lower element
        if (elements[0] > elements2[0]) {
            elements2 = elements;
            elements = other.elements;
        }

        // Fast path for when all the new elements are after the exiting ones
        if (elements[$ - 1] < elements2[0]) {
            elements ~= elements2;
            return this;
        }

        const len = elements.length;

        auto elem = appender!(TupleT[])();
        elem.reserve(len + elements2.length);

        elem.put(elements[0]);
        elements.popFront;
        if (elem.data[0] == elements2[0]) {
            elements2.popFront;
        }

        foreach (e; elements) {
            foreach (e2; elements2[].until!(a => a >= e)) {
                elem.put(e2);
                elements2.popFront;
            }
            foreach (_; elements2[].until!(a => a > e))
                elements2.popFront;
            elem.put(e);
        }

        // Finish draining second list
        foreach (e; elements2)
            elem.put(e);

        elements = elem.data;
        return this;
    }

    bool empty() const {
        return elements.length == 0;
    }

    void clear() {
        elements = null;
    }

    import std.range : isOutputRange;
    import std.format : FormatSpec;

    string toString() {
        import std.exception : assumeUnique;
        import std.format : FormatSpec;

        char[] buf;
        buf.reserve(100);
        auto fmt = FormatSpec!char("%s");
        toString((const(char)[] s) { buf ~= s; }, fmt);
        auto trustedUnique(T)(T t) @trusted {
            return assumeUnique(t);
        }

        return trustedUnique(buf);
    }

    void toString(Writer, Char)(scope Writer w, FormatSpec!Char fmt) const {
        import std.format : formattedWrite;
        import std.range.primitives : put;

        put(w, "[");
        foreach (e; elements) {
            static if (__traits(hasMember, TT, "value"))
                formattedWrite(w, "[%s,%s], ", e.key, e.value);
            else
                formattedWrite(w, "[%s], ", e.key);
        }
        put(w, "]");
    }
}

/// Create a Relation type with a tuple of the provided types (`Args`).
template relation(Args...) {
    import std.typecons : Tuple;
    import std.variant : Variant;

    static if (Args.length == 1) {
        alias relation = Relation!(Tuple!(Args[0], "key"));
    } else static if (Args.length == 2) {
        alias relation = Relation!(Tuple!(Args[0], "key", Args[1], "value"));
    } else {
        import std.conv : to;

        static assert(0, "1 or 2 parameters required. " ~ Args.length.to!string ~ " provided");
    }
}

@("shall create a Relation using the parallel sort strategy")
unittest {
    import std.algorithm : map;

    Relation!(KVTuple!(int, int)) a;
    a.__ctor!(ThreadStrategy.parallel)([kvTuple(1, 2)].map!"a", taskPool);
}

@("shall merge two relations")
unittest {
    auto a = relation!(int, int).from([[1, 0], [2, -1], [5, -20]]);
    auto b = relation!(int, int).from([[3, 0], [5, -10], [7, -20]]);

    a.merge(b);
    a.should == [tuple(1, 0), tuple(2, -1), tuple(3, 0), tuple(5, -20),
        tuple(5, -10), tuple(7, -20)];
}

@("shall create a sorted relation from unsorted elements")
unittest {
    auto a = relation!(int, int).from([[3, -1], [2, -3], [5, -24]]);
    a.should == [tuple(2, -3), tuple(3, -1), tuple(5, -24)];
}

/// True iff `T` has a key field.
package enum hasKeyField(T) = hasMember!(T, "key");

/// True iff `T` has a value field.
package enum hasValueField(T) = hasMember!(T, "value");

/// True iff `T` has the fields key and value.
package enum hasKeyValueFields(T) = hasKeyField!T && hasValueField!T;

/// True iff `T0` and `T1` keys are the same type.
package enum isSameKeyType(T0, T1) = hasKeyField!T0 && hasKeyField!T1
        && is(typeof(T0.key) == typeof(T1.key));

@("shall check that the keys are the same type")
unittest {
    auto a0 = kvTuple(1, "a0");
    auto a1 = kvTuple(2, "a1");

    static if (!isSameKeyType!(typeof(a0), typeof(a1)))
        static assert(0, "isSameKeyType: expected to pass because the key types are the same");

    auto b0 = kvTuple(3, "b0");
    auto b1 = kvTuple(1.1, "b1");

    static if (isSameKeyType!(typeof(b0), typeof(b1)))
        static assert(0, "isSameKeyType: expected to fail because the key types are different");
}

/// A type that can report on whether it has changed.
/// changed = Reports whether the variable has changed since it was last asked.
package enum isVariable(T) = is(T : VariableTrait);

enum ThreadStrategy {
    single,
    parallel
}

alias Iteration = IterationImpl!(ThreadStrategy.single);
alias ParallelIteration = IterationImpl!(ThreadStrategy.parallel);

/** Make an `Iteration`.
 *
 * It affects all `Variable`s created through the `variable` method.
 *
 * Returns: a parallel `Iteration`
 */
auto makeIteration(ThreadStrategy Kind)(TaskPool tpool = null) {
    import std.parallelism : taskPool;

    static if (Kind == ThreadStrategy.parallel) {
        return ParallelIteration(() {
            if (tpool is null)
                return taskPool;
            else
                return tpool;
        }());
    } else
        return Iteration();
}

/// An iterative context for recursive evaluation.
///
/// An `Iteration` tracks monotonic variables, and monitors their progress.
/// It can inform the user if they have ceased changing, at which point the
/// computation should be done.
struct IterationImpl(ThreadStrategy Kind) {
    static if (Kind == ThreadStrategy.parallel) {
        TaskPool taskPool;
        invariant {
            assert(taskPool !is null, "the taskpool is required to be initialized for a parallel");
        }
    }

    VariableTrait[] variables;

    /// Reports whether any of the monitored variables have changed since
    /// the most recent call.
    bool changed() {
        import std.algorithm : map;

        bool r = false;
        foreach (a; variables.map!"a.changed") {
            if (a)
                r = true;
        }

        return r;
        //TODO why didnt this work?
        //return variables.reduce!((a, b) => a.changed || b.changed);
    }

    /// Creates a new named variable associated with the iterative context.
    scope auto variable(T0, T1)(string s) {
        static if (Kind == ThreadStrategy.single) {
            auto v = new Variable!(KVTuple!(T0, T1), Kind)(s);
        } else {
            auto v = new Variable!(KVTuple!(T0, T1), Kind)(s, taskPool);
        }
        variables ~= v;
        return v;
    }

    /// Creates a new named variable associated with the iterative context.
    ///
    /// This variable will not be maintained distinctly, and may advertise tuples as
    /// recent multiple times (perhaps unbounded many times).
    scope auto variableInDistinct(T0, T1)(string s) {
        static if (Kind == ThreadStrategy.single) {
            auto v = new Variable!(KVTuple!(T0, T1), Kind)(s);
        } else {
            auto v = new Variable!(KVTuple!(T0, T1), Kind)(s, taskPool);
        }
        v.distinct = false;
        return v;
    }

    /// Returns: a range that continue until all variables stop changing.
    IteratorRange!(typeof(this)) range() {
        return typeof(return)(&this);
    }
}

/// A type that has a key and value member.
enum isTuple(T) = hasMember!(T, "key") && hasMember!(T, "value");

/// A type that can report on whether it has changed.
interface VariableTrait {
    /// Reports whether the variable has changed since it was last asked.
    bool changed();
}

/// An monotonically increasing set of `Tuple`s.
///
/// There are three stages in the lifecycle of a tuple:
///
///   1. A tuple is added to `this.toAdd`, but is not yet visible externally.
///   2. Newly added tuples are then promoted to `this.recent` for one iteration.
///   3. After one iteration, recent tuples are moved to `this.stable` for posterity.
///
/// Each time `this.changed()` is called, the `recent` relation is folded into `stable`,
/// and the `toAdd` relations are merged, potentially deduplicated against `stable`, and
/// then made  `recent`. This way, across calls to `changed()` all added tuples are in
/// `recent` at least once and eventually all are in `stable`.
///
/// A `Variable` may optionally be instructed not to de-duplicate its tuples, for reasons
/// of performance. Such a variable cannot be relied on to terminate iterative computation,
/// and it is important that any cycle of derivations have at least one de-duplicating
/// variable on it.
/// TODO: tuple should be constrainted to something with Key/Value.
// dfmt off
final class Variable(TupleT, ThreadStrategy TS = ThreadStrategy.single) : VariableTrait if (isTuple!TupleT) {
    import std.range : isInputRange, ElementType, isOutputRange;

    TaskPool taskPool;

    /// Convenient aliases to retrieve properties about the variable
    alias TT = TupleT;
    alias This = typeof(this);
    alias ThisTS = TS;

    version (unittest) {
        /// Used for testing purpose to ensure both paths produce the same result.
        bool forceFastPath;
    }

    /// Should the variable be maintained distinctly.
    bool distinct = true;

    /// A useful name for the variable.
    string name;

    /// A list of relations whose union are the accepted tuples.
    Relation!TupleT[] stable;

    /// A list of recent tuples, still to be processed.
    Relation!TupleT recent;

    /// A list of future tuples, to be introduced.
    Relation!TupleT[] toAdd;

    static if (TS == ThreadStrategy.single) {
        this() {
        }

        this(string name) {
            this.name = name;
        }
    }

    this(TaskPool tp) {
        static if (TS == ThreadStrategy.parallel)
            this.taskPool = tp;
    }

    this(string name, TaskPool tp) {
        this.name = name;
        static if (TS == ThreadStrategy.parallel)
            this.taskPool = tp;
    }

    /// Inserts a relation into the variable.
    ///
    /// This is most commonly used to load initial values into a variable.
    /// it is not obvious that it should be commonly used otherwise, but
    /// it should not be harmful.
    void insert(Relation!TupleT relation) {
        if (!relation.empty) {
            toAdd ~= relation;
        }
    }

    /// ditto
    void insert(TupleT[] relation) {
        if (relation.length != 0) {
            toAdd ~= Relation!TupleT(relation);
        }
    }

    /// ditto
    void insert(T)(T relation) if (isInputRange!T && is(ElementType!T == TupleT)) {
        if (relation.length != 0) {
            toAdd ~= Relation!TupleT(relation);
        }
    }

    /// ditto
    void insert(T)(T[][] relation) {
        import std.algorithm : map;

        if (relation.length == 0)
            return;

        toAdd ~= relation.map!(a => KVTuple!(T,T)(a[0], a[1])).Relation!(KVTuple!(T,T));
    }

    /// Consumes the variable and returns a relation.
    ///
    /// This method removes the ability for the variable to develop, and
    /// flattens all internal tuples down to one relation. The method
    /// asserts that iteration has completed, in that `self.recent` and
    /// `self.to_add` should both be empty.
    Relation!TupleT complete() {
        import std.array : empty;

        assert(recent.empty);
        assert(toAdd.empty);

        typeof(return) result;
        foreach (batch; stable) {
            result.merge(batch);
        }
        stable = null;

        return result;
    }

    override bool changed() {
        import std.array : popBack, back, appender, empty;

        // 1. Merge self.recent into self.stable.
        if (!recent.empty) {
            while (!stable.empty && stable.back.length <= 2 * recent.length) {
                auto last = stable[$ - 1];
                stable.popBack;
                recent.merge(last);
            }
            stable ~= recent;
            recent.clear;
        }

        if (toAdd.empty)
            return false;

        // 2. Move this.toAdd into this.recent.
        auto to_add = () { auto a = toAdd[$ - 1]; toAdd.popBack; return a; }();

        // 2b. merge the rest of this.toAdd into to_add
        foreach (to_add_more; toAdd) {
            to_add.merge(to_add_more);
        }
        toAdd = null;

        // 2c. Restrict `to_add` to tuples not in `self.stable`.
        if (distinct) {
            foreach (batch; stable) {
                auto retained = appender!(TupleT[])();
                void fastPath() {
                    foreach (elem; to_add) {
                        import datacat.join : gallop;

                        batch = batch.gallop!(y => y < elem);
                        if (batch.length == 0 || batch[0] != elem)
                            retained.put(elem);
                    }
                }

                void slowPath() {
                    version (unittest) {
                        if (forceFastPath) {
                            fastPath;
                            return;
                        }
                    }

                    foreach (elem; to_add) {
                        while (batch.length > 0 && batch[0] < elem) {
                            batch = batch[1 .. $];
                        }
                        if (batch.length == 0 || batch[0] != elem)
                            retained.put(elem);
                    }
                }

                if (batch.length > 4 * to_add.length) {
                    fastPath;
                } else {
                    slowPath;
                }
                to_add = retained.data;
            }
        }
        recent = to_add;

        return !recent.empty;
    }

    /// Returns: total elements in stable.
    size_t countStable() {
        import std.algorithm : map, sum;

        return stable.map!"a.length".sum;
    }

    void toString(Writer)(ref Writer w) if (isOutputRange!(Writer, char)) {
        import std.format : formattedWrite;

        if (!name.empty)
            formattedWrite(w, "name:%s ", name);
        formattedWrite(w, "distinc:%s stable:%s recent:%s toAdd:%s", distinct,
                stable, recent, toAdd);
    }
}
// dfmt on

/** Adds tuples that result from joining `input1` and `input2`.
 */
template fromJoin(Args...) if (Args.length == 1) {
    auto fromJoin(Self, I1, I2)(Self self, I1 i1, I2 i2) {
        import std.functional : unaryFun;
        static import datacat.join;

        alias fn_ = unaryFun!(Args[0]);
        return datacat.join.join!(fn_, Self.ThisTS)(i1, i2, self);
    }
}

/**
 * This example starts a collection with the pairs (x, x+1) and (x+1, x) for x in 0 .. 10.
 * It then adds pairs (y, z) for which (x, y) and (x, z) are present. Because the initial
 * pairs are symmetric, this should result in all pairs (x, y) for x and y in 0 .. 11.
 */
@("shall join two variables to produce all pairs (x,y) in the provided range")
unittest {
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

/** Adds tuples from `input1` whose key is not present in `input2`.
 */
template fromAntiJoin(Args...) if (Args.length == 1) {
    auto fromAntiJoin(Self, I1, I2)(Self self, I1 i1, I2 i2) {
        import std.functional : unaryFun;
        static import datacat.join;

        alias fn_ = unaryFun!(Args[0]);
        return datacat.join.antiJoin!(fn_, Self.ThisTS)(i1, i2, self);
    }
}

/**
 * This example starts a collection with the pairs (x, x+1) for x in 0 .. 10. It then
 * adds any pairs (x+1,x) for which x is not a multiple of three. That excludes four
 * pairs (for 0, 3, 6, and 9) which should leave us with 16 total pairs.
 */
@("shall anti-join two variables to produce only those pairs that are not multiples of 3")
unittest {
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

/** Adds tuples that result from mapping `input`.
 */
template fromMap(Args...) if (Args.length == 1) {
    auto fromMap(Self, I1)(Self self, I1 i1) {
        import std.functional : unaryFun;
        static import datacat.map;

        alias fn_ = unaryFun!(Args[0]);
        return datacat.map.mapInto!(fn_, Self.ThisTS)(i1, self);
    }
}

/**
 * This example starts a collection with the pairs (x, x) for x in 0 .. 10. It then
 * repeatedly adds any pairs (x, z) for (x, y) in the collection, where z is the Collatz
 * step for y: it is y/2 if y is even, and 3*y + 1 if y is odd. This produces all of the
 * pairs (x, y) where x visits y as part of its Collatz journey.
 */
@("shall be the tuples that result from applying a function on the input")
unittest {
    import std.algorithm : map, filter;
    import std.range : iota;

    // arrange
    Iteration iter;
    auto variable = iter.variable!(int, int)("source");
    variable.insert(iota(10).map!(x => kvTuple(x, x)));

    // act
    while (iter.changed) {
        variable.fromMap!((a) => a.value % 2 == 0 ? kvTuple(a.key, a.value / 2)
                : kvTuple(a.key, 3 * a.value + 1))(variable);
    }

    auto result = variable.complete;

    // assert
    result.length.should == 74;
}

/// Create a Variable type with a tuple of the provided types (`Args`).
template Variable(Args...) {
    import std.typecons : Tuple;
    import std.variant : Variant;

    static if (Args.length == 1) {
        alias Variable = Variable!(Tuple!(Args[0], "key"));
    } else static if (Args.length == 2) {
        alias Variable = Variable!(Tuple!(Args[0], "key", Args[1], "value"));
    } else {
        import std.conv : to;

        static assert(0, "1 or 2 parameters required. " ~ Args.length.to!string ~ " provided");
    }
}

@("shall complete a variable")
unittest {
    // arrange
    auto a = new Variable!(int, int);
    a.insert(relation!(int, int).from([[1, 10], [5, 51]]));
    a.insert(relation!(int, int).from([[1, 10], [5, 52]]));

    // act
    while (a.changed) {
    }

    // assert
    a.complete.should == [kvTuple(1, 10), kvTuple(5, 51), kvTuple(5, 52)];
}

@("shall progress a variable by moving newly added to the recent state")
unittest {
    import std.array : empty;

    // arrange
    auto a = new Variable!(int, int);
    a.insert(relation!(int, int).from([[1, 10], [2, 20], [5, 50]]));
    a.toAdd.empty.should == false;
    a.recent.empty.should == true;
    a.stable.empty.should == true;

    // act
    a.changed.shouldBeTrue;

    // assert
    a.toAdd.empty.should == true;
    a.recent.empty.should == false;
    a.stable.empty.should == true;
}

@("shall progress from toAdd to stable after two `changed`")
unittest {
    import std.array : empty;

    // arrange
    auto a = new Variable!(int, int);
    a.insert(relation!(int, int).from([[1, 10], [2, 20], [5, 50]]));

    // act
    a.changed.shouldBeTrue;
    a.changed.shouldBeFalse;

    // assert
    a.toAdd.empty.should == true;
    a.recent.empty.should == true;
    a.stable.empty.should == false;
}

@("shall be chunks in stable that have a size about 2x of recent")
unittest {
    import std.algorithm : map, count;
    import std.range : iota;

    // arrange
    Iteration iter;
    auto variable = iter.variable!(int, int)("source");
    variable.insert(iota(10).map!(x => kvTuple(x, x + 1)));
    variable.insert(iota(10).map!(x => kvTuple(x + 1, x)));

    // act
    while (iter.changed) {
        variable.fromJoin!((k, v1, v2) => kvTuple(v1, v2))(variable, variable);
    }

    // assert
    variable.stable.map!(a => a.count).should == [91, 30];
}

@("shall produce the same result between the fast and slow path when forcing distinct facts")
unittest {
    import std.algorithm : map, count;
    import std.range : iota;

    // arrange
    Iteration iter;
    auto fast = iter.variable!(int, int)("fast");
    fast.forceFastPath = true;
    auto slow = iter.variable!(int, int)("slow");
    foreach (a; [fast, slow]) {
        a.insert(iota(10).map!(x => kvTuple(x, x + 1)));
        a.insert(iota(10).map!(x => kvTuple(x + 1, x)));
    }

    // act
    while (iter.changed) {
        static auto helper(T0, T1, T2)(T0 k, T1 v1, T2 v2) {
            return kvTuple(v1, v2);
        }

        fast.fromJoin!(helper)(fast, fast);
        slow.fromJoin!(helper)(slow, slow);
    }

    // assert
    fast.complete.should == slow.complete;
}

@("shall produce the same result between the single and multithreaded Iteration")
unittest {
    import std.algorithm : map, count;
    import std.range : iota;

    // arrange
    auto iter_s = makeIteration!(ThreadStrategy.single);
    auto single = iter_s.variable!(int, int)("fast");
    single.insert(iota(10).map!(x => kvTuple(x, x + 1)));
    single.insert(iota(10).map!(x => kvTuple(x + 1, x)));

    auto iter_p = makeIteration!(ThreadStrategy.parallel);
    auto parallel = iter_p.variable!(int, int)("slow");
    parallel.insert(iota(10).map!(x => kvTuple(x, x + 1)));
    parallel.insert(iota(10).map!(x => kvTuple(x + 1, x)));

    // act
    static auto helper(T0, T1, T2)(T0 k, T1 v1, T2 v2) {
        return kvTuple(v1, v2);
    }

    while (iter_s.changed)
        single.fromJoin!(helper)(single, single);
    while (iter_p.changed)
        parallel.fromJoin!(helper)(parallel, parallel);

    // assert
    single.complete.should == parallel.complete;
}

@("shall count the elements in the nested arrays in stable")
unittest {
    auto var = new Variable!(int, int);
    var.stable ~= relation!(int, int)([kvTuple(3, 2), kvTuple(4, 2)]);
    var.stable ~= relation!(int, int)([kvTuple(3, 2), kvTuple(4, 2)]);

    var.countStable.should == 4;
}
