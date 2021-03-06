# datacat [![Build Status](https://travis-ci.org/joakim-brannstrom/datacat.svg?branch=master)](https://travis-ci.org/joakim-brannstrom/datacat)

**datacat** is a lightweight Datalog engine intended to be embedded in other D programs.

# Getting Started

datacat depends on the following software packages:

 * [D compiler](https://dlang.org/download.html) (dmd 2.079+, ldc 1.8.0+)

Download the D compiler of your choice, extract it and add to your PATH shell
variable.
```sh
# example with an extracted DMD
export PATH=/path/to/dmd/linux/bin64/:$PATH
```

Once the dependencies are installed it is time to download the source code to install datacat.
```sh
git clone https://github.com/joakim-brannstrom/datacat.git
cd datacat
dub build -b release
```

Done! Have fun.
Don't be shy to report any issue that you find.

# Example

Datacat has no runtime, and relies on you to build and repeatedly apply the update rules.
It tries to help you do this correctly. As an example, here is how you might write a reachability
query using Datacat (minus the part where we populate the `nodes` and `edges` initial relations).

```d
auto fun() {
    import datacat;

    // Create a new iteration context, ...
    Iteration!(uint, uint) iteration;

    // .. some variables, ..
    auto nodes = iteration.variable("nodes");
    auto edges = iteration.variable("edges");

    // .. load them with some initial values, ..
    nodes.insert(relation!(uint, uint).from(....));
    edges.insert(relation!(uint, uint).from(....));

    // .. and then start iterating rules!
    while iteration.changed() {
        static auto joiner(T0, T1, T2)(T0 b, T1 a, T2 c) {
            return kvTuple(c, a);
        }
        // nodes(a,c)  <-  nodes(a,b), edges(b,c)
        nodes.fromJoin!joiner(nodes, edges);
    }

    // extract the final results.
    return nodes.complete;
}
```

# Performance

In the end I expect datafrog and datacat to be equal in performance. The languages have similare capabilities.

This data is intended to show that datacat has achieved similare performance as the original implementation.

The biggest culprite when I did the port where that completeSort constantly
reordered the elements. See git commit b25827d and the method `Relation.merge`.

This goes to show how important it is to have data before doing any optimizations.

The dataset can be downloaded [from here](https://drive.google.com/drive/folders/0B8bQanV_QfNkbDJsOWc2WWk4SkE).
It is the file at "Graphs/Apache Httpd 2.2.18 Dataflow/http_df".

## Results

```sh
cd datafrog
cargo build --release
./target/release/graspan1 ~/httpd_df
Duration { secs: 1, nanos: 538892450 }  Data loaded
Duration { secs: 4, nanos: 106084073 }  Computation complete (nodes_final: 9393283)

# ----
cd datacat/test/standalone
dub build --compiler=ldc2 -b release
./build/graspan1 ~/httpd_df
Shall calculate the dataflow from the provided file
1 sec, 784 ms, 24 μs, and 8 hnsecs: Data loaded
Single threaded
3 secs, 383 ms, 906 μs, and 4 hnsecs: Computation complete (nodes_final: 9393283)
Multi threaded
2 secs, 286 ms, 703 μs, and 4 hnsecs: Computation complete (nodes_final: 9393283)
```

# Credit

All credit goes to Frank McSherry <fmcsherry@me.com> for the excellent blog post and implementation (this port). I highly recommend to read [Frank's blog](https://github.com/frankmcsherry/blog/blob/master/posts/2018-05-19.md).

Credit also goes to the team that spurred Frank McSherry and provided the datasets. See there [paper](https://www.ics.uci.edu/~guoqingx/papers/wang-asplos17.pdf) and [implementation](https://github.com/Graspan/graspan-cpp)
