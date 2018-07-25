# Build

There is a markedly difference between debug build and release. The performance suite should preferably be built with LDC and release mode.

```sh
dub build --compiler=ldc2 -b release && ./build/datacat_benchmark
```

The standalone applications should be built in a similare way.

To run with profiling:
```sh
dub build --compiler=dmd -b utProf
```

# Graph

The performance data is gathered in ".csv". These can be visualized as a graph with the `make_graph_from_benchmark.d` program.
It is though recommended to compile it once to an executable as to avoid having it recompiled by dub each time it is executed.

To compile it once to an executable:
```sh
dub build --single make_graph_from_benchmark.d
```

When make_graph_from_benchmark is executed it writes the graph to `profile_graph.png`.
