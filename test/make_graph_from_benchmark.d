#!/usr/bin/env dub
/+ dub.sdl:
    name "make_graph_from_benchmark"
    dependency "ggplotd" version="~>1.1.6"
+/
import logger = std.experimental.logger;
import std.algorithm;
import std.array;
import std.ascii;
import std.conv;
import std.csv;
import std.file;
import std.path;
import std.process;
import std.range;
import std.stdio;
import std.string;

immutable ResultFileExt = ".dat";

struct Row {
    int lowest;
    int total;
}

int main(string[] args) {
    import ggplotd.aes : aes;
    import ggplotd.axes : xaxisLabel, yaxisLabel;
    import ggplotd.geom : geomPoint, geomLine;
    import ggplotd.ggplotd : putIn, GGPlotD, Facets;

    auto data_files = dirEntries(".", "*" ~ ResultFileExt, SpanMode.shallow).map!"a.name".array;
    if (data_files.length == 0) {
        writefln("*%s do not exist. Nothing to do", ResultFileExt);
        return 1;
    }

    Facets facets;

    foreach (fname; data_files) {
        writeln("* Processing ", fname);

        auto rows = readText(fname).csvReader!Row(["lowest(usec)", "total(usec)"]).array;

        GGPlotD gg;
        gg = xaxisLabel(fname).putIn(gg);
        const double yscale = () {
            if (rows.map!"a.lowest".minElement > 1000) {
                gg = yaxisLabel("ms").putIn(gg);
                return 1000.0;
            }
            gg = yaxisLabel("μs").putIn(gg);
            return 1.0;
        }();

        gg = rows.enumerate.map!(a => aes!("x", "y")(a.index,
                a.value.lowest / yscale)).array.geomPoint.putIn(gg);
        gg = rows.enumerate.map!(a => aes!("x", "y")(a.index,
                a.value.lowest / yscale)).array.geomLine.putIn(gg);
        facets = gg.putIn(facets);
    }

    facets.save("profile_graph.png", 800, 800);

    return 0;
}
