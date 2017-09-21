# Lambda punter

## Installation

* Install OCAML 4.04.0 or later and OPAM

* Run `opam pin add ocamlgraph
 https://github.com/simonjf/ocamlgraph.git` (this is a version of
 `ocamlgraph` patched by Simon Fowler to implement a general version
 of Dijkstra's algorithm)

* Run `opam pin add lambda-punter .` (this is the actual
  lambda-punter server)

## Running lambda-punter

* To run the server in online mode, type `lampunt`.

* Running `lampunt --help` lists some command line options.

* Running `lampunt --offline` runs two punters that eagerly pick the
first river they comes across against one another.

* To play your own punters against each other in offline mode place
them in subdirectories of `punters` and use the `--punter-list`
command line option to specify a file containing a list of
punters. Each punter `foo` should be placed in subdirectory `foo` as a
binary called `punter`.

* You can find some different maps here:

  https://github.com/icfpcontest2017/icfpcontest2017.github.io/tree/master/maps

* To extract a JSON game from a log you can use
  `json-game-of-log.sh`. You can then watch a replay using
  [PuntTV](https://github.com/icfpcontest2017/icfpcontest2017.github.io/tree/master/punttv)

  (which you will have to work out how to set up for yourself).

To properly understand what's going on: read the source code---then
update this README.md to explain what's going on.
