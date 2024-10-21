# databricks sessionizer

a simple library that can be imported to easily configure a databricks session via `databricks-connect` to do development work on your local machine.  i really just set this up to make it easier for me to crank out notebooks in neovim instead of needing to work within the databricks ui directly.

this library currently only support 15.4 lts runtimes on python3.12.  if you want older runtimes, feel free to make an issue and submit a pr.

## installation
clone this repo onto your local machine, cd into the repo root, and run `python3 -m pip install .`

## usage
usage examples can be found in the [examples](./examples) folder.

## templater
if you want to generate a session template script easily, use the `gdxsh` cli command that gets installed with this package.  run `gdxsh -h` in your terminal for usage information about the session templater.
