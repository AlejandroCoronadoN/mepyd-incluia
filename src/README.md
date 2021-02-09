# Project-ETL

`project-etl` is meant for complementing [`criteria-etl`](https://pypi.org/project/criteria-etl/). 

## `utils` module
- `config`: used for setting-up global variables of the project, used for loading data.
- `dataload`: functions used for loading data. It imports global variables from `config` sub-module.

## Note
Before adding any function or class to the `project-etl` package, take a look at what is already available on `criteria-etl`. If new functions or classes are created for the project and if they have the potential to be used transversally in other projects, add them to `criteria-etl`.



