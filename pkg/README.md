# ![architect](architect_1-small.png) Architect

The goal of `architect` is to provide a set of tools to help with the design and arrangement of tabular data. Specifically, it provides a set of tools that encourage thinking in terms of design topology. The main theme for creating `architect` is to design and manipulate datasets using a formulaic interface expressing intent. A secondary theme is to be locally concise when performing chained operations.

## Formula for Transformation

The feature function of `architect` is `define()`, which *"allows one to operate on data using one or more formula-based definitions. Data transformation and selection can be achieved with formulas or using standard `data.table` syntax in a procedural manner with a single function call. Each operation operates on the previous result making `define()` a pipeline of operations in a compact framework."*

Model formula notation is familiar to R: `architect` was designed with the idea fomulaic model specification in mind. 

Take, for example, the following model specification:

$y \sim \beta_0 + \sum{\beta_{i}^\top X_i} + \text{err}$

The main takeaway is the *concept* of input defining output within a specified context (distribution, parameters, link function, etc.). Why not use something similar for datasets? 

In `define`: how a formula is specified determines the operation:

- Literal column selection: $\sim x_1 + x_2 + x_3 + \ldots + x_k$
- Transformation with a selection of columns: $f({\scriptstyle \Lambda_m})$ 
- Transformation with a selection of columns grouped by a different set of columns: $f(\Lambda_m)\sim X_n: \Lambda_m \cap X_k := \emptyset$

Underneath the hood, the libraries `data.table`, `rlang`, and `purrr` are combined to allow for the above to be expressed as a series of definitions of how to modify a dataset: $D_{i+1} \sim g\big(D_{i}\big) := g\big(D_i \Rightarrow f(\Lambda_m)\sim X_n\big)$

Each step in the transformation chain is immediately understood to result in a tabular result (enforced by `data.table`), and in many cases, the code written becomes much easier to read given the absence of needing pipes to define the chain. The reason is that the chain is an expression list  (`rlang`) iterated over (`purrr`) and evaluated, the current dataset being updated by the next evaluated expression.


## Join Maps
Joining datasets is handled in various ways; however, being able to know ahead of time how multiple datasets will combine is the idea behind `join_mapper()` and `join_reduce()`. 

- **`join_mapper()`** was designed to define a map describing the order of dataset joins based on a simple heuristic of field commonality. The join map can be viewed directly and updated like any other tabular object.
- **`join_reduce()`** resolves the map defined by `join_mapper()` to create the final dataset.

These functions are still a bit fragile in certain situations, but for simple tasks, all that is required is specifying *what* to join and the possible fields to join on.

## Installation

Use `remotes::install_github("delriaan/architect", subdir = "pkg")` to install. 
