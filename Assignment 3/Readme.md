# Distributed Mutex: Raymond

## Prerequisites

* `mpic++`
* `mpirun`

## Compile

```
$ make
```

## Generate test input

You can change the value of `k` in `gen.py` and run the following command.

```
# $N_PROCS is the number of processess.
$ make gen_input N=$N_PROCS
```

## Run

Input should be in `inp-params.txt`.

Raymond. Logs will be in `Raymond_logs.txt`.

```
# $N_PROCS is the same as what you gave for generating input.
$ make Raymond N=$N_PROCS
```

## Example

```
$ make gen_input N=6
$ make Raymond N=6
```