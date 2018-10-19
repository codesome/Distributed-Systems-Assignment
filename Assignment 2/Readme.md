# Chandy-Lamport and Lai-Yang Distributed Snapshots

## Prerequisites

* `mpic++`
* `mpirun`

## Compile

```
$ make
```

## Generate test input

You can change the value of `A` and `T` in `gen.py` and run the following command.

```
# $N_PROCS is the number of processess.
$ make gen_input N=$N_PROCS
```

## Run

Input should be in `inp-params.txt`.

Some test inputs are available under the name `inp5`, `inp6`, `inp7`, `inp8`, `inp9`, `inp10`.

Chandy-Lamport. Logs will be in `CL_logs.txt`. Snapshots will be in `snapshot_dump_CL.txt`.

```
# $N_PROCS is the same as what you gave for generating input.
$ make run_CL N=$N_PROCS
```

Lai-Yang. Logs will be in `LY_logs.txt`. Snapshots will be in `snapshot_dump_LY.txt`.

```
# $N_PROCS is the same as what you gave for generating input.
$ make run_LY N=$N_PROCS
```

## Example 1

```
$ cp inp7 inp-params.txt
$ make run_CL N=7
$ make run_LY N=7 
```

## Example 2

```
$ make gen_input N=6
$ make run_CL N=6
$ make run_LY N=6 
```