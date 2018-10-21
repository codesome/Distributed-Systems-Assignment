# Distributed Mutex: Raymond

## Prerequisites

* `mpic++`
* `mpirun`

## Compile

```
$ make -j2
```

## Generate test input

You can change the value of `k` in `gen.py` and run the following command.

```
# $N_PROCS is the number of processess.
$ make gen_input N=$N_PROCS
```

## Run

Input should be in `inp-params.txt`.

Some test inputs are available under the name `inp5`, `inp6`, `inp7`, `inp8`, `inp9`, `inp10`.

Raymond. Logs will be in `Raymond_logs.txt`.

```
# $N_PROCS is the same as what you gave for generating input.
$ make Raymond N=$N_PROCS
```

SusukiKasami. Logs will be in `SusukiKasami_logs.txt`.

```
# $N_PROCS is the same as what you gave for generating input.
$ make SusukiKasami N=$N_PROCS
```

## Example 1

```
$ make gen_input N=6
$ make Raymond N=6
$ make SusukiKasami N=6
```

## Example 2

```
$ cp inp9 inp-params.txt
$ make Raymond N=9
$ make SusukiKasami N=9
```