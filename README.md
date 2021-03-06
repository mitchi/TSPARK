![liflab](https://avatars1.githubusercontent.com/u/15191496?s=200&v=4)
By Edmond La Chance and Sylvain Hallé, Laboratoire d'informatique formelle ([LIF](https://github.com/liflab))


- [Introduction](#introduction)
- [Download](#Download)
- [Included Algorithms](#included-algorithms)
- [Command line usage](#command-line-usage)
- [SLURM cluster manager script](#slurm)

A paper that talks about TSPARK is currently pending.

## Introduction

TSPARK is a combinatorial testing tool (like [ACTS](https://www.nist.gov/programs-projects/automated-combinatorial-testing-software-acts)).
TSPARK can be used as a standalone process on a powerful server, or launched using a cluster manager like SLURM.

## Download
To download TSPARK, go to the [release page](https://github.com/mitchi/TSPARK/releases) and download the latest JAR.
TSPARK is programmed with Apache Spark 2.4.0 and requires Java 8 to run. Launch TSPARK by doing:

```
java -jar TSPARK.jar
```

## Included algorithms
TSPARK includes various algorithms to generate covering arrays:

Using Graph reductions:
- Distributed Hypergraph Cover
- Distributed Graph Coloring (Knights and Peasants algorithm, or Order Coloring)
- Single threaded Coloring (Ran multiple times)

With Hybrid algorithms based on IPOG:
- Distributed IPOG Coloring
- Distributed IPOG Hypergraph 

TSPARK currently only supports generation of uniform covering arrays for the Hybrid algorithms based on IPOG.
To use this generator with parameters of mixed sizes, and support for universal and existential constraints, a [generator](https://github.com/liflab/combinatorial-graph-generator) by Sylvain Hallé can be used, along with the Graphviz or EDN command line options. 

## Command line usage

The command line uses ANSI colors, and they are not supported by Windows.
However, when TSPARK is launched through the ANSICON console, they work as they should.
ANSICON: http://adoxa.altervista.org/ansicon/

TSPARK is used using a command line interface:

```

TSPARK - a distributed testing tool

Usage

 TSPARK [options] command [command options]

Commands

   color [command options] <t> <n> <v> : single threaded graph coloring
      --colorings=NUM : Number of parallel graph colorings to run
      -v, --verify    : verify the test suite
      <t> : interaction strength
      <n> : number of parameters
      <v> : domain size

   dcoloring [command options] <t> <n> <v> : Distributed Graph Coloring
      --algorithm=STRING : Which algorithm to use (KP or OC)
      --chunksize=NUM    : Chunk size, in vertices. Default is 20k
      -v, --verify       : verify the test suite
      <t> : interaction strength
      <n> : number of parameters
      <v> : domain size of a parameter

   dhgraph [command options] <t> <n> <v> : distributed hypergraph covering
      -v, --verify : verify the test suite
      --vstep=NUM  : Covering speed (optional)
      <t> : interaction strength
      <n> : number of parameters
      <v> : domain size of a parameter

   dicr [command options] <t> <n> <v> : Distributed Ipog-coloring
      --algorithm=STRING : Which algorithm to use (KP or OC)
      --chunksize=NUM    : Chunk size, in vertices. Default is 20k
      --hstep=NUM        : Number of parameters of tests to extend in parallel
      --seeding=STRING   : Seeding at param,file
      -v, --verify       : verify the test suite
      <t> : interaction strength
      <n> : number of parameters
      <v> : domain size

   dih [command options] <t> <n> <v> : distributed ipog hypergraph cover
      --hstep=NUM  : Number of parameters of tests to extend in parallel
      -v, --verify : verify the test suite
      --vstep=NUM  : Covering speed (optional)
      <t> : interaction strength
      <n> : number of parameters
      <v> : domain size

   edn <filename> : hypergraph covering from a file (edn format)
      <filename> : file name of the .dot file

   graphviz [command options] <filename> : graph coloring from a GraphViz file
      --colorings=NUM : Number of parallel graph colorings to run
      --memory=NUM    : memory for the graph structure on the cluster in megabytes
      -s, --st        : use single threaded coloring
      <filename> : file name of the .dot file

   phiway [command options] <clauses> : Phi-way testing from a list of clauses
      --algorithm=STRING : algorithm to use: OC,KP or HC
      --chunksize=NUM    : Chunk of vertices to use for graph coloring. Default is 4000
      --save             : Save the test suite to a text file
      --t=NUM            : Generate and join additional clauses using interaction strength t
      <clauses> : filename for the list of clauses

   pv <t> <n> : enumerate parameter vectors
      <t> : interaction strength
      <n> : number of parameters

   tway [command options] <t> <n> <v> : enumerate t-way combos
      -o, --order : enumerate the combos to cover in parameter order
      <t> : interaction strength
      <n> : number of parameters
      <v> : domain size of a parameter

No command found, expected one of color, dcoloring, dhgraph, dicr, dih, edn, graphviz, phiway, pv, tway
```


## Slurm

A sample script
Here, we use 160 cpus on 20 different computers
Each computer is an executor with 8 cpus

```
#!/bin/bash
#SBATCH --account=edmond
#SBATCH --time=01:00:00
#SBATCH --nodes=20
#SBATCH --mem=4G
#SBATCH --cpus-per-task=8
#SBATCH --ntasks-per-node=1

module load StdEnv/2020
module load spark/3.0.0

# Recommended settings for calling Intel MKL routines from multi-threaded applications
# https://software.intel.com/en-us/articles/recommended-settings-for-calling-intel-mkl-routines-from-multi-threaded-applications 
export MKL_NUM_THREADS=1
export SPARK_IDENT_STRING=$SLURM_JOBID
export SPARK_WORKER_DIR=$SLURM_TMPDIR
export SLURM_SPARK_MEM=$(printf "%.0f" $((${SLURM_MEM_PER_NODE} *95/100)))

start-master.sh
sleep 5
MASTER_URL=$(grep -Po '(?=spark://).*' $SPARK_LOG_DIR/spark-${SPARK_IDENT_STRING}-org.apache.spark.deploy.master*.out)

NWORKERS=$((SLURM_NTASKS - 1))
SPARK_NO_DAEMONIZE=1 srun -n ${NWORKERS} -N ${NWORKERS} --label --output=$SPARK_LOG_DIR/spark-%j-workers.out start-slave.sh -m ${SLURM_SPARK_MEM}M -c ${SLURM_CPUS_PER_TASK} ${MASTER_URL} &
slaves_pid=$!

SLURM_SPARK_SUBMIT="srun -n 1 -N 1 spark-submit --master ${MASTER_URL} --executor-memory ${SLURM_SPARK_MEM}M"
$SLURM_SPARK_SUBMIT --conf "spark.default.parallelism=160" --class cmdlineparser.TSPARK tspark.jar dcoloring 7 8 4


kill $slaves_pid
stop-master.sh
```