#TSPARK
By Edmond La Chance and Sylvain Hallé.

- [Included Algorithms](#included-algorithms)
- [Command line usage](#command-line-usage)
- [SLURM cluster manager script](#slurm)

The paper is currently pending.

## Included algorithms
TSPARK includes various algorithms to generate covering arrays:

Using Graph reductions:
- Distributed Hypergraph Cover
- Distributed Graph Coloring
- Single threaded Coloring (Ran multiple times)

With Hybrid algorithms based on IPOG:
- Distributed IPOG Coloring
- Distributed IPOG Hypergraph 

The included generator currently only supports uniform covering arrays.
To use this 

## Command line usage

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

   dcolor [command options] <t> <n> <v> : distributed graph coloring
      --algorithm=STRING : Algorithm (KP or Order Coloring)
      --memory=NUM       : memory for the graph structure on the cluster in megabytes
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

   dic [command options] <t> <n> <v> : distributed ipog coloring
      --colorings=NUM : Number of parallel graph colorings to run
      --hstep=NUM     : Number of parameters of tests to extend in parallel
      -s, --st        : use single threaded coloring
      -v, --verify    : verify the test suite
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

   pv <t> <n> : enumerate parameter vectors
      <t> : interaction strength
      <n> : number of parameters

   tway [command options] <t> <n> <v> : enumerate t-way combos
      -o, --order : enumerate the combos to cover in parameter order
      <t> : interaction strength
      <n> : number of parameters
      <v> : domain size of a parameter

No command found, expected one of color, dcolor, dhgraph, dic, dih, edn, graphviz, pv, tway
```


## Slurm

```
Here is a sample script:

    #!/bin/bash
    #SBATCH --account=********
    #SBATCH --time=24:00:00
    #SBATCH --nodes=16
    #SBATCH --mem=64G
    #SBATCH --cpus-per-task=8
    #SBATCH --ntasks-per-node=1
    
    module load spark/2.4.4
    
    
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
    #SPARK_NO_DAEMONIZE=1 srun -n ${NWORKERS} -N ${NWORKERS} --label --output=$SPARK_LOG_DIR/spark-%j-workers.out start-slave.sh -m ${SLURM_SPARK_MEM}M -c ${SLURM_CPUS_PER_TASK} ${MASTER_URL} &
    slaves_pid=$!
    
    #SLURM_SPARK_SUBMIT="srun -n 1 -N 1 spark-submit --master ${MASTER_URL} --executor-memory ${SLURM_SPARK_MEM}M"
    #$SLURM_SPARK_SUBMIT --class org.apache.spark.examples.SparkPi $SPARK_HOME/examples/jars/spark-examples_2.11-2.3.0.jar 1000
    #$SLURM_SPARK_SUBMIT  TSPARK.jar --type setcover --n 3 --t 2 --v 2
    
    srun -n 1 -N 1 spark-submit --class cmdline.MainConsole --master ${MASTER_URL} --executor-memory ${SLURM_SPARK_MEM}M TSPARK.jar  --save true --type parallel_ipog_m_setcover --resume 11,7-10-4.txt --n 20 --t 7 --v 4
    
    kill $slaves_pid
```