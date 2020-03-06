TSPARK is a tool that generates tests for t-way testing problems.
It includes a lot of different algorithms, but the best ones are (in order of test quality).

 1. Set Cover
 2. Parallel IPOG using Set Cover
 3. Parallel IPOG using Graph Coloring
 4. Graph Coloring

The tool also does other things. It can verify a test suite, resume a calculation.
You can use this tool with a computing cluster. For instance, here is a script that works with a SLURM scheduler on Compute Canada :

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