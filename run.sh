#!/bin/bash

#SBATCH -A m2404
#SBATCH --time=30:00
#SBATCH --nodes=1
#SBATCH --constraint=cpu
#SBATCH --qos regular

srun -n1 -c 8 --cpu-bind-cores ./work_steal/src/txn_processor_test
