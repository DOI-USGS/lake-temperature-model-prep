#!/bin/bash
#SBATCH --job-name=gcm_munge   	# name that you chose
#SBATCH --partition=cpu         	# the partition you want to use, for this case prod is best
#SBATCH --cpus-per-task=2		# Number of cores (None of this is actually parallelized)
#SBATCH --nodes=1			# Number of nodes
#SBATCH -A iidd                 	# List account
#SBATCH --output=gcm_munge.out	# file to save output messages to
#SBATCH --time=20:00:00        		# time at which the process will be cancelled if unfinished
#SBATCH --mem=192GB
#SBATCH --mail-type=ALL
#SBATCH --mail-user=lplatt@usgs.gov

module load singularity

ulimit -u 1541404

srun singularity exec \
    lake-temperature-model-prep.sif \
    Rscript gcm_download.R

