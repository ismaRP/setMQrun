#!/usr/bin/env python3

import argparse
import subprocess
import os
import sys
import pandas as pd

parser = argparse.ArgumentParser(description='Set MaxQuant run un sbatch HPC')
parser.add_argument('mqpar', type=str)
parser.add_argument(
    '-p', '--nsub', type=int, default=3, help="Number of subprocesses")
parser.add_argument(
    '-b', '--breaks', type=int, nargs='+', default=[])
parser.add_argument(
    '-m', '--mqversion', type=str, default='2.1.1.0')
parser.add_argument(
    '-c', '--cores', type=int, nargs='+', default=[56, 56, 56],
    help='Number of threads to specify in MaxQuant configuration file')
parser.add_argument(
    '-t', '--time', type=int, nargs="+", default=[8, 12, 8],
    help='Max wall time allowed in hours for each partial process')
parser.add_argument(
    '-a', '--account', type=str, default="COLLINS-SL3-CPU", help='Name of account')
parser.add_argument(
    '-o', '--slurm_output', type=str, default=None)
parser.add_argument(
    '-d', '--dryrun', action='store_true')

args = parser.parse_args()
if args.nsub == 3 and len(args.breaks) == 0:
    args.breaks = [12, 18]
if args.nsub == 1 and len(args.breaks) == 0:
    args.breaks = [47]

assert len(args.breaks) == args.nsub - 1
assert len(args.cores) == args.nsub or len(args.cores) == 1
assert len(args.time) == args.nsub or len(args.time) == 1

if len(args.cores) == 1:
    args.cores = args.cores * args.nsub
if len(args.time) == 1:
    args.time = args.time * args.nsub

script_path = os.path.dirname(os.path.realpath(sys.argv[0]))
mqp_file = f'mq_{args.mqversion}_processes.csv'
mqprocesses = pd.read_csv(os.path.join(script_path, mqp_file), index_col="id")

start_array = [1]
start_array.extend([i+1 for i in args.breaks])
end_array = args.breaks
last_task = mqprocesses.shape[0]
end_array.append(last_task)

with open(os.path.join(script_path, 'sbatch_template.txt'), 'r') as file:
    sbatch = file.read()


mqcmd_templ = 'MaxQuant {mqversion} {mqpar} -p {start} -e {end}'
jobid = None
depstring = '#SBATCH --dependency=afterok:{jobid}'
for i in range(args.nsub):
    s = start_array[i]
    e = end_array[i]
    mqcmd = mqcmd_templ.format(
        mqversion=args.mqversion,
        mqpar=args.mqpar,
        start=s, end=e
    )

    if jobid is not None:
        dep = depstring.format(jobid=jobid)
    else:
        dep = ""
    subproc_sbatch = sbatch.format(
        process=i + 1,
        account=args.account,
        cores=args.cores[i],
        walltime=args.time[i],
        mqcmd=mqcmd,
        chdir=args.slurm_output,
        dep=dep,
        wd=args.slurm_output
    )
    sbatch_file = f'mq_subproc_{i+1}.sh'
    sbatch_file = os.path.join(args.slurm_output, sbatch_file)
    with open(sbatch_file, 'w') as file:
        file.write(subproc_sbatch)
    # Submit job
    # print(['sbatch', sbatch_file])
    if not args.dryrun:
        x = subprocess.run(['sbatch', sbatch_file], capture_output=True)
        jobid = x.stdout.decode().rstrip()
    else:
        jobid = 100 + 1 + i  # Fake job
    print(f'Subprocess: {i+1}. Job ID: {jobid}')
    print(mqprocesses.loc[s:e])
    print(mqcmd)
    print("---------------------------------\n")
