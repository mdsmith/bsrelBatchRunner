#! /usr/bin/env python

import os, sys, subprocess, time
from subprocess import call
from threading import Thread
from queue import Queue, Empty
import argparse
import glob
import re
#node_list = range(13,31)
#local_processes = {}
jobs = Queue()

def get_files(in_dir, out_dir, suffix):
    finished_file_list = []
    if out_dir != "":
        finished_file_list = glob.glob(out_dir + os.sep + "*" + suffix + ".out.fit")
    if os.path.isdir(in_dir):
        file_list = glob.glob(in_dir + os.sep + "*" + suffix)
    else:
        file_list = [in_dir]
    for fin_file in finished_file_list:
        for todo_file in file_list:
            todo_file_file = os.path.basename(todo_file)
            fin_file_file = os.path.basename(fin_file)
            if todo_file_file in fin_file_file:
                file_list.remove(todo_file)
    return file_list

def get_out_dir(out_dir):
    if os.path.isdir(out_dir) == True:
        return os.path.abspath(out_dir)
    else:
        return os.path.dirname(os.path.abspath(__file__))


# XXX out_file is more of an out suffix... Fix that
def run_BSREL(  node,
                file_name,
                file_name_body,
                out_dir, #nodeI,
                var_beta,
                tree):
    batchfile = open(   out_dir
                        + os.sep
                        + file_name_body
                        + ".bf",
                        'w')
    batchfile.write('inputRedirect = {};\n\n')
    batchfile.write('inputRedirect["00"]= "Universal";\n')
    if var_beta == True:
        batchfile.write('inputRedirect["01"]= "Yes";\n')
    else:
        batchfile.write('inputRedirect["01"]= "No";\n')
    batchfile.write('inputRedirect["02"]="'
                    + os.path.abspath(file_name)
                    + '";\n')
    if tree == "None":
        batchfile.write('inputRedirect["03"]= "Y";\n')
    else:
        batchfile.write('inputRedirect["03"]= "' + tree + '";\n')
    batchfile.write('inputRedirect["04"]= "All";\n')
    batchfile.write('inputRedirect["05"]= "";\n')
    batchfile.write('inputRedirect["06"]="'
                    + out_dir
                    + os.sep
                    + file_name_body
                    + ".out"
                    + '";\n')
    batchfile.write('ExecuteAFile' \
                    '("/usr/local/lib/hyphy/TemplateBatchFiles/BranchSiteREL.bf"'
                    ', inputRedirect);')
    batchfile.close()
    call_list = [   'bpsh',
                    str(node),
                    'HYPHYMP',
                    out_dir + os.sep
                            + file_name_body
                            + ".bf"]
    output_file = open( out_dir
                        + os.sep
                        + file_name_body
                        + ".out.stdout", 'w')
    print(call_list)
    call(call_list, stdout=output_file)
    time.sleep(1)

def run_job(node):
    while True:
        try:
            bsrel_args = jobs.get(block=False)
            run_BSREL(node, bsrel_args)
            jobs.task_done()
        except Empty:
            break

def nodes(num):
    import shlex
    from subprocess import Popen, PIPE
    cmd = shlex.split('''beomap --all-nodes --no-local --exclude
    0:1:2:3:4:5:6''')
    proc = Popen(cmd, stdout=PIPE)
    stdout, _ = proc.communicate()
    node_list = [int(node) for node in
                stdout.decode('utf-8').strip().split(':')]
    node_list.reverse()
    node_list = node_list[:num]
    return node_list

def run_job(node):
    while True:
        try:
            bsrel_args = jobs.get(block=False)
            run_BSREL(node, *bsrel_args)
            jobs.task_done()
        except Empty:
            break


def run_all_BSREL(  in_file,
                    out_dir,
                    var_beta,
                    finish,
                    tree,
                    suffix):
    if finish:
        nex_file_list = get_files(in_file, out_dir, suffix)
    else:
        nex_file_list = get_files(in_file, "", suffix)
    out_dir = get_out_dir(out_dir)
    for file_name in nex_file_list:
        in_path, file_name_body = os.path.split(file_name)
        bsrel_args = (  file_name,
                        file_name_body,
                        out_dir,
                        var_beta,
                        tree)
        jobs.put(bsrel_args)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("input", help="input file or directory")
    parser.add_argument("output", help="output directory")
    parser.add_argument("--alphas",
                        help="let alphas vary on a per beta basis",
                        action="store_true")
    parser.add_argument("--finish",
                        help="skip already analyzed files",
                        action="store_true")
    parser.add_argument("--tree",
                        help="specify a tree file for all input files",
                        default="None")
    parser.add_argument("--suffix",
                        help="specify a suffix (e.g. .nex, .fasta)",
                        default=".nex")
    args = parser.parse_args()

    run_all_BSREL(  args.input,
                    args.output,
                    args.alphas,
                    args.finish,
                    args.tree,
                    args.suffix)

    for node in nodes(24):
        t = Thread(target=run_job, args=(node,))
        t.daemon = True
        t.start()
    jobs.join()
