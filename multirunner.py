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

def get_files(in_dir, out_dir):
    finished_file_list = []
    if out_dir != "":
        finished_file_list = glob.glob(out_dir + os.sep + "*.nex.out.fit")
    if os.path.isdir(in_dir):
        file_list = glob.glob(in_dir + os.sep + "*.nex")
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

def pad(num, len):
    return str(num).zfill(len)

# XXX out_file is more of an out suffix... Fix that
def run_BSREL(  node,
                file_name,
                file_name_body,
                out_dir, #nodeI,
                var_beta,
                test,
                tree_suffix):
    response_list = []
    response_list.append("Universal")
    if var_beta:
        response_list.append("Yes")
    else:
        response_list.append("No")
    response_list.append(os.path.abspath(file_name))
    if tree_suffix != "":
        response_list.append(os.path.abspath(file_name + tree_suffix))
    else:
        response_list.append("Y")
    if var_beta:
        response_list.append(out_dir + os.sep + file_name_body + ".out")
    elif test:
        response_list.append("All")
        response_list.append("")
        response_list.append(out_dir + os.sep + file_name_body + ".out")
    else: # no beta variance and not testing
        response_list.append("None")
        response_list.append("") # Check this
        response_list.append(out_dir + os.sep + file_name_body + ".out")

    batchfile = open(   out_dir
                        + os.sep
                        + file_name_body
                        + ".bf",
                        'w')

    batchfile.write('inputRedirect = {};\n\n')
    for i,response in enumerate(response_list):
        batchfile.write('inputRedirect["'
                        + pad(i,2)
                        + '"]= "'
                        + response
                        + '";\n')

#    in_red_i = 0
#    batchfile.write('inputRedirect = {};\n\n')
#    batchfile.write('inputRedirect["' + pad(in_red_i,2) + '"]= "Universal";\n')
#    in_red_i += 1
#    if var_beta == True:
#        batchfile.write('inputRedirect["' + pad(in_red_i,2) + '"]= "Yes";\n')
#    else:
#        batchfile.write('inputRedirect["' + pad(in_red_i,2) + '"]= "No";\n')
#    in_red_i += 1
#    batchfile.write('inputRedirect["' + pad(in_red_i,2) + '"]="'
#                    + os.path.abspath(file_name)
#                    + '";\n')
#    in_red_i += 1
#    if tree_suffix != "":
#        batchfile.write('inputRedirect["' + pad(in_red_i,2) + '"]="'
#                        + os.path.abspath(file_name + tree_suffix)
#                        + '";\n')
#        in_red_i += 1
#    else:
#        batchfile.write('inputRedirect["' + pad(in_red_i,2) + '"]= "Y";\n')
#        in_red_i += 1
#    if test:
#        batchfile.write('inputRedirect["' + pad(in_red_i,2) + '"]= "All";\n')
#        in_red_i += 1
#        batchfile.write('inputRedirect["' + pad(in_red_i,2) + '"]= "";\n')
#        in_red_i += 1
#    else:
#        batchfile.write('inputRedirect["' + pad(in_red_i,2) + '"]= "All";\n')
#        in_red_i += 1
#        batchfile.write('inputRedirect["' + pad(in_red_i,2) + '"]= "";\n')
#        in_red_i += 1
#    batchfile.write('inputRedirect["' + pad(in_red_i,2) + '"]="'
#                    + out_dir
#                    + os.sep
#                    + file_name_body
#                    + ".out"
#                    + '";\n')
    batchfile.write('ExecuteAFile' \
                    '("/home/martin/Software/multimodelBSREL/multiBSREL.bf"'
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
                    test,
                    tree_suffix=""):
    if finish:
        nex_file_list = get_files(in_file, out_dir)
    else:
        nex_file_list = get_files(in_file, "")
    out_dir = get_out_dir(out_dir)
    for file_name in nex_file_list:
        in_path, file_name_body = os.path.split(file_name)
        bsrel_args = (  file_name,
                        file_name_body,
                        out_dir,
                        var_beta,
                        test,
                        tree_suffix)
        jobs.put(bsrel_args)

def range_to_list(node_list):
    begin, end = node_list.split("-")
    new_list = list(range(int(begin), int(end)+1))
    return new_list

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
    parser.add_argument("--test",
                        help="test all branches for positive selection",
                        action="store_true")
    parser.add_argument("--nodes",
                        help="specify a range of nodes to use <X-Y>",
                        default="7-30")
    parser.add_argument("--trees",
                        help="use speparate tree files with <suffix>")
    args = parser.parse_args()

    if (args.trees):
        run_all_BSREL(  args.input,
                        args.output,
                        args.alphas,
                        args.finish,
                        args.test,
                        tree_suffix=args.trees)
    else:
        run_all_BSREL(  args.input,
                        args.output,
                        args.alphas,
                        args.test,
                        args.finish)

    node_list = range_to_list(args.nodes)

    #for node in nodes(24):
    for node in node_list:
        t = Thread(target=run_job, args=(node,))
        t.daemon = True
        t.start()
    jobs.join()
