#! /usr/bin/env python

# Take a CSV file, fit file, etc for one taxa and convolve them as a single
# CSV file.

import csv
import os
import sys
import argparse
from bsrelSimParsers import (   recover_csv, recover_settings,
                                recover_csv_mg94)

#def append_fit(buffer, filename):
#def append_simulated(buffer, filename):

def meandnds(omegas, props):
    mean = 0
    for omega, prop in zip(omegas, props):
        mean += float(omega) * float(prop)
    return mean

# take a column and transform into a column vector with the mean of the
# values in the old column, ignoring ignore_rows rows at the top, following
# leave_rows number of empty rows also at the top
def mean_column(column, ignore_rows=0, leave_rows=0):
    padding = leave_rows * [0]
    col_mean = sum(column[ignore_rows:]) / (len(column) - ignore_rows)
    return padding + [col_mean]

# take a column and transform it into a column vector with the max of the
# values in the old column, ignoring ignore_rows rows at the top, following
# leave_rows number of emtpy rows also at the top.
def max_column(column, ignore_rows=0, leave_rows=0):
    padding = leave_rows * [0]
    col_max = max(column[ignore_rows:])
    return padding + [col_max]

def sum_column(column, ignore_rows=0, leave_rows=0):
    padding = leave_rows * [0]
    col_max = sum(column[ignore_rows:])
    return padding + [col_max]

def min_column(column, ignore_rows=0, leave_rows=0):
    padding = leave_rows * [0]
    col_max = min(column[ignore_rows:])
    return padding + [col_max]

def append_BSREL3(buffer, filename, whole_tree):
    contents = recover_csv(filename)
    #print("buffer subset:")
    #print(buffer[1:][0].split(",")[0])
    branch_order = [line.split(",")[0] for line in buffer[1:]]
    try:
        len_column = rep_to_column(contents, "length", branch_order)
    except KeyError:
        print("Broken BSREL3 file: ", filename)
        return buffer

    len_column[0] = "BSREL3_length"
    buffer = append_column(buffer, len_column)

    #print(contents)
    omegas_column = rep_to_column(contents, "omegas", branch_order)
    props_column = rep_to_column(contents, "props", branch_order)
    omegas_column[0] = "BSREL3_meandnds"
    props_column[0] = "BSREL3_propOverOne"

    mean_omegas_column = [  meandnds(omegas, props)
                            for omegas,props in
                            zip(omegas_column[1:], props_column[1:])]
    if whole_tree:
        mean_omegas_column = mean_column(mean_omegas_column)
    mean_omegas_column.insert(0, "BSREL3_meandns")
    #print(mean_omegas_column)

    omega_over_one_column = [   max([float(o) for o in omegas])
                                if max([float(o) for o in omegas]) > 1
                                else 0
                                for omegas in omegas_column[1:]]
    orig_omega_over_one_column = omega_over_one_column
    if whole_tree:
        omega_over_one_column = max_column(omega_over_one_column)
    omega_over_one_column.insert(0, "BSREL3_OmegaOver1")
    #print(omega_over_one_column)

    prop_over_one_column = [props[len(props)-1]
                            if omegas[len(omegas)-1] > 1
                            else 0
                            for omegas, props in zip(omegas_column[1:],
                            props_column[1:])]
    if whole_tree:
        prop_over_one_column =  prop_over_one_column[
                                    orig_omega_over_one_column.index(
                                        omega_over_one_column[-1])]
    prop_over_one_column.insert(0, "BSREL3_propOverOne")
    #print(prop_over_one_column)

    # XXX whole_tree for the rest of this and append_MG94
    max_omega_column = [omegas[-1]
                        for omegas in omegas_column[1:]];
    max_omega_column.insert(0, "BSREL3_MaxOmega")
    #print(max_omega_column)

    max_prop_column = [props[-1]
                        for props in props_column[1:]];
    max_prop_column.insert(0, "BSREL3_MaxOmegaProp")
    #print(max_prop_column)

    buffer = append_column(buffer, omega_over_one_column)
    buffer = append_column(buffer, prop_over_one_column)
    buffer = append_column(buffer, mean_omegas_column)
    buffer = append_column(buffer, max_omega_column)
    buffer = append_column(buffer, max_prop_column)
    #print(buffer)
    return buffer

def append_MG94(buffer, filename, whole_tree):
# XXX may need to be configured to work with incomplete csv
    try:
        contents = recover_csv_mg94(filename)
    except IndexError:
        print("Broken MG94 file: ", filename)
        return buffer
    branch_order = [line.split(',')[0] for line in buffer[1:]]
    len_column = rep_to_column(contents, "length", branch_order)
    #print("Got MG94")
    len_column[0] = "MG94_length"
    buffer = append_column(buffer, len_column)

    #print(contents)
    omegas_column = rep_to_column(contents, "omegas", branch_order)
    omegas_column = [omegas[0][0] for omegas in omegas_column]
    omegas_column[0] = "MG94_meandnds"

    #mean_omegas_column = [  meandnds(omegas, props)
                            #for omegas,props in
                            #zip(omegas_column[1:], props_column[1:])]
    #mean_omegas_column.insert(0, "MG94_meandns")
    #print(mean_omegas_column)

    buffer = append_column(buffer, omegas_column)
    #print(buffer)
    return buffer

def get_columns(rows):
    columns = [[]] * len(rows[0].split(','))
    for row in rows:
        for j,value in enumerate(row.split(',')):
            columns[j].append(value)
    return columns

# Take list of strings (lines), return same
def flatten_csv(filename, contents):
    # the filename replaces the branch name as the first item in the list
    flat_contents = [filename]
    columns = get_columns(contents)
    max_omega_over_one_index = -1
    for column in columns:
        # The first column is the branchname, which isn't useful
        if column[0] == "Branch":
            continue
        elif column[0] == "RateClasses":
            flat_contents.append(max_column(column))
        elif column[0] == "OmegaOver1":
            flat_contents.append(max_column(column))
            max_omega_over_one_index = column.index(max_column(column))
        elif column[0] == "WtOmegaOver1":
            if max_omega_over_one_index != -1:
                flat_contents.append(column[max_omega_over_one_index])
            else:
                flat_contents.append(0)
        elif column[0] == "LRT":
            flat_contents.append(min_column(column))
        elif column[0] == "p":
            flat_contents.append(min_column(column))
        elif column[0] == "p_Holm":
            flat_contents.append(min_column(column))
        elif column[0] == "BranchLength":
            flat_contents.append(sum_column(column))
        else:
            flat_contents.append(mean_column(column))
    return [','.join(flat_contents)]

def analyze_csv_sig_branches(contents):
    sig_branches = []
    for line in contents:
        # XXX debug
        #print(line.split(',')[-2])
        if float(line.split(',')[-2]) < 0.05:
            sig_branches.append(line.split(',')[0])
    return sig_branches

def append_csv( buffer,
                filename,
                whole_tree):
    file = open(filename, 'r')
    contents = file.readlines()
    to_add = contents
    # There is a header, doesn't need to be analyzed
    sig_list = analyze_csv_sig_branches(contents[1:])
    if whole_tree:
        # The header ought not be flattened, but it is useful for guiding the
        # flattening
        to_add = flatten_csv(filename, contents)
    buffer += to_add
    return sig_list

def append_column(buffer, column):
    # strip newlines, add comma, replace newlines
    for i, line in enumerate(buffer):
        buffer[i] = line.strip("\n") + "," + str(column[i]) + "\n"
    return buffer

def concat_buffers(buffer1, buffer2):
    if len(buffer1) != 0:
        buffer2 = buffer2[1:]
        if buffer1[-1].split(',')[-1] != '\n':
            buffer1[-1] += '\n'
    return buffer1 + buffer2

def rep_to_column(rep, key, order):
    column = [key]
    values = [rep[branch][key] for branch in order]
    column += values
    return column

def rep_to_csv(rep):
    csv = [[]]
    header = []
    header.append("Branch")
    for key, value in rep.items():
        row = []
        row.append(key)
        for branch_key, branch_value in value.items():
            if header.count(branch_key) == 0:
                header.append(branch_key)
                row.append(branch_value)
            else:
                row.insert(header.index(branch_key), branch_value)
        csv.append(row)
    csv.insert(0, header)
    return csv

def write_buffer(buffer, filename):
    file = open(filename, 'w')
    file.writelines(buffer)
    return 0

# prefixes are a list of filenames that start sets "longPy.279" etc.
def run_batch(  buffer,
                prefixes,
                whole_tree):
    # for fileset in prefixes
    sig_branch_dict = {}
    sig_tree_count = 0
    for prefix in prefixes:
        buffer2 = []
        #csv_filename = prefix + ".sim.0.recovered"
        csv_filename = prefix
        this_sig_branches = append_csv( buffer2,
                                        csv_filename,
                                        whole_tree)
        # XXX debug
        #print("Sigs for this branch:")
        #print(this_sig_branches)
        # Accounting:
        if len(this_sig_branches) > 0:
            sig_tree_count += 1
        for branch in this_sig_branches:
            if branch not in sig_branch_dict:
                sig_branch_dict[branch] = 1
            else:
                sig_branch_dict[branch] += 1

        # Append others if available:
#        try:
#            append_BSREL3(buffer2, csv_filename + ".BSREL", whole_tree)
#        except FileNotFoundError:
#            continue
#        try:
#            append_MG94(buffer2, csv_filename + ".mglocal.csv",
#            whole_tree)
#        except FileNotFoundError:
#            continue
        buffer = concat_buffers(buffer, buffer2)
    return buffer, sig_branch_dict, sig_tree_count

def get_prefixes(sim_dir):
    import glob
    import re
    file_list = glob.glob(sim_dir + os.sep + "*.out")
    #file_list = [a for a in file_list if re.search("^\w+\/\w+\.\d+$", a) != None]
    return file_list

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("input", help="input csv file or directory")
    parser.add_argument("output", help="output file")
    parser.add_argument("--whole-tree",
                        dest="whole_tree",
                        help="treat the tree as a single unit, rather than \
                        the branches independently",
                        action='store_true')
    parser.add_argument("--stats",
                        dest="print_stats",
                        help="print the number of branches found to be \
                        under significant positive selection, the number of \
                        trees found to contain at least one branch under \
                        significant positive selection and a list of all \
                        branches and hit count for each branch found to be \
                        under significant positive selection",
                        action='store_true')
    args = parser.parse_args()

    buffer = []
    # dir input:
    sim_dir = args.input
    output_filename = args.output
    prefixes = get_prefixes(sim_dir)
    if len(prefixes) == 0:
        print("Error: no valid files found")
        exit(1)
    buffer, sig_branches_dict, sig_tree_count = run_batch( buffer,
                                                            prefixes,
                                                            args.whole_tree)
    if args.print_stats:
        print(  len(prefixes), " total trees")
        print(  str(sum(sig_branches_dict.values())),
                " significant branches found")
        print(str(sig_tree_count), " significant trees found")
        print("branches and hit number:")
        print(sig_branches_dict)
    write_buffer(buffer, output_filename)
