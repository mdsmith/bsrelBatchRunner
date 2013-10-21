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
        prop_over_one_column = 
    prop_over_one_column.insert(0, "BSREL3_propOverOne")
    #print(prop_over_one_column)

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

def append_csv(buffer, filename, whole_tree):
    file = open(filename, 'r')
    contents = file.readlines()
    buffer += contents
    return buffer

def append_column(buffer, column):
    # strip newlines, add comma, replace newlines
    for i, line in enumerate(buffer):
        buffer[i] = line.strip("\n") + "," + str(column[i]) + "\n"
    return buffer

def concat_buffers(buffer1, buffer2):
    if len(buffer1) != 0:
        buffer2 = buffer2[1:]
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
def run_batch(buffer, prefixes, whole_tree):
    # for fileset in prefixes
    for prefix in prefixes:
        buffer2 = []
        #csv_filename = prefix + ".sim.0.recovered"
        csv_filename = prefix
        settings_filename = prefix
        append_csv(buffer2, csv_filename, whole_tree)
        #append_settings(buffer2, settings_filename)
        append_BSREL3(buffer2, settings_filename + ".BSREL", whole_tree)
        try:
            append_MG94(buffer2, settings_filename + ".mglocal.csv",
            whole_tree)
        except KeyError:
            print(settings_filename)
        buffer = concat_buffers(buffer, buffer2)
    return buffer

def get_prefixes(sim_dir):
    import glob
    import re
    file_list = glob.glob(sim_dir + os.sep + "*.nex.out")
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
    args = parser.parse_args()

    buffer = []
    # dir input:
    sim_dir = args.input
    output_filename = args.output
    prefixes = get_prefixes(sim_dir)
    buffer = run_batch(buffer, prefixes, args.whole_tree)
    write_buffer(buffer, output_filename)

#    if len(sys.argv) == 4 and sys.argv[1] == "-d":
#        # Do dir stuff
#        sim_dir = sys.argv[2]
#        output_filename = sys.argv[3]
#        buffer = []
#        #prefixes = [os.path.join(sim_dir, "longPy." + str(number)) for number in
#        #range(0,10000)]
#        prefixes = get_prefixes(sim_dir)
#        buffer = run_batch(buffer, prefixes)
#        write_buffer(buffer, output_filename)
#    elif len(sys.argv) == 4:
#        csv_filename = sys.argv[1]
#        settings_filename = sys.argv[2]
#        output_filename = sys.argv[3]
#
#        buffer = []
#        append_csv(buffer, csv_filename)
#        # append_settings(buffer, settings_filename)
#        # append_settings(buffer, settings_filename)
#        # append_simulated(buffer, settings_filename)
#        # append_fit(buffer, settings_filename)
#        write_buffer(buffer, output_filename)
#    else:
#        print(  "Valid usage:\n\t- bsrelSimCSVconvolve.py csv_filename " \
#                "settings_filename output_filename\n" \
#                "\t- Or bsrelSimCSVconvolve.py -d in_dir out_file\n",
#                file = sys.stderr)
#        exit(1)
