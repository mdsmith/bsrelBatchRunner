#! /usr/bin/env python

import os, sys, subprocess, time
node_list = range(13,31)
local_processes = {}

def get_files(in_file):
    if os.path.isfile(in_file) == True:
        return [in_file]
    file_list = []
    for path, dirs, files in os.walk(in_file):
        temp_files = [os.path.join(in_file,f) for f in files if f.upper().find("NEX") != -1]
        temp_files = [t for t in temp_files
                        if t.upper().endswith("NEX")
                        or t.upper()[-1].isdigit()]
        file_list = file_list + temp_files
    return file_list

def get_out_dir(out_dir):
    if os.path.isdir(out_dir) == True:
        return os.path.abspath(out_dir)
    else:
        return os.path.dirname(os.path.abspath(__file__))


# XXX out_file is more of an out suffix... Fix that
def run_BSREL(  file_name,
                file_name_body,
                out_dir, #nodeI,
                var_beta, #rep,
                nodeI,
                node_processes = local_processes,
                nodes = node_list):
    #file_name_body = file_name[:-4]
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
    batchfile.write('inputRedirect["03"]= "Y";\n')
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
                    str(nodes[nodeI]),
                    'HYPHYMP',
                    out_dir + os.sep
                            + file_name_body
                            + ".bf"]
    output_file = open( out_dir
                        + os.sep
                        + file_name_body
                        + ".out.stdout", 'w')
    node_processes[str(nodeI)] = subprocess.Popen( call_list,
                                                    stdout=output_file)
    time.sleep(1)

def run_all_BSREL(  in_file,
                    out_dir,
                    var_beta,
                    node_processes = local_processes,
                    nodes = node_list):
    nex_file_list = get_files(in_file)
    out_dir = get_out_dir(out_dir)
    files_done = 0
    while files_done < len(nex_file_list):
        for sub_file_num in range( min(len(nex_file_list) - files_done,
                                    len(nodes))):
            this_file_num = files_done + sub_file_num
            file_name = nex_file_list[this_file_num]
            in_path, file_name_body = os.path.split(file_name)
            #file_name_body = file_name_body[:-4]
            run_BSREL(  file_name,
                        file_name_body,
                        out_dir,
                        var_beta,
                        sub_file_num,
                        node_processes,
                        nodes)
        for sub_p in node_processes.values():
            sub_p.wait()
        files_done += min(len(nex_file_list) - files_done, len(nodes))

if __name__ == "__main__":
    var_beta = False
    if len(sys.argv) == 2:
        in_file = sys.argv[1]
        out_dir = ""
    elif len(sys.argv) == 3:
        in_file, out_dir = sys.argv[1:3]
    elif len(sys.argv) == 4:
        in_file, out_dir, var_beta = sys.argv[1:4]
        if var_beta.upper() == "TRUE":
            var_beta = True
        else:
            var_beta = False
    else:
        print(  "Valid usage:\n" \
                "\t- bsrelrun <in_file or directory> [<out_dir>] [<var_beta>]\n" \
                "Where:\n" \
                "\t- <in_file or directory>: Nex file to run or directory " \
                " of Nex files to run\n" \
                "\t- <out_dir>: directory into which output files are dumped\n" \
                "\tIf not specified output will be saved in" \
                " the current working directory\n" \
                "\t- <varBeta>: beta will vary from branch to branch if" \
                " this is true, be constant if this is false or not" \
                " specified\n",
                file=sys.stderr)
        exit(1)
    run_all_BSREL(  in_file,
                    out_dir,
                    var_beta)
