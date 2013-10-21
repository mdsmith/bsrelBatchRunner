# XXX remove extraneous parameters!

def tokenize(line):
    line = [token.strip("\" :{},")
            for token in line.split()
            if token.strip("\" :{},") != ""]
    return line

# recover one dists worth of settings
def recover_settings(file_name):
    tree = {}
    sim_file = open(file_name, 'r')
    lines = sim_file.readlines()

    # narrow to settings:
    sentry_val = "bsrel_settings"
    settings = False
    new_lines = []
    for line in lines:
        if line[:2] == "};":
            settings = False
        if settings == True:
            new_lines.append(line)
        elif line[:len(sentry_val)]  == sentry_val:
            settings = True
    lines = new_lines

    in_branch = False
    in_omega = False
    current_branch = {}
    current_omegas = []
    current_props = []
    current_branch_number = 0
    # pull out the settings:
    #print("lines:")
    #print(lines)
    for line in lines:
        if in_branch == True and line[:2] == "},":
            tree[current_branch_number] = current_branch
            #print("current_branch")
            #print(current_branch)
            in_branch = False
        elif in_branch == False and line[0] == "\"":
            #print("starting a branch...")
            current_branch = {}
            current_omeags = []
            current_props = []
            in_branch = True

            line = tokenize(line)
            number = line[0]
            current_branch_number = number
            current_branch["name"] = number
            line = line[1:]
            if line[0] == "length":
                in_omega = False
                #print("length:")
                #print(line)
                variable, value = line
                current_branch[variable] = float(value)
            elif line[0] == "omegas":
                #print("omegas:")
                #print(line)
                # the variable name is omega, but we know there is a prop as
                # well
                in_omega = True
                _, omega_v, prop_v = line
                current_omegas.append(float(omega_v))
                current_props.append(float(prop_v))
        elif in_branch == True and line[0] == "}":
            #print("saving omegas and props...")
            current_branch["omegas"] = current_omegas
            current_branch["props"] = current_props
            current_omegas = []
            current_props = []
            in_omega = False
        elif in_branch == True:
            #print("expanding branch details...")
            #print(line)
            line = tokenize(line)
            #print(line)
            if in_omega == True:
                #print("extending omega")
                omega_v, prop_v = line
                current_omegas.append(float(omega_v))
                current_props.append(float(prop_v))
            if in_omega == False:
                #print("starting new parameter type")
                if line[0] == "length":
                    #print("length:")
                    #print(line)
                    variable, value = line
                    current_branch[variable] = float(value)
                elif line[0] == "omegas":
                    #print("omegas:")
                    #print(line)
                    # the variable name is omega, but we know there is a prop as
                    # well
                    in_omega = True
                    _, omega_v, prop_v = line
                    #print("appending!")
                    current_omegas.append(float(omega_v))
                    current_props.append(float(prop_v))
    return tree

# return a dict of the different taxa (which are themselves dicts
def recover_fit(num_taxa, rec_file_name, rep):
    results = {}
    fullfilename = rec_file_name + ".sim." + str(rep) + ".recovered.fit"
    print(fullfilename)
    resultsfile = open(fullfilename, 'r')
    lines = resultsfile.readlines()
    lines = [line for line in lines if line[:11] == "mixtureTree"]
    for taxaI in range(1, (2*num_taxa) - 2):
        results[str(taxaI)] = {}
    for line in lines:
        tokens = line.split('=')
        # check for and dispense with constraints
        if len(tokens) == 2:
            tokens[0] = tokens[0][12:]
            name, parameter = tokens[0].split('.')
            value = tokens[1].split(';')[0]
        results[name.upper()][parameter] = value
    for taxa in results.keys():
        #try:
        results[taxa] = format_results(taxa, results[taxa])
        #except KeyError:
            #print("dist: ", dist)
            #print("taxa: ", taxa)
            #print(results[taxa])
            #exit(1)
    return results

def recover_csv_mg94(file_name):
    results = {}
    results_file = open(file_name, 'r')
    lines = results_file.readlines()
    header = lines[0]
    lines = lines[1:]
    for line in lines:
        line = line.split(',')
        results[line[0]] = {}
        results[line[0]]["name"] = line[0]
        results[line[0]]["length"] = line[-1]
        results[line[0]]["omegas"] = [[line[1]]]
        results[line[0]]["props"] = [[1.0]]
    return results

def recover_csv(file_name, rep=-1):
    if rep != -1:
        file_name = file_name + ".sim." + str(rep) + ".recovered"
    results = {}
    results_file = open(file_name, 'r')
    lines = results_file.readlines()
    lines = lines[1:]
    for line in lines:
        line = line.split(',')
        results[line[0]] = {}
        results[line[0]]["name"] = line[0]
        results[line[0]]["length"] = line[-1]
        results[line[0]]["omegas"] = []
        results[line[0]]["props"] = []
        over_one = 0
        over_one_val = 0
        results[line[0]]["pval"] = line[7]
        if (line[3]) != "0":
            if line[3] == "inf":
                results[line[0]]["omegas"].append(10000)
                over_one_val = 10000
            else:
                results[line[0]]["omegas"].append(line[3])
                over_one_val = float(line[3])
            results[line[0]]["props"].append(line[4])
            over_one = 1
        for oI in range(1, (int(line[2]) + 1 - int(over_one))):
            if float(line[4]) == 1:
                omegas = 0
            else:
                omegas =    (float(line[1]) - (float(over_one_val) *
                            float(line[4]))) / (1 - float(line[4]))
            if omegas < 0:
                omegas = 0
            #results[line[0]]["omegas"].append(str(omegas))
            results[line[0]]["omegas"].append(omegas)
            #results[line[0]]["props"].append(str(   (1 - float(line[4]))
                                                    #/(float(line[2]) -
                                                    #over_one)))
            results[line[0]]["props"].append((  1 - float(line[4]))
                                                /(float(line[2]) -
                                                over_one))
    return results

def recover_simulated(num_taxa, rec_file_name, rep):
    input = {}
    sim_file = open(rec_file_name + ".sim.txt", 'r')
    lines = sim_file.readlines()
    names = [line for line in lines if line[:4] == "Node"]
    names = [name.split('.')[1].rstrip() for name in names]
    lengths = [line.strip() for line in lines if line.strip()[:6] == "Length"]
    lengths = [length.split('=')[1] for length in lengths]
    for name, length in zip(names, lengths):
        input[name] = {}
        input[name]["name"] = name
        input[name]["length"] = length
    # ad hoc FSM:
    cur_name = ""
    cur_omega_list = []
    cur_prop_list = []
    for line in lines:
        if line[:4] == "Node":
            if cur_name != "":
                input[cur_name]["omegas"] = cur_omega_list
                input[cur_name]["props"] = cur_prop_list
                cur_omega_list = []
                cur_prop_list = []
            cur_name = line.split('.')[1].strip()
        if line.lstrip()[:5] == "omega":
            cur_omega_list.append(line.split('=')[1].strip())
        if line.lstrip()[:6] == "weight":
            cur_prop_list.append(line.split('=')[1].strip())
    input[cur_name]["omegas"] = cur_omega_list
    input[cur_name]["props"] = cur_prop_list
    return input

def format_results(taxa, taxa_dict):
    entry = {}
    entry["name"] = taxa
    entry["omegas"] = []
    entry["props"] = []
    omega = False
    syn = False
    mulsyn = False
    for key in taxa_dict.keys():
        if key == 't':
            omega = True
        elif key == 'syn':
            syn = True
        elif key == 'syn1':
            mulsyn = True
    if omega == True:
        entry["length"] = taxa_dict["t"]
        for key in taxa_dict.keys():
            if key[:5] == "omega":
                entry["omegas"].append(taxa_dict[key])
            if key[:4] == "Paux":
                entry["props"].append(taxa_dict[key])
        entry["props"] = convolve_props(entry["props"])
    elif syn == True:
        entry["length"] = taxa_dict['syn']
        if taxa_dict['syn'] == '0':
            entry["omegas"].append('20000')
        else:
            entry["omegas"].append(str(float(taxa_dict["nonsyn"])/float(taxa_dict["syn"])))
        entry["props"] = 1
    elif mulsyn == True:
        syns = []
        nonsyns = []
        pauxs = []
        for key,value in taxa_dict.items():
            if key[:3] == 'syn':
                syns.append(taxa_dict[key])
                nonsyns.append(taxa_dict["nonsyn" + key[-1]])
                try:
                    pauxs.append(taxa_dict["Paux" + key[-1]])
                except KeyError:
                    continue
        props = convolve_props(pauxs)
        length = sum([float(syns[i]) * float(props[i]) for i in range(len(syns))])
        print(length)
        omegas = [  float(nonsyns[i])/float(syns[i]) if float(syns[i]) != 0 else 20000 for i in
                    range(len(syns))]
        print(omegas)
        entry["length"] = length
        entry["omegas"] = omegas
        entry["props"] = props
    else:
        print("Entry unrecognizable")
    return entry

def convolve_props(prop_list):
    # go from n-1 pauxs to n props
    tbr = []
    remaining_weight = 1
    for prop in prop_list:
        this_weight = str(float(prop)*remaining_weight)
        tbr.append(this_weight)
        remaining_weight -= float(this_weight)
    tbr.append(remaining_weight)
    return tbr
