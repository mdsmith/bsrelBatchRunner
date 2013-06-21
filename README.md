bsrelBatchRunner
================

A python program for running BSREL in HyPhy on batches of Nexus files that
uses bpsh to assign these analyses to cluster nodes.

Usage
-----

    Valid usage:
    	- bsrelrun <in_file or directory> [<out_dir>] [<var_beta>]
    Where:
    	- <in_file or directory>: Nex file to run or directory  of Nex files to run
    	- <out_dir>: directory into which output files are dumped
    	If not specified output will be saved in the current working directory
    	- <varBeta>: beta will vary from branch to branch if this is true, be constant if this is false or not specified

