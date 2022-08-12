## Cluster cheat sheet

For more info visit https://www.nsc.liu.se/support/index.html

### Basic operations

- _cd_ to move between directories, _\~_ is home and _/_ is root

- _cp_ to copy files

- _mv_ to move or rename files

- _ls_ to list files in directory (ls -a to also view hidden files)

### Move files between local computer and remote

Use the following commands to move files to the cluster or from the cluster to your local computer. Use these commands from your local computer (i.e. not when accessing the cluster using ssh). You can also add the _-r_ flag to scp to move folders. It is often a good idea to compress folders to save space. To do so, just google something like "linux tar gz command line".

- _scp path/to/local/file username@tetralith.nsc.liu.se:path/to/remote/directory_

- _scp username@tetralith.nsc.liu.se:path/to/remote/file path/to/local/directory_

### Login

- _ssh username@tetralith.nsc.liu.se_

- password

- authentication

## Working on the cluster

You have a home directory where you store some smaller files and packages. Bigger files are stored at the project's directory. You can find that directory by writing either _projinfo_ or _snicquota_.

### Explore and edit files

- view contents of files using _cat_ or _head_ or _tail_

- edit file with _nano_

- load modules

### Modules

In order to run things on the cluster you need to load the appropriate modules.

- _module avail_ lists all available modules, you can modify it to list only relevant modules by writing for example _module avail java_.

- _module load_ or _ml_ can be used to load modules

- For topic modeling we use the java installation _Java/1.8.0_181-nsc1_

- For python we have set up a virtual environment from which the relevant installation is being used.

### Running jobs

While you can run things in the terminal as usual, this is not recommended for demanding jobs. These demaning jobs should be sent to the computation nodes using _sbatch_.

Start of by loading the relevant modules (these will be cloned when starting an _sbatch_ jobs), you can also do this within the _.sh_ scripts.

To then run the job, write _sbatch script_name.sh_ in the terminal to start the job. These scripts look something like:

>
#!/bin/sh
#SBATCH -t 120:00:00
#SBATCH -N 1
#SBATCH --exclusive
java -jar -Xmx120g -Xms120g target/PCPLDA-9.1.0.jar --run_cfg=src/main/resources/configuration/dn_100.cfg

### Progress of jobs

To view all of your jobs either in queue or in progress write _squeue --user $USER_. You can cancel jobs using some variation of _scancel_. All printed output that usually would show up in the console is instead redirected to slurm files which you can view using _cat_.

