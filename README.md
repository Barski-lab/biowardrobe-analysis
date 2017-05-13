## BioWardorbe Basic Analysis

Note:
1. Create ***/wardrobe/indices/BOWTIE*** folder
and place there subfolders ***dm3***, ***mm10***, ***hg19*** etc.
In each of subfolders make `ln -s` on real bowtie indices and
put ***chrNameLength.txt***.
We need the following structure to allow mount indices folder to
container which runs Bowtie.
If using different names for files or folders make sure that all
updates of constant variables are properly made in ***constants.py***
file
2. Make sure that you've pulled all docker images used by workflow.
If not, it'll take a lot of time to pull it while executing task and it
will be marked as failed one
