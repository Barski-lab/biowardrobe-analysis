## BioWardorbe Basic Analysis

Note:
1. Create **/wardrobe/indices/[BOWTIE_INDICES]** folder
and place there subfolders **dm3**, **mm10**, **mm10c**, **hg19**, 
**hg19c** etc. (corresponds to `select findex from ems.genome;`).
In each subfolders make `ln -s` on real bowtie indices and
put **chrNameLength.txt** (**CHR_LENGTH_GENERIC_TSV**).
We need the following structure to allow mount indices folder to
container which runs Bowtie.

2. Create **/wardrobe/indices/[ANNOTATIONS]** folder and place there subfolders
***dm3***, ***mm10***, ***mm10c***, ***hg19***, 
***hg19c*** etc. (corresponds to `select findex from ems.genome;`).
Each of these subfolders may include various annotation files
for correspondent genome. Tab-separated files should be named **refgene.tsv**
(**ANNOTATION_GENERIC_TSV**)

3. Make sure that you've pulled all docker images used by workflow.
If not, it'll take a lot of time to pull it while executing task and it
will be marked as failed one
