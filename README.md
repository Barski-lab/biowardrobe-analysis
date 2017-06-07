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


### Installation on virtual machine in edit mode
1. Copy source code for the following repositories
```
    git clone https://github.com/Barski-lab/biowardrobe-analysis.git
    git clone https://github.com/Barski-lab/incubator-airflow.git
    git clone https://github.com/SciDAP/workflows.git
```
 
2. Make sure ***pip*** is installed or install it with the following commands
```
    wget --no-check-certificate https://bootstrap.pypa.io/get-pip.py
    sydo get-pip.py python
```
4. Make sure that ***nodejs*** in installed 
```bash
    sudo apt-get install nodejs
```
5. If you are planning to use MySQL DB insatall the following dependency
```bash
    sudo pip install mysql 
```
6. Install ***incubator-airflow*** in edit mode
```
    sudo pip install mysql
    cd incubator-airflow
    sudo pip install -e .
```
>> if you have problems with ***mysql-python*** (mysql_config not found) make sure that
 ***PATH*** includes path to ***mysql_config***. If there is no ***mysql_config***,
 run the following command
>>```bash
>>    sudo apt-get install libmysqlclient-dev -y
>>```
>>or
>>```bash
>>    sudo apt-get install libmariadbclient-dev -y
>>```
>> if you have problems with ***Python.h*** not found, run the following command
>>```bash
>>    sudo apt-get install python-dev
>>```
7. Install ***biowardrobe-analysis*** in edit mode
```
    cd biowardrobe-analysis
    sudo pip install -e .
```
8. Create the following folders
```bash
    mkdir -p ~/cwl/jobs/fail ~/cwl/jobs/new ~/cwl/jobs/running ~/cwl/jobs/success
    mkdir -p ~/cwl/output ~/cwl/tmp ~/cwl/workflows
```
```bash
    cwl
    ├── jobs
    │   ├── fail
    │   ├── new
    │   ├── running
    │   └── success
    ├── output
    ├── tmp
    └── workflows
```
9. Move ***workflow*** repository to `~/cwl/workflows/`
10. Update `~/airflow/airflow.cfg`
```bash
dags_folder = /home/biowardrobe/workspace/airflow/incubator-airflow/airflow/cwl_runner/cwl_dag/cwl_dag.py
executor = LocalExecutor
﻿sql_alchemy_conn = mysql://wardrobe:biowardrobe@127.0.0.1:3306/airflow
dags_are_paused_at_creation = False
load_examples = False

﻿[biowardrobe]
cwl_jobs = /home/biowardrobe/cwl/jobs
cwl_workflows = /home/biowardrobe/cwl/workflows
output_folder = /home/biowardrobe/cwl/output
tmp_folder = /home/biowardrobe/cwl/tmp
max_jobs_to_run = 1
log_level = INFO
strict = False

```
11. Create ***airflow*** database
```bash
    CREATE DATABSE AIRFLOW;
```
12. Init airflow database
```bash
    airflow initdb
```
13. [Install](https://linoxide.com/ubuntu-how-to/install-setup-docker-ubuntu-15-04/) ***Docker***
- Install packages to allow apt to use a repository over HTTPS
```bash
    sudo apt-get install \
        apt-transport-https \
        ca-certificates \
        curl \
        software-properties-common
```
- Add Docker’s official GPG key
```bash
    curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo apt-key add -
```
- Use the following command to set up the stable repository. You always need
the stable repository, even if you want to install edge builds as well.
```bash
    sudo add-apt-repository \
       "deb [arch=amd64] https://download.docker.com/linux/ubuntu \
       $(lsb_release -cs) \
       stable"
```
- Update the apt package index.
```bash
    sudo apt-get update
```
- Install the latest version of Docker
```bash
    sudo apt-get install docker.io
```
- Start doker service
```bash
    sudo service docker start
```
- Add current user to docker group
```bash
    sudo usermod -a -G docker biowardrobe
```
14. Update crontab job
```
    ﻿#*/10 * * * *    . ~/.profile && /wardrobe/bin/RunDNA.py >> /wardrobe/tmp/RunDNA.log 2>&1
    */1 * * * *    . ~/.profile && /home/biowardrobe/workspace/airflow/biowardrobe-analysis/basic_analysis/run_dna_cron.py /home/biowardrobe/cwl/jobs >> /wardrobe/tmp/RunAirflowDNA.log 2>&1
```
