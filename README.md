## BioWardorbe Basic Analysis

### General
`run_dna_cron.py` and `run_rna_cron.py` scripts should be set as cron jobs to
periodically perform the following operations:
- check newly added **ChIP-Seq** / **RNA-Seq** experiments, generate and export JSON job files;
- check the state of all running **ChIP-Seq** / **RNA-Seq** experiments, update their
states in BioWardrobe DB;
- when experiment is finished with success, upload generated data to BioWardrobe DB

Usage:
```bash
  -c CONFIG, --config CONFIG BioWardrobe configuration file
  -j JOBS,   --jobs   JOBS   Folder to export generated jobs
```


### Installation
1. Install **biowardrobe-analysis** from source
  ```sh
  $ git clone https://github.com/Barski-lab/biowardrobe-analysis.git
  $ cd biowardrobe-analysis
  $ pip install .
  ```

### Configuration and running
***To make configuration process easier we are assuming that:***
1. you home directory is `/home/biowardrobe/`
2. you have already installed and configured:
    * ***[BioWardrobe](https://github.com/Barski-lab/biowardrobe)***
        * BioWardrobe configuration file is saved as `/etc/wardrobe/wardrobe`
        and has the following structure (the order of the first five not commented lines is
        mandatory)
            ```
                #MySQL host to connect
                127.0.0.1
                
                #MySQL User (Pay attention, the user should also have read access to Airflow DB)
                username
                
                #MySQL password
                userpassword
                
                #Wardrobe DB
                ems
                
                #MySQL port
                3306
                
                #Custom additional configuration data
             ```
                
        * the user who run `run_dna_cron.py` and `run_rna_cron.py` scripts has read access
        to BioWardrobe configuration file
        * BioWardrobe DB `ems.settings` table includes
            ```
            +---------------+-------------+------------------------------------------------------------------------+
            | key           | value       | description                                                            |
            +---------------+-------------+------------------------------------------------------------------------+
            | indices       | /indices    | Relative path to the directory for mapping software indices files      |
            | preliminary   | /RAW-DATA   | Relative path where fastq and all preliminary results are stored       |
            | wardrobe      | /wardrobe   | Absolute path to the Wardrobe directory                                |
            +---------------+-------------+------------------------------------------------------------------------+
            ```
    * ***[cwl-airflow](https://github.com/Barski-lab/cwl-airflow)***
        * Airflow DB with the name `airflow` is saved on the same MySQL server as
        BioWardrobe DB and is accessable by the user set in BioWardrobe
        configuration file
        * Airflow configuratin file `airflow.cfg` includes fields 
            * `cwl_jobs = /home/biowardrobe/cwl/jobs`
            * `cwl_workflows = /home/biowardrobe/cwl/workflows`
        * Directory set as `cwl_jobs` in `airflow.cfg` has the following structure
            ```
            /home/biowardrobe/cwl/jobs
                                     ├── fail
                                     ├── new
                                     ├── running
                                     └── success
            ```
    * ***[biowardrobe-analysis](https://github.com/Barski-lab/biowardrobe-analysis.git)***
        * the ***[constants.py](https://github.com/Barski-lab/biowardrobe-analysis/blob/master/basic_analysis/constants.py)***
          includes the following constants:
            ```python
            BOWTIE_INDICES = "bowtie"
            RIBO_SUFFIX = "_ribo"
            STAR_INDICES = "STAR"
            ANNOTATIONS = "annotations"
            JOBS_NEW = 'new'
            JOBS_SUCCESS = 'success'
            JOBS_FAIL = 'fail'
            JOBS_RUNNING = 'running'
            CHR_LENGTH_GENERIC_TSV = "chrNameLength.txt"
            ANNOTATION_GENERIC_TSV = "refgene.tsv"
            ```

3. You have cloned the latest ***[Workflows](https://github.com/Barski-lab/workflows)***
into `/home/biowardrobe/cwl/workflows` (currently it's recommended to use
`v1.0.2` branch instead of `master`)

#### Steps:
1. To allow `run_dna_cron.py` and `run_rna_cron.py` scripts find the Airflow DB, the following record
   should be added into `ems.settings` table
    ```sql
        INSERT INTO ems.settings  VALUES ('airflowdb','airflow','Database name to be used by Airflow', 0, 3);
    ```
    > where `airflowdb` is the key by which the name of the Airflow DB `airflow` is returned.
      The Airflow DB is used to check the state of the running workflows and their steps
      (performs select query from  `dag_run` and `task_instance` tables).
    
2. Create **/wardrobe/indices/bowtie** folder
   > This folder name is formed as  
   > `ems.settings[wardrobe] + ems.settings[indices] + constants.py[BOWTIE_INDICES]`

3. Get the genome types list as `SELECT findex FROM ems.genome`. For each genome type
create subfolder within **/wardrobe/indices/bowtie**. The subfolder name should
be equal to the genome type received from SELECT query
   >  For example, if `SELECT` query returned `hg19`, `mm10`, `dm3`, your
      directories should look like:  
      `/wardrobe/indices/bowtie/hg19`  
      `/wardrobe/indices/bowtie/mm10`  
      `/wardrobe/indices/bowtie/dm3`

4. In each subfolder created in the previous step put corespondent
to the genome type Bowtie indices and TAB-delimited chromosome length
file **chrNameLength.txt**
    > The name for chromosome length file should be equal to
      `CHR_LENGTH_GENERIC_TSV` from `constants.py`

5. For running **RNA-Seq** analysis the ribosomal Bowtie indices should be added too.
For each of the genome type folders in **/wardrobe/indices/bowtie** create
additional folder with the suffix **_ribo**
    > Suffix `_ribo` should be equal to the `RIBO_SUFFIX` from `constants.py`  
      For example, if you already have directories `hg19`, `mm10`, `dm3`
      in `/wardrobe/indices/bowtie/` folder, you should add  
      `/wardrobe/indices/bowtie/hg19_ribo`  
      `/wardrobe/indices/bowtie/mm10_ribo`  
      `/wardrobe/indices/bowtie/dm3_ribo`

6. In each subfolder created in the previous step put corespondent
to the genome type ribosomal Bowtie indices
    
7. Create **/wardrobe/indices/annotations** folder
   > This folder name is formed as  
   > `ems.settings[wardrobe] + ems.settings[indices] + constants.py[ANNOTATIONS]` 

8. Get the genome types list as `SELECT findex FROM ems.genome` (you should
already have this list from some step before). For each genome type
create subfolder within **/wardrobe/indices/annotations**. The subfolder name should
be equal to the genome type received from SELECT query
   >  For example, if `SELECT` query returned `hg19`, `mm10`, `dm3`, your
      directories should look like:  
      `/wardrobe/indices/annotations/hg19`  
      `/wardrobe/indices/annotations/mm10`  
      `/wardrobe/indices/annotations/dm3`

9. In each subfolder created in the previous step put corespondent
to the genome type TAB-delimited annotation file **refgene.tsv**.
This file is not mandatory to be sorted. 
    > The TAB-delimited annotation file name should be equal to
      `ANNOTATION_GENERIC_TSV` from `constants.py`

10. To make Genome Browser to display genome coverage tracks from bigWig files, apply patches from
***[biowardrobe_patched_view](https://github.com/Barski-lab/biowardrobe-analysis/tree/master/sql_patch/biowardrobe_patched_view)***

11. Because the new status `"JOB_CREATED": 1010` was added into `LIBSTATUS` from `constants.py`,
***[app.css](https://github.com/Barski-lab/biowardrobe/blob/master/EMS/ems/app.css)*** file from
***[BioWardrobe](https://github.com/Barski-lab/biowardrobe)*** should be
updated to display correct icon
    ```bash
    .gear-1-10 {
        background-image: url(images/gear_new.png) !important;
        width: 16px;
        height: 16px;
    }
    ```
    > Basically you should change `gear_warning.png` to `gear_new.png` for `.gear-1-10`

12. To drop all of the created by **biowardrobe-analysis** tables from BioWardrobe DB, as long as
    all of tables from Airflow DB, related to the expeminent to be restarted,
    update original ***[ForceRun.py](https://github.com/Barski-lab/biowardrobe/blob/master/scripts/ForceRun.py)***
    with the following commands
    ```python
    # Airflow specific tables
    settings.cursor.execute("DROP TABLE IF EXISTS `" + DB[0] + "`.`" + string.replace(UID, "-", "_") + "_f_wtrack`;")
    settings.cursor.execute("DROP TABLE IF EXISTS `" + DB[0] + "`.`" + string.replace(UID, "-", "_") + "_upstream_f_wtrack`;")
    settings.cursor.execute("DROP TABLE IF EXISTS `" + DB[0] + "`.`" + string.replace(UID, "-", "_") + "_downstream_f_wtrack`;")
    ```
    ```python
    # Clean up airflowdb
    airflowDB = settings.settings["airflowdb"]
    settings.cursor.execute("DELETE FROM `{0}`.`xcom` WHERE dag_id LIKE '%{1}%';".format(airflowDB,UID))
    settings.cursor.execute("DELETE FROM `{0}`.`task_instance` WHERE dag_id LIKE '%{1}%';".format(airflowDB,UID))
    settings.cursor.execute("DELETE FROM `{0}`.`task_fail` WHERE dag_id LIKE '%{1}%';".format(airflowDB,UID))
    settings.cursor.execute("DELETE FROM `{0}`.`sla_miss` WHERE dag_id LIKE '%{1}%';".format(airflowDB,UID))
    settings.cursor.execute("DELETE FROM `{0}`.`log` WHERE dag_id LIKE '%{1}%';".format(airflowDB,UID))
    settings.cursor.execute("DELETE FROM `{0}`.`job` WHERE dag_id LIKE '%{1}%';".format(airflowDB,UID))
    settings.cursor.execute("DELETE FROM `{0}`.`dag_run` WHERE dag_id LIKE '%{1}%';".format(airflowDB,UID))
    settings.cursor.execute("DELETE FROM `{0}`.`dag` WHERE dag_id LIKE '%{1}%';".format(airflowDB,UID))
    ```
    > The location to insert this commands can be checked from updated
      ***[ForceRun.py](https://github.com/Barski-lab/biowardrobe-analysis/blob/master/basic_analysis/ForceRun.py)***
      
13. Update crontab job
    ```
        # For ChIP-Seq analysis
        */1 * * * *    . ~/.profile && run_dna_cron.py -c /etc/wardrobe/wardrobe -j /home/biowardrobe/cwl/jobs >> /wardrobe/tmp/RunAirflowDNA.log 2>&1
        # For RNA-Seq analysis
        */1 * * * *    . ~/.profile && run_rna_cron.py -c /etc/wardrobe/wardrobe -j /home/biowardrobe/cwl/jobs >> /wardrobe/tmp/RunAirflowDNA.log 2>&1
    ```
    > Both the `run_dna_cron.py` and `run_rna_cron.py` scripts use BioWardrobe configuration file
      set as `--config`/`-c` argument (`/etc/wardrobe/wardrobe` by default).
      This file is used to get access to BioWardrobe DB. Make sure that scripts have read access
      to this configuration file.