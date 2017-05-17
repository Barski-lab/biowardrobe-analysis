#!/usr/bin/env python
import os
import DefFunctions as util
from basic_analysis import Settings
import datetime
import sys
from basic_analysis.exceptions import BiowBasicException
from basic_analysis.run_dna_func import check_job, submit_job
from basic_analysis.constants import LIBSTATUS


biow_db_settings = Settings.Settings()
WORKFLOW = 'run-dna-single-end.cwl'
TEMPLATE_JOB = ('{{'
                  '"fastq_input_file": {{"class": "File", "location": "{fastq_input_file}", "format": "http://edamontology.org/format_1930"}},'
                  '"bowtie_indices_folder": {{"class": "Directory", "location": "{bowtie_indices_folder}"}},'
                  '"clip_3p_end": {clip_3p_end},'
                  '"clip_5p_end": {clip_5p_end},'
                  '"threads": {threads},'
                  '"remove_duplicates": "{remove_duplicates}",'
                  '"control_file": {{"class": "File", "location": "{control_file}", "format": "http://edamontology.org/format_2572"}},'
                  '"exp_fragment_size": {exp_fragment_size},'
                  '"force_fragment_size": "{force_fragment_size}",'
                  '"broad_peak": "{broad_peak}",'
                  '"chrom_length": {{"class": "File", "location": "{chrom_length}", "format": "http://edamontology.org/format_2330"}},'
                  '"genome_size": "{genome_size}",'
                  '"output_folder": "{output_folder}",' # required
                  '"uid": "{uid}"'                      # required
                '}}')

def use_ems():
    biow_db_settings.cursor.execute ('use ems')

def use_airflow():
    biow_db_settings.cursor.execute ('use airflow')



print str(datetime.datetime.now())

# SUBMIT JOB

use_ems()
biow_db_settings.cursor.execute((
    "update labdata set libstatustxt='ready for process',libstatus={START_PROCESS} "
    "where libstatus={SUCCESS_DOWNLOAD} and experimenttype_id in "
    "(select id from experimenttype where etype like 'DNA%') "
    "and COALESCE(egroup_id,'') <> '' and COALESCE(name4browser,'') <> '' and deleted=0 ").format(**LIBSTATUS))
biow_db_settings.cursor.execute((
    "select e.etype,g.db,g.findex,g.annotation,l.uid,fragmentsizeexp,fragmentsizeforceuse,forcerun, "
    "COALESCE(l.trim5,0), COALESCE(l.trim3,0),COALESCE(a.properties,0), COALESCE(l.rmdup,0),g.gsize, "
    "COALESCE(control,0), COALESCE(control_id,'') "
    "from labdata l "
    "inner join (experimenttype e,genome g ) ON (e.id=experimenttype_id and g.id=genome_id) "
    "LEFT JOIN (antibody a) ON (l.antibody_id=a.id) "
    "where e.etype like 'DNA%' and libstatus in ({START_PROCESS},1010) "
    "and deleted=0 and COALESCE(egroup_id,'') <> '' and COALESCE(name4browser,'') <> '' "
    " order by control DESC,dateadd").format(**LIBSTATUS))

rows = biow_db_settings.cursor.fetchall()

for row in rows:
    print "ROW: " + str(row)
    sys.stdout.flush()
    try:
        use_airflow()
        util.check_if_duplicate_dag(row[4], biow_db_settings)
        use_ems()
        submit_job (db_settings=biow_db_settings,
                   row=row,
                   raw_data=os.path.join(biow_db_settings.settings['wardrobe'], biow_db_settings.settings['preliminary']),
                   indices=os.path.join(biow_db_settings.settings['wardrobe'], biow_db_settings.settings['indices']),
                   workflow=WORKFLOW,
                   template_job=TEMPLATE_JOB,
                   threads=biow_db_settings.settings['maxthreads'],
                   jobs_folder=sys.argv[1]) # sys.argv[1] - path where to save generated job files
        util.update_status(row[4], 'Processing', 11, biow_db_settings)
    except BiowBasicException as ex:
        use_ems()
        util.submit_err (ex, biow_db_settings)
        continue


# CHECK STATUS
use_ems()
biow_db_settings.cursor.execute((
    "select e.etype,l.uid,l.libstatustxt "
    "from labdata l "
    "inner join experimenttype e ON e.id=experimenttype_id "
    "where e.etype like 'DNA%' and libstatus = {PROCESSING} "
    "and deleted=0 and COALESCE(egroup_id,'') <> '' and COALESCE(name4browser,'') <> '' "
    "order by control DESC,dateadd").format(**LIBSTATUS))

rows = biow_db_settings.cursor.fetchall()

for row in rows:
    print "ROW: " + str(row)
    try:
        use_airflow()
        libstatus, libstatustxt = check_job (db_settings=biow_db_settings,
                                             row=row,
                                             workflow=WORKFLOW,
                                             jobs_folder=sys.argv[1])
        if libstatus:
            use_ems()
            util.update_status(row[1], libstatustxt, libstatus, biow_db_settings)
    except BiowBasicException as ex:
        use_ems()
        util.submit_err (ex, biow_db_settings)
        continue

