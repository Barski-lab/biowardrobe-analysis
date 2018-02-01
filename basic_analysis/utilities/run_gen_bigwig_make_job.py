"""Creates job file to be run in Airflow to generate bigWig files for all ChIP-Seq experiments"""
import datetime
import sys
import string
from basic_analysis.Settings import Settings
from basic_analysis.constants import (LIBSTATUS,
                                      CHIP_SEQ_GEN_BIGWIG_WORKFLOW,
                                      CHIP_SEQ_GEN_BIGWIG_TEMPLATE_JOB)
from basic_analysis.biow_exceptions import BiowBasicException
from run_gen_bigwig_func import submit_job
from basic_analysis.DefFunctions import (raise_if_dag_exists,
                                         check_job,
                                         raise_if_table_exists)


# Set experiment type to get correct workflow and template from EXP_TYPE
exp_type = 'GEN-bigWig'


# Get access to DB
biow_db_settings = Settings()
print str(datetime.datetime.now())


# Get all ChIP-Seq SE/PE experiments
biow_db_settings.use_ems()
biow_db_settings.cursor.execute((
    "select    e.etype,   g.db,   g.findex,   l.uid,   COALESCE(l.fragmentsize,0),   COALESCE(l.tagsmapped,0) from labdata l "
    "inner join (experimenttype e,genome g) ON (e.id=experimenttype_id and g.id=genome_id) "
    "where e.etype like 'DNA%' and libstatus={SUCCESS_PROCESS} "
    "and deleted=0 and COALESCE(egroup_id,'') <> '' and COALESCE(name4browser,'') <> '' ").format(**LIBSTATUS))
rows = biow_db_settings.cursor.fetchall()


for row in rows:
    # Generate job files for all found experiments where bigWig is not present
    print "SUBMIT JOB ROW: " + str(row)
    sys.stdout.flush()
    try:
        # First line of protection from generating the job files for the experiments that are already run by Airflow
        raise_if_dag_exists(uid=row[3], db_settings=biow_db_settings)
        raise_if_table_exists (db_settings=biow_db_settings, uid=row[3], table=string.replace(row[3], "-", "_") + "_f_wtrack", db=row[1])
        # Second line of protection: if the same file already exists in new or running folder
        # or if bigwig file the with the necessary name already exists
        submit_job (db_settings=biow_db_settings,
                   row=row,
                   workflow=CHIP_SEQ_GEN_BIGWIG_WORKFLOW,
                   template_job=CHIP_SEQ_GEN_BIGWIG_TEMPLATE_JOB,
                   jobs_folder=sys.argv[1]) # sys.argv[1] - path where to save generated job files
    except BiowBasicException as ex:
        print "   SKIP: generating job file for ", row[3], " - ", str(ex)
