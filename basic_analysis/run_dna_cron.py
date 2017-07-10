#!/usr/bin/env python
"""ChIP-Seq SE/PE script"""

import os
from DefFunctions import check_if_duplicate_dag, update_status, submit_err, check_job
from Settings import Settings
import datetime
import sys
from biow_exceptions import BiowBasicException
from run_dna_func import submit_job
from constants import (LIBSTATUS,
                       CHIP_SEQ_SE_WORKFLOW,
                       CHIP_SEQ_SE_TEMPLATE_JOB,
                       CHIP_SEQ_PE_WORKFLOW,
                       CHIP_SEQ_PE_TEMPLATE_JOB)
from db_uploader import upload_results_to_db
from db_upload_list import CHIP_SEQ_UPLOAD


# Get access to DB
biow_db_settings = Settings()
print str(datetime.datetime.now())

# Get all new experiments
biow_db_settings.use_ems()
biow_db_settings.cursor.execute((
    "update labdata set libstatustxt='ready for process',libstatus={START_PROCESS} "
    "where libstatus={SUCCESS_DOWNLOAD} and experimenttype_id in "
    "(select id from experimenttype where etype like 'DNA%') "
    "and COALESCE(egroup_id,'') <> '' and COALESCE(name4browser,'') <> '' and deleted=0 "
    "and notes like '%use airflow%' ").format(**LIBSTATUS))
biow_db_settings.conn.commit()
biow_db_settings.cursor.execute((
    "select e.etype,g.db,g.findex,g.annotation,l.uid,fragmentsizeexp,fragmentsizeforceuse,forcerun, "
    "COALESCE(l.trim5,0), COALESCE(l.trim3,0),COALESCE(a.properties,0), COALESCE(l.rmdup,0),g.gsize, "
    "COALESCE(control,0), COALESCE(control_id,'') "
    "from labdata l "
    "inner join (experimenttype e,genome g ) ON (e.id=experimenttype_id and g.id=genome_id) "
    "LEFT JOIN (antibody a) ON (l.antibody_id=a.id) "
    "where e.etype like 'DNA%' and libstatus in ({START_PROCESS},1010) "
    "and deleted=0 and COALESCE(egroup_id,'') <> '' and COALESCE(name4browser,'') <> '' "
    "and l.notes like '%use airflow%' "
    "order by control DESC,dateadd").format(**LIBSTATUS))
rows = biow_db_settings.cursor.fetchall()

# Generate job files for all found experiments
for row in rows:
    print "SUBMIT JOB ROW: " + str(row)
    sys.stdout.flush()
    try:
        check_if_duplicate_dag(uid=row[4],
                               db_settings=biow_db_settings)
        submit_job (db_settings=biow_db_settings,
                   row=row,
                   raw_data=os.path.join(biow_db_settings.settings['wardrobe'], biow_db_settings.settings['preliminary']),
                   indices=os.path.join(biow_db_settings.settings['wardrobe'], biow_db_settings.settings['indices']),
                   workflow=CHIP_SEQ_PE_WORKFLOW if 'pair' in row[0] else CHIP_SEQ_SE_WORKFLOW,
                   template_job=CHIP_SEQ_PE_TEMPLATE_JOB if 'pair' in row[0] else CHIP_SEQ_SE_TEMPLATE_JOB,
                   threads=biow_db_settings.settings['maxthreads'],
                   jobs_folder=sys.argv[1]) # sys.argv[1] - path where to save generated job files
        update_status(uid=row[4],
                      db_settings=biow_db_settings,
                      message='Scheduled for processing',
                      code=11,
                      option_string="forcerun=0, dateanalyzes=now()")
    except BiowBasicException as ex:
        submit_err (error=ex, db_settings=biow_db_settings)
        continue


# Get all running jobs
biow_db_settings.use_ems()
biow_db_settings.cursor.execute((
    "select e.etype,l.uid,l.libstatustxt "
    "from labdata l "
    "inner join experimenttype e ON e.id=experimenttype_id "
    "where e.etype like 'DNA%' and libstatus = {PROCESSING} "
    "and deleted=0 and COALESCE(egroup_id,'') <> '' and COALESCE(name4browser,'') <> '' "
    "and notes like '%use airflow%' "
    "order by control DESC,dateadd").format(**LIBSTATUS))
rows = biow_db_settings.cursor.fetchall()

# Check status of running jobs
for row in rows:
    print "CHEK JOB ROW: " + str(row)
    try:
        libstatus, libstatustxt = check_job (uid=row[1],
                                             db_settings=biow_db_settings,
                                             workflow=CHIP_SEQ_PE_WORKFLOW if 'pair' in row[0] else CHIP_SEQ_SE_WORKFLOW,
                                             jobs_folder=sys.argv[1]) # sys.argv[1] - path where to save generated job files
        if libstatus:
            update_status(uid=row[1],
                          message=libstatustxt,
                          code=libstatus,
                          db_settings=biow_db_settings)
            if libstatus==LIBSTATUS["SUCCESS_PROCESS"]:
                update_status(uid=row[1],
                              message=libstatustxt,
                              code=libstatus,
                              db_settings=biow_db_settings,
                              option_string="dateanalyzee=now()") # Set the date of last analysis
                upload_results_to_db(upload_set=CHIP_SEQ_UPLOAD,
                                     uid=row[1],
                                     raw_data=os.path.join(biow_db_settings.settings['wardrobe'], biow_db_settings.settings['preliminary']),
                                     db_settings=biow_db_settings)
    except BiowBasicException as ex:
        submit_err (error=ex, db_settings=biow_db_settings)
        continue

