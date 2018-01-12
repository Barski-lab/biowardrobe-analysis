#!/usr/bin/env python
"""RNA-Seq SE(dUTP)/PE(dUTP), script produces jobs for four workflows"""

import os
from DefFunctions import raise_if_dag_exists, update_status, submit_err, check_job
from Settings import Settings
import datetime
import sys
from biow_exceptions import BiowBasicException
from run_rna_func import submit_job
from constants import (LIBSTATUS, RNA_SEQ_SET)
from db_uploader import upload_results_to_db
from db_upload_list import RNA_SEQ_UPLOAD


# Get access to DB
biow_db_settings = Settings()
print str(datetime.datetime.now())

# Get job file folder
EXPORT_JOBS_TO = sys.argv[1]

# Get all new experiments
biow_db_settings.use_ems()
biow_db_settings.cursor.execute((
    "select e.etype, l.uid, g.db, g.findex, g.annotation, g.annottable, g.genome, l.forcerun, "
    "COALESCE(l.trim5,0), COALESCE(l.trim3,0) "
    "from labdata l, experimenttype e, genome g "
    "where e.id=experimenttype_id and g.id=genome_id and e.etype like 'RNA%' "
    "and libstatus in ({SUCCESS_DOWNLOAD},{START_PROCESS}) "
    "and COALESCE(egroup_id,'') <> '' and COALESCE(name4browser,'') <> '' and deleted=0 "
    "order by dateadd").format(**LIBSTATUS))

rows = biow_db_settings.cursor.fetchall()

# Generate job files for all found experiments
for row in rows:
    print "SUBMIT JOB ROW: " + str(row)
    sys.stdout.flush()
    try:
        raise_if_dag_exists(uid=row[1], db_settings=biow_db_settings)
        submit_job (db_settings=biow_db_settings,
                   row=row,
                   raw_data=os.path.join(biow_db_settings.settings['wardrobe'], biow_db_settings.settings['preliminary']),
                   indices=os.path.join(biow_db_settings.settings['wardrobe'], biow_db_settings.settings['indices']),
                   workflow=RNA_SEQ_SET[row[0]][0],
                   template_job=RNA_SEQ_SET[row[0]][1],
                   threads=biow_db_settings.settings['maxthreads'],
                   jobs_folder=EXPORT_JOBS_TO) # path where to save generated job files
        update_status(uid=row[1],
                      db_settings=biow_db_settings,
                      message='Scheduled',
                      code=LIBSTATUS["JOB_CREATED"],
                      optional_column="forcerun=0, dateanalyzes=NULL")
    except BiowBasicException as ex:
        submit_err (error=ex, db_settings=biow_db_settings)
        continue


# Get all running or just created jobs
biow_db_settings.use_ems()
biow_db_settings.cursor.execute((
    "select e.etype,l.uid,l.libstatustxt "
    "from labdata l "
    "inner join experimenttype e ON e.id=experimenttype_id "
    "where e.etype like 'RNA%' and libstatus in ({JOB_CREATED}, {PROCESSING}) "
    "and deleted=0 and COALESCE(egroup_id,'') <> '' and COALESCE(name4browser,'') <> '' "
    "order by dateadd").format(**LIBSTATUS))
rows = biow_db_settings.cursor.fetchall()

# Check status of running jobs
for row in rows:
    print "CHEK JOB ROW: " + str(row)
    try:
        libstatus, libstatustxt = check_job (uid=row[1],
                                             db_settings=biow_db_settings,
                                             workflow=RNA_SEQ_SET[row[0]][0],
                                             jobs_folder=EXPORT_JOBS_TO) # path where to save generated job files
        if libstatus:
            update_status(uid=row[1],
                          message=libstatustxt,
                          code=libstatus,
                          db_settings=biow_db_settings)
            update_status(uid=row[1],  # is used only to set dateanalyzes value which is always NULL after we restarted or created new experiment
                          message=libstatustxt,
                          code=libstatus,
                          db_settings=biow_db_settings,
                          optional_column="forcerun=0, dateanalyzes=now()",
                          optional_where="and dateanalyzes is null")
            if libstatus==LIBSTATUS["SUCCESS_PROCESS"]:
                update_status(uid=row[1],
                              message=libstatustxt,
                              code=libstatus,
                              db_settings=biow_db_settings,
                              optional_column="dateanalyzee=now()") # Set the date of last analysis
                upload_results_to_db(upload_set=RNA_SEQ_UPLOAD,
                                     uid=row[1],
                                     raw_data=os.path.join(biow_db_settings.settings['wardrobe'], biow_db_settings.settings['preliminary']),
                                     db_settings=biow_db_settings)
    except BiowBasicException as ex:
        submit_err (error=ex, db_settings=biow_db_settings)
        continue

