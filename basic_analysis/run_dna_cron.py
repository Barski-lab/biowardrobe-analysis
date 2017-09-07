#!/usr/bin/env python
"""ChIP-Seq SE/PE script"""

import os
from DefFunctions import raise_if_dag_exists, update_status, submit_err, check_job
from Settings import Settings
import datetime
import sys
from biow_exceptions import BiowBasicException
from run_dna_func import submit_job
from constants import (LIBSTATUS,
                       CHIP_SEQ_SE_WORKFLOW,
                       CHIP_SEQ_SE_TEMPLATE_JOB,
                       CHIP_SEQ_PE_WORKFLOW,
                       CHIP_SEQ_PE_TEMPLATE_JOB,
                       CHIP_SEQ_TRIM_SE_WORKFLOW,
                       CHIP_SEQ_TRIM_PE_WORKFLOW,
                       FOLDER_ID)
from db_uploader import upload_results_to_db
from db_upload_list import CHIP_SEQ_UPLOAD


# Get access to DB
biow_db_settings = Settings()
print str(datetime.datetime.now())

# Get all new experiments
biow_db_settings.use_ems()
biow_db_settings.cursor.execute((
    "select e.etype,g.db,g.findex,g.annotation,l.uid,fragmentsizeexp,fragmentsizeforceuse,forcerun, "
    "COALESCE(l.trim5,0), COALESCE(l.trim3,0),COALESCE(a.properties,0), COALESCE(l.rmdup,0),g.gsize, "
    "COALESCE(control,0), COALESCE(control_id,''), COALESCE(l.egroup_id,'') "
    "from labdata l "
    "inner join (experimenttype e,genome g ) ON (e.id=experimenttype_id and g.id=genome_id) "
    "LEFT JOIN (antibody a) ON (l.antibody_id=a.id) "
    "where e.etype like 'DNA%' and libstatus in ({SUCCESS_DOWNLOAD},{START_PROCESS}) "
    "and deleted=0 and COALESCE(egroup_id,'') <> '' and COALESCE(name4browser,'') <> '' "
    "order by control DESC,dateadd").format(**LIBSTATUS))
rows = biow_db_settings.cursor.fetchall()

# Generate job files for all found experiments
for row in rows:
    print "SUBMIT JOB ROW: " + str(row)
    sys.stdout.flush()
    try:
        raise_if_dag_exists(uid=row[4],
                            db_settings=biow_db_settings)

        current_workflow = {"True":  {"False": CHIP_SEQ_PE_WORKFLOW,
                                      "True":  CHIP_SEQ_TRIM_PE_WORKFLOW},
                            "False": {"False": CHIP_SEQ_SE_WORKFLOW,
                                      "True":  CHIP_SEQ_TRIM_SE_WORKFLOW}}[str('pair' in row[0])][str(FOLDER_ID == row[15])]

        submit_job (db_settings=biow_db_settings,
                   row=row,
                   raw_data=os.path.join(biow_db_settings.settings['wardrobe'], biow_db_settings.settings['preliminary']),
                   indices=os.path.join(biow_db_settings.settings['wardrobe'], biow_db_settings.settings['indices']),
                   workflow=current_workflow,
                   template_job=CHIP_SEQ_PE_TEMPLATE_JOB if 'pair' in row[0] else CHIP_SEQ_SE_TEMPLATE_JOB,
                   threads=biow_db_settings.settings['maxthreads'],
                   jobs_folder=sys.argv[1]) # sys.argv[1] - path where to save generated job files
        update_status(uid=row[4],
                      db_settings=biow_db_settings,
                      message='Scheduled',
                      code=LIBSTATUS["JOB_CREATED"],
                      optional_column="forcerun=0, dateanalyzes=NULL")
    except BiowBasicException as ex:
        submit_err (error=ex, db_settings=biow_db_settings)
        continue


# Get all running jobs
biow_db_settings.use_ems()
biow_db_settings.cursor.execute((
    "select e.etype,l.uid,l.libstatustxt,COALESCE(l.egroup_id,'') "
    "from labdata l "
    "inner join experimenttype e ON e.id=experimenttype_id "
    "where e.etype like 'DNA%' and libstatus in ({JOB_CREATED}, {PROCESSING}) "
    "and deleted=0 and COALESCE(egroup_id,'') <> '' and COALESCE(name4browser,'') <> '' "
    "order by control DESC,dateadd").format(**LIBSTATUS))
rows = biow_db_settings.cursor.fetchall()

# Check status of running jobs
for row in rows:
    print "CHEK JOB ROW: " + str(row)
    try:

        current_workflow = {"True":  {"False": CHIP_SEQ_PE_WORKFLOW,
                                      "True":  CHIP_SEQ_TRIM_PE_WORKFLOW},
                            "False": {"False": CHIP_SEQ_SE_WORKFLOW,
                                      "True":  CHIP_SEQ_TRIM_SE_WORKFLOW}}[str('pair' in row[0])][str(FOLDER_ID == row[3])]

        libstatus, libstatustxt = check_job (uid=row[1],
                                             db_settings=biow_db_settings,
                                             workflow=current_workflow,
                                             jobs_folder=sys.argv[1]) # sys.argv[1] - path where to save generated job files
        if libstatus:
            update_status(uid=row[1],
                          message=libstatustxt,
                          code=libstatus,
                          db_settings=biow_db_settings)
            update_status(uid=row[1],  # is used only to set dateanalyzes value which is laways NULL after we restarted or created new experiment
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
                upload_results_to_db(upload_set=CHIP_SEQ_UPLOAD,
                                     uid=row[1],
                                     raw_data=os.path.join(biow_db_settings.settings['wardrobe'], biow_db_settings.settings['preliminary']),
                                     db_settings=biow_db_settings)
    except BiowBasicException as ex:
        submit_err (error=ex, db_settings=biow_db_settings)
        continue

