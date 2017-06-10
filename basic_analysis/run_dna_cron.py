#!/usr/bin/env python
import os
from DefFunctions import check_if_duplicate_dag, update_status, submit_err
from Settings import Settings
import datetime
import sys
from biow_exceptions import BiowBasicException
from run_dna_func import check_job, submit_job
from constants import LIBSTATUS, CHIP_SEQ_SE_WORKFLOW, CHIP_SEQ_SE_TEMPLATE_JOB
from db_uploader import upload_results_to_db, CHIP_SEQ_SE_UPLOAD


biow_db_settings = Settings()


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
        check_if_duplicate_dag(row[4], biow_db_settings)
        use_ems()
        submit_job (db_settings=biow_db_settings,
                   row=row,
                   raw_data=os.path.join(biow_db_settings.settings['wardrobe'], biow_db_settings.settings['preliminary']),
                   indices=os.path.join(biow_db_settings.settings['wardrobe'], biow_db_settings.settings['indices']),
                   workflow=CHIP_SEQ_SE_WORKFLOW,
                   template_job=CHIP_SEQ_SE_TEMPLATE_JOB,
                   threads=biow_db_settings.settings['maxthreads'],
                   jobs_folder=sys.argv[1]) # sys.argv[1] - path where to save generated job files
        update_status(row[4], 'Processing', 11, biow_db_settings, "forcerun=0, dateanalyzes=now()")
    except BiowBasicException as ex:
        use_ems()
        submit_err (ex, biow_db_settings)
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
                                             workflow=CHIP_SEQ_SE_WORKFLOW,
                                             jobs_folder=sys.argv[1])
        if libstatus:
            use_ems()
            update_status(row[1], libstatustxt, libstatus, biow_db_settings)
            if libstatus==LIBSTATUS["SUCCESS_PROCESS"]:
                update_status(row[1], libstatustxt, libstatus, biow_db_settings, "dateanalyzee=now()")
                upload_results_to_db(upload_set=CHIP_SEQ_SE_UPLOAD,
                                     uid=row[1],
                                     raw_data=os.path.join(biow_db_settings.settings['wardrobe'], biow_db_settings.settings['preliminary']),
                                     db_settings=biow_db_settings)
    except BiowBasicException as ex:
        use_ems()
        submit_err (ex, biow_db_settings)
        continue
