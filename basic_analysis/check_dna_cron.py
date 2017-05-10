#!/usr/bin/env python
import DefFunctions as util
from basic_analysis import Settings
import sys
from basic_analysis.exceptions import BiowBasicException
from basic_analysis.check_dna_func import check_job
from basic_analysis.constants import LIBSTATUS


biow_db_settings = Settings.Settings()


def use_ems():
    biow_db_settings.cursor.execute ('use ems')


def use_airflow():
    biow_db_settings.cursor.execute ('use airflow')


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
        libstatus, libstatustxt = check_job (biow_db_settings,row)
        if libstatus:
            use_ems()
            util.update_status(row[1], libstatustxt, libstatus, biow_db_settings)
    except BiowBasicException as ex:
        use_ems()
        util.submit_err (ex, biow_db_settings)
        continue
