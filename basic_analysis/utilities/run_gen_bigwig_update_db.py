"""Creates job file to be run in Airflow to generate bigWig files for all ChIP-Seq experiments"""
import datetime
import sys
import os
import string
from basic_analysis.Settings import Settings
from basic_analysis.constants import LIBSTATUS
from basic_analysis.biow_exceptions import BiowBasicException
from basic_analysis.DefFunctions import (raise_if_table_exists,
                                         raise_if_file_absent)
from basic_analysis.db_uploader import upload_results_to_db
from basic_analysis.db_upload_list import CHIP_SEQ_GEN_BIGWIG_UPLOAD


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
    print "CHECK JOB ROW: " + str(row)
    sys.stdout.flush()
    try:
        raise_if_table_exists (db_settings=biow_db_settings, uid=row[3], table=string.replace(row[3], "-", "_") + "_f_wtrack", db=row[1])
        raise_if_file_absent (row[3],os.path.join(os.path.join(biow_db_settings.settings['wardrobe'], biow_db_settings.settings['preliminary']), row[3], row[3] + '.bigWig'))
        upload_results_to_db(upload_set=CHIP_SEQ_GEN_BIGWIG_UPLOAD,
                             uid=row[3],
                             raw_data=os.path.join(biow_db_settings.settings['wardrobe'], biow_db_settings.settings['preliminary']),
                             db_settings=biow_db_settings)
    except BiowBasicException as ex:
        print "   SKIP: uploading generated bigWig ", row[3], " - ", str(ex)
