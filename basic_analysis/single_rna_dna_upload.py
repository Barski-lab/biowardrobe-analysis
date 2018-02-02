#!/usr/bin/env python

import os
from constants import (EXP_TYPE_UPLOAD)
from db_uploader import upload_results_to_db
from single_settings import get_db_settings


def get_job(id, connection):
    db_settings = get_db_settings(connection)
    db_settings.cursor.execute((
        "select e.etype, e.workflow, e.template, l.uid, l.libstatustxt "
        "from labdata l "
        "inner join experimenttype e ON e.id=experimenttype_id "
        "where l.id={} and deleted=0 and COALESCE(egroup_id,'') <> '' and COALESCE(name4browser,'') <> '' "
        "order by dateadd").format(id))
    row = db_settings.cursor.fetchone()
    upload_results_to_db(upload_set=EXP_TYPE_UPLOAD[row[0]],
                         uid=row[3],
                         raw_data=os.path.join(db_settings.settings['wardrobe'], db_settings.settings['preliminary']),
                         db_settings=db_settings)
