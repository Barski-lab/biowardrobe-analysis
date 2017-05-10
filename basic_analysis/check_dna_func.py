import os
import json
import DefFunctions as util
import collections
from basic_analysis.exceptions import (BiowFileNotFoundException,
                                       BiowJobException,
                                       BiowWorkflowException)
from basic_analysis.constants import LIBSTATUS


def check_job(db_settings, row):
    tasks, total = util.get_tasks(row[1], db_settings)
    tasks = {k: v for k,v in tasks.iteritems() if v}
    if not tasks: return (None, None)
    if tasks.get("failed"):
        raise BiowWorkflowException (row[1], message = tasks)
    elif len(tasks.get("success")) == total:
        return (LIBSTATUS["SUCCESS_PROCESS"], "Complete")
    else:
        return (LIBSTATUS["PROCESSING"], tasks)
