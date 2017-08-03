#!/usr/bin/env python

# #/****************************************************************************
# #**
# #** Copyright (C) 2011 Andrey Kartashov .
##** All rights reserved.
##** Contact: Andrey Kartashov (porter@porter.st)
##**
##** This file is part of the global module of the genome-tools.
##**
##** GNU Lesser General Public License Usage
##** This file may be used under the terms of the GNU Lesser General Public
##** License version 2.1 as published by the Free Software Foundation and
##** appearing in the file LICENSE.LGPL included in the packaging of this
##** file. Please review the following information to ensure the GNU Lesser
##** General Public License version 2.1 requirements will be met:
##** http://www.gnu.org/licenses/old-licenses/lgpl-2.1.html.
##**
##** Other Usage
##** Alternatively, this file may be used in accordance with the terms and
##** conditions contained in a signed written agreement between you and Andrey Kartashov.
##**
##****************************************************************************/

import os
import sys

import smtplib
import glob
import subprocess as s
import time
import re
import random
import MySQLdb
import warnings
import string
import subprocess
import regex
from biow_exceptions import (BiowJobException,
                             BiowWorkflowException,
                             BiowFileNotFoundException,
                             BiowBasicException)
from constants import LIBSTATUS, JOBS_FAIL

def send_mail(toaddrs, body):
    fromaddr = 'biowrdrobe@biowardrobe.com'
    # Add the From: and To: headers at the start!
    msg = ("From: %s\r\nTo: %s\r\nSubject: %s\r\n\r\n"
           % (fromaddr, toaddrs, body))
    msg = msg + body

    server = smtplib.SMTP('localhost')
    server.set_debuglevel(1)
    server.sendmail(fromaddr, toaddrs, msg)
    server.quit()


def check_pid(pid):
    """ Check For the existence of a unix pid. """
    try:
        os.kill(pid, 0)
    except OSError:
        return False
    else:
        return True


def check_running(fname):
    if os.path.isfile(fname):
        old_pid = file(fname, 'r').readline()
        if old_pid and check_pid(int(old_pid)):
            sys.exit()
    file(fname, 'w').write(str(os.getpid()))


def file_exist(basedir, fname, extension):
    return os.path.isfile(os.path.join(basedir,fname + '.' + extension))

def del_in_dir(path, exc=""):
    #TODO: ? do we need / protection?
    try:
        os.chdir(path)
        for root, dirs, files in os.walk("./", topdown=False):
            for name in files:
                if exc != "" and exc in name:
                    continue
                os.remove(os.path.join(root, name))
    except:
        pass


def do_trimm(uid, pair, _left, _right):
    left = str(_left)
    right = str(_right)
    if pair:
        cmd = 'cat ' + uid + '.fastq | trimmer.py -l' + left + ' -r' + right + '>' + uid + '_trimmed.fastq;'
        cmd += 'cat ' + uid + '_2.fastq | trimmer.py -l' + left + ' -r' + right + '>' + uid + '_trimmed_2.fastq'
    else:
        cmd = 'cat ' + uid + '.fastq | trimmer.py -l' + left + ' -r' + right + '>' + uid + '_trimmed.fastq;'
    try:
        s.check_output(cmd, shell=True)
        return ['Success', ' Trim is done ']
    except Exception, e:
        return ['Error', str(e)]


def getFolderSize(folder):
    total_size = os.path.getsize(folder)
    for item in os.listdir(folder):
        itempath = os.path.join(folder, item)
        if os.path.isfile(itempath):
            total_size += os.path.getsize(itempath)
        elif os.path.isdir(itempath):
            total_size += getFolderSize(itempath)
    return total_size


#================#
#      CWL       #
#================#

def check_job(db_settings, uid, workflow, jobs_folder):
    """Check status for current job from Airflow DB"""
    tasks, total = get_tasks(uid, db_settings)
    tasks = {k: v for k,v in tasks.iteritems() if v}
    if not tasks:
        failed_file = os.path.join(jobs_folder, JOBS_FAIL, os.path.splitext(os.path.basename(workflow))[0] + '-' + uid + '.json')
        if os.path.isfile(failed_file): # If job file was moved to failed folder before even started
            raise BiowJobException(uid, message="Job file is already marked as failed one")
        return None, None
    if tasks.get("failed"):
        raise BiowWorkflowException (uid, message = tasks)
    elif total > 0 and len(tasks.get("success", [])) == total: # All the tasks exit with success
        return LIBSTATUS["SUCCESS_PROCESS"], "Complete"
    else:
        percent_complete = 0
        try:
            percent_complete = int(float(len(tasks.get("success",[])))/total*100)
        except ZeroDivisionError:
            pass
        return LIBSTATUS["PROCESSING"], "Processing: " + str(percent_complete)+"%"


def raise_if_table_exists (db_settings, uid, table, db):
    try:
        raise_if_table_absent(db_settings, uid, table, db)
    except BiowBasicException:
        pass
    else:
        raise BiowJobException (uid, message="Table {0}.{1} already exists".format(db,table))


def raise_if_table_absent (db_settings, uid, table, db):
    db_settings.cursor.execute("SHOW TABLES FROM {0} LIKE '{1}'".format(db,table))
    if not db_settings.cursor.fetchone():
        raise BiowFileNotFoundException (uid, message="Table {0}.{1} doesn't exist".format(db,table))


def raise_if_file_exists (uid, filename):
    try:
        raise_if_file_absent(uid, filename)
    except BiowBasicException:
        pass
    else:
        raise BiowJobException(uid, message="File already exists {0}".format(filename))


def raise_if_file_absent (uid, filename):
    if not os.path.isfile(filename):
        raise BiowFileNotFoundException(uid, message="Cannot find file {0}".format(filename))


def update_status (uid, message, code, db_settings, optional_column="", optional_where=""):
    """Update libstatus for current uid"""
    db_settings.use_ems()
    if optional_column and not optional_column.startswith(','):
        optional_column = ',' + optional_column
    db_settings.cursor.execute("update labdata set libstatustxt='{0}', libstatus={1} {2} where uid='{3}' {4}".format(str(message).replace("'", '"'), code, optional_column, uid, optional_where))
    db_settings.conn.commit()


def submit_err(error, db_settings):
    update_status (error.uid, error.message, error.code, db_settings)


def get_last_dag_id (uid, db_settings):
    """Get the latest dag run for current uid"""
    db_settings.use_airflow()
    db_settings.cursor.execute("select dag_id from dag_run where dag_id like '%{0}%'".format(uid))
    dags = db_settings.cursor.fetchall()
    return sorted([dag[0] for dag in dags])[-1] if dags else None


def get_tasks (uid, db_settings):
    """Get all tasks splitted into status groups for running dag by uid"""
    db_settings.use_airflow()
    db_settings.cursor.execute("select task_id, state from task_instance where dag_id='{0}'".format(get_last_dag_id(uid, db_settings)))
    collected = {}
    tasks = db_settings.cursor.fetchall()
    for state in ['queued','running','success','shutdown','failed','up_for_retry','upstream_failed','skipped']:
        collected[state] = [task[0] for task in tasks if task[1]==state]
    return collected, len(tasks)


def raise_if_dag_absent (uid, db_settings):
    if not get_last_dag_id(uid, db_settings):
        raise BiowJobException (uid, message='DAG is not found')


def raise_if_dag_exists (uid, db_settings):
    """Raise if DAG run with the dag_id already exists"""
    try:
        raise_if_dag_absent(uid, db_settings)
    except BiowBasicException:
        pass
    else:
        raise BiowJobException(uid, message='Duplicate dag_id. Use ForceRun')


def recursive_check(item,monitor):
    if item=='null' or item=='None':
        monitor["found_none"] = True
    elif isinstance(item, dict):
        dict((k, v) for k, v in item.iteritems() if recursive_check(v,monitor))
    elif isinstance(item, list):
        list(v for v in item.iteritems() if recursive_check(v,monitor))


def complete_input(item):
    monitor = {"found_none": False}
    recursive_check(item,monitor)
    return not monitor["found_none"]


def remove_not_set_inputs(job_object):
    """Remove all input parameters from job which are not set"""
    job_object_filtered ={}
    for key,value in job_object.iteritems():
        if complete_input(value):
            job_object_filtered[key]=value
    return job_object_filtered
