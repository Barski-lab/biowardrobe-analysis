#!/usr/bin/env python
import os
import DefFunctions as util
import Settings
import airflow_db_settings
import datetime
import sys
import json
import time
from basic_analysis.exceptions import (BiowFileNotFoundException,
                                       BiowBasicException,
                                       BiowJobException,
                                       BiowWorkflowException)

biow_db_settings = Settings.Settings()
airflow_db_settings = airflow_db_settings.Settings()

EDB = biow_db_settings.settings['experimentsdb']
MTH = str(biow_db_settings.settings['maxthreads'])
WARDROBEROOT = biow_db_settings.settings['wardrobe']
PRELIMINARYDATA = WARDROBEROOT + '/' + biow_db_settings.settings['preliminary']
TEMP = WARDROBEROOT + '/' + biow_db_settings.settings['temp']
BOWTIE_INDICES = WARDROBEROOT + '/' + biow_db_settings.settings['indices']

pidfile = TEMP+"/runDNA.pid"
d.check_running(pidfile)
print str(datetime.datetime.now())

template_job = '{{\n\
                  "fastq_input_file": {{"class": "File", "location": "{fastq_input_file}", "format": "http://edamontology.org/format_1930"}},\n\
                  "bowtie_indices_folder": {{"class": "Directory", "location": "{bowtie_indices_folder}"}},\n\
                  "clip_3p_end": {clip_3p_end},\n\
                  "clip_5p_end": {clip_5p_end},\n\
                  "threads": {threads},\n\
                  "remove_duplicates": {remove_duplicates},\n\
                  "control_file": {{"class": "File", "location": "{control_file}", "format": "http://edamontology.org/format_2572"}},\n\
                  "exp_fragment_size": {exp_fragment_size},\n\
                  "force_fragment_size": {force_fragment_size},\n\
                  "broad_peak": {broad_peak},\n\
                  "chrom_length": {{"class": "File", "location": "{chrom_length}", "format": "http://edamontology.org/format_2330"}},\n\
                  "genome_size": "{genome_size}"\n\
                }}'


def get_control(uid, control_id, basedir, db_settings):
    if not control_id: return None
    db_settings.cursor.execute("select libstatus from labdata where uid=%s", (control_id,))
    crow = db_settings.cursor.fetchone()
    if int(crow[0]) < 12 or int(crow[0]) > 100:
        raise BiowFileNotFoundException(uid, message="Control dataset has not been analyzed yet")
    return os.path.join(basedir,uid+'.bam')


def make_job_file(pair, row, template_job, db_settings, preliminary_folder, indices, threads):
    uid = row[4]
    findex = row[2]
    basedir = os.path.join(preliminary_folder, uid)
    input_parameters_path = os.path.join(basedir, uid + '.json')

    try:
        control_file = get_control(uid, row[14], basedir, db_settings)
    except BiowFileNotFoundException:
        raise

    if not util.file_exist(basedir, uid, 'fastq'):
        raise BiowFileNotFoundException(uid)

    filled_job = template_job.format(fastq_input_file=os.path.join(basedir, uid + '.fastq'),
                                       bowtie_indices_folder=os.path.join(indices, findex),
                                       clip_3p_end=int(row[8]),
                                       clip_5p_end=int(row[9]),
                                       threads=threads,
                                       remove_duplicates=(int(row[11]) == 1),
                                       exp_fragment_size=int(row[5]),
                                       force_fragment_size=(int(row[6]) == 1),
                                       broad_peak=(int(row[10]) == 2),
                                       chrom_length=os.path.join(basedir, findex, 'chrNameLength.txt'),
                                       genome_size=row[12],
                                       control_file=control_file
                                       )
    util.remove_not_set_inputs(filled_job)
    try:
        with open(input_parameters_path, 'w') as output_file:
            output_file.write(filled_job)
    except Exception as ex:
        raise BiowJobException(uid, message=str(ex))
    return input_parameters_path



def submit_results(uid, biow_db_settings):
    # settings.cursor.execute("update labdata set fragmentsize=%s,fragmentsizeest=%s,islandcount=%s where uid=%s",(FRAGMENT, FRAGMENTE, ISLANDS, UID))
    # settings.conn.commit()
    # util.upload_macsdata(settings.conn, UID, EDB, DB)
    biow_db_settings.cursor.execute("update labdata set libstatustxt='Complete',libstatus=12,dateanalyzee=now() where uid=%s",(uid,))
    biow_db_settings.conn.commit()


biow_db_settings.cursor.execute(
    "update labdata set libstatustxt='ready for process',libstatus=10 where libstatus=2 and experimenttype_id in (select id from experimenttype where etype like 'DNA%') "
    " and COALESCE(egroup_id,'') <> '' and COALESCE(name4browser,'') <> '' and deleted=0 ")

while True:
    biow_db_settings.cursor.execute(
        "select e.etype,g.db,g.findex,g.annotation,l.uid,fragmentsizeexp,fragmentsizeforceuse,forcerun, "
        "COALESCE(l.trim5,0), COALESCE(l.trim3,0),COALESCE(a.properties,0), COALESCE(l.rmdup,0),g.gsize, COALESCE(control,0), COALESCE(control_id,'') "
        "from labdata l "
        "inner join (experimenttype e,genome g ) ON (e.id=experimenttype_id and g.id=genome_id) "
        "LEFT JOIN (antibody a) ON (l.antibody_id=a.id) "
        "where e.etype like 'DNA%' and libstatus in (10,1010) "
        "and deleted=0 and COALESCE(egroup_id,'') <> '' and COALESCE(name4browser,'') <> '' "
        " order by control DESC,dateadd limit 1")
    row = biow_db_settings.cursor.fetchone()
    if not row: break
    print "ROW: " + str(row)
    sys.stdout.flush()

    uid = row[4]
    pair = row[0]
    basedir = os.path.join(PRELIMINARYDATA, uid)

    try:
        input_parameters_path = make_job_file (pair, row, template_job, biow_db_settings, PRELIMINARYDATA, BOWTIE_INDICES, MTH)
    except BiowBasicException as ex:
        util.submit_err (ex, biow_db_settings)
        continue

    try:
        proc = util.start_workflow("cwl_descriptor_path", input_parameters_path)
        while util.still_running(uid, biow_db_settings, airflow_db_settings, proc): time.sleep(60)
    except BiowWorkflowException as ex:
        util.submit_err(ex, biow_db_settings)
        continue

    biow_db_settings.def_close()
    biow_db_settings.def_connect()

    submit_results(uid, biow_db_settings)







    
