import os
import json
from DefFunctions import remove_not_set_inputs
import collections
from biow_exceptions import (BiowFileNotFoundException,
                             BiowJobException,
                             BiowWorkflowException)
from constants import (LIBSTATUS,
                       BOWTIE_INDICES,
                       CHR_LENGTH_GENERIC_TSV,
                       JOBS_NEW,
                       JOBS_RUNNING,
                       ANNOTATIONS,
                       ANNOTATION_GENERIC_TSV)


def get_control(db_settings, **kwargs):
    """Return path to control file
    """
    db_settings.use_ems()
    if not kwargs['control_id']:
        return None
    db_settings.cursor.execute("select libstatus from labdata where uid=%s", (kwargs['control_id'],))
    row = db_settings.cursor.fetchone()
    if not row or int(row[0]) != LIBSTATUS['SUCCESS_PROCESS']:
        raise BiowFileNotFoundException(kwargs['uid'], code=LIBSTATUS["SUCCESS_DOWNLOAD"], message="Control dataset has not been analyzed yet")
    control_filename = os.path.join(kwargs['raw_data'],kwargs['control_id'],kwargs['control_id']+'.bam')
    if not os.path.isfile(control_filename):
        raise BiowFileNotFoundException(kwargs['uid'], message="Control file {} is not found".format(kwargs['control_id']))
    return control_filename


def submit_job(db_settings, row, raw_data, indices, workflow, template_job, threads, jobs_folder):
    """Generate and export job file to a specific folder"""
    jobs_folder = jobs_folder if os.path.isabs(jobs_folder) else os.path.join(os.getcwd(),jobs_folder)
    workflow = os.path.splitext(os.path.basename(workflow))[0]
    kwargs = {
        "pair": ('pair' in row[0]),
        "genome_db": row[1],
        "genome": row[2],
        "uid": row[4],
        "exp_fragment_size": int(row[5]),
        "force_fragment_size": (int(row[6]) == 1),
        "forcerun": (int(row[7]) == 1),
        "clip_5p_end": int(row[8]),
        "clip_3p_end": int(row[9]),
        "broad_peak": (int(row[10]) == 2),
        "remove_duplicates": (int(row[11]) == 1),
        "genome_size": row[12],
        "control_id": row[14],
        "raw_data": raw_data,
        "indices": indices,
        "workflow": workflow,
        "template_job": template_job,
        "threads": threads
    }
    #  We always create both upstream and downstream, even if we gonna use only upstream
    kwargs["fastq_file_upstream"] = os.path.join(kwargs["raw_data"], kwargs["uid"], kwargs["uid"] + '.fastq')
    kwargs["fastq_file_downstream"] = os.path.join(kwargs["raw_data"], kwargs["uid"], kwargs["uid"] + '_2.fastq')
    kwargs["bowtie_indices_folder"] = os.path.join(kwargs["indices"], BOWTIE_INDICES, kwargs["genome"])
    kwargs["chrom_length"] = os.path.join(kwargs["indices"], BOWTIE_INDICES, kwargs["genome"], CHR_LENGTH_GENERIC_TSV)
    kwargs["annotation_input_file"] = os.path.join(kwargs["indices"], ANNOTATIONS, kwargs["genome"],
                                                   ANNOTATION_GENERIC_TSV)
    kwargs["output_folder"] = os.path.join(kwargs["raw_data"], kwargs["uid"])

    output_filename = os.path.join(jobs_folder, JOBS_NEW, kwargs["workflow"] + '-' + kwargs["uid"] + '.json')
    running_filename = os.path.join(jobs_folder, JOBS_RUNNING, kwargs["workflow"] + '-' + kwargs["uid"] + '.json')
    
    try:
        kwargs["control_file"] = get_control(db_settings, **kwargs)
    except BiowFileNotFoundException:
        raise

    if not os.path.isfile(kwargs["fastq_file_upstream"]) or (kwargs['pair'] and not os.path.isfile(kwargs["fastq_file_downstream"])):
        raise BiowFileNotFoundException(kwargs["uid"])

    filled_job_object = remove_not_set_inputs(json.loads(template_job.format(**kwargs).replace("'True'",'true').replace("'False'",'false').replace('"True"','true').replace('"False"','false')))
    filled_job_str = json.dumps(collections.OrderedDict(sorted(filled_job_object.items())),indent=4)

    # Check if file exists in job folder (running or new)
    if os.path.isfile(output_filename) or os.path.isfile(running_filename):
        raise BiowJobException(kwargs['uid'], message="Duplicate job file. Use ForceRun")

    try:
        with open(output_filename, 'w') as output_file:
            output_file.write(filled_job_str)
    except Exception as ex:
        raise BiowJobException(kwargs['uid'], message=str(ex))
