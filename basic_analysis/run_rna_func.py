import os
import json
from DefFunctions import remove_not_set_inputs
import collections
from biow_exceptions import (BiowFileNotFoundException,
                             BiowJobException,
                             BiowWorkflowException)
from constants import (STAR_INDICES,
                       BOWTIE_INDICES,
                       RIBO_SUFFIX,
                       CHR_LENGTH_GENERIC_TSV,
                       JOBS_NEW,
                       JOBS_RUNNING,
                       ANNOTATIONS,
                       ANNOTATION_GENERIC_TSV)


def submit_job(db_settings, row, raw_data, indices, threads, jobs_folder):
    """Generate and export job file to a specific folder"""
    kwargs = {
        "pair": ('pair' in row[0]),
        "dUTP": ('dUTP' in row[0]),
        "workflow": os.path.splitext(os.path.basename(row[1]))[0],
        "template": row[2],
        "uid": row[3],
        "genome_db": row[4],            # not used
        "genome": row[5],
        "annotation": row[6],           # not used
        "annottable": row[7],           # not used
        "spike": ('spike' in row[8]),
        "forcerun": (int(row[9]) == 1),
        "clip_5p_end": int(row[10]),
        "clip_3p_end": int(row[11]),
        "raw_data": raw_data,
        "indices": indices,
        "threads": threads
    }

    jobs_folder = jobs_folder if os.path.isabs(jobs_folder) else os.path.join(os.getcwd(), jobs_folder)

    # Setting values to fill in job template
    #  We always create both upstream and downstream, even if we gonna use only upstream
    kwargs["fastq_file_upstream"] = os.path.join(kwargs["raw_data"], kwargs["uid"], kwargs["uid"] + '.fastq')
    kwargs["fastq_file_downstream"] = os.path.join(kwargs["raw_data"], kwargs["uid"], kwargs["uid"] + '_2.fastq')
    kwargs["star_indices_folder"] = os.path.join(kwargs["indices"], STAR_INDICES, kwargs["genome"])
    # we need to add RIBO_SUFFIX to "genome" to get folder name for ribosomal bowtie indices
    kwargs["bowtie_indices_folder"] = os.path.join(kwargs["indices"], BOWTIE_INDICES, kwargs["genome"]+RIBO_SUFFIX)
    kwargs["chrom_length"] = os.path.join(kwargs["indices"], BOWTIE_INDICES, kwargs["genome"], CHR_LENGTH_GENERIC_TSV)
    kwargs["annotation_input_file"] = os.path.join(kwargs["indices"], ANNOTATIONS, kwargs["genome"],ANNOTATION_GENERIC_TSV)
    kwargs["exclude_chr"] = "control" if kwargs["spike"] else ""
    kwargs["output_folder"] = os.path.join(kwargs["raw_data"], kwargs["uid"])

    job_file_basename = kwargs["workflow"] + '-' + kwargs["uid"] + '.json'
    output_filename = os.path.join(jobs_folder, JOBS_NEW, job_file_basename)
    running_filename = os.path.join(jobs_folder, JOBS_RUNNING, job_file_basename)
    
    if not os.path.isfile(kwargs["fastq_file_upstream"]) or (kwargs['pair'] and not os.path.isfile(kwargs["fastq_file_downstream"])):
        raise BiowFileNotFoundException(kwargs["uid"])

    filled_job_object = remove_not_set_inputs(json.loads(kwargs['template'].format(**kwargs).replace("'True'",'true').replace("'False'",'false').replace('"True"','true').replace('"False"','false')))
    filled_job_str = json.dumps(collections.OrderedDict(sorted(filled_job_object.items())),indent=4)

    # Check if file exists in running or new job folder
    if os.path.isfile(output_filename) or os.path.isfile(running_filename):
        raise BiowJobException(kwargs['uid'], message="Duplicate job file [{}]. It has been already created".format(job_file_basename))

    try:
        with open(output_filename, 'w') as output_file:
            output_file.write(filled_job_str)
    except Exception as ex:
        raise BiowJobException(kwargs['uid'], message="Failed to write job file: "+str(ex))
