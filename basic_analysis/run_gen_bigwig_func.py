import os
import json
import collections
from constants import (CHR_LENGTH_GENERIC_TSV,
                       JOBS_NEW,
                       JOBS_RUNNING,
                       BOWTIE_INDICES)
from biow_exceptions import BiowJobException
from DefFunctions import raise_if_file_absent,raise_if_file_exists


def submit_job(db_settings, row, workflow, template_job, jobs_folder):
    """Generate and export job file to a specific folder"""
    # select    e.etype,   g.db,   g.findex,   l.uid,   COALESCE(l.fragmentsize,0),   COALESCE(l.tagsmapped,0) from labdata l "
    jobs_folder = jobs_folder if os.path.isabs(jobs_folder) else os.path.join(os.getcwd(), jobs_folder)
    raw_data = os.path.join(db_settings.settings['wardrobe'], db_settings.settings['preliminary'])
    indices = os.path.join(db_settings.settings['wardrobe'], db_settings.settings['indices'])
    workflow_basename = os.path.splitext(os.path.basename(workflow))[0]

    kwargs = {
        "bam_file": os.path.join(raw_data, row[3], row[3] + '.bam'),
        "bai_file": os.path.join(raw_data, row[3], row[3] + '.bam.bai'),
        "chrom_length_file": os.path.join(indices, BOWTIE_INDICES, row[2], CHR_LENGTH_GENERIC_TSV),
        "mapped_reads": int(row[5]),
        "fragment_size": int(row[4]),
        "paired": ('pair' in row[0]),
        "output_folder": os.path.join(raw_data, row[3]),
        "uid": row[3]
    }

    output_filename = os.path.join (jobs_folder, JOBS_NEW,     workflow_basename + '-' + kwargs["uid"] + '.json')
    running_filename = os.path.join(jobs_folder, JOBS_RUNNING, workflow_basename + '-' + kwargs["uid"] + '.json')

    raise_if_file_absent(kwargs['uid'], kwargs["bam_file"])
    raise_if_file_absent(kwargs['uid'], kwargs["bai_file"])
    raise_if_file_absent(kwargs['uid'], kwargs["chrom_length_file"])
    raise_if_file_exists(kwargs['uid'], os.path.join(raw_data, kwargs["uid"], kwargs["uid"] + '.bigwig'))
    # Check if file exists in job folder (running or new)
    raise_if_file_exists(kwargs['uid'], output_filename)
    raise_if_file_exists(kwargs['uid'], running_filename)

    if not kwargs["mapped_reads"] or not kwargs["fragment_size"]:
        raise BiowJobException(kwargs["uid"], message="mapped_reads or fragment_size are not set")

    filled_job_object = json.loads(template_job.format(**kwargs).replace("'True'", 'true').replace("'False'", 'false').replace('"True"','true').replace('"False"', 'false'))
    filled_job_str = json.dumps(collections.OrderedDict(sorted(filled_job_object.items())), indent=4)

    try:
        with open(output_filename, 'w') as output_file:
            output_file.write(filled_job_str)
    except Exception as ex:
        raise BiowJobException(kwargs['uid'], message="Failed to write job file: " + str(ex))

