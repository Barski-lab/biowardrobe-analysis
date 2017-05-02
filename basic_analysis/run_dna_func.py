import os
import DefFunctions as util
from basic_analysis.exceptions import BiowFileNotFoundException, BiowJobException


def get_control(db_settings, **kwargs):
    if not kwargs['control_id']: return None
    db_settings.cursor.execute("select libstatus from labdata where uid=%s", (kwargs['control_id'],))
    crow = db_settings.cursor.fetchone()
    if int(crow[0]) < 12 or int(crow[0]) > 100:
        raise BiowFileNotFoundException(kwargs['uid'], message="Control dataset has not been analyzed yet")
    return os.path.join(kwargs['raw_data'],kwargs['uid'],kwargs['uid']+'.bam')


def submit_job(db_settings, row, raw_data, indices, workflow, template_job, threads, jobs_folder):
    """Generate and writes job file to a specific folder"""
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

    kwargs["fastq_input_file"] = os.path.join(kwargs["raw_data"], kwargs["uid"], kwargs["uid"] + '.fastq')
    kwargs["bowtie_indices_folder"] = os.path.join(kwargs["indices"], kwargs["genome"])
    kwargs["chrom_length"] = os.path.join(kwargs["raw_data"], kwargs["uid"], kwargs["genome"], 'chrNameLength.txt')

    output_filename = os.path.join(jobs_folder, "new", kwargs["workflow"] + '-' + kwargs["uid"] + '.json')

    try:
        kwargs["control_file"] = get_control(db_settings, **kwargs)
    except BiowFileNotFoundException:
        raise

    if not util.file_exist(os.path.join(kwargs['raw_data'],kwargs['uid']), kwargs["uid"], 'fastq'):
        raise BiowFileNotFoundException(kwargs["uid"])

    filled_job = template_job.format(**kwargs)
    # util.remove_not_set_inputs(filled_job)
    try:
        with open(output_filename, 'w') as output_file:
            output_file.write(filled_job)
    except Exception as ex:
        raise BiowJobException(kwargs['uid'], message=str(ex))