"""Creates job file to be run in Airflow to generate bigWig files for all ChIP-Seq experiments"""

import os
import datetime
import sys
import json
import collections
from basic_analysis.Settings import Settings
from basic_analysis.constants import (LIBSTATUS,
                                      CHR_LENGTH_GENERIC_TSV,
                                      JOBS_NEW,
                                      JOBS_RUNNING,
                                      BOWTIE_INDICES)
from basic_analysis.biow_exceptions import (BiowFileNotFoundException,
                                            BiowJobException,
                                            BiowBasicException)

# CHiP-Seq SE/PE generate bigWig
CHIP_SEQ_GEN_BIGWIG_WORKFLOW = 'chipseq-gen-bigwig.cwl'
CHIP_SEQ_GEN_BIGWIG_TEMPLATE_JOB = ('{{'
                  '"bam_bai_pair_file": {{"class": "File", "location": "{bam_file}", "secondaryFiles": [{{ "class": "File", "location": "{bai_file}"  }}]}},'
                  '"chrom_length_file": {{"class": "File", "location": "{chrom_length_file}"}},'
                  '"mapped_reads": {mapped_reads},'
                  '"fragment_size": {fragment_size},'
                  '"paired": "{paired}",'
                  '"output_folder": "{output_folder}",' # required
                  '"uid": "{uid}"'                      # required
                '}}')



def submit_job(db_settings, row, workflow, template_job, jobs_folder):
    """Generate and export job file to a specific folder"""
    # "select   e.etype,   g.findex,   l.uid,   l.fragmentsize,   l.tagsmapped   from labdata l "
    jobs_folder = jobs_folder if os.path.isabs(jobs_folder) else os.path.join(os.getcwd(), jobs_folder)
    raw_data = os.path.join(db_settings.settings['wardrobe'], db_settings.settings['preliminary'])
    indices = os.path.join(db_settings.settings['wardrobe'], db_settings.settings['indices'])
    workflow_basename = os.path.splitext(os.path.basename(workflow))[0]

    kwargs = {
        "bam_file": os.path.join(raw_data, row[2], row[2] + '.bam'),
        "bai_file": os.path.join(raw_data, row[2], row[2] + '.bam.bai'),
        "chrom_length_file": os.path.join(indices, BOWTIE_INDICES, row[1], CHR_LENGTH_GENERIC_TSV),
        "mapped_reads": int(row[4]),
        "fragment_size": int(row[3]),
        "paired": ('pair' in row[0]),
        "output_folder": os.path.join(raw_data, row[2]),
        "uid": row[2]
    }

    output_filename = os.path.join( jobs_folder, JOBS_NEW,     workflow_basename + '-' + kwargs["uid"] + '.json')
    running_filename = os.path.join(jobs_folder, JOBS_RUNNING, workflow_basename + '-' + kwargs["uid"] + '.json')
    bigwig_filename = os.path.join(raw_data, kwargs["uid"], kwargs["uid"] + '.bigwig')

    if not os.path.isfile(kwargs["bam_file"]) or not os.path.isfile(kwargs["bai_file"]):
        raise BiowFileNotFoundException(kwargs["uid"], message="Missing BAM or BAI file")

    if not os.path.isfile(kwargs["chrom_length_file"]):
        raise BiowFileNotFoundException(kwargs["uid"], message="chrom_length_file is not found")

    if not kwargs["mapped_reads"] or not kwargs["fragment_size"]:
        raise BiowJobException(kwargs["uid"], message="mapped_reads or fragment_size are not set")

    if os.path.isfile(bigwig_filename):
        raise BiowJobException(kwargs["uid"], message="bigWig is already generated")

    filled_job_object = json.loads(template_job.format(**kwargs).replace("'True'", 'true').replace("'False'", 'false').replace('"True"','true').replace('"False"', 'false'))
    filled_job_str = json.dumps(collections.OrderedDict(sorted(filled_job_object.items())), indent=4)

    # Check if file exists in job folder (running or new)
    if os.path.isfile(output_filename) or os.path.isfile(running_filename):
        raise BiowJobException(kwargs['uid'], message="Duplicate job file")

    try:
        with open(output_filename, 'w') as output_file:
            output_file.write(filled_job_str)
    except Exception as ex:
        raise BiowJobException(kwargs['uid'], message="Failed to write job file: " + str(ex))


# Get access to DB
biow_db_settings = Settings()
print str(datetime.datetime.now())

# Get all ChIP-Seq SE/PE experiments
biow_db_settings.use_ems()
biow_db_settings.cursor.execute((
    "select    e.etype,   g.findex,   l.uid,   COALESCE(l.fragmentsize,0),   COALESCE(l.tagsmapped,0) from labdata l "
    "inner join (experimenttype e,genome g) ON (e.id=experimenttype_id and g.id=genome_id) "
    "where e.etype like 'DNA%' and libstatus={SUCCESS_PROCESS} "
    "and deleted=0 and COALESCE(egroup_id,'') <> '' and COALESCE(name4browser,'') <> '' ").format(**LIBSTATUS))
rows = biow_db_settings.cursor.fetchall()


# Generate job files for all found experiments where bigWig is not present
for row in rows:
    print "SUBMIT JOB ROW: " + str(row)
    sys.stdout.flush()
    try:
        submit_job (db_settings=biow_db_settings,
                   row=row,
                   workflow=CHIP_SEQ_GEN_BIGWIG_WORKFLOW,
                   template_job=CHIP_SEQ_GEN_BIGWIG_TEMPLATE_JOB,
                   jobs_folder=sys.argv[1]) # sys.argv[1] - path where to save generated job files
    except BiowBasicException as ex:
        print "SKIP: generating job file: ", str(ex)
        continue
