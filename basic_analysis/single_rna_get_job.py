#!/usr/bin/env python

import os
import json
import collections
from DefFunctions import remove_not_set_inputs
from single_settings import get_db_settings
from constants import (BOWTIE_INDICES,
                       RIBO_SUFFIX,
                       STAR_INDICES,
                       ANNOTATIONS,
                       CHR_LENGTH_GENERIC_TSV,
                       ANNOTATION_GENERIC_TSV)



def make_job(row, raw_data, indices, threads):
    kwargs = {
        "pair": ('pair' in row[0]),
        "dUTP": ('dUTP' in row[0]),
        "workflow": os.path.splitext(os.path.basename(row[1]))[0],
        "template": row[2],
        "uid": row[3],
        "genome_db": row[4],   # not used
        "genome": row[5],
        "annotation": row[6],  # not used
        "annottable": row[7],  # not used
        "spike": ('spike' in row[8]),
        "forcerun": (int(row[9]) == 1),
        "clip_5p_end": int(row[10]),
        "clip_3p_end": int(row[11]),
        "raw_data": raw_data,
        "indices": indices,
        "threads": threads
    }
    kwargs["fastq_file_upstream"] = os.path.join(kwargs["raw_data"], kwargs["uid"], kwargs["uid"] + '.fastq')
    kwargs["fastq_file_downstream"] = os.path.join(kwargs["raw_data"], kwargs["uid"], kwargs["uid"] + '_2.fastq')
    kwargs["star_indices_folder"] = os.path.join(kwargs["indices"], STAR_INDICES, kwargs["genome"])
    kwargs["bowtie_indices_folder"] = os.path.join(kwargs["indices"], BOWTIE_INDICES, kwargs["genome"] + RIBO_SUFFIX)
    kwargs["chrom_length"] = os.path.join(kwargs["indices"], BOWTIE_INDICES, kwargs["genome"], CHR_LENGTH_GENERIC_TSV)
    kwargs["annotation_input_file"] = os.path.join(kwargs["indices"], ANNOTATIONS, kwargs["genome"], ANNOTATION_GENERIC_TSV)
    kwargs["exclude_chr"] = "control" if kwargs["spike"] else ""
    kwargs["output_folder"] = os.path.join(kwargs["raw_data"], kwargs["uid"])

    filled_job_object = remove_not_set_inputs(json.loads(kwargs['template'].replace('\n', ' ').format(**kwargs).replace("'True'", 'true').replace("'False'",'false').replace('"True"', 'true').replace('"False"', 'false')))
    return json.dumps(collections.OrderedDict(sorted(filled_job_object.items())), indent=4)


def get_job(id, connection):
    db_settings = get_db_settings(connection)
    db_settings.cursor.execute((
        "select e.etype, e.workflow, e.template, l.uid, g.db, g.findex, g.annotation, g.annottable, g.genome, l.forcerun, "
        "COALESCE(l.trim5,0), COALESCE(l.trim3,0) "
        "from labdata l, experimenttype e, genome g "
        "where l.id= {} and e.id=experimenttype_id and g.id=genome_id "
        "and COALESCE(egroup_id,'') <> '' and COALESCE(name4browser,'') <> '' and deleted=0 "
        "order by dateadd").format(id))
    return make_job(row=db_settings.cursor.fetchone(),
                   raw_data=os.path.join(db_settings.settings['wardrobe'], db_settings.settings['preliminary']),
                   indices=os.path.join(db_settings.settings['wardrobe'], db_settings.settings['indices']),
                   threads=db_settings.settings['maxthreads'])
