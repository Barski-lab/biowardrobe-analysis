#!/usr/bin/env python

import os
import json
import collections
from DefFunctions import remove_not_set_inputs
from single_settings import DBSettings
from constants import (BOWTIE_INDICES,
                       ANNOTATIONS,
                       CHR_LENGTH_GENERIC_TSV,
                       ANNOTATION_GENERIC_TSV)


def make_job(row, raw_data, indices, threads):
    kwargs = {
        "pair": ('pair' in row[0]),
        "workflow": os.path.splitext(os.path.basename(row[1]))[0],
        "template": row[2],
        "genome_db": row[3],
        "genome": row[4],
        "uid": row[6],
        "exp_fragment_size": int(row[7]),
        "force_fragment_size": (int(row[8]) == 1),
        "forcerun": (int(row[9]) == 1),
        "clip_5p_end": int(row[10]),
        "clip_3p_end": int(row[11]),
        "broad_peak": (int(row[12]) == 2),
        "remove_duplicates": (int(row[13]) == 1),
        "genome_size": row[14],
        "control_id": row[15],
        "raw_data": raw_data,
        "indices": indices,
        "threads": threads
    }
    kwargs["fastq_file_upstream"] = os.path.join(kwargs["raw_data"], kwargs["uid"], kwargs["uid"] + '.fastq')
    kwargs["fastq_file_downstream"] = os.path.join(kwargs["raw_data"], kwargs["uid"], kwargs["uid"] + '_2.fastq')
    kwargs["bowtie_indices_folder"] = os.path.join(kwargs["indices"], BOWTIE_INDICES, kwargs["genome"])
    kwargs["chrom_length"] = os.path.join(kwargs["indices"], BOWTIE_INDICES, kwargs["genome"], CHR_LENGTH_GENERIC_TSV)
    kwargs["annotation_input_file"] = os.path.join(kwargs["indices"], ANNOTATIONS, kwargs["genome"],ANNOTATION_GENERIC_TSV)
    kwargs["output_folder"] = os.path.join(kwargs["raw_data"], kwargs["uid"])
    kwargs["control_file"] = os.path.join(kwargs['raw_data'],kwargs['control_id'],kwargs['control_id']+'.bam')
    filled_job_object = remove_not_set_inputs(json.loads(kwargs['template'].replace('\n', ' ').format(**kwargs).replace("'True'", 'true').replace("'False'",'false').replace('"True"', 'true').replace('"False"', 'false')))
    return json.dumps(collections.OrderedDict(sorted(filled_job_object.items())), indent=4)


def get_job(id, connection):
    db_settings = DBSettings(connection)
    db_settings.cursor.execute((
        "select e.etype, e.workflow, e.template, g.db, g.findex, g.annotation, l.uid, fragmentsizeexp, fragmentsizeforceuse, forcerun, "
        "COALESCE(l.trim5,0), COALESCE(l.trim3,0),COALESCE(a.properties,0), COALESCE(l.rmdup,0),g.gsize, "
        "COALESCE(control,0), COALESCE(control_id,'') "
        "from labdata l "
        "inner join (experimenttype e,genome g ) ON (e.id=experimenttype_id and g.id=genome_id) "
        "LEFT JOIN (antibody a) ON (l.antibody_id=a.id) "
        "where l.id={} and e.etype like 'DNA%' and deleted=0 and COALESCE(egroup_id,'') <> '' and COALESCE(name4browser,'') <> '' "
        "order by control DESC,dateadd").format(id))
    return make_job(row=db_settings.cursor.fetchone(),
                   raw_data=os.path.join(db_settings.settings['wardrobe'], db_settings.settings['preliminary']),
                   indices=os.path.join(db_settings.settings['wardrobe'], db_settings.settings['indices']),
                   threads=db_settings.settings['maxthreads'])
