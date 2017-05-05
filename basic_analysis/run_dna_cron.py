#!/usr/bin/env python
import os
import DefFunctions as util
from basic_analysis import Settings
import datetime
import sys
from basic_analysis.exceptions import BiowBasicException
from basic_analysis.run_dna_func import submit_job

biow_db_settings = Settings.Settings()

WORKFLOW = 'run-dna-se.cwl'
TEMPLATE_JOB = ('{{'
                  '"fastq_input_file": {{"class": "File", "location": "{fastq_input_file}", "format": "http://edamontology.org/format_1930"}},'
                  '"bowtie_indices_folder": {{"class": "Directory", "location": "{bowtie_indices_folder}"}},'
                  '"clip_3p_end": {clip_3p_end},'
                  '"clip_5p_end": {clip_5p_end},'
                  '"threads": {threads},'
                  '"remove_duplicates": "{remove_duplicates}",'
                  '"control_file": {{"class": "File", "location": "{control_file}", "format": "http://edamontology.org/format_2572"}},'
                  '"exp_fragment_size": {exp_fragment_size},'
                  '"force_fragment_size": "{force_fragment_size}",'
                  '"broad_peak": "{broad_peak}",'
                  '"chrom_length": {{"class": "File", "location": "{chrom_length}", "format": "http://edamontology.org/format_2330"}},'
                  '"genome_size": "{genome_size}"'
                '}}')

print str(datetime.datetime.now())

biow_db_settings.cursor.execute(
    "update labdata set libstatustxt='ready for process',libstatus=10 where libstatus=2 and experimenttype_id in (select id from experimenttype where etype like 'DNA%') "
    " and COALESCE(egroup_id,'') <> '' and COALESCE(name4browser,'') <> '' and deleted=0 ")
biow_db_settings.cursor.execute(
    "select e.etype,g.db,g.findex,g.annotation,l.uid,fragmentsizeexp,fragmentsizeforceuse,forcerun, "
    "COALESCE(l.trim5,0), COALESCE(l.trim3,0),COALESCE(a.properties,0), COALESCE(l.rmdup,0),g.gsize, COALESCE(control,0), COALESCE(control_id,'') "
    "from labdata l "
    "inner join (experimenttype e,genome g ) ON (e.id=experimenttype_id and g.id=genome_id) "
    "LEFT JOIN (antibody a) ON (l.antibody_id=a.id) "
    "where e.etype like 'DNA%' and libstatus in (10,1010) "
    "and deleted=0 and COALESCE(egroup_id,'') <> '' and COALESCE(name4browser,'') <> '' "
    " order by control DESC,dateadd limit 1")

rows = biow_db_settings.cursor.fetchall()

for row in rows:
    print "ROW: " + str(row)
    sys.stdout.flush()
    
    try:
        submit_job (db_settings=biow_db_settings,
                       row=row,
                       raw_data=os.path.join(biow_db_settings.settings['wardrobe'], biow_db_settings.settings['preliminary']),
                       indices=os.path.join(biow_db_settings.settings['wardrobe'], biow_db_settings.settings['indices']),
                       workflow=WORKFLOW,
                       template_job=TEMPLATE_JOB,
                       threads=biow_db_settings.settings['maxthreads'],
                       jobs_folder=sys.argv[1]) # sys.argv[1] - path where to save generated job files
    except BiowBasicException as ex:
        util.submit_err (ex, biow_db_settings)
        continue







    
