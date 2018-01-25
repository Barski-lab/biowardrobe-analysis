from db_uploader import (upload_macs2_fragment_stat,
                         upload_iaintersect_result,
                         upload_get_stat,
                         upload_atdp,
                         upload_bigwig,
                         upload_bigwig_upstream,
                         upload_bigwig_downstream,
                         upload_rpkm,
                         upload_dateanalyzed,
                         upload_folder_size,
                         delete_files)


LIBSTATUS = {
    "START_DOWNLOAD":    0,    # Start downloading
    "FAIL_DOWNLOAD":     2000, # Downloading error
    "SUCCESS_DOWNLOAD":  2,    # Downloading succeed
    "JOB_CREATED":       1010, # Job file is created
    "RESTART_DOWNLOAD":  1000, # Restart downloading
    "DOWNLOADING":       1,    # Downloading
    "START_PROCESS":     10,   # Start processing
    "FAIL_PROCESS":      2010, # Processing failed
    "SUCCESS_PROCESS":   12,   # Processing succeed
    "PROCESSING":        11    # Processing
}


BOWTIE_INDICES = "bowtie"
RIBO_SUFFIX = "_ribo"
STAR_INDICES = "STAR"
ANNOTATIONS = "annotations"
JOBS_NEW = 'new'
JOBS_SUCCESS = 'success'
JOBS_FAIL = 'fail'
JOBS_RUNNING = 'running'
CHR_LENGTH_GENERIC_TSV = "chrNameLength.txt"
ANNOTATION_GENERIC_TSV = "refgene.tsv"


# CHiP-Seq single end
CHIP_SEQ_SE_WORKFLOW = 'chipseq-se.cwl'
CHIP_SEQ_SE_TEMPLATE_JOB = ('{{'
                  '"fastq_file": {{"class": "File", "location": "{fastq_file_upstream}", "format": "http://edamontology.org/format_1930"}},'
                  '"indices_folder": {{"class": "Directory", "location": "{bowtie_indices_folder}"}},'
                  '"annotation_file": {{"class": "File", "location": "{annotation_input_file}", "format": "http://edamontology.org/format_3475"}},'
                  '"clip_3p_end": {clip_3p_end},'
                  '"clip_5p_end": {clip_5p_end},'
                  '"threads": {threads},'
                  '"remove_duplicates": "{remove_duplicates}",'
                  '"control_file": {{"class": "File", "location": "{control_file}", "format": "http://edamontology.org/format_2572"}},'
                  '"exp_fragment_size": {exp_fragment_size},'
                  '"force_fragment_size": "{force_fragment_size}",'
                  '"broad_peak": "{broad_peak}",'
                  '"chrom_length": {{"class": "File", "location": "{chrom_length}", "format": "http://edamontology.org/format_2330"}},'
                  '"genome_size": "{genome_size}",'
                  '"output_folder": "{output_folder}",' # required
                  '"uid": "{uid}"'                      # required
                '}}')
# CHiP-Seq pair end
CHIP_SEQ_PE_WORKFLOW = 'chipseq-pe.cwl'
CHIP_SEQ_PE_TEMPLATE_JOB = ('{{'
                  '"fastq_file_upstream": {{"class": "File", "location": "{fastq_file_upstream}", "format": "http://edamontology.org/format_1930"}},'
                  '"fastq_file_downstream": {{"class": "File", "location": "{fastq_file_downstream}", "format": "http://edamontology.org/format_1930"}},'
                  '"indices_folder": {{"class": "Directory", "location": "{bowtie_indices_folder}"}},'
                  '"annotation_file": {{"class": "File", "location": "{annotation_input_file}", "format": "http://edamontology.org/format_3475"}},'
                  '"clip_3p_end": {clip_3p_end},'
                  '"clip_5p_end": {clip_5p_end},'
                  '"threads": {threads},'
                  '"remove_duplicates": "{remove_duplicates}",'
                  '"control_file": {{"class": "File", "location": "{control_file}", "format": "http://edamontology.org/format_2572"}},'
                  '"exp_fragment_size": {exp_fragment_size},'
                  '"force_fragment_size": "{force_fragment_size}",'
                  '"broad_peak": "{broad_peak}",'
                  '"chrom_length": {{"class": "File", "location": "{chrom_length}", "format": "http://edamontology.org/format_2330"}},'
                  '"genome_size": "{genome_size}",'
                  '"output_folder": "{output_folder}",' # required
                  '"uid": "{uid}"'                      # required
                '}}')
# For both paired-end and single end
CHIP_SEQ_UPLOAD = {
                    '{}_fragment_stat.tsv': upload_macs2_fragment_stat,
                    '{}_macs_peaks_iaintersect.tsv': upload_iaintersect_result,
                    '{}.stat': upload_get_stat,
                    '{}_atdp.tsv': upload_atdp,
                    '{}.bigWig': upload_bigwig,
                    'set_dateanalyzed': upload_dateanalyzed,
                    'upload_folder_size': upload_folder_size,
                    '{}*.fastq': delete_files
                  }


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
CHIP_SEQ_GEN_BIGWIG_UPLOAD = {'{}.bigWig': upload_bigwig}


# RNA-Seq single end
RNA_SEQ_SE_WORKFLOW = 'rnaseq-se.cwl'
RNA_SEQ_SE_TEMPLATE_JOB = ('{{'
                  '"fastq_file": {{"class": "File", "location": "{fastq_file_upstream}", "format": "http://edamontology.org/format_1930"}},'
                  '"star_indices_folder": {{"class": "Directory", "location": "{star_indices_folder}"}},'
                  '"bowtie_indices_folder": {{"class": "Directory", "location": "{bowtie_indices_folder}"}},'
                  '"chrom_length_file": {{"class": "File", "location": "{chrom_length}", "format": "http://edamontology.org/format_2330"}},'
                  '"annotation_file": {{"class": "File", "location": "{annotation_input_file}", "format": "http://edamontology.org/format_3475"}},'
                  '"exclude_chr": "{exclude_chr}",'
                  '"clip_3p_end": {clip_3p_end},'
                  '"clip_5p_end": {clip_5p_end},'
                  '"threads": {threads},'
                  '"output_folder": "{output_folder}",' # required
                  '"uid": "{uid}"'                      # required
                '}}')
# RNA-Seq single end dUTP
RNA_SEQ_SE_DUTP_WORKFLOW = 'rnaseq-se-dutp.cwl'
RNA_SEQ_SE_DUTP_TEMPLATE_JOB = ('{{'
                  '"fastq_file": {{"class": "File", "location": "{fastq_file_upstream}", "format": "http://edamontology.org/format_1930"}},'
                  '"star_indices_folder": {{"class": "Directory", "location": "{star_indices_folder}"}},'
                  '"bowtie_indices_folder": {{"class": "Directory", "location": "{bowtie_indices_folder}"}},'
                  '"chrom_length_file": {{"class": "File", "location": "{chrom_length}", "format": "http://edamontology.org/format_2330"}},'
                  '"annotation_file": {{"class": "File", "location": "{annotation_input_file}", "format": "http://edamontology.org/format_3475"}},'
                  '"exclude_chr": "{exclude_chr}",'
                  '"clip_3p_end": {clip_3p_end},'
                  '"clip_5p_end": {clip_5p_end},'
                  '"threads": {threads},'
                  '"output_folder": "{output_folder}",' # required
                  '"uid": "{uid}"'                      # required
                '}}')
# RNA-Seq paired end
RNA_SEQ_PE_WORKFLOW = 'rnaseq-pe.cwl'
RNA_SEQ_PE_TEMPLATE_JOB = ('{{'
                  '"fastq_file_upstream": {{"class": "File", "location": "{fastq_file_upstream}", "format": "http://edamontology.org/format_1930"}},'
                  '"fastq_file_downstream": {{"class": "File", "location": "{fastq_file_downstream}", "format": "http://edamontology.org/format_1930"}},'
                  '"star_indices_folder": {{"class": "Directory", "location": "{star_indices_folder}"}},'
                  '"bowtie_indices_folder": {{"class": "Directory", "location": "{bowtie_indices_folder}"}},'
                  '"chrom_length_file": {{"class": "File", "location": "{chrom_length}", "format": "http://edamontology.org/format_2330"}},'
                  '"annotation_file": {{"class": "File", "location": "{annotation_input_file}", "format": "http://edamontology.org/format_3475"}},'
                  '"exclude_chr": "{exclude_chr}",'
                  '"clip_3p_end": {clip_3p_end},'
                  '"clip_5p_end": {clip_5p_end},'
                  '"threads": {threads},'
                  '"output_folder": "{output_folder}",' # required
                  '"uid": "{uid}"'                      # required
                '}}')
# RNA-Seq paired end dUTP
RNA_SEQ_PE_DUTP_WORKFLOW = 'rnaseq-pe-dutp.cwl'
RNA_SEQ_PE_DUTP_TEMPLATE_JOB = ('{{'
                  '"fastq_file_upstream": {{"class": "File", "location": "{fastq_file_upstream}", "format": "http://edamontology.org/format_1930"}},'
                  '"fastq_file_downstream": {{"class": "File", "location": "{fastq_file_downstream}", "format": "http://edamontology.org/format_1930"}},'
                  '"star_indices_folder": {{"class": "Directory", "location": "{star_indices_folder}"}},'
                  '"bowtie_indices_folder": {{"class": "Directory", "location": "{bowtie_indices_folder}"}},'
                  '"chrom_length_file": {{"class": "File", "location": "{chrom_length}", "format": "http://edamontology.org/format_2330"}},'
                  '"annotation_file": {{"class": "File", "location": "{annotation_input_file}", "format": "http://edamontology.org/format_3475"}},'
                  '"exclude_chr": "{exclude_chr}",'
                  '"clip_3p_end": {clip_3p_end},'
                  '"clip_5p_end": {clip_5p_end},'
                  '"threads": {threads},'
                  '"output_folder": "{output_folder}",' # required
                  '"uid": "{uid}"'                      # required
                '}}')
# For both SE and PE (not dUTP)
RNA_SEQ_UPLOAD = {
                    '{}.bigWig': upload_bigwig,
                    '{}.stat': upload_get_stat,
                    '{}.isoforms.csv': upload_rpkm,
                    'set_dateanalyzed': upload_dateanalyzed,
                    'upload_folder_size': upload_folder_size,
                    '{}*.fastq': delete_files
                  }
# For both SE and PE (dUTP)
RNA_SEQ_DUTP_UPLOAD = {
                        '{}_upstream.bigWig': upload_bigwig_upstream,
                        '{}_downstream.bigWig': upload_bigwig_downstream,
                        '{}.stat': upload_get_stat,
                        '{}.isoforms.csv': upload_rpkm,
                        'set_dateanalyzed': upload_dateanalyzed,
                        'upload_folder_size': upload_folder_size,
                        '{}*.fastq': delete_files
                      }


# Dict to save experimenttype.etype and workflow file & template correspondeces
EXP_TYPE = {
    "RNA-Seq":           (RNA_SEQ_SE_WORKFLOW,          RNA_SEQ_SE_TEMPLATE_JOB,          RNA_SEQ_UPLOAD),
    "RNA-Seq dUTP":      (RNA_SEQ_SE_DUTP_WORKFLOW,     RNA_SEQ_SE_DUTP_TEMPLATE_JOB,     RNA_SEQ_DUTP_UPLOAD),
    "RNA-Seq pair":      (RNA_SEQ_PE_WORKFLOW,          RNA_SEQ_PE_TEMPLATE_JOB,          RNA_SEQ_UPLOAD),
    "RNA-Seq dUTP pair": (RNA_SEQ_PE_DUTP_WORKFLOW,     RNA_SEQ_PE_DUTP_TEMPLATE_JOB,     RNA_SEQ_DUTP_UPLOAD),
    "DNA-Seq":           (CHIP_SEQ_SE_WORKFLOW,         CHIP_SEQ_SE_TEMPLATE_JOB,         CHIP_SEQ_UPLOAD),
    "DNA-Seq pair":      (CHIP_SEQ_PE_WORKFLOW,         CHIP_SEQ_PE_TEMPLATE_JOB,         CHIP_SEQ_UPLOAD),
    "GEN-bigWig":        (CHIP_SEQ_GEN_BIGWIG_WORKFLOW, CHIP_SEQ_GEN_BIGWIG_TEMPLATE_JOB, CHIP_SEQ_GEN_BIGWIG_UPLOAD)
}