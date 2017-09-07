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
ANNOTATIONS = "annotations"
JOBS_NEW = 'new'
JOBS_SUCCESS = 'success'
JOBS_FAIL = 'fail'
JOBS_RUNNING = 'running'
CHR_LENGTH_GENERIC_TSV = "chrNameLength.txt"
ANNOTATION_GENERIC_TSV = "refgene.tsv"

# strand
UPSTREAM = "+"
DOWNSTREAM = "-"

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


# For TrimGalore testing
CHIP_SEQ_TRIM_SE_WORKFLOW = 'trim-chipseq-se.cwl'
CHIP_SEQ_TRIM_PE_WORKFLOW = 'trim-chipseq-pe.cwl'
FOLDER_ID = 'h765C750-E801-A805-B289-AE8F3947EE14'
