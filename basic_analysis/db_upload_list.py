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


CHIP_SEQ_GEN_BIGWIG_UPLOAD = { '{}.bigWig': upload_bigwig }
