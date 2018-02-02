from Settings import Settings
import single_rna_get_job
import single_dna_get_job


biow_db_settings = Settings()
print single_rna_get_job.get_job(id, biow_db_settings.conn)
print single_dna_get_job(id, biow_db_settings.conn)