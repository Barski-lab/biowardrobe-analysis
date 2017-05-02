import unittest
import testing.mysqld
import os
import MySQLdb
import tempfile
import shutil
import random
import string
from basic_analysis.run_dna_func import submit_job

MYSQL_INIT = os.path.normpath(os.path.abspath(os.path.join(__file__, "../../sql/wardrobe.sql")))

WORKFLOW = 'run-dna.cwl'
TEMPLATE_JOB_SE = '{{\n\
                  "fastq_input_file": {{"class": "File", "location": "{fastq_input_file}", "format": "http://edamontology.org/format_1930"}},\n\
                  "bowtie_indices_folder": {{"class": "Directory", "location": "{bowtie_indices_folder}"}},\n\
                  "clip_3p_end": {clip_3p_end},\n\
                  "clip_5p_end": {clip_5p_end},\n\
                  "threads": {threads},\n\
                  "remove_duplicates": {remove_duplicates},\n\
                  "control_file": {{"class": "File", "location": "{control_file}", "format": "http://edamontology.org/format_2572"}},\n\
                  "exp_fragment_size": {exp_fragment_size},\n\
                  "force_fragment_size": {force_fragment_size},\n\
                  "broad_peak": {broad_peak},\n\
                  "chrom_length": {{"class": "File", "location": "{chrom_length}", "format": "http://edamontology.org/format_2330"}},\n\
                  "genome_size": "{genome_size}"\n\
                }}'






def handler(mysqld):
    conn = MySQLdb.connect(**mysqld.dsn())
    cursor = conn.cursor()
    with open(MYSQL_INIT, 'r') as sql_init_file:
        for line in sql_init_file.read().split(';'):
            if not line.strip(): continue
            cursor.execute(line.strip())
    cursor.close()
    conn.commit()
    conn.close()


Mysqld = testing.mysqld.MysqldFactory(cache_initialized_db=True, on_initialized=handler)


def tearDownModule():
    Mysqld.clear_cache()


class TestRunDNA(unittest.TestCase):


    def def_connect(self):
        self.conn = MySQLdb.connect (**self.mysqld.dsn())
        self.conn.set_character_set('utf8')
        self.cursor = self.conn.cursor ()


    def set_tmp_settings (self):
        wardrobe = tempfile.mkdtemp()+'/'
        random_settings = {
            "wardrobe": wardrobe,
            "bin": tempfile.mkdtemp(prefix=wardrobe),
            "temp": tempfile.mkdtemp(prefix=wardrobe),
            "upload": tempfile.mkdtemp(prefix=wardrobe),
            "preliminary": tempfile.mkdtemp(prefix=wardrobe),
            "indices": tempfile.mkdtemp(prefix=wardrobe),
            "advanced": tempfile.mkdtemp(prefix=wardrobe)
        }
        self.cursor.execute("use ems;")
        for key,value in random_settings.iteritems():
            self.cursor.execute("update settings set value='{0}' where `key`='{1}'".format(value,key))
        self.conn.commit()


    def get_settings(self):
        self.settings={}
        self.cursor.execute("use ems;")
        self.cursor.execute ("select * from settings")
        for (key,value,descr,stat,group) in self.cursor.fetchall():
            self.settings[key]=value


    def setUp(self):
        self.mysqld = Mysqld()
        self.def_connect()
        self.set_tmp_settings()
        self.get_settings()


    def tearDown(self):
        self.mysqld.stop()
        print self.settings["wardrobe"]
        # shutil.rmtree(self.settings["wardrobe"], ignore_errors=False)


    def gen_dna_exp(self, uid, control_id='',libstatus=2, libstatustxt="downloaded"):
        labdata_rec = {
            "uid": uid,
            "fragmentsizeexp": 150,
            "fragmentsizeforceuse": 0,
            "libstatus": libstatus,
            "libstatustxt": libstatustxt,
            "forcerun": 0,
            "trim5": 0,
            "trim3": 0,
            "rmdup": 0,
            "control_id": control_id,
            "experimenttype_id": 2,
            "genome_id": 1,
            "cells": 'Embryo',
            "conditions": '8-16h',
            "dateadd": '2015-05-01',
            "deleted": 0
        }
        experimenttype_rec = {
            "etype": "DNA-Seq pair"
        }
        return labdata_rec, experimenttype_rec

    def prepare_single_end_fastq_files(self, save_file=True):
        rnd_uid = ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(10))
        if save_file:
            os.mkdir (os.path.join(self.settings["preliminary"], rnd_uid))
            with open(os.path.join(self.settings["preliminary"],rnd_uid,rnd_uid+".fastq"), 'w') as fastq_file:
                fastq_file.write(rnd_uid)
        return rnd_uid

    def prepare_control_file (self, save_file=True, update_db=True, complete=True):
        rnd_uid = ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(10))
        if save_file:
            os.mkdir (os.path.join(self.settings["preliminary"], rnd_uid))
            with open(os.path.join(self.settings["preliminary"],rnd_uid,rnd_uid+".bam"), 'w') as bam_file:
                bam_file.write(rnd_uid)
        if update_db:
            if complete:
                labdata_rec, experimenttype_rec = self.gen_dna_exp(uid = rnd_uid, libstatus=12, libstatustxt="Complete")
            else:
                labdata_rec, experimenttype_rec = self.gen_dna_exp(uid = rnd_uid, libstatus=2, libstatustxt="downloaded")
            self.add_dna_exp(labdata_rec, experimenttype_rec)
        return rnd_uid

    def add_dna_exp(self, labdata_rec, experimenttype_rec):
        self.cursor.execute("use ems;")
        self.cursor.execute ("INSERT INTO labdata(uid,fragmentsizeexp,fragmentsizeforceuse,libstatus,libstatustxt,\
                                          forcerun,trim5,trim3,rmdup,control_id,experimenttype_id,genome_id,cells,\
                                          conditions,dateadd,deleted) \
                              VALUES('{uid}',{fragmentsizeexp},{fragmentsizeforceuse},{libstatus},'{libstatustxt}',\
                                      {forcerun},{trim5},{trim3},{rmdup},'{control_id}',{experimenttype_id},\
                                      {genome_id},'{cells}','{conditions}','{dateadd}',{deleted})".format(**labdata_rec))
        self.conn.commit()


    def get_row_from_db (self):
        self.cursor.execute(
            "update labdata set libstatustxt='ready for process',libstatus=10 where libstatus=2 and experimenttype_id in (select id from experimenttype where etype like 'DNA%') "
            " and COALESCE(egroup_id,'') <> '' and COALESCE(name4browser,'') <> '' and deleted=0 ")
        self.cursor.execute(
            "select e.etype,g.db,g.findex,g.annotation,l.uid,fragmentsizeexp,fragmentsizeforceuse,forcerun, "
            "COALESCE(l.trim5,0), COALESCE(l.trim3,0),COALESCE(a.properties,0), COALESCE(l.rmdup,0),g.gsize, COALESCE(control,0), COALESCE(control_id,'') "
            "from labdata l "
            "inner join (experimenttype e,genome g ) ON (e.id=experimenttype_id and g.id=genome_id) "
            "LEFT JOIN (antibody a) ON (l.antibody_id=a.id) "
            "where e.etype like 'DNA%' and libstatus in (10,1010) "
            "and deleted=0 and COALESCE(egroup_id,'') <> '' and COALESCE(name4browser,'') <> '' "
            " order by control DESC,dateadd limit 1")
        return self.cursor.fetchall()

    def print_labdata(self):
        self.cursor.execute('SELECT * FROM LABDATA')
        print "LABDATA:\n", self.cursor.fetchall()

# "CREATE TABLE `experimenttype` (
#   `id` int(11) NOT NULL AUTO_INCREMENT,
#   `etype` varchar(100) DEFAULT NULL,"
#
#
# "CREATE TABLE `labdata` (
#   `id` int(11) NOT NULL AUTO_INCREMENT,
#   `uid` varchar(36) DEFAULT NULL,
#   `deleted` int(1) NOT NULL DEFAULT '0',
#   `groupping` varchar(200) DEFAULT NULL,
#   `author` varchar(300) DEFAULT NULL,
#   `cells` varchar(1000) NOT NULL,
#   `conditions` varchar(1000) NOT NULL,
#   `spikeinspool` varchar(200) DEFAULT '',
#   `spikeins_id` int(11) DEFAULT NULL,
#   `tagstotal` int(11) DEFAULT '0',
#   `tagsmapped` int(11) DEFAULT '0',
#   `tagsribo` int(11) DEFAULT '0',
#   `fragmentsize` int(11) DEFAULT NULL COMMENT 'fragment shifting for Chip-seq',
#   `fragmentsizeest` int(5) DEFAULT NULL COMMENT 'Estimated fragment size by MACS',
#   `fragmentsizeexp` int(5) DEFAULT '150' COMMENT 'Expected fragment size from wet lab',
#   `fragmentsizeforceuse` int(1) DEFAULT '0',
#   `islandcount` int(7) DEFAULT NULL,
#   `notes` text,
#   `protocol` text,
#   `filename` varchar(40) DEFAULT NULL,
#   `dateadd` date NOT NULL,
#   `libstatus` int(11) DEFAULT '0',
#   `libstatustxt` varchar(2000) DEFAULT 'created',
#   `url` varchar(2000) DEFAULT NULL COMMENT 'direct link to a file',
#   `name4browser` varchar(300) DEFAULT NULL,
#   `browsergrp` varchar(150) DEFAULT '',
#   `browsershare` int(1) DEFAULT '1',
#   `forcerun` int(1) DEFAULT '0',
#   `antibodycode` varchar(100) DEFAULT NULL,
#   `genome_id` int(11) DEFAULT '1',
#   `crosslink_id` int(11) DEFAULT '1',
#   `fragmentation_id` int(11) DEFAULT '1',
#   `antibody_id` varchar(36) DEFAULT NULL,
#   `experimenttype_id` int(11) DEFAULT '1',
#   `worker_id` int(11) DEFAULT NULL,
#   `laboratory_id` varchar(36) DEFAULT NULL,
#   `egroup_id` varchar(36) DEFAULT NULL,
#   `download_id` int(3) DEFAULT NULL,"



    def test_submit_job(self):
        print self.settings
        uid = self.prepare_single_end_fastq_files()
        control_id = self.prepare_control_file()
        labdata_rec, experimenttype_rec = self.gen_dna_exp(uid, control_id)
        self.add_dna_exp(labdata_rec, experimenttype_rec)

        # self.print_labdata()
        # self.cursor.execute(
        #     "update labdata set libstatustxt='ready for process',libstatus=10 where libstatus=2 and experimenttype_id in (select id from experimenttype where etype like 'DNA%') "
        #     " and COALESCE(egroup_id,'') <> '' and COALESCE(name4browser,'') <> '' and deleted=0 ")
        # self.print_labdata()

        print "ROW: ", self.get_row_from_db()

        # submit_job (db_settings=self,
        #             row=self.get_row_from_db(),
        #             raw_data=os.path.join(self.settings['wardrobe'], self.settings['preliminary']),
        #             indices= os.path.join(self.settings['wardrobe'], self.settings['indices']),
        #             workflow=WORKFLOW,
        #             template_job=TEMPLATE_JOB_SE,
        #             threads=self.settings['maxthreads'],
        #             jobs_folder=tempfile.mkdtemp(prefix=self.settings["wardrobe"]))



if __name__ == '__main__':
    unittest.main()