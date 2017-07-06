"""Strategy pattern to run BaseUploader.execute depending on types of files to be uploaded"""

import warnings
import os
import types
import MySQLdb
import string
from biow_exceptions import BiowUploadException
from constants import UPSTREAM, DOWNSTREAM
import glob

class BaseUploader:
    def __init__(self, database_settings, func=None):
        self.db_settings = database_settings
        if func is not None:
            self.execute = types.MethodType(func, self)


def upload_results_to_db (upload_set, uid, raw_data, db_settings):
    """Call execute function for all created BaseUploader"""
    for key,value in upload_set.iteritems():
        try:
            BaseUploader(db_settings, value).execute(uid, os.path.join(raw_data, uid, key.format(uid)))
        except Exception as ex:
            raise BiowUploadException (uid, message="Failed to upload " + key.format(uid) + " : " + str(ex))


def upload_macs2_fragment_stat(self, uid, filename):
    self.db_settings.use_ems()
    with open(filename, 'r') as input_file:
        data = input_file.read().strip().split()
        data.append(uid)
        self.db_settings.cursor.execute("update labdata set fragmentsize=%s,fragmentsizeest=%s,islandcount=%s where uid=%s", tuple(data))
        self.db_settings.conn.commit()


def upload_iaintersect_result(self, uid, filename):
    self.db_settings.use_ems()
    warnings.filterwarnings('ignore', category=MySQLdb.Warning)
    table_name = self.db_settings.settings['experimentsdb'] + '.`' + uid + '_islands`'
    self.db_settings.cursor.execute("select g.db from labdata l inner join genome g ON g.id=genome_id where uid=%s", (uid,))
    db_tuple = self.db_settings.cursor.fetchone()
    if not db_tuple:
        raise BiowUploadException(uid, message="DB not found")
    gb_table_name = db_tuple[0] + '.`' + string.replace(uid, "-", "_") + '_islands`'

    self.db_settings.cursor.execute("DROP TABLE IF EXISTS " + table_name)
    self.db_settings.cursor.execute("DROP TABLE IF EXISTS " + gb_table_name)

    self.db_settings.cursor.execute(""" CREATE TABLE """ + table_name +
                                    """ ( refseq_id VARCHAR(500) NULL,
                                        gene_id VARCHAR(500) NULL,
                                        txStart INT NULL,
                                        txEnd INT NULL,
                                        strand VARCHAR(1),
                                        region VARCHAR(50),
                                        chrom VARCHAR(255) NOT NULL,
                                        start INT(10) UNSIGNED NOT NULL,
                                        end INT(10) UNSIGNED NOT NULL,
                                        length INT(10) UNSIGNED NOT NULL,
                                        abssummit INT(10),
                                        pileup FLOAT,
                                        log10p FLOAT,
                                        foldenrich FLOAT,
                                        log10q FLOAT,
                                        INDEX chrom_idx (chrom) USING BTREE,
                                        INDEX start_idx (start) USING BTREE,
                                        INDEX end_idx (end) USING BTREE,
                                        INDEX region_idx (region ASC) USING BTREE,
                                        INDEX txEnd_idx (txEnd ASC) USING BTREE,
                                        INDEX txStart_idx (txStart ASC) USING BTREE,
                                        INDEX gene_idx (gene_id(100) ASC) USING BTREE,
                                        INDEX strand (strand ASC) USING BTREE,
                                        INDEX refseq_idx (refseq_id(100) ASC) USING BTREE
                                        ) ENGINE=MyISAM DEFAULT CHARSET=utf8 """)
    self.db_settings.conn.commit()

    self.db_settings.cursor.execute(""" CREATE TABLE """ + gb_table_name +
                                    """ ( bin int(7) unsigned NOT NULL,
                                        chrom varchar(255) NOT NULL,
                                        chromStart int(10) unsigned NOT NULL,
                                        chromEnd int(10) unsigned NOT NULL,
                                        name varchar(255) NOT NULL,
                                        score int(5) not null,
                                        INDEX bin_idx (bin) using btree,
                                        INDEX chrom_idx (chrom) using btree,
                                        INDEX chrom_start_idx (chromStart) using btree,
                                        INDEX chrom_end_idx (chromEnd) using btree
                                        ) ENGINE=MyISAM DEFAULT CHARSET=utf8 """)
    self.db_settings.conn.commit()

    SQL = "INSERT INTO " + table_name + " (refseq_id,gene_id,txStart,txEnd,strand,chrom,start,end,length,abssummit,pileup,log10p,foldenrich,log10q,region) VALUES"
    with open(filename, 'r') as input_file:
        for line in input_file.readlines():
            line = line.strip()
            if not line or "gene_id" in line or "pileup" in line:
                continue
            line_splitted = [None if item=="NULL" else item for item in line.split()]
            self.db_settings.cursor.execute(SQL + " (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)", tuple(line_splitted))
        self.db_settings.conn.commit()

    self.db_settings.cursor.execute("""insert into  """ + gb_table_name +
                   """ (bin, chrom, chromStart, chromEnd, name, score)
                    select 0 as bin, chrom, start as chromStart, end as chromEnd,
                    max(log10p) as name, max(log10q) as score
                    from """ + table_name + """ group by chrom,start,end; """)
    self.db_settings.conn.commit()

    self.db_settings.cursor.execute(
        """update labdata set params='{"promoter":1000}' where uid=%s""", (uid,))
    self.db_settings.conn.commit()


def upload_get_stat (self, uid, filename):
    self.db_settings.use_ems()
    with open(filename, 'r') as input_file:
        data = input_file.read().strip().split()
        # TOTAL, ALIGNED, SUPRESSED, USED
        data.append(uid)
        self.db_settings.cursor.execute(
            "update labdata set tagstotal=%s,tagsmapped=%s,tagsribo=%s,tagssuppressed=%s,tagsused=%s where uid=%s",
            (data[0], data[1], data[2], data[2], data[3], data[4]))
        self.db_settings.conn.commit()


def upload_atdp(self, uid, filename):
    warnings.filterwarnings('ignore', category=MySQLdb.Warning)
    table_name = self.db_settings.settings['experimentsdb'] + '.`' + uid + '_atdp`'
    self.db_settings.cursor.execute("DROP TABLE IF EXISTS " + table_name)
    self.db_settings.conn.commit()
    self.db_settings.cursor.execute(""" CREATE TABLE """ + table_name +
                                    """ ( X INT NULL,
                                        Y FLOAT NULL,
                                        INDEX X_idx (X) USING BTREE
                                        ) ENGINE=MyISAM DEFAULT CHARSET=utf8 COMMENT = 'created by atdp' """)
    self.db_settings.conn.commit()
    SQL = "INSERT INTO " + table_name + " (X,Y) VALUES"
    with open(filename, 'r') as input_file:
        for line in input_file.readlines():
            line = line.strip()
            if not line or "X" in line or "Y" in line:
                continue
            self.db_settings.cursor.execute(SQL + " (%s,%s)", tuple(line.split()))
        self.db_settings.conn.commit()


def upload_bigwig (self, uid, filename, strand=None):
    self.db_settings.use_ems()
    self.db_settings.cursor.execute("SELECT g.db FROM labdata l INNER JOIN genome g ON g.id=genome_id WHERE uid=%s", (uid,))
    db_tuple = self.db_settings.cursor.fetchone()
    if not db_tuple:
        raise BiowUploadException(uid, message="DB not found")
    gb_bigwig_table_name = {
        UPSTREAM: db_tuple[0] + '.`' + string.replace(uid, "-", "_") + '_upstream_wtrack`',
        DOWNSTREAM: db_tuple[0] + '.`' + string.replace(uid, "-", "_") + '_downstream_wtrack`',
        None: db_tuple[0] + '.`' + string.replace(uid, "-", "_") + '_wtrack`'
    }[strand]
    self.db_settings.cursor.execute(" DROP TABLE IF EXISTS " + gb_bigwig_table_name)
    self.db_settings.cursor.execute(" CREATE TABLE " + gb_bigwig_table_name +
                                    " (fileName VARCHAR(255) not NULL) ENGINE=MyISAM DEFAULT CHARSET=utf8")
    self.db_settings.cursor.execute(" INSERT INTO " + gb_bigwig_table_name + "VALUES (%s)", (filename,))
    self.db_settings.conn.commit()


def upload_bigwig_upstream (self, uid, filename):
     upload_bigwig (self, uid, filename, strand=UPSTREAM)


def upload_bigwig_downstream (self, uid, filename):
     upload_bigwig (self, uid, filename, strand=DOWNSTREAM)


def upload_dateanalyzed(self, uid, filename):
    self.db_settings.use_ems()
    self.db_settings.cursor.execute("update labdata set dateanalyzed=now() where uid=%s and dateanalyzed is null", (uid,))
    self.db_settings.conn.commit()


def upload_folder_size(self, uid, filename):
    self.db_settings.use_ems()
    total_size = 0
    for root, dirs, files in os.walk(os.path.dirname(filename)):
        for f in files:
            fp = os.path.join(root, f)
            total_size += os.path.getsize(fp)
    self.db_settings.cursor.execute("update labdata set size = %s where uid=%s", (int(total_size)/1024.0,uid))
    self.db_settings.conn.commit()

def delete_files(self, uid, filename):
    for item_file in glob.glob(filename):
        os.remove(item_file)
