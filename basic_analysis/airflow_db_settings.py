#! /usr/bin/env python

import argparse
import MySQLdb
from typing import Text

class Settings:

    def arg_parser(self):
        parser = argparse.ArgumentParser(description='BioWardrobe settings parser for airflow db connection')
        parser.add_argument("--wardrobe", type=Text, help="Wardrobe config file")
        return parser

    def __init__(self):
        args, _ = self.arg_parser().parse_known_args()
        def_conf = args.wardrobe if args.wardrobe else "airflow_db_conf.txt"
        try:
            with open(def_conf, 'r') as conf_file:
                config = [line.strip() for line in conf_file.readlines()]
        except Exception:
            print "Can't open file " + def_conf
            return
        self.def_connect(config)

    def def_connect(self,config):
        try:
            self.conn = MySQLdb.connect (host = config[0],
                                         port = int(config[1]),
                                         user = config[2],
                                         passwd = config[3],
                                         db = config[4])
            self.conn.set_character_set('utf8')
            self.cursor = self.conn.cursor()
        except Exception as e:
            print("Database connection error: " + str(e))
        return self.cursor
