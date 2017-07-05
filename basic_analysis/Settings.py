#! /usr/bin/env python

import argparse
import MySQLdb
from typing import Text

class Settings:
    """Class to get access to DB"""

    def arg_parser(self):
        """Returns argument parser"""
        parser = argparse.ArgumentParser(description='BioWardrobe settings parser for DB connection')
        parser.add_argument("--wardrobe", type=Text, help="Wardrobe config file")
        return parser


    def __init__(self):
        args, _ = self.arg_parser().parse_known_args()
        def_conf = args.wardrobe if args.wardrobe else "/etc/wardrobe/wardrobe"
        try:
            with open(def_conf, 'r') as conf_file:
                self.config = [line.strip() for line in conf_file.readlines() if not line.startswith("#") and line.strip()]
        except Exception:
            print "Can't open file " + def_conf
            return
        self.def_connect()
        self.get_settings()


    def def_connect(self):
        try:
            self.conn = MySQLdb.connect (host = self.config[0],
                                         user = self.config[1],
                                         passwd = self.config[2],
                                         db = self.config[3])
            self.conn.set_character_set('utf8')
            self.cursor = self.conn.cursor()
        except Exception as e:
            print("Database connection error: " + str(e))
        return self.cursor


    def def_close(self):
        try:
            self.conn.close()
        except:
            pass


    def get_settings(self):
        self.settings={"emsdb": self.config[3]}
        self.cursor.execute ("select * from settings")
        for (key,value,descr,stat,group) in self.cursor.fetchall():
            if key in ['advanced','bin','indices','preliminary','temp','upload']:
                value=value.lstrip('/')
            self.settings[key]=value


    def use_ems(self):
        self.cursor.execute ( 'use {}'.format(self.settings["emsdb"]) )


    def use_airflow(self):
        self.cursor.execute( 'use {}'.format(self.settings["airflowdb"]) )
