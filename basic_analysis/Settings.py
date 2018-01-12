#! /usr/bin/env python

import argparse
import MySQLdb
from typing import Text
import os

class Settings:
    """Class to get access to DB"""

    args = argparse.Namespace()


    def get_args(self):
        return self.args


    def arg_parser(self):
        """Returns argument parser"""
        parser = argparse.ArgumentParser(description='BioWardrobe settings parser for DB connection', add_help=True)
        parser.add_argument("-c", "--config", type=Text, help="Wardrobe config file", default="/etc/wardrobe/wardrobe")
        parser.add_argument("-j", "--jobs", type=Text, help="Folder to export generated jobs", default="./")
        return parser


    def normalize(not_normalized_args):
        """Resolves all relative paths to be absolute relatively to current working directory"""
        normalized_args = {}
        for key, value in not_normalized_args.__dict__.iteritems():
            normalized_args[key] = value if not value or os.path.isabs(value) else os.path.normpath(
                os.path.join(os.getcwd(), value))
        return argparse.Namespace(**normalized_args)


    def __init__(self):
        not_normalized_args, _ = self.arg_parser().parse_known_args()
        self.args = self.normalize(not_normalized_args)
        try:
            with open(self.args.config, 'r') as conf_file:
                self.config = [line.strip() for line in conf_file.readlines() if not line.startswith("#") and line.strip()]
        except Exception:
            print "Can't open file " + self.args.config
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
