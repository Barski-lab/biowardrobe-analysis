#! /usr/bin/env python
##/****************************************************************************
##**
##** Copyright (C) 2011-2014 Andrey Kartashov .
##** All rights reserved.
##** Contact: Andrey Kartashov (porter@porter.st)
##**
##** This file is part of the global module of the genome-tools.
##**
##** GNU Lesser General Public License Usage
##** This file may be used under the terms of the GNU Lesser General Public
##** License version 2.1 as published by the Free Software Foundation and
##** appearing in the file LICENSE.LGPL included in the packaging of this
##** file. Please review the following information to ensure the GNU Lesser
##** General Public License version 2.1 requirements will be met:
##** http://www.gnu.org/licenses/old-licenses/lgpl-2.1.html.
##**
##** Other Usage
##** Alternatively, this file may be used in accordance with the terms and
##** conditions contained in a signed written agreement between you and Andrey Kartashov.
##**
##****************************************************************************/


import argparse
import MySQLdb
from typing import Text

class Settings:


    def arg_parser(self):
        parser = argparse.ArgumentParser(description='BioWardrobe settings parser for DB connection')
        parser.add_argument("--wardrobe", type=Text, help="Wardrobe config file")
        return parser


    def __init__(self):
        args, _ = self.arg_parser().parse_known_args()
        def_conf = args.wardrobe if args.wardrobe else "/etc/wardrobe/wardrobe"
        try:
            with open(def_conf, 'r') as conf_file:
                config = [line.strip() for line in conf_file.readlines() if not line.startswith("#") and line.strip()]
        except Exception:
            print "Can't open file " + def_conf
            return
        self.def_connect(config)
        self.get_settings()


    def def_connect(self,config):
        try:
            self.conn = MySQLdb.connect (host = config[0],
                                         user = config[1],
                                         passwd = config[2],
                                         db = config[3])
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
        self.settings={}
        self.cursor.execute ("select * from settings")
        for (key,value,descr,stat,group) in self.cursor.fetchall():
            if key in ['advanced','bin','indices','preliminary',\
                       'temp','upload']:
                value=value.lstrip('/')
            self.settings[key]=value











