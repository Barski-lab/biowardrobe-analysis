class DBSettings:

    def __init__(self, connection):
        self.settings = {}
        self.conn = connection
        self.cursor = connection.cursor()
        self.cursor.execute ("select * from settings")

        for (key,value,descr,stat,group) in cursor.fetchall():
            if key in ['advanced','bin','indices','preliminary','temp','upload']:
                value = value.lstrip('/')
            self.settings[key] = value

    def use_ems(self):
        pass
