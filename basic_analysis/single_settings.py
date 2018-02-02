def dummy():
    pass


def get_db_settings (connection):
    settings = {}
    cursor = connection.cursor()
    cursor.execute ("select * from settings")
    for (key,value,descr,stat,group) in cursor.fetchall():
        if key in ['advanced','bin','indices','preliminary','temp','upload']:
            value = value.lstrip('/')
        settings[key] = value

    return {
        "use_ems": dummy,
        "settings": settings,
        "cursor": cursor,
        "conn": connection
    }