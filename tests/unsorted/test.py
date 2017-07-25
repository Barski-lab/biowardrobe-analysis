from basic_analysis.DefFunctions import (raise_if_table_absent,
                                         raise_if_table_exists,
                                         raise_if_file_absent,
                                         raise_if_file_exists)
from basic_analysis.Settings import Settings
from basic_analysis.biow_exceptions import BiowBasicException

# Get access to DB
biow_db_settings = Settings()

uid="random_uid"
table="q9C50F36_0BFF_0971_AFF9_60A3616E1F33_f_wtrack"
db="dm3"

try:
    raise_if_table_absent (biow_db_settings, uid, table, db)
except BiowBasicException as ex:
    print str(ex)
except Exception as ex:
    print "Unrecognized exception. Find a bug", str(ex)

try:
    raise_if_table_exists (biow_db_settings, uid, table, db)
except BiowBasicException as ex:
    print str(ex)
except Exception as ex:
    print "Unrecognized exception. Find a bug", str(ex)


uid, filename = "random_uid", "../../basic_analysis/constants.py"
try:
    raise_if_file_absent (uid, filename)
except BiowBasicException as ex:
    print str(ex)
except Exception as ex:
    print "Unrecognized exception. Find a bug", str(ex)

try:
    raise_if_file_exists (uid, filename)
except BiowBasicException as ex:
    print str(ex)
except Exception as ex:
    print "Unrecognized exception. Find a bug", str(ex)
