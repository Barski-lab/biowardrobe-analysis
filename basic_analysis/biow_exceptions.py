from constants import LIBSTATUS


class BiowBasicException(Exception):
    """Basic BioWardrobe exception class"""
    def __init__(self, uid, code, message):
        self.uid = uid
        self.code = code
        self.message = message
        super(BiowBasicException, self).__init__(self.message)


class BiowFileNotFoundException(BiowBasicException):
    """File not found BioWardrobe exception class"""
    def __init__(self, uid, code=None, message=None):
        self.uid = uid
        self.code = code if code else LIBSTATUS["FAIL_PROCESS"]
        self.message = message if message else "File not found for {0}".format(uid)
        super(BiowFileNotFoundException, self).__init__(self.uid, self.code, self.message)

class BiowJobException(BiowBasicException):
    """Failed to generate input parameters file BioWardrobe exception class"""
    def __init__(self, uid, code=None, message=None):
        self.uid = uid
        self.code = code if code else LIBSTATUS["FAIL_PROCESS"]
        self.message = message if message else "Failed to generate input parameters file for {0}".format(uid)
        super(BiowJobException, self).__init__(self.uid, self.code, self.message)

class BiowUploadException(BiowBasicException):
    """Failed to upload data to Biowardrobe DB"""
    def __init__(self, uid, code=None, message=None):
        self.uid = uid
        self.code = code if code else LIBSTATUS["FAIL_PROCESS"]
        self.message = message if message else "Failed to upload results to DB for {0}".format(uid)
        super(BiowJobException, self).__init__(self.uid, self.code, self.message)

class BiowWorkflowException(BiowBasicException):
    """Failed to run workflow BioWardrobe exception class"""
    def __init__(self, uid, code=None, message=None):
        self.uid = uid
        self.code = code if code else LIBSTATUS["FAIL_PROCESS"]
        self.message = message if message else "Failed to run workflow for {0}".format(uid)
        super(BiowWorkflowException, self).__init__(self.uid, self.code, self.message)
