import  sys
def error_message_detailed(error, error_detailed:sys):
    _,_,exc_tb = error_detailed.exc_info()
    filename = exc_tb.tb_frame.f_code.co_filename
    error_message = "error occured in python script name [{0}] line number [{1}] error message [{2}]".format(filename, exc_tb.tb_lineno, str(error))
    return error_message

class CustomMessage(Exception):
    def __init__(self, error_message, error_detailed:sys):
        super().__init__(error_message)
        self.error_message = error_message_detailed(error_message, error_detailed=error_detailed)

    def __str__(self):
        return self.error_message