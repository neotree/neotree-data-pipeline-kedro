def formatError(e):
    trace = {}
    tb = e.__traceback__
    # Stream lining the error to point to the major error and line number
    if tb is not None:

        trace = {
            "filePath": tb.tb_frame.f_code.co_filename,
            "fileName": tb.tb_frame.f_code.co_name,
            "lineNumber": tb.tb_lineno
        }

    return str({
        'type': type(e).__name__,
        'message':  str(e),
        'trace': trace
    })
