import logging.handlers
import logging
import sys



def get_module_logger(module_name, level='INFO'):

    module_name = "fin.{}".format(module_name)
    module_logger = logging.getLogger(module_name)
    handler = logging.StreamHandler(sys.stdout)
    handler.formatter = logging.Formatter("%(levelname)s %(message)s")
    module_logger.addHandler(handler)
    module_logger.setLevel(level)
    return module_logger
