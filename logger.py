import logging


logger = logging.getLogger('ScriptsLibraryLogger_logger')
logging.basicConfig(format='[%(asctime)s] - %(levelname)s %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
logger.setLevel(logging.INFO)
