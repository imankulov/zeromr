import logging
logger = logging.getLogger(__name__)
# todo: move the code in scripts
logger.setLevel('DEBUG')
logger.addHandler(logging.StreamHandler())
