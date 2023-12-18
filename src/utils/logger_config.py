import logging

def get_logger(name):

    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)

    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)

    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    console_handler.setFormatter(formatter)

    # Agregar el manejador de consola al logger
    logger.addHandler(console_handler)

    return logger