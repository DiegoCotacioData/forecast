import logging

def get_logger(name):
    # Configurar el objeto logger
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)

    # Crear un manejador de consola y establecer el nivel de registro
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)

    # Crear un formato para los mensajes de registro
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    console_handler.setFormatter(formatter)

    # Agregar el manejador de consola al logger
    logger.addHandler(console_handler)

    return logger