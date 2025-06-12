# utils/logger.py
import logging
from src.utils.config import LOG_LEVEL

def get_logger(name: str) -> logging.Logger:
    """
    Crée et retourne un logger configuré avec le niveau de log défini dans config.py.
    
    :param name: Nom du logger.
    :return: Instance de logging.Logger.
    """
    logger = logging.getLogger(name)
    
    # Si aucun handler n'est configuré, on en crée un pour éviter les doublons
    if not logger.handlers:
        # Définir le niveau de log
        logger.setLevel(getattr(logging, LOG_LEVEL.upper(), logging.INFO))
        
        # Création d'un handler de console
        console_handler = logging.StreamHandler()
        console_handler.setLevel(getattr(logging, LOG_LEVEL.upper(), logging.INFO))
        
        # Définir un formatteur simple
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        console_handler.setFormatter(formatter)
        
        logger.addHandler(console_handler)
    
    return logger
