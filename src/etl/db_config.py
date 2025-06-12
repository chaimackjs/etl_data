#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Module de configuration de la base de données pour les pipelines ETL.
Gère la connexion à PostgreSQL sur AWS RDS.
"""

import os
import logging
import socket
import time
from sqlalchemy import create_engine, exc
from dotenv import load_dotenv

# Charger les variables d'environnement
load_dotenv()

logger = logging.getLogger(__name__)

# Configuration de la base de données
DB_CONFIG = {
    'host': os.getenv('DB_HOST'),
    'port': os.getenv('DB_PORT'),
    'database': os.getenv('DB_NAME'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD')
}

def is_rds_accessible(host, port, timeout=5):
    """
    Vérifie si le serveur RDS est accessible.
    
    Args:
        host (str): Adresse du serveur RDS
        port (int): Port du serveur
        timeout (int): Délai d'attente maximum en secondes
        
    Returns:
        bool: True si le serveur est accessible, False sinon
    """
    try:
        socket_obj = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        socket_obj.settimeout(timeout)
        result = socket_obj.connect_ex((host, int(port)))
        socket_obj.close()
        return result == 0
    except Exception as e:
        logger.error(f"Erreur lors de la vérification de l'accès au serveur RDS: {e}")
        return False

def get_db_engine(max_retries=3, retry_interval=5):
    """
    Crée et retourne un moteur de connexion SQLAlchemy pour PostgreSQL.
    Inclut une logique de retry et des diagnostics améliorés.
    
    Args:
        max_retries (int): Nombre maximum de tentatives de connexion
        retry_interval (int): Intervalle entre les tentatives en secondes
        
    Returns:
        sqlalchemy.engine.base.Engine: Moteur de connexion SQLAlchemy ou None en cas d'échec
    """
    host = DB_CONFIG['host']
    port = DB_CONFIG['port']
    database = DB_CONFIG['database']
    user = DB_CONFIG['user']
    password = DB_CONFIG['password']
    
    # Vérifier que tous les paramètres nécessaires sont disponibles
    if not all([host, port, database, user, password]):
        missing_params = [param for param in ['host', 'port', 'database', 'user', 'password'] 
                        if not DB_CONFIG[param]]
        logger.error(f"Paramètres de connexion manquants: {', '.join(missing_params)}")
        return None
    
    # Vérifier si le serveur est accessible
    if not is_rds_accessible(host, port):
        logger.error(f"Le serveur RDS à l'adresse {host}:{port} n'est pas accessible")
        logger.warning("Vérifiez que:")
        logger.warning("1. L'instance RDS est démarrée")
        logger.warning("2. Le groupe de sécurité autorise les connexions de votre adresse IP actuelle")
        logger.warning("3. L'instance est configurée pour permettre les connexions publiques")
        return None
    
    # URL de connexion
    conn_str = f"postgresql://{user}:{password}@{host}:{port}/{database}"
    
    # Tentatives de connexion avec retry
    for attempt in range(1, max_retries + 1):
        try:
            logger.info(f"Tentative de connexion à la base de données ({attempt}/{max_retries})...")
            
            # Créer le moteur avec un timeout augmenté
            engine = create_engine(
                conn_str, 
                connect_args={
                    'connect_timeout': 10,
                    'application_name': 'ETL_Pipeline'
                }
            )
            
            # Tester la connexion
            with engine.connect() as connection:
                logger.info(f"Connexion établie avec succès à {database} sur {host}")
                return engine
                
        except exc.SQLAlchemyError as e:
            logger.error(f"Tentative {attempt} échouée: {str(e)}")
            if attempt < max_retries:
                logger.info(f"Nouvelle tentative dans {retry_interval} secondes...")
                time.sleep(retry_interval)
                retry_interval *= 1.5  # Augmenter l'intervalle entre les tentatives
            else:
                logger.error(f"Échec de connexion après {max_retries} tentatives")
                
                # Détailler l'erreur pour faciliter le diagnostic
                error_msg = str(e).lower()
                if "timeout" in error_msg:
                    logger.error("Erreur de timeout: vérifiez que l'instance RDS est accessible depuis votre réseau")
                elif "authentication" in error_msg or "password" in error_msg:
                    logger.error("Erreur d'authentification: vérifiez vos identifiants")
                elif "does not exist" in error_msg:
                    logger.error(f"La base de données '{database}' n'existe pas")
                    
                return None
    
    return None

def test_db_connection():
    """
    Teste la connexion à la base de données et imprime un diagnostic détaillé.
    
    Returns:
        bool: True si la connexion est établie, False sinon
    """
    logger.info("=== Test de connexion à la base de données ===")
    logger.info(f"Host: {DB_CONFIG['host']}")
    logger.info(f"Port: {DB_CONFIG['port']}")
    logger.info(f"Database: {DB_CONFIG['database']}")
    logger.info(f"User: {DB_CONFIG['user']}")
    
    # Vérifier l'accessibilité réseau
    if not is_rds_accessible(DB_CONFIG['host'], DB_CONFIG['port']):
        logger.error("Le serveur RDS n'est pas accessible au niveau réseau")
        return False
    
    # Tester la connexion à la base de données
    engine = get_db_engine()
    if engine is not None:
        # Vérifier que les tables nécessaires existent
        try:
            from sqlalchemy import inspect
            inspector = inspect(engine)
            tables = inspector.get_table_names()
            logger.info(f"Tables disponibles dans la base de données: {tables}")
            
            required_tables = ['france_travail_jobs', 'welcome_jungle_jobs']
            missing_tables = [table for table in required_tables if table not in tables]
            
            if missing_tables:
                logger.warning(f"Tables nécessaires non trouvées: {missing_tables}")
                logger.info("Les tables manquantes seront créées lors de l'exécution du pipeline ETL")
            
            return True
        except Exception as e:
            logger.error(f"Erreur lors de la vérification des tables: {e}")
            return False
    else:
        return False

if __name__ == "__main__":
    # Configuration du logging pour les tests
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[logging.StreamHandler()]
    )
    
    # Tester la connexion à la base de données
    test_db_connection()
