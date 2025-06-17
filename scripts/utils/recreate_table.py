#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Script pour recréer la table france_travail_jobs avec la nouvelle structure.
Ce script supprime la table existante et la recrée avec des colonnes de type Text.
"""

import os
import sys
import logging
from datetime import datetime
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

# Ajouter le répertoire parent au path pour pouvoir importer les modules
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Configuration du logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f"logs/recreate_table_{datetime.now().strftime('%Y%m%d')}.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def setup_environment():
    """Configure les variables d'environnement pour la connexion à la base de données."""
    # Charger les variables d'environnement depuis le fichier .env
    from etl.api.dotenv_utils import load_dotenv
    load_dotenv()
    
    # Vérifier que les variables essentielles sont définies
    required_vars = ['DB_HOST', 'DB_PORT', 'DB_NAME', 'DB_USER', 'DB_PASSWORD']
    
    missing_vars = [var for var in required_vars if not os.getenv(var)]
    if missing_vars:
        logger.error(f"Variables d'environnement manquantes: {', '.join(missing_vars)}")
        logger.error("Veuillez vérifier votre fichier .env")
        return False
    
    logger.info("Variables d'environnement configurées")
    return True

def get_db_connection():
    """
    Établit une connexion à la base de données PostgreSQL.
    
    Returns:
        sqlalchemy.engine.base.Engine: Moteur de connexion SQLAlchemy
    """
    try:
        # Récupérer les paramètres de connexion depuis les variables d'environnement
        host = os.getenv('DB_HOST')
        port = os.getenv('DB_PORT')
        database = os.getenv('DB_NAME')
        user = os.getenv('DB_USER')
        password = os.getenv('DB_PASSWORD')

        # Créer l'URL de connexion
        conn_str = f"postgresql://{user}:{password}@{host}:{port}/{database}"
        
        # Créer le moteur avec un timeout augmenté pour tenir compte des latences réseau
        engine = create_engine(conn_str, connect_args={'connect_timeout': 30})
        
        # Tester la connexion
        with engine.connect() as connection:
            logger.info(f"Connexion établie avec succès à la base de données {database} sur {host}")
            
        return engine
    except SQLAlchemyError as e:
        logger.error(f"Erreur de connexion à la base de données: {e}")
        return None

def recreate_table(engine):
    """
    Supprime et recrée la table france_travail_jobs.
    
    Args:
        engine: Moteur de connexion SQLAlchemy
        
    Returns:
        bool: True si la table a été recréée avec succès, False sinon
    """
    if engine is None:
        logger.error("Impossible de recréer la table: pas de connexion à la base de données")
        return False
    
    try:
        with engine.connect() as connection:
            # Vérifier si la table existe
            check_query = text("SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'france_travail_jobs')")
            result = connection.execute(check_query).fetchone()
            
            if result[0]:
                # Supprimer la table existante
                logger.info("Suppression de la table france_travail_jobs existante...")
                drop_query = text("DROP TABLE france_travail_jobs")
                connection.execute(drop_query)
                connection.commit()
                logger.info("Table france_travail_jobs supprimée avec succès")
            
            # Créer la nouvelle table avec des colonnes de type Text
            create_query = text("""
            CREATE TABLE france_travail_jobs (
                id VARCHAR(100) PRIMARY KEY,
                intitule TEXT,
                description_clean TEXT,
                entreprise_clean TEXT,
                lieu_travail TEXT,
                type_contrat TEXT,
                contract_type_std TEXT,
                experience_level TEXT,
                min_salary FLOAT,
                max_salary FLOAT,
                salary_periodicity TEXT,
                currency TEXT,
                date_creation TIMESTAMP,
                date_actualisation TIMESTAMP,
                keyword_count INTEGER,
                has_python INTEGER,
                has_java INTEGER,
                has_javascript INTEGER,
                has_sql INTEGER,
                has_aws INTEGER,
                has_machine_learning INTEGER,
                etl_timestamp TIMESTAMP,
                source TEXT,
                extracted_keywords_text TEXT
            )
            """)
            
            connection.execute(create_query)
            connection.commit()
            logger.info("Table france_travail_jobs créée avec succès")
            
            return True
    except SQLAlchemyError as e:
        logger.error(f"Erreur SQLAlchemy lors de la recréation de la table: {e}")
        return False
    except Exception as e:
        logger.error(f"Erreur générale lors de la recréation de la table: {e}", exc_info=True)
        return False

def main():
    """Fonction principale."""
    print("=== Recréation de la table france_travail_jobs ===\n")
    
    # 1. Configuration des variables d'environnement
    print("1. Configuration des variables d'environnement...")
    setup_environment()
    
    # 2. Connexion à la base de données
    print("2. Connexion à la base de données...")
    engine = get_db_connection()
    
    if engine is None:
        print("   - Échec de la connexion à la base de données")
        return
    
    print("   - Connexion à la base de données établie avec succès")
    
    # 3. Recréation de la table
    print("\n3. Recréation de la table france_travail_jobs...")
    success = recreate_table(engine)
    
    if success:
        print("   - Table france_travail_jobs recréée avec succès")
    else:
        print("   - Échec de la recréation de la table france_travail_jobs")
    
    print("\n=== Recréation de la table terminée ===")

if __name__ == "__main__":
    main()
