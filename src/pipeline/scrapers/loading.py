#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Module de chargement des données Welcome to the Jungle dans la base de données PostgreSQL.
"""

import os
import pandas as pd
import logging
from datetime import datetime
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

# Configuration du logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Import des modules d'extraction et de transformation
from src.pipeline.scrapers.extraction import extract_welcome_jungle_data
from src.pipeline.scrapers.transformation import transform_welcome_jungle_data, save_transformed_data

def get_db_connection():
    """
    Établit une connexion à la base de données PostgreSQL.
    
    Returns:
        sqlalchemy.engine.Engine: Objet de connexion à la base de données
    """
    try:
        # Charger les variables d'environnement si ce n'est pas déjà fait
        from src.pipeline.api.dotenv_utils import load_dotenv
        load_dotenv()
        
        # Récupérer les paramètres de connexion depuis les variables d'environnement
        db_host = os.environ.get('DB_HOST')
        db_port = os.environ.get('DB_PORT')
        db_name = os.environ.get('DB_NAME')
        db_user = os.environ.get('DB_USER')
        db_password = os.environ.get('DB_PASSWORD')
        
        # Vérifier que toutes les variables sont définies
        if not all([db_host, db_port, db_name, db_user, db_password]):
            logger.error("Variables d'environnement de connexion à la base de données manquantes")
            return None
        
        # Créer l'URL de connexion
        db_url = f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
        
        # Créer le moteur SQLAlchemy
        engine = create_engine(db_url)
        
        # Tester la connexion
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
            logger.info("Connexion à la base de données établie avec succès")
        
        return engine
        
    except SQLAlchemyError as e:
        logger.error(f"Erreur lors de la connexion à la base de données: {e}")
        return None
    except Exception as e:
        logger.error(f"Erreur inattendue lors de la connexion à la base de données: {e}")
        return None

def create_welcome_jungle_table(engine):
    """
    Crée la table welcome_jungle_jobs si elle n'existe pas.
    
    Args:
        engine (sqlalchemy.engine.Engine): Objet de connexion à la base de données
        
    Returns:
        bool: True si la table a été créée ou existe déjà, False sinon
    """
    try:
        # Définition du schéma de la table
        create_table_query = """
        CREATE TABLE IF NOT EXISTS welcome_jungle_jobs (
            id TEXT PRIMARY KEY,
            title TEXT,
            company_name TEXT,
            location TEXT,
            city TEXT,
            country TEXT,
            contract_type TEXT,
            contract_type_std TEXT,
            date DATE,
            url TEXT,
            description TEXT,
            category TEXT,
            experience TEXT,
            min_experience INTEGER,
            max_experience INTEGER,
            experience_level TEXT,
            salary TEXT,
            min_salary FLOAT,
            max_salary FLOAT,
            salary_currency TEXT,
            salary_period TEXT,
            source TEXT,
            processing_date DATE,
            has_python BOOLEAN DEFAULT FALSE,
            has_java BOOLEAN DEFAULT FALSE,
            has_javascript BOOLEAN DEFAULT FALSE,
            has_react BOOLEAN DEFAULT FALSE,
            has_angular BOOLEAN DEFAULT FALSE,
            has_vue BOOLEAN DEFAULT FALSE,
            has_node BOOLEAN DEFAULT FALSE,
            has_php BOOLEAN DEFAULT FALSE,
            has_sql BOOLEAN DEFAULT FALSE,
            has_nosql BOOLEAN DEFAULT FALSE,
            has_mongodb BOOLEAN DEFAULT FALSE,
            has_postgresql BOOLEAN DEFAULT FALSE,
            has_mysql BOOLEAN DEFAULT FALSE,
            has_aws BOOLEAN DEFAULT FALSE,
            has_azure BOOLEAN DEFAULT FALSE,
            has_gcp BOOLEAN DEFAULT FALSE,
            has_docker BOOLEAN DEFAULT FALSE,
            has_kubernetes BOOLEAN DEFAULT FALSE,
            has_devops BOOLEAN DEFAULT FALSE,
            has_machine_learning BOOLEAN DEFAULT FALSE,
            has_data_science BOOLEAN DEFAULT FALSE,
            has_ai BOOLEAN DEFAULT FALSE
        );
        """
        
        # Exécuter la requête
        with engine.connect() as conn:
            conn.execute(text(create_table_query))
            conn.commit()
            
        logger.info("Table welcome_jungle_jobs créée ou déjà existante")
        return True
        
    except SQLAlchemyError as e:
        logger.error(f"Erreur lors de la création de la table welcome_jungle_jobs: {e}")
        return False
    except Exception as e:
        logger.error(f"Erreur inattendue lors de la création de la table: {e}")
        return False

def load_welcome_jungle_data(df, engine):
    """
    Charge les données Welcome to the Jungle dans la base de données.
    
    Args:
        df (pandas.DataFrame): DataFrame contenant les données transformées
        engine (sqlalchemy.engine.Engine): Objet de connexion à la base de données
        
    Returns:
        bool: True si le chargement a réussi, False sinon
    """
    try:
        if df.empty:
            logger.warning("DataFrame vide, aucun chargement effectué")
            return False
        
        if not create_welcome_jungle_table(engine):
            logger.error("Impossible de créer la table welcome_jungle_jobs")
            return False
        
        # Vérifier le schéma réel de la table
        try:
            with engine.connect() as conn:
                # Récupérer les noms de colonnes de la table existante
                result = conn.execute(text("SELECT column_name FROM information_schema.columns WHERE table_name = 'welcome_jungle_jobs'"))
                existing_columns = [row[0] for row in result]
                logger.info(f"Colonnes existantes dans la table: {existing_columns}")
                
                # Vérifier si 'company_name' existe et 'company' n'existe pas
                if 'company_name' in existing_columns and 'company' not in existing_columns:
                    if 'company' in df.columns:
                        df = df.rename(columns={'company': 'company_name'})
                        logger.info("Colonne 'company' renommée en 'company_name'")
                
                # Adapter d'autres colonnes si nécessaire
                # ...
        except Exception as e:
            logger.warning(f"Impossible de vérifier le schéma de la table: {e}")
        
        # S'assurer que toutes les colonnes du DataFrame correspondent au schéma de la table
        df_columns = set(df.columns)
        logger.info(f"Colonnes du DataFrame: {df_columns}")
        
        # Remplacer les données existantes au lieu de les ajouter pour éviter les erreurs de clé en double
        df.to_sql('welcome_jungle_jobs', engine, if_exists='replace', index=False)
        
        logger.info(f"Données chargées avec succès: {len(df)} enregistrements")
        return True
        
    except SQLAlchemyError as e:
        logger.error(f"Erreur SQL lors du chargement des données: {e}")
        return False
    except Exception as e:
        logger.error(f"Erreur inattendue lors du chargement des données: {e}")
        return False

def check_table_exists(engine, table_name):
    """
    Vérifie si une table existe dans la base de données.
    
    Args:
        engine (sqlalchemy.engine.Engine): Objet de connexion à la base de données
        table_name (str): Nom de la table à vérifier
        
    Returns:
        bool: True si la table existe, False sinon
    """
    try:
        query = text(f"""
        SELECT EXISTS (
            SELECT FROM information_schema.tables 
            WHERE table_name = '{table_name}'
        );
        """)
        
        with engine.connect() as conn:
            result = conn.execute(query).scalar()
            
        return result
        
    except Exception as e:
        logger.error(f"Erreur lors de la vérification de l'existence de la table {table_name}: {e}")
        return False

def check_data_exists(engine, table_name):
    """
    Vérifie si des données existent déjà dans la table.
    
    Args:
        engine (sqlalchemy.engine.Engine): Objet de connexion à la base de données
        table_name (str): Nom de la table à vérifier
        
    Returns:
        bool: True si des données existent, False sinon
    """
    try:
        if not check_table_exists(engine, table_name):
            return False
            
        query = text(f"SELECT COUNT(*) FROM {table_name}")
        
        with engine.connect() as conn:
            count = conn.execute(query).scalar()
            
        return count > 0
        
    except Exception as e:
        logger.error(f"Erreur lors de la vérification des données dans la table {table_name}: {e}")
        return False

def run_welcome_jungle_etl(force=False, specific_file=None, all_files=True):
    """
    Exécute le pipeline ETL complet pour Welcome to the Jungle.
    
    Args:
        force (bool): Force l'exécution même si des données existent déjà
        specific_file (str): Fichier spécifique à traiter (optionnel)
        all_files (bool): Si True, extrait tous les fichiers disponibles au lieu du plus récent uniquement
        
    Returns:
        bool: True si le pipeline a réussi, False sinon
    """
    try:
        # Établir la connexion à la base de données
        engine = get_db_connection()
        if not engine:
            logger.error("Impossible de se connecter à la base de données")
            return False
        
        # Vérifier si des données existent déjà
        if not force and check_data_exists(engine, 'welcome_jungle_jobs'):
            logger.info("Des données Welcome to the Jungle existent déjà dans la base de données")
            logger.info("Utilisez l'option --force pour forcer l'exécution du pipeline")
            return True
        
        # Extraction
        logger.info("Début de l'étape d'extraction")
        df, local_file = extract_welcome_jungle_data(specific_file=specific_file, all_files=all_files)
        
        if df.empty:
            logger.error("Aucune donnée extraite")
            return False
            
        logger.info(f"Extraction terminée: {len(df)} enregistrements")
        
        # Transformation
        logger.info("Début de l'étape de transformation")
        transformed_df = transform_welcome_jungle_data(df)
        
        if transformed_df.empty:
            logger.error("Erreur lors de la transformation des données")
            return False
            
        logger.info(f"Transformation terminée: {len(transformed_df)} enregistrements")
        
        # Sauvegarde des données transformées (optionnel)
        output_file = save_transformed_data(transformed_df)
        
        # Chargement
        logger.info("Début de l'étape de chargement")
        success = load_welcome_jungle_data(transformed_df, engine)
        
        if success:
            logger.info("Pipeline ETL Welcome to the Jungle terminé avec succès")
            return True
        else:
            logger.error("Erreur lors du chargement des données")
            return False
            
    except Exception as e:
        logger.error(f"Erreur lors de l'exécution du pipeline ETL Welcome to the Jungle: {e}")
        return False

def main():
    """
    Fonction principale pour tester le pipeline ETL.
    """
    import argparse
    
    parser = argparse.ArgumentParser(description="Pipeline ETL Welcome to the Jungle")
    parser.add_argument("--force", action="store_true", help="Force l'exécution même si des données existent déjà")
    parser.add_argument("--file", type=str, help="Fichier spécifique à traiter")
    
    args = parser.parse_args()
    
    success = run_welcome_jungle_etl(force=args.force, specific_file=args.file)
    
    if success:
        logger.info("Pipeline ETL Welcome to the Jungle terminé avec succès")
    else:
        logger.error("Erreur lors de l'exécution du pipeline ETL Welcome to the Jungle")

if __name__ == "__main__":
    main()