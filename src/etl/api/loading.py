#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Module de chargement des données pour l'API France Travail.
Gère le chargement des données transformées dans la base de données PostgreSQL.
"""

import os
import sys
import logging
import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine, Column, String, Integer, Float, Text, DateTime, MetaData, Table, inspect
from sqlalchemy.exc import SQLAlchemyError
import psycopg2

# Configuration du logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f"logs/etl_loading_{datetime.now().strftime('%Y%m%d')}.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

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
        logger.warning("Vérifiez que l'instance RDS est accessible depuis votre réseau")
        logger.warning("La base de données doit être configurée pour permettre l'accès public ou via un VPN")
        return None

def create_jobs_table(engine):
    """
    Crée la table des offres d'emploi si elle n'existe pas déjà.
    
    Args:
        engine: Moteur de connexion SQLAlchemy
        
    Returns:
        bool: True si la table existe ou a été créée avec succès, False sinon
    """
    if engine is None:
        logger.error("Impossible de créer la table: pas de connexion à la base de données")
        return False
    
    try:
        metadata = MetaData()
        
        # Définition du schéma de la table
        jobs_table = Table(
            'france_travail_jobs', 
            metadata,
            Column('id', String(100), primary_key=True),
            Column('intitule', String(255)),
            Column('description_clean', Text),
            Column('entreprise_clean', String(255), nullable=True),
            Column('lieu_travail', String(255), nullable=True),
            Column('type_contrat', String(50), nullable=True),
            Column('contract_type_std', String(50), nullable=True),
            Column('experience_level', String(20), nullable=True),
            Column('min_salary', Float, nullable=True),
            Column('max_salary', Float, nullable=True),
            Column('salary_periodicity', String(20), nullable=True),
            Column('currency', String(5), nullable=True),
            Column('date_creation', DateTime, nullable=True),
            Column('date_actualisation', DateTime, nullable=True),
            Column('keyword_count', Integer, nullable=True),
            Column('has_python', Integer, nullable=True),
            Column('has_java', Integer, nullable=True),
            Column('has_javascript', Integer, nullable=True),
            Column('has_sql', Integer, nullable=True),
            Column('has_aws', Integer, nullable=True),
            Column('has_machine_learning', Integer, nullable=True),
            Column('etl_timestamp', DateTime),
            Column('source', String(50), default='FRANCE_TRAVAIL')
        )
        
        # Vérifier si la table existe déjà
        inspector = inspect(engine)
        existing_tables = inspector.get_table_names()
        
        if 'france_travail_jobs' not in existing_tables:
            # Créer la table
            metadata.create_all(engine)
            logger.info("Table france_travail_jobs créée avec succès")
        else:
            logger.info("Table france_travail_jobs existe déjà")
            
        return True
        
    except SQLAlchemyError as e:
        logger.error(f"Erreur lors de la création de la table: {e}")
        return False

def prepare_job_data_for_loading(df):
    """
    Prépare le DataFrame d'offres d'emploi pour le chargement dans la base de données.
    
    Args:
        df (pandas.DataFrame): DataFrame transformé d'offres d'emploi
        
    Returns:
        pandas.DataFrame: DataFrame prêt pour le chargement
    """
    if df is None or len(df) == 0:
        logger.warning("DataFrame vide, aucune préparation effectuée")
        return None
    
    # Créer un DataFrame avec les colonnes requises pour la table
    logger.info("Préparation des données pour le chargement dans la base de données")
    
    # Colonnes à conserver et renommer si nécessaire
    column_mapping = {
        'id': 'id',
        'intitule': 'intitule',
        'description_clean': 'description_clean',
        'entreprise_clean': 'entreprise_clean',
        'lieuTravail': 'lieu_travail',
        'typeContrat': 'type_contrat',
        'contract_type_std': 'contract_type_std',
        'experience_level': 'experience_level',
        'min_salary': 'min_salary',
        'max_salary': 'max_salary',
        'salary_periodicity': 'salary_periodicity',
        'currency': 'currency',
        'dateCreation_iso': 'date_creation',
        'dateActualisation_iso': 'date_actualisation',
        'keyword_count': 'keyword_count',
        'has_python': 'has_python',
        'has_java': 'has_java',
        'has_javascript': 'has_javascript',
        'has_sql': 'has_sql',
        'has_aws': 'has_aws',
        'has_machine_learning': 'has_machine_learning',
        'etl_timestamp': 'etl_timestamp'
    }
    
    # Créer un nouveau DataFrame avec les colonnes mappées
    columns_to_keep = [col for col in column_mapping.keys() if col in df.columns]
    if len(columns_to_keep) == 0:
        logger.error("Aucune colonne requise trouvée dans le DataFrame")
        return None
    
    # Sélectionner et renommer les colonnes
    result_df = df[columns_to_keep].copy()
    result_df.columns = [column_mapping[col] for col in columns_to_keep]
    
    # Ajouter les colonnes manquantes avec des valeurs NULL
    for target_col in column_mapping.values():
        if target_col not in result_df.columns:
            result_df[target_col] = None
    
    # Ajouter la source
    result_df['source'] = 'FRANCE_TRAVAIL'
    
    # Convertir les dates en format datetime
    date_columns = ['date_creation', 'date_actualisation', 'etl_timestamp']
    for col in date_columns:
        if col in result_df.columns:
            result_df[col] = pd.to_datetime(result_df[col], errors='coerce')
    
    logger.info(f"Données préparées: {len(result_df)} offres d'emploi prêtes à être chargées")
    return result_df

def load_jobs_to_database(df, engine, table_name='france_travail_jobs'):
    """
    Charge les données d'offres d'emploi dans la base de données.
    
    Args:
        df (pandas.DataFrame): DataFrame prêt pour le chargement
        engine: Moteur de connexion SQLAlchemy
        table_name (str): Nom de la table cible
        
    Returns:
        int: Nombre d'enregistrements chargés
    """
    if df is None or engine is None:
        logger.error("Impossible de charger les données: DataFrame vide ou pas de connexion à la base de données")
        return 0
    
    try:
        # Chargement des données avec gestion des doublons (update si l'ID existe déjà)
        df.to_sql(
            name=table_name,
            con=engine,
            if_exists='append',
            index=False,
            method='multi',
            chunksize=500  # Charger par lots de 500 enregistrements
        )
        
        logger.info(f"Chargement réussi: {len(df)} offres d'emploi insérées dans {table_name}")
        return len(df)
    
    except SQLAlchemyError as e:
        logger.error(f"Erreur lors du chargement des données: {e}")
        return 0

def execute_etl_pipeline(start_date=None, end_date=None):
    """
    Exécute le pipeline ETL complet pour les offres d'emploi France Travail.
    
    Args:
        start_date (str): Date de début au format YYYYMMDD
        end_date (str): Date de fin au format YYYYMMDD
        
    Returns:
        int: Nombre d'enregistrements chargés dans la base de données
    """
    # Importer les modules d'extraction et de transformation
    sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    from api.extraction import extract_by_date_range
    from api.transformation import transform_job_dataframe, apply_keyword_analysis
    
    # Utiliser la date du jour si aucune date n'est fournie
    if not start_date:
        start_date = datetime.now().strftime("%Y%m%d")
    
    # Étape 1: Extraction
    logger.info(f"Début du pipeline ETL pour les offres France Travail du {start_date} au {end_date or 'aujourd\'hui'}")
    raw_df = extract_by_date_range(start_date, end_date)
    
    if raw_df is None:
        logger.warning("Aucune donnée extraite, fin du pipeline")
        return 0
    
    # Étape 2: Transformation
    transformed_df = transform_job_dataframe(raw_df)
    if transformed_df is not None:
        transformed_df = apply_keyword_analysis(transformed_df)
    
    if transformed_df is None:
        logger.warning("Échec de la transformation, fin du pipeline")
        return 0
    
    # Étape 3: Préparation pour le chargement
    load_ready_df = prepare_job_data_for_loading(transformed_df)
    
    if load_ready_df is None:
        logger.warning("Échec de la préparation des données, fin du pipeline")
        return 0
    
    # Étape 4: Connexion à la base de données
    engine = get_db_connection()
    
    if engine is None:
        logger.error("Impossible de se connecter à la base de données, fin du pipeline")
        return 0
    
    # Étape 5: Création de la table si nécessaire
    table_exists = create_jobs_table(engine)
    
    if not table_exists:
        logger.error("Impossible de créer la table des offres d'emploi, fin du pipeline")
        return 0
    
    # Étape 6: Chargement des données
    records_loaded = load_jobs_to_database(load_ready_df, engine)
    
    logger.info(f"Pipeline ETL terminé: {records_loaded} offres d'emploi chargées dans la base de données")
    return records_loaded

if __name__ == "__main__":
    # Paramètres de ligne de commande
    import argparse
    
    parser = argparse.ArgumentParser(description="Pipeline ETL pour les offres d'emploi France Travail")
    parser.add_argument('--start-date', type=str, help="Date de début (YYYYMMDD)")
    parser.add_argument('--end-date', type=str, help="Date de fin (YYYYMMDD)")
    
    args = parser.parse_args()
    
    # Exécuter le pipeline
    execute_etl_pipeline(args.start_date, args.end_date)
