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
            Column('intitule', Text),
            Column('description_clean', Text),
            Column('entreprise_clean', Text, nullable=True),
            Column('lieu_travail', Text, nullable=True),
            Column('type_contrat', Text, nullable=True),
            Column('contract_type_std', Text, nullable=True),
            Column('experience_level', Text, nullable=True),
            Column('min_salary', Float, nullable=True),
            Column('max_salary', Float, nullable=True),
            Column('salary_periodicity', Text, nullable=True),
            Column('currency', Text, nullable=True),
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
            Column('source', Text, default='FRANCE_TRAVAIL'),
            Column('extracted_keywords_text', Text, nullable=True)
        )
        
        # Vérifier si la table existe déjà
        inspector = inspect(engine)
        existing_tables = inspector.get_table_names()
        
        if 'france_travail_jobs' not in existing_tables:
            # Créer la table
            metadata.create_all(engine)
            logger.info("Table france_travail_jobs créée avec succès")
            
            # Test d'insertion avec un seul enregistrement
            try:
                test_df = pd.DataFrame({'id': ['test'], 'intitule': ['test'], 'description_clean': ['test'], 'entreprise_clean': ['test'], 'lieu_travail': ['test'], 'type_contrat': ['test'], 'contract_type_std': ['test'], 'experience_level': ['test'], 'min_salary': [0.0], 'max_salary': [0.0], 'salary_periodicity': ['test'], 'currency': ['test'], 'date_creation': [datetime.now()], 'date_actualisation': [datetime.now()], 'keyword_count': [0], 'has_python': [0], 'has_java': [0], 'has_javascript': [0], 'has_sql': [0], 'has_aws': [0], 'has_machine_learning': [0], 'etl_timestamp': [datetime.now()], 'source': ['FRANCE_TRAVAIL'], 'extracted_keywords_text': ['test']})
                test_df.to_sql(name='france_travail_jobs', con=engine, if_exists='append', index=False)
                logger.info("Test d'insertion réussi")
            except Exception as e:
                logger.error(f"Erreur lors du test d'insertion: {e}")
                # Afficher plus de détails sur l'erreur
                logger.error(f"Détails de l'erreur: {str(e)}")
                return False
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
        'etl_timestamp': 'etl_timestamp',
        'extracted_keywords': 'extracted_keywords_text'
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
    if df is None or df.empty:
        logger.error("Impossible de charger les données: DataFrame vide")
        return 0
        
    if engine is None:
        logger.error("Impossible de charger les données: pas de connexion à la base de données")
        return 0
    
    try:
        # Suppression des doublons basés sur l'ID
        df_deduplicated = df.drop_duplicates(subset=['id'])
        if len(df_deduplicated) < len(df):
            logger.warning(f"Suppression de {len(df) - len(df_deduplicated)} doublons basés sur l'ID")
        
        # Vérifier si des colonnes sont dupliquées dans le DataFrame
        duplicate_columns = df_deduplicated.columns[df_deduplicated.columns.duplicated()].tolist()
        if duplicate_columns:
            logger.warning(f"Colonnes dupliquées détectées: {duplicate_columns}")
            # Garder une seule instance de chaque colonne dupliquée
            df_deduplicated = df_deduplicated.loc[:, ~df_deduplicated.columns.duplicated()]
            
        # S'assurer que les colonnes du DataFrame correspondent aux colonnes de la table
        # On doit sélectionner uniquement les colonnes qui existent dans la table
        inspector = inspect(engine)
        columns = inspector.get_columns(table_name)
        table_cols = [col['name'] for col in columns]
        
        logger.info(f"Colonnes de la table {table_name} dans la BDD: {table_cols}")
        logger.info(f"Colonnes du DataFrame: {list(df_deduplicated.columns)}")
        
        common_columns = [col for col in df_deduplicated.columns if col in table_cols]
        if len(common_columns) < len(df_deduplicated.columns):
            missing_cols = set(df_deduplicated.columns) - set(table_cols)
            extra_cols = set(table_cols) - set(df_deduplicated.columns)
            logger.warning(f"Colonnes du DataFrame non présentes dans la table: {missing_cols}")
            logger.warning(f"Colonnes de la table non présentes dans le DataFrame: {extra_cols}")
        # Utiliser seulement les colonnes qui existent dans la table
        filtered_df = df_deduplicated[common_columns].copy()
        logger.info(f"Chargement avec {len(common_columns)} colonnes communes: {common_columns}")
        
        # Vérifier si des enregistrements existent déjà
        with engine.connect() as connection:
            # Obtenir la liste des IDs existants dans la table
            from sqlalchemy import text
            existing_ids_query = text(f"SELECT id FROM {table_name}")
            existing_ids_result = connection.execute(existing_ids_query)
            existing_ids = {row[0] for row in existing_ids_result}
            
            # Filtrer pour ne garder que les nouveaux enregistrements - utiliser .loc pour éviter le warning pandas
            new_records_mask = ~filtered_df['id'].isin(existing_ids)
            filtered_df = filtered_df.loc[new_records_mask, :].copy()
            
            if len(filtered_df) == 0:
                logger.info("Tous les enregistrements existent déjà dans la base de données")
                return 0
            
            logger.info(f"{len(filtered_df)} nouveaux enregistrements à charger")
            
        # Convertir tous les dictionnaires en JSON avant insertion
        import json
        for col in filtered_df.columns:
            if any(isinstance(item, dict) for item in filtered_df[col].dropna().head(100)):
                logger.info(f"Conversion de la colonne {col} de dict à JSON")
                filtered_df[col] = filtered_df[col].apply(lambda x: json.dumps(x) if isinstance(x, dict) else x)
        
        # Vérifier et corriger les types de données pour les colonnes numériques
        for col in ['min_salary', 'max_salary', 'keyword_count', 'has_python', 'has_java', 'has_javascript', 'has_sql', 'has_aws', 'has_machine_learning']:
            if col in filtered_df.columns:
                if filtered_df[col].dtype == 'object':
                    filtered_df.loc[:, col] = pd.to_numeric(filtered_df[col], errors='coerce')
            
        # Charger toutes les données en une seule fois, par lots
        filtered_df.to_sql(
            name=table_name,
            con=engine,
            if_exists='append',
            index=False,
            method='multi',
            chunksize=100  # Charger par lots plus petits (100 au lieu de 500) pour éviter les problèmes
        )
        logger.info(f"Chargement réussi avec {len(filtered_df)} enregistrements")
        
        logger.info(f"Chargement réussi: {len(filtered_df)} offres d'emploi insérées dans {table_name}")
        return len(filtered_df)
    
    except SQLAlchemyError as e:
        logger.error(f"Erreur SQLAlchemy lors du chargement des données: {e}")
        logger.error(f"Détails de l'erreur SQLAlchemy: {str(e.__dict__)}")
        return 0
    except Exception as e:
        logger.error(f"Erreur générale lors du chargement des données: {e}", exc_info=True)
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
