#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Script d'initialisation de la base de données pour le pipeline ETL.
Crée les tables nécessaires dans la base de données PostgreSQL si elles n'existent pas.
"""

import os
import sys
import logging
import argparse
from datetime import datetime
from sqlalchemy import (
    MetaData, Table, Column, Integer, String, 
    Float, Text, DateTime, Boolean, ForeignKey, create_engine, inspect
)

# Configurer le logging
os.makedirs('logs', exist_ok=True)

log_file = f"logs/db_init_{datetime.now().strftime('%Y%m%d')}.log"

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Ajouter le chemin du projet au PYTHONPATH
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from etl.db_config import get_db_engine, test_db_connection

def create_france_travail_table(engine):
    """
    Crée la table France Travail si elle n'existe pas.
    
    Args:
        engine: Moteur de connexion SQLAlchemy
        
    Returns:
        bool: True si la table existe ou a été créée avec succès, False sinon
    """
    try:
        metadata = MetaData()
        
        # Définir la structure de la table France Travail
        france_travail_jobs = Table(
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
        if 'france_travail_jobs' in inspector.get_table_names():
            logger.info("La table france_travail_jobs existe déjà")
            return True
        
        # Créer la table
        metadata.create_all(engine)
        logger.info("Table france_travail_jobs créée avec succès")
        return True
    
    except Exception as e:
        logger.error(f"Erreur lors de la création de la table france_travail_jobs: {e}")
        return False

def create_welcome_jungle_table(engine):
    """
    Crée la table Welcome to the Jungle si elle n'existe pas.
    
    Args:
        engine: Moteur de connexion SQLAlchemy
        
    Returns:
        bool: True si la table existe ou a été créée avec succès, False sinon
    """
    try:
        metadata = MetaData()
        
        # Définir la structure de la table Welcome to the Jungle
        welcome_jungle_jobs = Table(
            'welcome_jungle_jobs', 
            metadata,
            Column('id', String(100), primary_key=True),
            Column('title', String(255)),
            Column('company', String(255)),
            Column('location', String(255)),
            Column('contract_type', String(50)),
            Column('url', String(512)),
            Column('description_clean', Text),
            Column('min_salary', Float, nullable=True),
            Column('max_salary', Float, nullable=True),
            Column('salary_periodicity', String(20), nullable=True),
            Column('currency', String(5), nullable=True),
            Column('remote_policy', String(50), nullable=True),
            Column('experience_level', String(20), nullable=True),
            Column('published_at', DateTime, nullable=True),
            Column('scrape_date', DateTime),
            Column('keyword_count', Integer, nullable=True),
            Column('has_python', Integer, nullable=True),
            Column('has_java', Integer, nullable=True),
            Column('has_javascript', Integer, nullable=True),
            Column('has_sql', Integer, nullable=True),
            Column('has_aws', Integer, nullable=True),
            Column('has_machine_learning', Integer, nullable=True),
            Column('etl_timestamp', DateTime),
            Column('source', String(50), default='WELCOME_JUNGLE')
        )
        
        # Vérifier si la table existe déjà
        inspector = inspect(engine)
        if 'welcome_jungle_jobs' in inspector.get_table_names():
            logger.info("La table welcome_jungle_jobs existe déjà")
            return True
        
        # Créer la table
        metadata.create_all(engine)
        logger.info("Table welcome_jungle_jobs créée avec succès")
        return True
    
    except Exception as e:
        logger.error(f"Erreur lors de la création de la table welcome_jungle_jobs: {e}")
        return False

def create_skills_table(engine):
    """
    Crée la table des compétences si elle n'existe pas.
    
    Args:
        engine: Moteur de connexion SQLAlchemy
        
    Returns:
        bool: True si la table existe ou a été créée avec succès, False sinon
    """
    try:
        metadata = MetaData()
        
        # Définir la structure de la table des compétences
        skills = Table(
            'job_skills', 
            metadata,
            Column('id', Integer, primary_key=True),
            Column('job_id', String(100), nullable=False),
            Column('source', String(50), nullable=False),
            Column('skill', String(100), nullable=False),
            Column('etl_timestamp', DateTime)
        )
        
        # Vérifier si la table existe déjà
        inspector = inspect(engine)
        if 'job_skills' in inspector.get_table_names():
            logger.info("La table job_skills existe déjà")
            return True
        
        # Créer la table
        metadata.create_all(engine)
        logger.info("Table job_skills créée avec succès")
        return True
    
    except Exception as e:
        logger.error(f"Erreur lors de la création de la table job_skills: {e}")
        return False

def initialize_database():
    """
    Initialise la base de données en créant toutes les tables nécessaires.
    
    Returns:
        bool: True si toutes les tables ont été créées avec succès, False sinon
    """
    logger.info("=== Initialisation de la base de données ===")
    
    # Tester la connexion à la base de données
    if not test_db_connection():
        logger.error("Impossible d'initialiser la base de données: problème de connexion")
        return False
    
    # Obtenir le moteur de connexion
    engine = get_db_engine()
    if engine is None:
        logger.error("Échec de la création du moteur de connexion")
        return False
    
    # Créer les tables
    success = True
    success = create_france_travail_table(engine) and success
    success = create_welcome_jungle_table(engine) and success
    success = create_skills_table(engine) and success
    
    if success:
        logger.info("Initialisation de la base de données réussie")
    else:
        logger.error("Des erreurs sont survenues lors de l'initialisation de la base de données")
    
    return success

if __name__ == "__main__":
    # Créer le dossier de logs s'il n'existe pas
    os.makedirs('logs', exist_ok=True)
    
    # Définir les arguments de la ligne de commande
    parser = argparse.ArgumentParser(description="Initialisation de la base de données pour le pipeline ETL")
    parser.add_argument('--test-only', action='store_true', help="Tester uniquement la connexion sans créer les tables")
    
    args = parser.parse_args()
    
    if args.test_only:
        test_db_connection()
    else:
        initialize_database()
