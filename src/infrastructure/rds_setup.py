#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Module de configuration et d'interaction avec AWS RDS PostgreSQL.
"""

import os
import logging
import psycopg2
from dotenv import load_dotenv

# Charger les variables d'environnement
load_dotenv()

# Configuration du logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
)
logger = logging.getLogger(__name__)

def get_rds_connection():
    """
    Établit une connexion à la base de données RDS PostgreSQL.
    
    Returns:
        psycopg2.connection: Connexion à la base de données
    """
    try:
        # Récupérer les paramètres de connexion
        host = os.getenv('DB_HOST')
        port = os.getenv('DB_PORT', '5432')
        dbname = os.getenv('DB_NAME')
        user = os.getenv('DB_USER')
        password = os.getenv('DB_PASSWORD')
        
        # Établir la connexion
        connection = psycopg2.connect(
            host=host,
            port=port,
            dbname=dbname,
            user=user,
            password=password
        )
        
        logger.info(f"Connexion établie avec succès à la base de données {dbname} sur {host}")
        return connection
    except Exception as e:
        logger.error(f"Erreur lors de la connexion à la base de données RDS: {e}")
        return None

def create_tables():
    """
    Crée les tables nécessaires dans la base de données.
    
    Returns:
        bool: True si les tables ont été créées avec succès, False sinon
    """
    # Établir la connexion
    connection = get_rds_connection()
    if not connection:
        return False
    
    try:
        # Créer un curseur
        cursor = connection.cursor()
        
        # Créer la table des offres d'emploi
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS jobs (
                id SERIAL PRIMARY KEY,
                title VARCHAR(255) NOT NULL,
                company VARCHAR(255),
                location VARCHAR(255),
                contract_type VARCHAR(50),
                description TEXT,
                url VARCHAR(512) UNIQUE,
                source VARCHAR(50),
                scraped_at TIMESTAMP,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Créer la table des compétences
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS skills (
                id SERIAL PRIMARY KEY,
                name VARCHAR(255) UNIQUE NOT NULL,
                category VARCHAR(50),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Créer la table de liaison entre offres et compétences
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS job_skills (
                job_id INTEGER REFERENCES jobs(id),
                skill_id INTEGER REFERENCES skills(id),
                PRIMARY KEY (job_id, skill_id)
            )
        """)
        
        # Valider les modifications
        connection.commit()
        
        logger.info("Tables créées avec succès dans la base de données")
        return True
    except Exception as e:
        logger.error(f"Erreur lors de la création des tables: {e}")
        connection.rollback()
        return False
    finally:
        # Fermer la connexion
        if connection:
            connection.close()

def test_connection():
    """
    Teste la connexion à la base de données RDS.
    
    Returns:
        bool: True si la connexion a réussi, False sinon
    """
    try:
        # Établir la connexion
        connection = get_rds_connection()
        if not connection:
            return False
        
        # Exécuter une requête simple
        cursor = connection.cursor()
        cursor.execute("SELECT version()")
        version = cursor.fetchone()
        
        # Afficher la version
        logger.info(f"Connexion réussie à PostgreSQL: {version[0]}")
        
        # Fermer la connexion
        connection.close()
        
        return True
    except Exception as e:
        logger.error(f"Erreur lors du test de connexion à RDS: {e}")
        return False

def setup_rds():
    """
    Configure RDS pour le projet.
    
    Returns:
        bool: True si la configuration a réussi, False sinon
    """
    # Tester la connexion
    if not test_connection():
        logger.error("Impossible de se connecter à la base de données RDS")
        return False
    
    # Créer les tables
    if not create_tables():
        logger.error("Impossible de créer les tables dans la base de données RDS")
        return False
    
    logger.info("Configuration RDS terminée avec succès")
    return True

if __name__ == "__main__":
    # Test de configuration RDS
    setup_rds()
