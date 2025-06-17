#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Script de vérification des données chargées dans la base de données.
Ce script se connecte à la base PostgreSQL et affiche un échantillon des données chargées
pour vérifier que le pipeline ETL a correctement fonctionné.
"""

import os
import json
import pandas as pd
from sqlalchemy import create_engine, text, inspect, select, func
from sqlalchemy.orm import sessionmaker
from dotenv import load_dotenv
import logging
from job_skills_loader import get_db_connection, inspect_table_structure

# Configuration du logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def get_table_stats(engine):
    """
    Récupère des statistiques sur les tables de la base de données.
    
    Args:
        engine: Le moteur de connexion SQLAlchemy
        
    Returns:
        dict: Statistiques (nombre d'enregistrements) pour chaque table
    """
    try:
        with engine.connect() as conn:
            tables = ['france_travail_jobs', 'skills', 'job_skills']
            stats = {}
            
            for table in tables:
                query = text(f"SELECT COUNT(*) FROM {table}")
                result = conn.execute(query).scalar()
                stats[table] = result
                
            return stats
    except Exception as e:
        logger.error(f"Erreur lors de la récupération des statistiques: {e}")
        return {}

def display_jobs_sample(engine, limit=10):
    """
    Affiche un échantillon des offres d'emploi.
    
    Args:
        engine: Le moteur de connexion SQLAlchemy
        limit (int): Nombre d'offres à afficher
    """
    try:
        query = text(f"SELECT id, job_id, title, entreprise_clean, location, job_type, CAST(publication_date AS TEXT), url, api_source FROM france_travail_jobs LIMIT {limit}")
        
        df = pd.read_sql_query(query, engine)
        if not df.empty:
            print("\n===== ÉCHANTILLON D'OFFRES D'EMPLOI =====")
            for _, row in df.iterrows():
                print(f"\nID: {row['id']}")
                print(f"Job ID: {row['job_id']}")
                print(f"Titre: {row['title']}")
                print(f"Entreprise: {parse_json_field(row['entreprise_clean'])}")
                print(f"Lieu: {row['location']}")
                print(f"Type: {row['job_type']}")
                print(f"Date de publication: {row['publication_date']}")
                print(f"URL: {row['url']}")
                print(f"Source API: {row['api_source']}")
                print("-" * 50)
        else:
            print("Aucune offre d'emploi trouvée dans la base.")
    except Exception as e:
        logger.error(f"Erreur lors de l'affichage des offres: {e}")
        
def display_skills_sample(engine, limit=15):
    """
    Affiche un échantillon des compétences.
    
    Args:
        engine: Le moteur de connexion SQLAlchemy
        limit (int): Nombre de compétences à afficher
    """
    try:
        query = text(f"SELECT id, skill FROM skills LIMIT {limit}")
        
        df = pd.read_sql_query(query, engine)
        if not df.empty:
            print("\n===== ÉCHANTILLON DE COMPÉTENCES =====")
            for _, row in df.iterrows():
                print(f"ID: {row['id']}, Compétence: {row['skill']}")
        else:
            print("Aucune compétence trouvée dans la base.")
    except Exception as e:
        logger.error(f"Erreur lors de l'affichage des compétences: {e}")

def display_job_skills_sample(engine, limit=15):
    """
    Affiche un échantillon des relations offres-compétences.
    
    Args:
        engine: Le moteur de connexion SQLAlchemy
        limit (int): Nombre de relations à afficher
    """
    try:
        query = text(f"""
            SELECT js.id, js.job_id, j.title as job_title, js.skill, js.source
            FROM job_skills js 
            JOIN france_travail_jobs j ON js.job_id = j.job_id 
            LIMIT {limit}
        """)
        
        df = pd.read_sql_query(query, engine)
        if not df.empty:
            print("\n===== ÉCHANTILLON DE RELATIONS OFFRES-COMPÉTENCES =====")
            for _, row in df.iterrows():
                print(f"ID: {row['id']}")
                print(f"Job ID: {row['job_id']}")
                print(f"Titre offre: {row['job_title']}")
                print(f"Compétence: {row['skill']}")
                print(f"Source: {row['source']}")
                print("-" * 50)
        else:
            print("Aucune relation offre-compétence trouvée dans la base.")
    except Exception as e:
        logger.error(f"Erreur lors de l'affichage des relations offres-compétences: {e}")

def parse_json_field(json_str):
    """
    Transforme une chaîne JSON en dictionnaire lisible.
    
    Args:
        json_str: Chaîne JSON à transformer
        
    Returns:
        dict: Dictionnaire correspondant à la chaîne JSON ou la chaîne d'origine si échec
    """
    if isinstance(json_str, str):
        try:
            return json.loads(json_str)
        except:
            return json_str
    return json_str

def display_db_schema(engine):
    """
    Affiche le schéma des tables de la base de données.
    
    Args:
        engine: Le moteur de connexion SQLAlchemy
    """
    try:
        inspector = inspect(engine)
        tables = ['france_travail_jobs', 'skills', 'job_skills']
        
        print("\n===== SCHÉMA DE LA BASE DE DONNÉES =====")
        for table in tables:
            columns = inspector.get_columns(table)
            print(f"\nTable: {table}")
            for col in columns:
                print(f"  - {col['name']}: {col['type']}")
    except Exception as e:
        logger.error(f"Erreur lors de l'affichage du schéma: {e}")

def main():
    """
    Fonction principale qui exécute toutes les vérifications.
    """
    # Affichage du début de l'exécution
    print("===== Début de la vérification de la base de données =====\n")
    
    # Chargement des variables d'environnement
    load_dotenv()
    
    # Afficher les paramètres de connexion (sans mot de passe pour sécurité)
    host = os.environ.get('DB_HOST') or os.environ.get('RDS_HOST')
    database = os.environ.get('DB_NAME') or os.environ.get('RDS_DATABASE')
    user = os.environ.get('DB_USER') or os.environ.get('RDS_USER')
    port = os.environ.get('DB_PORT') or os.environ.get('RDS_PORT')
    print(f"Tentative de connexion à la base de données:\n - Host: {host}\n - Port: {port}\n - Database: {database}\n - User: {user}")
    
    # Récupération de la connexion à la base de données
    engine, session = get_db_connection()
    if engine is None or session is None:
        print("\n\033[91mERREUR: Impossible de se connecter à la base de données\033[0m")
        print("Vérifiez que:\n - La base de données est accessible depuis votre réseau\n - Les informations de connexion dans le fichier .env sont correctes\n - Les variables d'environnement sont bien chargées")
        return
    
    try:
        # Afficher le schéma de la base de données
        display_db_schema(engine)
        
        # Récupérer et afficher les statistiques des tables
        stats = get_table_stats(engine)
        print("\n===== STATISTIQUES DES TABLES =====")
        for table, count in stats.items():
            print(f"{table}: {count} enregistrements")
        
        # Afficher un échantillon des données
        display_jobs_sample(engine)
        display_skills_sample(engine)
        display_job_skills_sample(engine)
        
        logger.info("Vérification des données terminée avec succès")
        
    except Exception as e:
        logger.error(f"Erreur lors de la vérification des données: {e}")
    
    finally:
        # Fermeture de la session
        if session:
            session.close()
        
if __name__ == "__main__":
    main()
