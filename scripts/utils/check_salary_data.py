#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Script pour vérifier la disponibilité et la structure des données de salaire.
"""

import os
import sys
import pandas as pd
import sqlalchemy
from sqlalchemy import text
import logging

# Configurer le logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Ajouter le chemin du projet au PYTHONPATH
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

# Importer l'utilitaire de chargement des variables d'environnement
from src.pipeline.api.dotenv_utils import load_dotenv

def main():
    """
    Fonction principale pour vérifier les données de salaire.
    """
    # Charger les variables d'environnement
    load_dotenv()
    
    # Récupérer les informations de connexion depuis les variables d'environnement
    db_host = os.getenv('DB_HOST')
    db_port = os.getenv('DB_PORT')
    db_name = os.getenv('DB_NAME')
    db_user = os.getenv('DB_USER')
    db_password = os.getenv('DB_PASSWORD')
    
    # Construire l'URL de connexion
    db_url = f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
    
    try:
        # Créer le moteur de connexion
        engine = sqlalchemy.create_engine(db_url)
        
        # Récupérer la liste des colonnes de la table france_travail_jobs
        query = "SELECT column_name FROM information_schema.columns WHERE table_name = 'france_travail_jobs'"
        columns = pd.read_sql(query, engine)
        
        logger.info("Colonnes disponibles dans la table france_travail_jobs:")
        for col in columns['column_name']:
            logger.info(f"- {col}")
        
        # Vérifier si des colonnes liées au salaire existent
        salary_columns = [col for col in columns['column_name'] if 'salaire' in col.lower() or 'remuneration' in col.lower()]
        
        if salary_columns:
            logger.info(f"\nColonnes liées au salaire trouvées: {salary_columns}")
            
            # Examiner la structure des données de salaire
            for col in salary_columns:
                logger.info(f"\nExamen de la colonne {col}:")
                
                # Récupérer un échantillon de données
                sample_query = f"SELECT {col} FROM france_travail_jobs WHERE {col} IS NOT NULL LIMIT 10"
                sample_data = pd.read_sql(sample_query, engine)
                
                logger.info(f"Échantillon de données ({len(sample_data)} lignes):")
                for i, value in enumerate(sample_data[col]):
                    logger.info(f"{i+1}: {value}")
                
                # Calculer le pourcentage de valeurs non nulles
                count_query = f"SELECT COUNT(*) as total, COUNT({col}) as non_null FROM france_travail_jobs"
                count_data = pd.read_sql(count_query, engine)
                
                total = count_data['total'][0]
                non_null = count_data['non_null'][0]
                percentage = (non_null / total) * 100 if total > 0 else 0
                
                logger.info(f"Statistiques de la colonne {col}:")
                logger.info(f"- Total de lignes: {total}")
                logger.info(f"- Valeurs non nulles: {non_null} ({percentage:.2f}%)")
        else:
            logger.info("\nAucune colonne liée au salaire n'a été trouvée.")
            logger.info("Recherche de champs potentiellement liés à la rémunération dans les descriptions...")
            
            # Vérifier si les informations de salaire pourraient être dans la description
            sample_query = """
            SELECT intitule, description_clean 
            FROM france_travail_jobs 
            WHERE description_clean ILIKE '%salaire%' OR description_clean ILIKE '%rémunération%' 
            LIMIT 5
            """
            sample_data = pd.read_sql(sample_query, engine)
            
            if not sample_data.empty:
                logger.info(f"\nTrouvé {len(sample_data)} offres avec mentions de salaire/rémunération dans la description.")
                for i, row in sample_data.iterrows():
                    logger.info(f"\nOffre {i+1}: {row['intitule']}")
                    desc = row['description_clean']
                    # Extraire un extrait autour des mentions de salaire
                    if 'salaire' in desc.lower():
                        start = max(0, desc.lower().find('salaire') - 50)
                        end = min(len(desc), desc.lower().find('salaire') + 100)
                        logger.info(f"Extrait: ...{desc[start:end]}...")
                    elif 'rémunération' in desc.lower():
                        start = max(0, desc.lower().find('rémunération') - 50)
                        end = min(len(desc), desc.lower().find('rémunération') + 100)
                        logger.info(f"Extrait: ...{desc[start:end]}...")
            else:
                logger.info("Aucune mention explicite de salaire trouvée dans les descriptions.")
        
        # Vérifier les données temporelles pour l'analyse d'évolution
        logger.info("\nVérification des données temporelles pour l'analyse d'évolution:")
        time_columns = [col for col in columns['column_name'] if 'date' in col.lower() or 'time' in col.lower()]
        
        if time_columns:
            logger.info(f"Colonnes temporelles trouvées: {time_columns}")
            
            # Examiner les données temporelles
            for col in time_columns:
                logger.info(f"\nExamen de la colonne {col}:")
                
                # Récupérer un échantillon de données
                sample_query = f"SELECT {col} FROM france_travail_jobs WHERE {col} IS NOT NULL LIMIT 5"
                sample_data = pd.read_sql(sample_query, engine)
                
                logger.info(f"Échantillon de données ({len(sample_data)} lignes):")
                for i, value in enumerate(sample_data[col]):
                    logger.info(f"{i+1}: {value}")
                
                # Calculer la plage temporelle
                range_query = f"SELECT MIN({col}) as min_date, MAX({col}) as max_date FROM france_travail_jobs WHERE {col} IS NOT NULL"
                range_data = pd.read_sql(range_query, engine)
                
                logger.info(f"Plage temporelle de la colonne {col}:")
                logger.info(f"- Date minimale: {range_data['min_date'][0]}")
                logger.info(f"- Date maximale: {range_data['max_date'][0]}")
        else:
            logger.info("Aucune colonne temporelle explicite n'a été trouvée.")
        
    except Exception as e:
        logger.error(f"Erreur lors de la vérification des données: {e}")

if __name__ == "__main__":
    main()
