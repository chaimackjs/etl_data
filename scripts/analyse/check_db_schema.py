#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Script pour vérifier le schéma de la base de données et les données disponibles.
"""

import os
import sys
import pandas as pd
from sqlalchemy import create_engine, text

# Ajouter le répertoire parent au path pour pouvoir importer les modules
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

# Importer l'utilitaire de chargement des variables d'environnement
from src.pipeline.api.dotenv_utils import load_dotenv

# Charger les variables d'environnement
load_dotenv()

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
        
        # Créer le moteur
        engine = create_engine(conn_str)
        
        # Tester la connexion
        with engine.connect() as connection:
            print(f"Connexion établie avec succès à la base de données {database} sur {host}")
            
        return engine
    except Exception as e:
        print(f"Erreur de connexion à la base de données: {e}")
        return None

def main():
    """Fonction principale."""
    print("=== Vérification du schéma de la base de données ===\n")
    
    # Connexion à la base de données
    engine = get_db_connection()
    
    if engine is None:
        print("Échec de la connexion à la base de données")
        return
    
    try:
        # Récupérer la liste des tables
        with engine.connect() as connection:
            query = text("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
            result = connection.execute(query)
            tables = [row[0] for row in result]
            
            print(f"Tables disponibles: {tables}")
            
            # Vérifier si la table job_offers existe
            if 'job_offers' in tables:
                # Récupérer les colonnes de la table job_offers
                query = text("SELECT column_name, data_type FROM information_schema.columns WHERE table_name = 'job_offers'")
                result = connection.execute(query)
                columns = [(row[0], row[1]) for row in result]
                
                print("\nColonnes de la table job_offers:")
                for col, dtype in columns:
                    print(f"  - {col} ({dtype})")
                
                # Récupérer un échantillon de données
                df = pd.read_sql("SELECT * FROM job_offers LIMIT 5", engine)
                
                print("\nÉchantillon de données:")
                print(df.head())
                
                # Vérifier les valeurs uniques pour certaines colonnes clés
                if 'contract_type_std' in df.columns:
                    print("\nTypes de contrat disponibles:")
                    print(df['contract_type_std'].unique())
                
                if 'lieu_travail' in df.columns:
                    print("\nExemple de valeurs pour lieu_travail:")
                    for val in df['lieu_travail'].head(3).values:
                        print(f"  - {val}")
                
                # Vérifier les colonnes de technologies
                tech_columns = [col for col in df.columns if col.startswith('has_')]
                if tech_columns:
                    print("\nColonnes de technologies disponibles:")
                    for col in tech_columns:
                        print(f"  - {col}")
            else:
                print("La table job_offers n'existe pas dans la base de données.")
                
                # Vérifier si la table france_travail_jobs existe
                if 'france_travail_jobs' in tables:
                    print("\nLa table france_travail_jobs existe. Vérification des colonnes...")
                    
                    # Récupérer les colonnes de la table france_travail_jobs
                    query = text("SELECT column_name, data_type FROM information_schema.columns WHERE table_name = 'france_travail_jobs'")
                    result = connection.execute(query)
                    columns = [(row[0], row[1]) for row in result]
                    
                    print("\nColonnes de la table france_travail_jobs:")
                    for col, dtype in columns:
                        print(f"  - {col} ({dtype})")
                    
                    # Récupérer un échantillon de données
                    df = pd.read_sql("SELECT * FROM france_travail_jobs LIMIT 5", engine)
                    
                    print("\nÉchantillon de données:")
                    print(df.head())
    
    except Exception as e:
        print(f"Erreur lors de la vérification du schéma: {e}")

if __name__ == "__main__":
    main()
