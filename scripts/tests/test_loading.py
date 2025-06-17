#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Script de test pour le chargement des données transformées dans la base de données PostgreSQL.
"""

import os
import sys
import pandas as pd
import logging
from datetime import datetime
import glob

# Ajouter le chemin du projet au PYTHONPATH
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Importer les fonctions nécessaires
from src.pipeline.api.loading import prepare_job_data_for_loading, get_db_connection
from src.pipeline.api.loading import create_jobs_table, load_jobs_to_database

# Configuration du logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def find_latest_transformed_file():
    """
    Trouve le fichier CSV transformé le plus récent dans les dossiers possibles.
    
    Returns:
        str: Chemin vers le dernier fichier CSV transformé
    """
    # Chercher dans tous les emplacements possibles
    patterns = [
        os.path.join("data", "processed", "pole_emploi", "france_travail_processed_*.csv"),
        os.path.join("data", "pole_emploi", "france_travail_processed_*.csv"),
        os.path.join("data", "processed", "france_travail", "france_travail_processed_*.csv")
    ]
    
    csv_files = []
    for pattern in patterns:
        csv_files.extend(glob.glob(pattern))
    
    if not csv_files:
        print("Aucun fichier CSV transformé trouvé dans les dossiers de données transformées.")
        return None
    
    # Trouver le fichier le plus récent
    latest_file = max(csv_files, key=os.path.getctime)
    print(f"Fichier transformé trouvé: {latest_file}")
    return latest_file

def test_loading():
    """
    Teste le processus de chargement des données dans la base de données.
    """
    print("=== Test du module de chargement ===\n")
    
    # 1. Configurer les variables d'environnement pour la base de données
    print("1. Configuration des variables d'environnement...")
    # Utiliser les mêmes paramètres que dans deploy_infrastructure_aws.py
    os.environ['DB_HOST'] = 'datas.c32ygg4oyapa.eu-north-1.rds.amazonaws.com'
    os.environ['DB_PORT'] = '5432'
    os.environ['DB_NAME'] = 'postgres'
    os.environ['DB_USER'] = 'postgres'
    os.environ['DB_PASSWORD'] = 'm!wgz#$gsPD}d7x'
    
    # 2. Trouver et charger le dernier fichier CSV transformé
    latest_csv = find_latest_transformed_file()
    if not latest_csv:
        print("Impossible de procéder sans fichier transformé.")
        return
    
    print(f"2. Chargement des données depuis {latest_csv}...")
    try:
        # Essayer d'abord avec utf-8
        transformed_df = pd.read_csv(latest_csv, low_memory=False, encoding='utf-8')
    except UnicodeDecodeError:
        # Si ça échoue, essayer avec latin-1 qui est plus permissif
        print("   - Encodage utf-8 échoué, essai avec latin-1...")
        transformed_df = pd.read_csv(latest_csv, low_memory=False, encoding='latin-1')
    print(f"   - {len(transformed_df)} offres d'emploi chargées")
    
    # 3. Préparer les données pour le chargement
    print("\n3. Préparation des données pour le chargement...")
    load_ready_df = prepare_job_data_for_loading(transformed_df)
    
    if load_ready_df is None:
        print("Échec de la préparation des données pour le chargement.")
        return
    
    print(f"   - {len(load_ready_df)} offres prêtes pour le chargement")
    print(f"   - Colonnes préparées: {load_ready_df.columns.tolist()}")
    
    # 4. Tester la connexion à la base de données
    print("\n4. Test de connexion à la base de données...")
    try:
        # Vérifier que les variables d'environnement sont correctement configurées
        print(f"   - Host: {os.environ.get('DB_HOST')}")
        print(f"   - Port: {os.environ.get('DB_PORT')}")
        print(f"   - Database: {os.environ.get('DB_NAME')}")
        print(f"   - User: {os.environ.get('DB_USER')}")
        
        # Essayer de se connecter directement avec psycopg2 pour avoir plus de détails
        print("   - Test de connexion directe avec psycopg2...")
        try:
            import psycopg2
            conn = psycopg2.connect(
                host=os.environ.get('DB_HOST'),
                port=os.environ.get('DB_PORT'),
                database=os.environ.get('DB_NAME'),
                user=os.environ.get('DB_USER'),
                password=os.environ.get('DB_PASSWORD'),
                connect_timeout=10
            )
            conn.close()
            print("   - Connexion directe réussie avec psycopg2")
        except Exception as e:
            print(f"   - Erreur de connexion directe avec psycopg2: {e}")
            print("   - Cela indique probablement que l'instance RDS n'est pas accessible publiquement")
            print("   - Vérifiez les paramètres de sécurité de votre instance RDS")
        
        # Essayer avec SQLAlchemy
        print("   - Test de connexion avec SQLAlchemy...")
        engine = get_db_connection()
        if engine is None:
            print("   - Échec de la connexion à la base de données avec SQLAlchemy.")
            print("   - Vérifiez que l'instance RDS est accessible depuis votre réseau.")
            return
        
        print("   - Connexion à la base de données établie avec succès")
        
        # 5. Créer la table si elle n'existe pas
        print("\n5. Création/vérification de la table france_travail_jobs...")
        if create_jobs_table(engine):
            print("   - Table créée ou déjà existante")
            
            # 6. Charger les données
            print("\n6. Chargement des données dans la base de données...")
            records_loaded = load_jobs_to_database(load_ready_df, engine)
            
            if records_loaded > 0:
                print(f"   - Chargement réussi: {records_loaded} offres insérées dans la base de données")
            else:
                print("   - Aucune offre insérée dans la base de données")
        else:
            print("Échec de la création de la table.")
            
    except Exception as e:
        print(f"Erreur lors de la connexion ou du chargement: {e}")
    
    print("\n=== Test du module de chargement terminé ===")

if __name__ == "__main__":
    test_loading()
