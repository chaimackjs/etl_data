#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Script pour examiner la structure des données de localisation.
"""

import os
import sys
import pandas as pd
import sqlalchemy
from sqlalchemy import text
import json

# Ajouter le chemin du projet au PYTHONPATH
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

# Importer l'utilitaire de chargement des variables d'environnement
from src.pipeline.api.dotenv_utils import load_dotenv

def main():
    """
    Fonction principale pour examiner les données de localisation.
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
        
        # Récupérer un échantillon de données de localisation
        df = pd.read_sql("SELECT lieu_travail FROM france_travail_jobs LIMIT 10", engine)
        
        print("Structure des données de localisation:")
        for i, lieu in enumerate(df['lieu_travail']):
            print(f"\nEnregistrement {i+1}:")
            try:
                # Convertir la chaîne en dictionnaire
                lieu_dict = eval(lieu)
                print(f"Type: {type(lieu_dict)}")
                print(f"Clés: {lieu_dict.keys()}")
                
                # Afficher les détails
                if 'libelle' in lieu_dict:
                    print(f"Libellé: {lieu_dict['libelle']}")
                if 'commune' in lieu_dict:
                    print(f"Code commune INSEE: {lieu_dict['commune']}")
                if 'codePostal' in lieu_dict:
                    print(f"Code postal: {lieu_dict['codePostal']}")
                
                # Extraire le nom de la ville à partir du libellé
                if 'libelle' in lieu_dict:
                    libelle = lieu_dict['libelle']
                    # Le format semble être "XX - NOM_VILLE"
                    if ' - ' in libelle:
                        ville = libelle.split(' - ')[1]
                        print(f"Ville extraite du libellé: {ville}")
            except Exception as e:
                print(f"Erreur lors de l'analyse: {e}")
        
        # Créer un dictionnaire de correspondance entre codes INSEE et noms de villes
        print("\nCréation d'un dictionnaire de correspondance codes INSEE -> noms de villes...")
        
        # Récupérer tous les lieux de travail uniques
        df_all = pd.read_sql("SELECT DISTINCT lieu_travail FROM france_travail_jobs", engine)
        
        # Créer le dictionnaire de correspondance
        insee_to_city = {}
        for lieu in df_all['lieu_travail']:
            try:
                lieu_dict = eval(lieu)
                if 'libelle' in lieu_dict and 'commune' in lieu_dict:
                    libelle = lieu_dict['libelle']
                    commune = lieu_dict['commune']
                    
                    # Extraire le nom de la ville du libellé
                    if ' - ' in libelle:
                        ville = libelle.split(' - ')[1]
                        insee_to_city[commune] = ville
            except Exception:
                pass
        
        # Afficher les 10 premières correspondances
        print("\nExemple de correspondances codes INSEE -> noms de villes:")
        for i, (code, ville) in enumerate(list(insee_to_city.items())[:10]):
            print(f"{code}: {ville}")
        
        print(f"\nNombre total de correspondances: {len(insee_to_city)}")
        
    except Exception as e:
        print(f"Erreur lors de l'examen des données de localisation: {e}")

if __name__ == "__main__":
    main()
