#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Script de test pour visualiser les données à chaque étape du pipeline ETL Welcome to the Jungle.
"""

import os
import argparse
import logging
import pandas as pd
from datetime import datetime

# Configuration du logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Import des modules ETL
from src.pipeline.api.dotenv_utils import load_dotenv
from src.pipeline.scrapers.extraction import extract_welcome_jungle_data
from src.pipeline.scrapers.transformation import transform_welcome_jungle_data, save_transformed_data
from src.pipeline.scrapers.loading import get_db_connection, load_welcome_jungle_data

def display_dataframe_info(df, stage_name):
    """
    Affiche des informations sur un DataFrame.
    
    Args:
        df (pandas.DataFrame): DataFrame à analyser
        stage_name (str): Nom de l'étape (extraction, transformation, etc.)
    """
    print(f"\n{'='*80}")
    print(f"APERÇU DES DONNÉES - ÉTAPE: {stage_name}")
    print(f"{'='*80}")
    
    if df is None or df.empty:
        print("Aucune donnée disponible!")
        return
    
    print(f"Nombre d'enregistrements: {len(df)}")
    print(f"Colonnes ({len(df.columns)}): {', '.join(df.columns.tolist())}")
    
    # Afficher les types de données
    print("\nTypes de données:")
    for col, dtype in df.dtypes.items():
        print(f"  - {col}: {dtype}")
    
    # Afficher un aperçu des données
    print("\nAperçu des données:")
    print(df.head(5).to_string())
    
    # Afficher des statistiques sur les valeurs manquantes
    missing_values = df.isnull().sum()
    if missing_values.sum() > 0:
        print("\nValeurs manquantes par colonne:")
        for col, count in missing_values.items():
            if count > 0:
                print(f"  - {col}: {count} ({count/len(df)*100:.2f}%)")
    
    print(f"{'='*80}\n")

def test_extraction():
    """
    Teste l'étape d'extraction et affiche un aperçu des données extraites.
    """
    logger.info("Test de l'étape d'extraction")
    
    # Charger les variables d'environnement
    load_dotenv()
    
    # Extraction des données
    df, local_file = extract_welcome_jungle_data(all_files=True)
    
    if df is None or df.empty:
        logger.error("Aucune donnée extraite")
        return None
    
    logger.info(f"Extraction réussie: {len(df)} enregistrements")
    
    # Afficher un aperçu des données extraites
    display_dataframe_info(df, "EXTRACTION")
    
    return df, local_file

def test_transformation(df=None):
    """
    Teste l'étape de transformation et affiche un aperçu des données transformées.
    
    Args:
        df (pandas.DataFrame): DataFrame à transformer (optionnel)
    """
    logger.info("Test de l'étape de transformation")
    
    if df is None:
        # Si aucun DataFrame n'est fourni, exécuter l'extraction
        df, _ = test_extraction()
        
        if df is None or df.empty:
            logger.error("Impossible de tester la transformation sans données")
            return None
    
    # Transformation des données
    transformed_df = transform_welcome_jungle_data(df)
    
    if transformed_df is None or transformed_df.empty:
        logger.error("Erreur lors de la transformation des données")
        return None
    
    logger.info(f"Transformation réussie: {len(transformed_df)} enregistrements")
    
    # Afficher un aperçu des données transformées
    display_dataframe_info(transformed_df, "TRANSFORMATION")
    
    # Sauvegarder les données transformées
    output_file = save_transformed_data(transformed_df)
    logger.info(f"Données transformées sauvegardées dans: {output_file}")
    
    return transformed_df

def test_loading(df=None):
    """
    Teste l'étape de chargement (sans réellement charger les données).
    
    Args:
        df (pandas.DataFrame): DataFrame à charger (optionnel)
    """
    logger.info("Test de l'étape de chargement (simulation)")
    
    if df is None:
        # Si aucun DataFrame n'est fourni, exécuter la transformation
        df = test_transformation()
        
        if df is None or df.empty:
            logger.error("Impossible de tester le chargement sans données")
            return False
    
    # Vérifier la connexion à la base de données
    engine = get_db_connection()
    if not engine:
        logger.error("Impossible de se connecter à la base de données")
        return False
    
    logger.info("Connexion à la base de données établie avec succès")
    
    # Afficher la structure finale des données avant chargement
    display_dataframe_info(df, "CHARGEMENT (données prêtes à être chargées)")
    
    # Demander confirmation avant de charger les données
    confirmation = input("Voulez-vous charger ces données dans la base de données? (oui/non): ")
    
    if confirmation.lower() in ['oui', 'o', 'yes', 'y']:
        logger.info("Chargement des données dans la base de données...")
        success = load_welcome_jungle_data(df, engine, force=True)
        
        if success:
            logger.info("Chargement des données terminé avec succès")
            return True
        else:
            logger.error("Erreur lors du chargement des données")
            return False
    else:
        logger.info("Chargement annulé par l'utilisateur")
        return False

def main():
    """
    Fonction principale.
    """
    parser = argparse.ArgumentParser(description="Test des étapes du pipeline ETL Welcome to the Jungle")
    
    # Options pour les étapes à tester
    parser.add_argument("--extract", action="store_true", help="Tester uniquement l'étape d'extraction")
    parser.add_argument("--transform", action="store_true", help="Tester l'extraction et la transformation")
    parser.add_argument("--load", action="store_true", help="Tester le pipeline complet (sans charger les données)")
    
    args = parser.parse_args()
    
    # Si aucune option n'est spécifiée, afficher l'aide
    if not any(vars(args).values()):
        parser.print_help()
        return
    
    # Exécuter les tests en fonction des options
    if args.extract:
        test_extraction()
    elif args.transform:
        test_transformation()
    elif args.load:
        test_loading()

if __name__ == "__main__":
    main()
