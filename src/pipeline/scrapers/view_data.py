#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Script pour visualiser les données à chaque étape du pipeline ETL Welcome to the Jungle.
"""

import os
import pandas as pd
import logging
from tabulate import tabulate

# Configuration du logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Import des modules ETL
from src.pipeline.api.dotenv_utils import load_dotenv
from src.pipeline.scrapers.extraction import extract_welcome_jungle_data
from src.pipeline.scrapers.transformation import transform_welcome_jungle_data

def view_extraction_data():
    """
    Visualise les données extraites.
    """
    print("\n" + "="*80)
    print("DONNÉES APRÈS EXTRACTION")
    print("="*80)
    
    # Charger les variables d'environnement
    load_dotenv()
    
    # Extraction des données
    df, _ = extract_welcome_jungle_data(all_files=True)
    
    if df is None or df.empty:
        print("Aucune donnée extraite!")
        return None
    
    # Afficher les informations sur le DataFrame
    print(f"Nombre d'enregistrements: {len(df)}")
    print(f"Colonnes ({len(df.columns)}): {', '.join(df.columns.tolist())}")
    
    # Afficher les premières lignes
    print("\nAperçu des données (5 premières lignes):")
    print(tabulate(df.head(5), headers='keys', tablefmt='psql', showindex=True))
    
    # Afficher les types de données
    print("\nTypes de données:")
    for col, dtype in df.dtypes.items():
        print(f"  - {col}: {dtype}")
    
    return df

def view_transformation_data(df=None):
    """
    Visualise les données transformées.
    
    Args:
        df (pandas.DataFrame): DataFrame à transformer (optionnel)
    """
    print("\n" + "="*80)
    print("DONNÉES APRÈS TRANSFORMATION")
    print("="*80)
    
    if df is None:
        # Si aucun DataFrame n'est fourni, exécuter l'extraction
        df = view_extraction_data()
        
        if df is None or df.empty:
            print("Impossible de visualiser la transformation sans données!")
            return None
    
    # Transformation des données
    transformed_df = transform_welcome_jungle_data(df)
    
    if transformed_df is None or transformed_df.empty:
        print("Erreur lors de la transformation des données!")
        return None
    
    # Afficher les informations sur le DataFrame
    print(f"Nombre d'enregistrements: {len(transformed_df)}")
    print(f"Colonnes ({len(transformed_df.columns)}): {', '.join(transformed_df.columns.tolist())}")
    
    # Afficher les premières lignes
    print("\nAperçu des données (5 premières lignes):")
    print(tabulate(transformed_df.head(5), headers='keys', tablefmt='psql', showindex=True))
    
    # Afficher les types de données
    print("\nTypes de données:")
    for col, dtype in transformed_df.dtypes.items():
        print(f"  - {col}: {dtype}")
    
    # Afficher des statistiques sur les types de contrat
    if 'contract_type_std' in transformed_df.columns:
        print("\nDistribution des types de contrat:")
        contract_counts = transformed_df['contract_type_std'].value_counts()
        for contract_type, count in contract_counts.items():
            print(f"  - {contract_type}: {count} ({count/len(transformed_df)*100:.2f}%)")
    
    # Afficher des statistiques sur les technologies détectées
    tech_columns = [col for col in transformed_df.columns if col.startswith('has_')]
    if tech_columns:
        print("\nTechnologies détectées:")
        for col in tech_columns:
            tech_count = transformed_df[col].sum()
            if tech_count > 0:
                tech_name = col.replace('has_', '')
                print(f"  - {tech_name}: {tech_count} ({tech_count/len(transformed_df)*100:.2f}%)")
    
    return transformed_df

def main():
    """
    Fonction principale.
    """
    import argparse
    
    parser = argparse.ArgumentParser(description="Visualisation des données du pipeline ETL Welcome to the Jungle")
    parser.add_argument("--step", choices=["extract", "transform", "all"], default="all",
                        help="Étape du pipeline à visualiser (extract, transform, all)")
    
    args = parser.parse_args()
    
    if args.step == "extract":
        view_extraction_data()
    elif args.step == "transform":
        view_transformation_data()
    else:  # all
        df = view_extraction_data()
        if df is not None and not df.empty:
            view_transformation_data(df)

if __name__ == "__main__":
    main()
