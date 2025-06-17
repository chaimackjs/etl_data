#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Script de test pour la transformation des données d'offres d'emploi France Travail.
Utilise un fichier CSV intermédiaire existant au lieu de faire une nouvelle extraction.
"""

import os
import pandas as pd
import glob
from datetime import datetime
from src.pipeline.api.transformation import transform_job_dataframe, apply_keyword_analysis

def find_latest_csv():
    """
    Trouve le fichier CSV intermédiaire le plus récent dans le dossier data/intermediate.
    
    Returns:
        str: Chemin vers le dernier fichier CSV d'extraction
    """
    pattern = os.path.join("data", "raw/france_travail", "france_travail_data_*.csv")
    csv_files = glob.glob(pattern)
    
    if not csv_files:
        print("Aucun fichier CSV d'extraction trouvé.")
        return None
    
    # Trouver le fichier le plus récent
    latest_file = max(csv_files, key=os.path.getctime)
    return latest_file

def test_transformation():
    """
    Teste le processus de transformation sur un fichier CSV existant.
    """
    print("=== Test du module de transformation ===\n")
    
    # 1. Trouver et charger le dernier fichier CSV
    latest_csv = find_latest_csv()
    if not latest_csv:
        print("Impossible de procéder sans fichier d'extraction.")
        return
    
    print(f"1. Chargement des données depuis {latest_csv}...")
    raw_df = pd.read_csv(latest_csv, low_memory=False)
    print(f"   - {len(raw_df)} offres d'emploi chargées")
    print(f"   - Colonnes disponibles: {raw_df.columns.tolist()[:10]}...")
    
    # 2. Appliquer la transformation
    print("\n2. Application des transformations...")
    transformed_df = transform_job_dataframe(raw_df)
    if transformed_df is None:
        print("Échec de la transformation.")
        return
    
    print(f"   - {len(transformed_df)} offres transformées")
    print(f"   - Nouvelles colonnes ajoutées: {[col for col in transformed_df.columns if col not in raw_df.columns][:10]}...")
    
    # 3. Appliquer l'analyse par mots-clés
    print("\n3. Application de l'analyse par mots-clés...")
    analyzed_df = apply_keyword_analysis(transformed_df)
    
    if analyzed_df is None:
        print("Échec de l'analyse par mots-clés.")
        return
    
    # 4. Exporter les résultats
    output_dir = os.path.join("data", "processed/france_travail")
    os.makedirs(output_dir, exist_ok=True)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_path = os.path.join(output_dir, f"france_travail_processed_{timestamp}.csv")
    
    print("\n4. Exportation des données transformées...")
    analyzed_df.to_csv(output_path, index=False)
    
    print(f"   - Données transformées exportées vers: {output_path}")
    print(f"   - Dimensions: {analyzed_df.shape}")
    
    # 5. Afficher quelques statistiques
    print("\n5. Statistiques sur les données transformées:")
    
    # Statistiques sur les types de contrat
    if 'contract_type_std' in analyzed_df.columns:
        contract_counts = analyzed_df['contract_type_std'].value_counts()
        print("   - Types de contrat:")
        for contract, count in contract_counts.items():
            # Afficher les types de contrat avec leurs noms plus explicites
            if contract == "APPRENTICESHIP":
                print(f"     * Alternance: {count}")
            elif contract == "INTERNSHIP":
                print(f"     * Stage: {count}")
            else:
                print(f"     * {contract}: {count}")
    
    # Statistiques sur les compétences techniques
    # Au lieu d'utiliser uniquement les colonnes booléennes, utilisons la colonne extracted_keywords
    # pour afficher toutes les technologies trouvées
    print("   - Technologies mentionnées:")
    
    # Collecter toutes les technologies trouvées dans les offres
    all_keywords = {}
    for keywords in analyzed_df['extracted_keywords']:
        if isinstance(keywords, list):
            for kw in keywords:
                all_keywords[kw] = all_keywords.get(kw, 0) + 1
    
    # Afficher les technologies trouvées par ordre décroissant de fréquence
    if all_keywords:
        for tech, count in sorted(all_keywords.items(), key=lambda x: x[1], reverse=True):
            print(f"     * {tech}: {count}")
    else:
        print("     * Aucune technologie spécifique détectée")
        
    # Afficher aussi les statistiques des colonnes booléennes pour vérification
    tech_columns = [col for col in analyzed_df.columns if col.startswith('has_')]
    print("\n   - Vérification des colonnes booléennes:")
    for col in tech_columns:
        tech_name = col.replace('has_', '')
        tech_count = analyzed_df[col].sum()
        print(f"     * {tech_name}: {tech_count}")
    
    # Top 10 des villes avec le plus d'offres
    if 'city' in analyzed_df.columns:
        city_counts = analyzed_df['city'].value_counts().head(10)
        print("   - Top 10 des villes avec le plus d'offres:")
        for city, count in city_counts.items():
            print(f"     * {city}: {count}")
            
    # Statistiques sur les pays
    if 'country' in analyzed_df.columns:
        country_counts = analyzed_df['country'].value_counts()
        print("\n   - Répartition par pays:")
        for country, count in country_counts.items():
            print(f"     * {country}: {count} offres ({count/len(analyzed_df)*100:.1f}%)")
        
        # Analyser les offres sans nom de ville
        print("\n6. Analyse des offres sans nom de ville:")
        empty_city_df = analyzed_df[analyzed_df['city'].isna() | (analyzed_df['city'] == '')]
        print(f"   - Nombre d'offres sans nom de ville: {len(empty_city_df)}")
        
        if not empty_city_df.empty:
            print("   - Exemples de données de localisation pour ces offres:")
            sample_rows = empty_city_df.head(5)
            for idx, row in sample_rows.iterrows():
                print(f"     * ID: {row['id']}")
                print(f"       Lieu de travail brut: {row['lieuTravail']}")
                print(f"       Lieu de travail nettoyé: {row['lieuTravail_clean']}")
                print(f"       Location full: {row.get('location_full', 'N/A')}")
                print(f"       Code postal: {row.get('postal_code', 'N/A')}")
                print()
    
    # Vérification des doublons et des champs vides
    print("\n7. Vérification de la qualité des données:")
    
    # Vérification des doublons sur l'identifiant
    duplicate_ids = analyzed_df[analyzed_df.duplicated('id', keep=False)]
    print(f"   - Doublons d'identifiants: {len(duplicate_ids)} offres")
    if len(duplicate_ids) > 0:
        print("     Exemples de doublons:")
        for id_val in duplicate_ids['id'].unique()[:3]:  # Afficher max 3 exemples
            print(f"     * ID {id_val} apparaît {len(duplicate_ids[duplicate_ids['id'] == id_val])} fois")
    
    # Vérification des champs vides ou manquants
    print("\n   - Champs vides ou manquants:")
    important_columns = ['id', 'intitule_clean', 'entreprise_clean', 'city', 'country', 'contract_type_std']
    for col in important_columns:
        if col in analyzed_df.columns:
            missing_count = analyzed_df[col].isna().sum()
            empty_count = 0
            if analyzed_df[col].dtype == 'object':
                empty_count = (analyzed_df[col] == '').sum()
            total_missing = missing_count + empty_count
            percent_missing = (total_missing / len(analyzed_df)) * 100
            print(f"     * {col}: {total_missing} valeurs manquantes ({percent_missing:.1f}%)")
    
    # Afficher un aperçu des données transformées
    print("\n8. Aperçu des données transformées:")
    
    # Sélectionner un sous-ensemble de colonnes intéressantes pour l'aperçu
    preview_columns = [
        'id', 'intitule_clean', 'entreprise_clean', 'city', 'country',
        'contract_type_std', 'min_salary', 'max_salary', 'salary_periodicity',
        'experience_level', 'extracted_keywords'
    ]
    
    # S'assurer que toutes les colonnes demandées existent
    preview_columns = [col for col in preview_columns if col in analyzed_df.columns]
    
    # Afficher un aperçu des 5 premières lignes
    if not preview_columns:
        print("   - Aucune colonne disponible pour l'aperçu")
    else:
        print("\n   - Aperçu des 5 premières lignes:")
        preview_df = analyzed_df[preview_columns].head(5)
        
        # Afficher chaque ligne avec un formatage lisible
        for i, row in preview_df.iterrows():
            print(f"\n     Offre #{i+1}:")
            for col in preview_columns:
                # Formater certaines colonnes spécifiquement
                if col == 'extracted_keywords' and isinstance(row[col], list):
                    print(f"       {col}: {', '.join(row[col]) if row[col] else 'Aucun'}")
                elif col in ['description_clean'] and isinstance(row[col], str) and len(row[col]) > 100:
                    # Tronquer les textes longs
                    print(f"       {col}: {row[col][:100]}...")
                else:
                    print(f"       {col}: {row[col]}")
    
    print("\n=== Test du module de transformation terminé ===")

if __name__ == "__main__":
    test_transformation()
