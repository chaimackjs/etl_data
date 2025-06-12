#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Module de transformation des données pour l'API France Travail.
Nettoie, structure et enrichit les données extraites des offres d'emploi.
"""

import re
import logging
import pandas as pd
import numpy as np
from datetime import datetime

# Configuration du logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f"logs/etl_transformation_{datetime.now().strftime('%Y%m%d')}.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def clean_text_field(text):
    """
    Nettoie un champ texte en supprimant les caractères HTML et en normalisant l'espacement.
    
    Args:
        text (str): Texte à nettoyer
        
    Returns:
        str: Texte nettoyé
    """
    if not isinstance(text, str):
        return ""
    
    # Supprimer les balises HTML
    text = re.sub(r'<.*?>', ' ', text)
    
    # Normaliser les espaces
    text = re.sub(r'\s+', ' ', text)
    
    # Supprimer les espaces en début et fin
    return text.strip()

def extract_salary_info(salary_text):
    """
    Extrait les informations de salaire à partir du texte.
    
    Args:
        salary_text (str): Texte contenant les informations de salaire
        
    Returns:
        tuple: (min_salary, max_salary, periodicity, currency)
    """
    if not isinstance(salary_text, str) or not salary_text:
        return None, None, None, "EUR"
    
    # Valeurs par défaut
    min_salary = None
    max_salary = None
    periodicity = None
    currency = "EUR"
    
    # Rechercher les montants
    amounts = re.findall(r'(\d+[\s\d]*[\d,.]*)(?:\s*[€$£]|\s*euros?|\s*euro)', salary_text.lower())
    
    # Déterminer la périodicité
    if "annuel" in salary_text.lower() or "par an" in salary_text.lower():
        periodicity = "yearly"
    elif "mensuel" in salary_text.lower() or "par mois" in salary_text.lower():
        periodicity = "monthly"
    elif "horaire" in salary_text.lower() or "de l'heure" in salary_text.lower():
        periodicity = "hourly"
    else:
        periodicity = "monthly"  # Par défaut
    
    # Détecter la devise
    if "£" in salary_text:
        currency = "GBP"
    elif "$" in salary_text:
        currency = "USD"
    
    # Extraire min et max
    if len(amounts) >= 2:
        try:
            min_salary = float(re.sub(r'[^\d.]', '', amounts[0].replace(',', '.')))
            max_salary = float(re.sub(r'[^\d.]', '', amounts[1].replace(',', '.')))
        except ValueError:
            pass
    elif len(amounts) == 1:
        try:
            min_salary = float(re.sub(r'[^\d.]', '', amounts[0].replace(',', '.')))
        except ValueError:
            pass
    
    return min_salary, max_salary, periodicity, currency

def categorize_contract_type(contract_text):
    """
    Catégorise le type de contrat selon une nomenclature standard.
    
    Args:
        contract_text (str): Description du type de contrat
        
    Returns:
        str: Type de contrat standardisé
    """
    if not isinstance(contract_text, str):
        return "UNKNOWN"
    
    contract_text = contract_text.lower()
    
    if "cdi" in contract_text:
        return "CDI"
    elif "cdd" in contract_text:
        return "CDD"
    elif "intérim" in contract_text or "interim" in contract_text:
        return "INTERIM"
    elif "apprentissage" in contract_text:
        return "APPRENTICESHIP"
    elif "stage" in contract_text:
        return "INTERNSHIP"
    elif "freelance" in contract_text or "indépendant" in contract_text:
        return "FREELANCE"
    elif "temps partiel" in contract_text:
        return "PART_TIME"
    elif "temps plein" in contract_text:
        return "FULL_TIME"
    else:
        return "OTHER"

def extract_experience_level(description):
    """
    Extrait le niveau d'expérience à partir de la description.
    
    Args:
        description (str): Description du poste
        
    Returns:
        str: Niveau d'expérience (ENTRY, MID, SENIOR, EXPERT)
    """
    if not isinstance(description, str):
        return "NOT_SPECIFIED"
    
    description = description.lower()
    
    if any(term in description for term in ["débutant", "junior", "0-2 ans", "0 à 2 ans"]):
        return "ENTRY"
    elif any(term in description for term in ["confirmé", "2-5 ans", "2 à 5 ans", "3 ans"]):
        return "MID"
    elif any(term in description for term in ["senior", "5-10 ans", "5 à 10 ans", "expérimenté"]):
        return "SENIOR"
    elif any(term in description for term in ["expert", "+ de 10 ans", "plus de 10 ans"]):
        return "EXPERT"
    else:
        return "NOT_SPECIFIED"

def transform_job_dataframe(df):
    """
    Applique les transformations nécessaires au DataFrame d'offres d'emploi.
    
    Args:
        df (pandas.DataFrame): DataFrame à transformer
        
    Returns:
        pandas.DataFrame: DataFrame transformé
    """
    if df is None or len(df) == 0:
        logger.warning("DataFrame vide, aucune transformation appliquée")
        return None
    
    logger.info("Début des transformations sur les données")
    
    # Créer une copie pour éviter de modifier l'original
    result_df = df.copy()
    
    # Gérer les valeurs manquantes dans les colonnes principales
    required_columns = ['id', 'intitule', 'description', 'dateCreation']
    for col in required_columns:
        if col not in result_df.columns:
            logger.warning(f"Colonne requise {col} manquante dans le DataFrame")
            result_df[col] = None
    
    # Nettoyer et transformer les colonnes textuelles
    text_columns = ['intitule', 'description', 'lieuTravail', 'entreprise']
    for col in text_columns:
        if col in result_df.columns:
            result_df[col + '_clean'] = result_df[col].apply(
                lambda x: clean_text_field(x) if isinstance(x, str) else x
            )
    
    # Extraire les informations sur le salaire
    if 'salaire' in result_df.columns:
        salary_info = result_df['salaire'].apply(extract_salary_info)
        result_df['min_salary'] = [info[0] for info in salary_info]
        result_df['max_salary'] = [info[1] for info in salary_info]
        result_df['salary_periodicity'] = [info[2] for info in salary_info]
        result_df['currency'] = [info[3] for info in salary_info]
    
    # Standardiser le type de contrat
    if 'typeContrat' in result_df.columns:
        result_df['contract_type_std'] = result_df['typeContrat'].apply(categorize_contract_type)
    
    # Extraire le niveau d'expérience à partir de la description
    if 'description' in result_df.columns:
        result_df['experience_level'] = result_df['description'].apply(extract_experience_level)
    
    # Convertir les dates au format ISO
    date_columns = ['dateCreation', 'dateActualisation']
    for col in date_columns:
        if col in result_df.columns:
            result_df[col + '_iso'] = pd.to_datetime(result_df[col], errors='coerce').dt.strftime('%Y-%m-%d')
    
    # Ajouter une colonne de datestamp ETL
    result_df['etl_timestamp'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    logger.info(f"Transformation terminée: {len(result_df)} offres d'emploi traitées")
    return result_df

def extract_keywords(description, keyword_list=None):
    """
    Extrait les mots-clés pertinents d'une description d'offre d'emploi.
    
    Args:
        description (str): Description de l'offre d'emploi
        keyword_list (list): Liste de mots-clés à rechercher
        
    Returns:
        list: Liste des mots-clés trouvés
    """
    if not isinstance(description, str) or not description:
        return []
    
    # Liste par défaut de technologies et compétences à détecter
    if keyword_list is None:
        keyword_list = [
            # Langages de programmation
            "python", "java", "javascript", "c\\+\\+", "c#", "php", "ruby", "swift",
            # Frameworks
            "django", "flask", "spring", "react", "angular", "vue", "laravel",
            # Base de données
            "sql", "postgresql", "mysql", "mongodb", "oracle", "sqlite",
            # Cloud
            "aws", "azure", "gcp", "cloud",
            # Data
            "data science", "machine learning", "deep learning", "ai", "big data",
            # DevOps
            "devops", "docker", "kubernetes", "jenkins", "git", "ci/cd"
        ]
    
    # Convertir en texte minuscule pour la recherche non sensible à la casse
    description_lower = description.lower()
    
    # Rechercher les mots-clés dans la description
    found_keywords = []
    for keyword in keyword_list:
        pattern = r'\b' + keyword + r'\b'
        if re.search(pattern, description_lower):
            found_keywords.append(keyword)
    
    return found_keywords

def apply_keyword_analysis(df):
    """
    Applique l'analyse de mots-clés aux offres d'emploi.
    
    Args:
        df (pandas.DataFrame): DataFrame contenant les offres d'emploi
        
    Returns:
        pandas.DataFrame: DataFrame avec les colonnes de mots-clés ajoutées
    """
    if df is None or 'description_clean' not in df.columns:
        return df
    
    logger.info("Application de l'analyse par mots-clés aux offres d'emploi")
    
    # Extraire les mots-clés des descriptions
    df['extracted_keywords'] = df['description_clean'].apply(extract_keywords)
    
    # Ajouter des colonnes booléennes pour les technologies principales
    main_techs = ["python", "java", "javascript", "sql", "aws", "machine learning"]
    for tech in main_techs:
        df[f'has_{tech.replace(" ", "_")}'] = df['extracted_keywords'].apply(
            lambda x: 1 if tech in x else 0)
    
    # Compter le nombre de mots-clés trouvés
    df['keyword_count'] = df['extracted_keywords'].apply(len)
    
    logger.info("Analyse par mots-clés terminée")
    return df

if __name__ == "__main__":
    # Test de transformation
    from extraction import extract_by_date_range
    
    # Récupérer les données du jour
    today = datetime.now().strftime("%Y%m%d")
    raw_df = extract_by_date_range(today)
    
    if raw_df is not None:
        # Appliquer les transformations
        transformed_df = transform_job_dataframe(raw_df)
        transformed_df = apply_keyword_analysis(transformed_df)
        
        print(f"Colonnes après transformation: {transformed_df.columns.tolist()}")
        print(f"Nombre d'offres transformées: {len(transformed_df)}")
