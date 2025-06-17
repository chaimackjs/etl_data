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
import json
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

def extract_location_data(location_obj):
    """
    Extrait les données de localisation à partir de l'objet lieuTravail.
    
    Args:
        location_obj: Objet contenant les informations de localisation (dict ou str)
        
    Returns:
        dict: Données de localisation structurées
    """
    location_data = {
        'city': None,
        'postal_code': None,
        'department': None,
        'region': None,
        'country': None,  # Nouveau champ pour le pays
        'latitude': None,
        'longitude': None,
        'location_full': None
    }
    
    if not location_obj:
        return location_data
    
    try:
        # Si l'objet est une chaîne JSON
        if isinstance(location_obj, str):
            try:
                location_dict = json.loads(location_obj.replace("'", "\""))
            except json.JSONDecodeError:
                # Si ce n'est pas un JSON valide, utiliser la chaîne comme libellé
                location_data['location_full'] = location_obj
                # Essayer d'extraire le département depuis le libellé (format "XX - Ville")
                dept_match = re.match(r'^(\d{1,2})\s*-\s*(.+)$', location_obj)
                if dept_match:
                    location_data['department'] = dept_match.group(1).zfill(2)
                    location_data['city'] = dept_match.group(2).strip()
                return location_data
        else:
            location_dict = location_obj
        
        # Extraire les informations de localisation
        if isinstance(location_dict, dict):
            # Récupérer le libellé complet
            libelle = location_dict.get('libelle', '')
            location_data['location_full'] = libelle
            
            # Cas spécial: France (offre nationale)
            if libelle == 'France':
                location_data['city'] = 'France'
                location_data['region'] = 'France'
                return location_data
                
            # Extraire le nom de la ville à partir du libellé (format "XX - Ville")
            if ' - ' in libelle:
                parts = libelle.split(' - ', 1)
                if len(parts) > 1:
                    # Le nom de la ville est après le tiret
                    location_data['city'] = parts[1].strip()
                    # Le département est avant le tiret
                    dept_code = parts[0].strip()
                    if dept_code.isdigit() or (len(dept_code) == 2 and dept_code[0].isdigit()):
                        location_data['department'] = dept_code.zfill(2) if len(dept_code) <= 2 else dept_code
            
            # Si on n'a pas pu extraire la ville du libellé mais qu'on a un code commune
            # Utiliser le nom de la commune si disponible
            commune = location_dict.get('commune', '')
            if not location_data['city'] and commune:
                # Si commune est un code INSEE, on ne l'utilise pas comme nom de ville
                if not (commune.isdigit() or (len(commune) == 5 and commune[:2].isdigit())):
                    location_data['city'] = commune
            
            # Récupérer les autres informations disponibles
            location_data['postal_code'] = location_dict.get('codePostal', '')
            location_data['latitude'] = location_dict.get('latitude')
            location_data['longitude'] = location_dict.get('longitude')
            
            # Extraire le département depuis le code postal
            if location_data['postal_code'] and len(location_data['postal_code']) >= 2:
                location_data['department'] = location_data['postal_code'][:2]
                # Cas spéciaux: Corse
                if location_data['postal_code'].startswith('20'):
                    # Déterminer s'il s'agit de la Haute-Corse (2B) ou de la Corse-du-Sud (2A)
                    if 20200 <= int(location_data['postal_code']) < 20620:
                        location_data['department'] = '2A'  # Corse-du-Sud
                    else:
                        location_data['department'] = '2B'  # Haute-Corse
            
            # Extraire la région depuis le libellé
            libelle = location_dict.get('libelle', '')
            if ' - ' in libelle:
                parts = libelle.split(' - ', 1)
                if len(parts) > 1 and len(parts[0]) <= 3:  # Si le premier segment ressemble à un code département
                    location_data['region'] = parts[1].strip()
    except Exception as e:
        logger.warning(f"Erreur lors de l'extraction des données de localisation: {e}")
    
    return location_data

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
    
    # Traiter les données de localisation
    if 'lieuTravail' in result_df.columns:
        # Fonction pour parser de façon sécurisée les données de localisation
        def safe_parse_location(x):
            if isinstance(x, dict):
                return x
            elif isinstance(x, str):
                try:
                    # Essayer d'abord un parsing JSON standard
                    return json.loads(x.replace("'", "\""))
                except json.JSONDecodeError:
                    try:
                        # Essayer avec une approche plus robuste pour les chaînes mal formées
                        # Remplacer les guillemets simples par des doubles, sauf ceux dans les valeurs
                        x = re.sub(r"([\{\s,])([a-zA-Z0-9_]+)\s*:\s*'([^']*)'([\}\s,])", r'\1"\2":"\3"\4', x)
                        return json.loads(x)
                    except json.JSONDecodeError:
                        # En dernier recours, utiliser ast.literal_eval qui est plus permissif
                        try:
                            import ast
                            return ast.literal_eval(x)
                        except (SyntaxError, ValueError):
                            # Si tout échoue, retourner un dictionnaire avec le libellé
                            logger.warning(f"Impossible de parser les données de localisation: {x}")
                            return {'libelle': x}
            return None
        
        # Nettoyer et extraire les données de localisation
        result_df['lieuTravail_clean'] = result_df['lieuTravail'].apply(safe_parse_location)
        
        # Extraire les données de localisation structurées
        location_data = result_df['lieuTravail_clean'].apply(extract_location_data)
        
        # Ajouter les colonnes de localisation au DataFrame
        for col in ['city', 'postal_code', 'department', 'region', 'latitude', 'longitude', 'location_full']:
            result_df[col] = location_data.apply(lambda x: x.get(col) if x else None)
            
        # Définir les listes de pays et régions pour la classification
        countries_list = [
            'France', 'Italie', 'Espagne', 'Allemagne', 'Belgique', 'Suisse',
            'Japon', 'Royaume-Uni', 'Portugal', 'Luxembourg', 'Pays-Bas',
            'Autriche', 'Danemark', 'Suède', 'Finlande', 'Norvège',
            'Irlande', 'Grèce', 'Pologne', 'République tchèque', 'Hongrie',
            'Roumanie', 'Bulgarie', 'Croatie', 'Slovénie', 'Slovaquie',
            'Estonie', 'Lettonie', 'Lituanie', 'Chypre', 'Malte',
            'Canada', 'États-Unis', 'Mexique', 'Brésil', 'Argentine',
            'Chili', 'Colombie', 'Pérou', 'Venezuela', 'Chine',
            'Inde', 'Indonésie', 'Corée du Sud', 'Australie', 'Nouvelle-Zélande',
            'Afrique du Sud', 'Maroc', 'Tunisie', 'Algérie', 'Égypte'
        ]
        
        regions_list = [
            'Île-de-France', 'Guadeloupe', 'Martinique', 'Guyane', 'La Réunion',
            'Mayotte', 'Nouvelle-Aquitaine', 'Occitanie', 'Provence-Alpes-Côte d\'Azur',
            'Grand Est', 'Hauts-de-France', 'Normandie', 'Bretagne',
            'Pays de la Loire', 'Centre-Val de Loire', 'Bourgogne-Franche-Comté',
            'Auvergne-Rhône-Alpes', 'Corse', 'Rhône-Alpes'
        ]
        
        # Normalisation des libellés avec/sans accents
        libelle_normalization = {
            'Ile-de-France': 'Île-de-France',
            'Suisse (Frontalier)': 'Suisse',
            'Rhone-Alpes': 'Rhône-Alpes',
            'Etats-Unis': 'États-Unis',
            'Egypte': 'Égypte',
            'Perou': 'Pérou'
        }
        
        # Fonction pour extraire le pays et normaliser les libellés
        def extract_country_and_location(row):
            if isinstance(row['lieuTravail_clean'], dict) and 'libelle' in row['lieuTravail_clean']:
                libelle = row['lieuTravail_clean']['libelle']
                
                # Normaliser le libellé si nécessaire
                if libelle in libelle_normalization:
                    libelle = libelle_normalization[libelle]
                
                # Extraire le pays/région si format "Pays (Commentaire)"
                parenthesis_match = re.match(r'^([^(]+)\s*\([^)]*\)\s*$', libelle)
                if parenthesis_match:
                    libelle_base = parenthesis_match.group(1).strip()
                    if libelle_base:  # Si on a extrait quelque chose avant la parenthèse
                        libelle = libelle_base
                
                # Déterminer si c'est un pays
                if libelle in countries_list:
                    return pd.Series({'country': libelle, 'is_country': True})
                
                # Déterminer si c'est une région
                if libelle in regions_list:
                    return pd.Series({'country': 'France', 'is_country': False})
                
                # Si le libellé n'est pas dans nos listes mais semble être un pays ou une région
                # (pas de code postal ni de département au début)
                if not re.match(r'^\d', libelle) and ' - ' not in libelle:
                    # Par défaut, on suppose que c'est en France
                    return pd.Series({'country': 'France', 'is_country': False})
            
            # Par défaut, on suppose que c'est en France
            return pd.Series({'country': 'France', 'is_country': False})
        
        # Appliquer l'extraction de pays
        country_data = result_df.apply(extract_country_and_location, axis=1)
        result_df['country'] = country_data['country']
        
        # Traiter les cas spéciaux: utiliser le libellé comme nom de ville pour les régions/pays
        def fill_missing_city(row):
            if pd.isna(row['city']) or row['city'] == '':
                if isinstance(row['lieuTravail_clean'], dict) and 'libelle' in row['lieuTravail_clean']:
                    libelle = row['lieuTravail_clean']['libelle']
                    
                    # Normaliser le libellé si nécessaire
                    if libelle in libelle_normalization:
                        libelle = libelle_normalization[libelle]
                    
                    # Extraire le pays/région si format "Pays (Commentaire)"
                    parenthesis_match = re.match(r'^([^(]+)\s*\([^)]*\)\s*$', libelle)
                    if parenthesis_match:
                        libelle_base = parenthesis_match.group(1).strip()
                        if libelle_base:  # Si on a extrait quelque chose avant la parenthèse
                            libelle = libelle_base
                    
                    # Si c'est un pays, ne pas l'utiliser comme ville
                    if libelle in countries_list:
                        return None
                    
                    # Si c'est une région, l'utiliser comme ville
                    if libelle in regions_list:
                        return libelle
                    
                    # Si le libellé n'est pas dans nos listes mais semble être un pays ou une région
                    # (pas de code postal ni de département au début)
                    if not re.match(r'^\d', libelle) and ' - ' not in libelle:
                        return libelle
                        
            return row['city']
        
        # Appliquer la fonction pour remplir les villes manquantes
        result_df['city'] = result_df.apply(fill_missing_city, axis=1)
        
        # Remplir les valeurs manquantes restantes de city avec le pays
        def fill_remaining_missing_city(row):
            if pd.isna(row['city']) or row['city'] == '':
                if not pd.isna(row['country']) and row['country'] != '':
                    return f"{row['country']} (Pays)"
                else:
                    return "Localisation non précisée"
            return row['city']
        
        # Appliquer la fonction pour remplir les villes manquantes restantes
        result_df['city'] = result_df.apply(fill_remaining_missing_city, axis=1)
        
        logger.info("Données de localisation extraites et normalisées")
    
    # Traiter les données d'entreprise si disponibles
    if 'entreprise' in result_df.columns:
        # Extraire le nom de l'entreprise
        def extract_company_name(x):
            # Si c'est déjà un dictionnaire
            if isinstance(x, dict):
                return x.get('nom', "Entreprise non spécifiée")
            # Si c'est une chaîne qui pourrait être un JSON
            elif isinstance(x, str):
                # Vérifier si c'est une représentation de dictionnaire
                if x.startswith('{') and x.endswith('}'): 
                    try:
                        # Convertir la représentation de dictionnaire en dictionnaire réel
                        # Remplacer les apostrophes simples par des guillemets doubles pour JSON valide
                        cleaned_json = x.replace("'", "\"") 
                        parsed = json.loads(cleaned_json)
                        return parsed.get('nom', "Entreprise non spécifiée")
                    except json.JSONDecodeError:
                        # Si ce n'est pas un JSON valide, utiliser la chaîne telle quelle
                        return x
                else:
                    # Chaîne normale
                    return x
            # Pour tout autre cas (None, etc.)
            return "Entreprise non spécifiée"
        
        # Avant d'appliquer la fonction, convertir les dictionnaires en chaînes pour éviter les avertissements
        result_df['entreprise_clean'] = result_df['entreprise'].apply(
            lambda x: x.get('nom', "Entreprise non spécifiée") if isinstance(x, dict) else x
        )
        
        logger.info("Données d'entreprise extraites et normalisées")
    
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
