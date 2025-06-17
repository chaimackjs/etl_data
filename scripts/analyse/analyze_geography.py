#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Module d'analyse géographique avancée des offres d'emploi France Travail.
"""

import os
import sys
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
import logging
import re
import argparse
from datetime import datetime
from sqlalchemy import create_engine, text

# Ajouter le chemin du projet au PYTHONPATH
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

# Importer l'utilitaire de chargement des variables d'environnement
from src.pipeline.api.dotenv_utils import load_dotenv

# Configuration du logging
os.makedirs("logs", exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f"logs/geography_analysis_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def extract_department_from_city(city_code):
    """
    Extrait le code département à partir du code INSEE de la ville.
    
    Args:
        city_code (str): Code INSEE de la ville
        
    Returns:
        str: Code du département
    """
    if not city_code or not isinstance(city_code, str):
        return None
    
    # Pour les DOM-TOM (97X et 98X)
    if city_code.startswith('97') or city_code.startswith('98'):
        return city_code[:3]
    
    # Pour la Corse (2A et 2B)
    if city_code.startswith('2A') or city_code.startswith('2B'):
        return city_code[:2]
    
    # Pour les autres départements
    return city_code[:2]

def get_department_name(department_code):
    """
    Retourne le nom du département à partir de son code.
    
    Args:
        department_code (str): Code du département
        
    Returns:
        str: Nom du département
    """
    department_names = {
        '01': 'Ain', '02': 'Aisne', '03': 'Allier', '04': 'Alpes-de-Haute-Provence',
        '05': 'Hautes-Alpes', '06': 'Alpes-Maritimes', '07': 'Ardèche', '08': 'Ardennes',
        '09': 'Ariège', '10': 'Aube', '11': 'Aude', '12': 'Aveyron',
        '13': 'Bouches-du-Rhône', '14': 'Calvados', '15': 'Cantal', '16': 'Charente',
        '17': 'Charente-Maritime', '18': 'Cher', '19': 'Corrèze', '2A': 'Corse-du-Sud',
        '2B': 'Haute-Corse', '21': 'Côte-d\'Or', '22': 'Côtes-d\'Armor', '23': 'Creuse',
        '24': 'Dordogne', '25': 'Doubs', '26': 'Drôme', '27': 'Eure',
        '28': 'Eure-et-Loir', '29': 'Finistère', '30': 'Gard', '31': 'Haute-Garonne',
        '32': 'Gers', '33': 'Gironde', '34': 'Hérault', '35': 'Ille-et-Vilaine',
        '36': 'Indre', '37': 'Indre-et-Loire', '38': 'Isère', '39': 'Jura',
        '40': 'Landes', '41': 'Loir-et-Cher', '42': 'Loire', '43': 'Haute-Loire',
        '44': 'Loire-Atlantique', '45': 'Loiret', '46': 'Lot', '47': 'Lot-et-Garonne',
        '48': 'Lozère', '49': 'Maine-et-Loire', '50': 'Manche', '51': 'Marne',
        '52': 'Haute-Marne', '53': 'Mayenne', '54': 'Meurthe-et-Moselle', '55': 'Meuse',
        '56': 'Morbihan', '57': 'Moselle', '58': 'Nièvre', '59': 'Nord',
        '60': 'Oise', '61': 'Orne', '62': 'Pas-de-Calais', '63': 'Puy-de-Dôme',
        '64': 'Pyrénées-Atlantiques', '65': 'Hautes-Pyrénées', '66': 'Pyrénées-Orientales', '67': 'Bas-Rhin',
        '68': 'Haut-Rhin', '69': 'Rhône', '70': 'Haute-Saône', '71': 'Saône-et-Loire',
        '72': 'Sarthe', '73': 'Savoie', '74': 'Haute-Savoie', '75': 'Paris',
        '76': 'Seine-Maritime', '77': 'Seine-et-Marne', '78': 'Yvelines', '79': 'Deux-Sèvres',
        '80': 'Somme', '81': 'Tarn', '82': 'Tarn-et-Garonne', '83': 'Var',
        '84': 'Vaucluse', '85': 'Vendée', '86': 'Vienne', '87': 'Haute-Vienne',
        '88': 'Vosges', '89': 'Yonne', '90': 'Territoire de Belfort', '91': 'Essonne',
        '92': 'Hauts-de-Seine', '93': 'Seine-Saint-Denis', '94': 'Val-de-Marne', '95': 'Val-d\'Oise',
        '971': 'Guadeloupe', '972': 'Martinique', '973': 'Guyane', '974': 'La Réunion',
        '976': 'Mayotte'
    }
    
    return department_names.get(department_code, f"Département {department_code}")

def extract_region_from_department(department):
    """
    Détermine la région à partir du code département.
    
    Args:
        department (str): Code du département
        
    Returns:
        str: Nom de la région
    """
    if not department:
        return "Inconnue"
    
    # Mapping des départements aux régions (après réforme territoriale 2016)
    region_mapping = {
        # Auvergne-Rhône-Alpes
        '01': 'Auvergne-Rhône-Alpes', '03': 'Auvergne-Rhône-Alpes', 
        '07': 'Auvergne-Rhône-Alpes', '15': 'Auvergne-Rhône-Alpes',
        '26': 'Auvergne-Rhône-Alpes', '38': 'Auvergne-Rhône-Alpes', 
        '42': 'Auvergne-Rhône-Alpes', '43': 'Auvergne-Rhône-Alpes',
        '63': 'Auvergne-Rhône-Alpes', '69': 'Auvergne-Rhône-Alpes', 
        '73': 'Auvergne-Rhône-Alpes', '74': 'Auvergne-Rhône-Alpes',
        
        # Bourgogne-Franche-Comté
        '21': 'Bourgogne-Franche-Comté', '25': 'Bourgogne-Franche-Comté', 
        '39': 'Bourgogne-Franche-Comté', '58': 'Bourgogne-Franche-Comté',
        '70': 'Bourgogne-Franche-Comté', '71': 'Bourgogne-Franche-Comté', 
        '89': 'Bourgogne-Franche-Comté', '90': 'Bourgogne-Franche-Comté',
        
        # Bretagne
        '22': 'Bretagne', '29': 'Bretagne', '35': 'Bretagne', '56': 'Bretagne',
        
        # Centre-Val de Loire
        '18': 'Centre-Val de Loire', '28': 'Centre-Val de Loire', '36': 'Centre-Val de Loire',
        '37': 'Centre-Val de Loire', '41': 'Centre-Val de Loire', '45': 'Centre-Val de Loire',
        
        # Corse
        '2A': 'Corse', '2B': 'Corse',
        
        # Grand Est
        '08': 'Grand Est', '10': 'Grand Est', '51': 'Grand Est', '52': 'Grand Est',
        '54': 'Grand Est', '55': 'Grand Est', '57': 'Grand Est', '67': 'Grand Est',
        '68': 'Grand Est', '88': 'Grand Est',
        
        # Hauts-de-France
        '02': 'Hauts-de-France', '59': 'Hauts-de-France', '60': 'Hauts-de-France',
        '62': 'Hauts-de-France', '80': 'Hauts-de-France',
        
        # Île-de-France
        '75': 'Île-de-France', '77': 'Île-de-France', '78': 'Île-de-France',
        '91': 'Île-de-France', '92': 'Île-de-France', '93': 'Île-de-France',
        '94': 'Île-de-France', '95': 'Île-de-France',
        
        # Normandie
        '14': 'Normandie', '27': 'Normandie', '50': 'Normandie',
        '61': 'Normandie', '76': 'Normandie',
        
        # Nouvelle-Aquitaine
        '16': 'Nouvelle-Aquitaine', '17': 'Nouvelle-Aquitaine', '19': 'Nouvelle-Aquitaine',
        '23': 'Nouvelle-Aquitaine', '24': 'Nouvelle-Aquitaine', '33': 'Nouvelle-Aquitaine',
        '40': 'Nouvelle-Aquitaine', '47': 'Nouvelle-Aquitaine', '64': 'Nouvelle-Aquitaine',
        '79': 'Nouvelle-Aquitaine', '86': 'Nouvelle-Aquitaine', '87': 'Nouvelle-Aquitaine',
        
        # Occitanie
        '09': 'Occitanie', '11': 'Occitanie', '12': 'Occitanie', '30': 'Occitanie',
        '31': 'Occitanie', '32': 'Occitanie', '34': 'Occitanie', '46': 'Occitanie',
        '48': 'Occitanie', '65': 'Occitanie', '66': 'Occitanie', '81': 'Occitanie',
        '82': 'Occitanie',
        
        # Pays de la Loire
        '44': 'Pays de la Loire', '49': 'Pays de la Loire', '53': 'Pays de la Loire',
        '72': 'Pays de la Loire', '85': 'Pays de la Loire',
        
        # Provence-Alpes-Côte d'Azur
        '04': 'Provence-Alpes-Côte d\'Azur', '05': 'Provence-Alpes-Côte d\'Azur',
        '06': 'Provence-Alpes-Côte d\'Azur', '13': 'Provence-Alpes-Côte d\'Azur',
        '83': 'Provence-Alpes-Côte d\'Azur', '84': 'Provence-Alpes-Côte d\'Azur',
        
        # DOM-TOM
        '971': 'Guadeloupe', '972': 'Martinique', '973': 'Guyane',
        '974': 'La Réunion', '975': 'Saint-Pierre-et-Miquelon', '976': 'Mayotte',
        '977': 'Saint-Barthélemy', '978': 'Saint-Martin',
        '984': 'Terres australes et antarctiques françaises',
        '986': 'Wallis-et-Futuna', '987': 'Polynésie française', '988': 'Nouvelle-Calédonie',
        '989': 'Île de Clipperton'
    }
    
    return region_mapping.get(department, "Autre")

def analyze_geographic_distribution(df):
    """
    Analyse avancée de la distribution géographique des offres d'emploi.
    
    Args:
        df (pandas.DataFrame): DataFrame contenant les données
        
    Returns:
        str: Chemin vers la visualisation générée
    """
    try:
        logger.info("Analyse avancée de la distribution géographique des offres...")
        
        # Créer une copie du DataFrame pour éviter les modifications en place
        geo_df = df.copy()
        
        # Extraire les villes à partir de lieu_travail
        def extract_city_info(lieu_travail):
            try:
                lieu_dict = eval(lieu_travail) if isinstance(lieu_travail, str) else {}
                
                city_name = None
                city_code = None
                department = None
                
                # Extraire le nom de la ville et le code INSEE
                if 'libelle' in lieu_dict:
                    libelle = lieu_dict['libelle']
                    # Le format est "XX - NOM_VILLE"
                    if ' - ' in libelle:
                        city_name = libelle.split(' - ')[1]
                    else:
                        city_name = libelle  # Pour les cas comme "Belgique"
                
                if 'commune' in lieu_dict:
                    city_code = lieu_dict['commune']
                    department = extract_department_from_city(city_code)
                
                return pd.Series([city_name, city_code, department])
            except Exception:
                return pd.Series([None, None, None])
        
        # Appliquer la fonction d'extraction
        geo_df[['ville', 'code_insee', 'departement']] = geo_df['lieu_travail'].apply(extract_city_info)
        
        # Ajouter la région
        geo_df['region'] = geo_df['departement'].apply(extract_region_from_department)
        
        # Analyse par région
        region_counts = geo_df['region'].value_counts()
        
        # Filtrer pour ne garder que les régions significatives
        significant_regions = region_counts[region_counts > 10].index
        filtered_region_counts = region_counts[region_counts.index.isin(significant_regions)]
        
        # Créer la figure
        plt.figure(figsize=(16, 12))
        
        # Graphique des offres par région
        plt.subplot(2, 2, 1)
        sns.barplot(x=filtered_region_counts.values, y=filtered_region_counts.index, palette='viridis')
        plt.title('Nombre d\'offres d\'emploi par région')
        plt.xlabel('Nombre d\'offres')
        plt.ylabel('Région')
        
        # Analyse par département (top 20)
        dept_counts = geo_df['departement'].value_counts().head(20)
        
        # Créer des étiquettes avec code et nom du département
        dept_labels = [f"{code} - {get_department_name(code)}" for code in dept_counts.index]
        
        plt.subplot(2, 2, 2)
        sns.barplot(x=dept_counts.values, y=range(len(dept_labels)), palette='viridis')
        plt.yticks(range(len(dept_labels)), dept_labels)
        plt.title('Top 20 des départements avec le plus d\'offres')
        plt.xlabel('Nombre d\'offres')
        plt.ylabel('Département')
        
        # Analyse croisée: types de contrat par région
        if 'contract_type_std' in geo_df.columns:
            # Limiter aux types de contrats les plus courants pour la lisibilité
            top_contracts = geo_df['contract_type_std'].value_counts().nlargest(3).index
            
            # Filtrer pour ne garder que les régions et types de contrat significatifs
            filtered_geo_df = geo_df[
                (geo_df['region'].isin(significant_regions)) & 
                (geo_df['contract_type_std'].isin(top_contracts))
            ]
            
            # Créer un tableau croisé
            contract_region_cross = pd.crosstab(
                index=filtered_geo_df['region'],
                columns=filtered_geo_df['contract_type_std'],
                normalize='index'
            )
            
            plt.subplot(2, 2, 3)
            sns.heatmap(contract_region_cross, annot=True, cmap='YlGnBu', fmt='.1%')
            plt.title('Répartition des types de contrat par région')
            plt.ylabel('Région')
            plt.xlabel('Type de contrat')
        
        # Analyse croisée: technologies par région
        tech_columns = ['has_python', 'has_java', 'has_javascript', 'has_sql', 'has_aws', 'has_machine_learning']
        available_tech_columns = [col for col in tech_columns if col in geo_df.columns]
        
        if available_tech_columns:
            # Créer un DataFrame pour stocker les résultats
            tech_region_data = []
            
            for region in significant_regions:
                region_df = geo_df[geo_df['region'] == region]
                total_offers = len(region_df)
                
                for tech_col in available_tech_columns:
                    tech_name = tech_col.replace('has_', '').replace('_', ' ').title()
                    tech_count = region_df[tech_col].sum()
                    tech_proportion = tech_count / total_offers if total_offers > 0 else 0
                    
                    tech_region_data.append({
                        'Region': region,
                        'Technology': tech_name,
                        'Proportion': tech_proportion
                    })
            
            tech_region_df = pd.DataFrame(tech_region_data)
            
            # Créer un tableau croisé pour la visualisation
            pivot_df = tech_region_df.pivot(index='Region', columns='Technology', values='Proportion')
            
            plt.subplot(2, 2, 4)
            sns.heatmap(pivot_df, annot=True, cmap='YlGnBu', fmt='.1%')
            plt.title('Proportion des technologies par région')
            plt.ylabel('Région')
            plt.xlabel('Technologie')
        
        # Ajuster la mise en page
        plt.tight_layout()
        
        # Sauvegarder la figure
        os.makedirs("data/analysis/visualizations", exist_ok=True)
        output_path = "data/analysis/visualizations/geographic_analysis.png"
        plt.savefig(output_path, dpi=300, bbox_inches='tight')
        plt.close()
        
        # Générer des statistiques supplémentaires pour le logging
        logger.info(f"Nombre total de régions identifiées: {len(region_counts)}")
        logger.info(f"Nombre total de départements identifiés: {len(geo_df['departement'].value_counts())}")
        
        top_5_regions = region_counts.head(5)
        logger.info("Top 5 des régions avec le plus d'offres:")
        for region, count in top_5_regions.items():
            percentage = (count / len(geo_df)) * 100
            logger.info(f"  {region}: {count} offres ({percentage:.1f}%)")
        
        logger.info(f"Analyse géographique terminée, visualisation sauvegardée: {output_path}")
        return output_path
        
    except Exception as e:
        logger.error(f"Erreur lors de l'analyse géographique: {e}")
        return None

def setup_environment():
    """
    Configure les variables d'environnement nécessaires pour l'analyse.
    """
    # Charger les variables d'environnement depuis le fichier .env
    load_dotenv()
    
    # Vérifier que les variables essentielles sont définies
    required_vars = ['DB_HOST', 'DB_PORT', 'DB_NAME', 'DB_USER', 'DB_PASSWORD']
    
    missing_vars = [var for var in required_vars if not os.getenv(var)]
    if missing_vars:
        logger.error(f"Variables d'environnement manquantes: {', '.join(missing_vars)}")
        logger.error("Veuillez vérifier votre fichier .env")
        return False
    
    # Créer le dossier pour les visualisations
    os.makedirs("data/analysis/visualizations", exist_ok=True)
    
    logger.info("Environnement configuré avec succès")
    return True

def get_db_connection():
    """
    Établit une connexion à la base de données PostgreSQL.
    
    Returns:
        sqlalchemy.engine.Engine: Objet de connexion à la base de données
    """
    try:
        # Récupérer les informations de connexion depuis les variables d'environnement
        db_host = os.getenv('DB_HOST')
        db_port = os.getenv('DB_PORT')
        db_name = os.getenv('DB_NAME')
        db_user = os.getenv('DB_USER')
        db_password = os.getenv('DB_PASSWORD')
        
        # Construire l'URL de connexion
        db_url = f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
        
        # Créer le moteur de connexion
        engine = create_engine(db_url)
        
        # Tester la connexion
        with engine.connect() as conn:
            result = conn.execute(text("SELECT 1"))
            if result.scalar() == 1:
                logger.info("Connexion à la base de données établie avec succès")
                return engine
            else:
                logger.error("La connexion à la base de données a échoué")
                return None
    
    except Exception as e:
        logger.error(f"Erreur lors de la connexion à la base de données: {e}")
        return None

def load_data_from_db(engine):
    """
    Charge les données depuis la base de données.
    
    Args:
        engine (sqlalchemy.engine.Engine): Objet de connexion à la base de données
        
    Returns:
        pandas.DataFrame: DataFrame contenant les données chargées
    """
    try:
        # Requête SQL pour récupérer les données
        query = """
        SELECT * FROM france_travail_jobs
        """
        
        # Exécuter la requête et charger les résultats dans un DataFrame
        df = pd.read_sql(query, engine)
        
        logger.info(f"Données chargées avec succès: {len(df)} lignes")
        return df
    
    except Exception as e:
        logger.error(f"Erreur lors du chargement des données: {e}")
        return None

def parse_arguments():
    """
    Parse les arguments de ligne de commande.
    """
    parser = argparse.ArgumentParser(description='Analyse géographique des offres d\'emploi')
    parser.add_argument('--force', action='store_true',
                        help='Forcer l\'analyse même si les données existent déjà')
    return parser.parse_args()

def main():
    """
    Fonction principale qui orchestre l'analyse géographique.
    """
    start_time = datetime.now()
    logger.info(f"=== Démarrage de l'analyse géographique des offres d'emploi ===")
    logger.info(f"Date et heure de début: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    
    try:
        # Parser les arguments
        args = parse_arguments()
        
        # Configurer l'environnement
        if not setup_environment():
            logger.error("Échec de la configuration de l'environnement")
            return 1
        
        # Se connecter à la base de données
        engine = get_db_connection()
        if not engine:
            logger.error("Impossible de se connecter à la base de données")
            return 1
        
        # Charger les données
        df = load_data_from_db(engine)
        if df is None or df.empty:
            logger.error("Aucune donnée à analyser")
            return 1
        
        logger.info(f"Analyse géographique de {len(df)} offres d'emploi")
        
        # Analyse géographique
        geo_viz = analyze_geographic_distribution(df)
        
        # Fin de l'analyse
        end_time = datetime.now()
        duration = end_time - start_time
        
        logger.info(f"=== Analyse géographique terminée ===")
        logger.info(f"Date et heure de fin: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info(f"Durée totale: {duration}")
        
        if geo_viz:
            logger.info(f"Visualisation disponible à: {geo_viz}")
        
        return 0
        
    except Exception as e:
        logger.exception(f"Erreur lors de l'analyse géographique: {e}")
        return 1

if __name__ == "__main__":
    sys.exit(main())
