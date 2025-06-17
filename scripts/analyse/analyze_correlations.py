#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Module d'analyse des corrélations entre technologies et types de contrat.
"""

import os
import sys
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
import logging
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
        logging.FileHandler(f"logs/correlations_analysis_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def analyze_tech_contract_correlation(df):
    """
    Analyse la corrélation entre les technologies demandées et les types de contrat.
    
    Args:
        df (pandas.DataFrame): DataFrame contenant les données
        
    Returns:
        str: Chemin vers la visualisation générée
    """
    try:
        logger.info("Analyse de la corrélation entre technologies et types de contrat...")
        
        # Créer une copie du DataFrame pour éviter les modifications en place
        corr_df = df.copy()
        
        # Vérifier si les colonnes nécessaires existent
        tech_columns = ['has_python', 'has_java', 'has_javascript', 'has_sql', 'has_aws', 'has_machine_learning']
        missing_tech_columns = [col for col in tech_columns if col not in corr_df.columns]
        
        if 'contract_type_std' not in corr_df.columns:
            logger.warning("Colonne contract_type_std manquante")
            return None
        
        if missing_tech_columns:
            logger.warning(f"Colonnes de technologies manquantes: {missing_tech_columns}")
            # Utiliser uniquement les colonnes disponibles
            tech_columns = [col for col in tech_columns if col in corr_df.columns]
        
        if not tech_columns:
            logger.warning("Aucune colonne de technologie trouvée")
            return None
        
        # Filtrer pour ne garder que les types de contrat les plus courants
        top_contracts = corr_df['contract_type_std'].value_counts().nlargest(5).index
        corr_df = corr_df[corr_df['contract_type_std'].isin(top_contracts)]
        
        # Créer un tableau croisé pour chaque technologie et type de contrat
        result_data = []
        
        for tech_col in tech_columns:
            # Extraire le nom de la technologie (sans le préfixe 'has_')
            tech_name = tech_col.replace('has_', '').replace('_', ' ').title()
            
            # Calculer la proportion d'offres avec cette technologie pour chaque type de contrat
            for contract_type in top_contracts:
                # Nombre total d'offres pour ce type de contrat
                total_contract = len(corr_df[corr_df['contract_type_std'] == contract_type])
                
                # Nombre d'offres avec cette technologie pour ce type de contrat
                tech_contract = len(corr_df[(corr_df['contract_type_std'] == contract_type) & (corr_df[tech_col] == True)])
                
                # Calculer la proportion
                proportion = tech_contract / total_contract if total_contract > 0 else 0
                
                # Ajouter à la liste des données
                result_data.append({
                    'Technology': tech_name,
                    'ContractType': contract_type,
                    'Proportion': proportion,
                    'Count': tech_contract,
                    'Total': total_contract
                })
        
        # Convertir en DataFrame
        result_df = pd.DataFrame(result_data)
        
        # Créer un tableau croisé pour la visualisation
        pivot_df = result_df.pivot(index='Technology', columns='ContractType', values='Proportion')
        
        # Créer la figure
        plt.figure(figsize=(12, 10))
        
        # Créer la heatmap
        ax = plt.subplot(2, 1, 1)
        sns.heatmap(pivot_df, annot=True, cmap='YlGnBu', fmt='.1%', cbar_kws={'label': 'Proportion'})
        plt.title('Proportion des technologies par type de contrat')
        plt.ylabel('Technologie')
        plt.xlabel('Type de contrat')
        
        # Créer un graphique à barres empilées pour une autre perspective
        # Préparer les données
        # Utiliser uniquement les colonnes disponibles pour éviter l'erreur de longueur
        tech_columns_available = [col for col in tech_columns if col in corr_df.columns]
        
        tech_contract_counts = pd.crosstab(
            index=corr_df['contract_type_std'],
            columns=[corr_df[col] for col in tech_columns_available],
            rownames=['ContractType'],
            colnames=tech_columns_available
        )
        
        # Renommer les colonnes pour plus de clarté
        tech_contract_counts.columns = [col.replace('has_', '').replace('_', ' ').title() for col in tech_columns_available]
        
        # Normaliser pour obtenir des proportions
        tech_contract_proportions = tech_contract_counts.div(tech_contract_counts.sum(axis=1), axis=0)
        
        # Créer le graphique à barres empilées
        ax = plt.subplot(2, 1, 2)
        tech_contract_proportions.plot(kind='bar', stacked=True, ax=ax, colormap='tab10')
        plt.title('Répartition des technologies par type de contrat')
        plt.xlabel('Type de contrat')
        plt.ylabel('Proportion')
        plt.legend(title='Technologie', bbox_to_anchor=(1.05, 1), loc='upper left')
        plt.xticks(rotation=45)
        
        # Ajuster la mise en page
        plt.tight_layout()
        
        # Sauvegarder la figure
        os.makedirs("data/analysis/visualizations", exist_ok=True)
        output_path = "data/analysis/visualizations/tech_contract_correlation.png"
        plt.savefig(output_path, dpi=300, bbox_inches='tight')
        plt.close()
        
        logger.info(f"Analyse de corrélation terminée, visualisation sauvegardée: {output_path}")
        return output_path
        
    except Exception as e:
        logger.error(f"Erreur lors de l'analyse de corrélation: {e}")
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
    parser = argparse.ArgumentParser(description='Analyse des corrélations entre technologies et types de contrat')
    parser.add_argument('--force', action='store_true',
                        help='Forcer l\'analyse même si les données existent déjà')
    return parser.parse_args()

def main():
    """
    Fonction principale qui orchestre l'analyse des corrélations.
    """
    start_time = datetime.now()
    logger.info(f"=== Démarrage de l'analyse des corrélations entre technologies et types de contrat ===")
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
        
        logger.info(f"Analyse de {len(df)} offres d'emploi")
        
        # Analyse des corrélations
        correlation_viz = analyze_tech_contract_correlation(df)
        
        # Fin de l'analyse
        end_time = datetime.now()
        duration = end_time - start_time
        
        logger.info(f"=== Analyse terminée ===")
        logger.info(f"Date et heure de fin: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info(f"Durée totale: {duration}")
        
        if correlation_viz:
            logger.info(f"Visualisation disponible à: {correlation_viz}")
        
        return 0
        
    except Exception as e:
        logger.exception(f"Erreur lors de l'analyse des corrélations: {e}")
        return 1

if __name__ == "__main__":
    sys.exit(main())
