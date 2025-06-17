#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Module d'analyse de l'évolution temporelle des offres d'emploi France Travail.
"""

import os
import sys
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
import logging
import argparse
from datetime import datetime, timedelta
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
        logging.FileHandler(f"logs/temporal_analysis_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def analyze_temporal_evolution(df):
    """
    Analyse l'évolution temporelle des offres d'emploi.
    
    Args:
        df (pandas.DataFrame): DataFrame contenant les données
        
    Returns:
        str: Chemin vers la visualisation générée
    """
    try:
        logger.info("Analyse de l'évolution temporelle des offres...")
        
        # Créer une copie du DataFrame pour éviter les modifications en place
        temp_df = df.copy()
        
        # Vérifier si les colonnes de date existent
        date_columns = ['date_creation', 'date_actualisation']
        missing_columns = [col for col in date_columns if col not in temp_df.columns]
        
        if missing_columns:
            logger.warning(f"Colonnes de date manquantes: {missing_columns}")
            # Utiliser uniquement les colonnes disponibles
            date_columns = [col for col in date_columns if col in temp_df.columns]
        
        if not date_columns:
            logger.warning("Aucune colonne de date trouvée")
            return None
        
        # Convertir les colonnes de date en datetime
        for col in date_columns:
            try:
                temp_df[col] = pd.to_datetime(temp_df[col], errors='coerce')
            except Exception as e:
                logger.warning(f"Erreur lors de la conversion de la colonne {col} en datetime: {e}")
                date_columns.remove(col)
        
        if not date_columns:
            logger.warning("Aucune colonne de date valide après conversion")
            return None
        
        # Utiliser la première colonne de date disponible pour l'analyse
        date_col = date_columns[0]
        logger.info(f"Utilisation de la colonne {date_col} pour l'analyse temporelle")
        
        # Filtrer les lignes avec des dates valides
        temp_df = temp_df.dropna(subset=[date_col])
        
        if len(temp_df) == 0:
            logger.warning("Aucune donnée avec des dates valides")
            return None
        
        # Créer la figure
        plt.figure(figsize=(16, 12))
        
        # 1. Évolution du nombre d'offres par jour
        plt.subplot(2, 2, 1)
        
        # Grouper par jour
        daily_counts = temp_df.groupby(temp_df[date_col].dt.date).size()
        
        # Trier par date
        daily_counts = daily_counts.sort_index()
        
        # Tracer l'évolution
        sns.lineplot(x=daily_counts.index, y=daily_counts.values)
        plt.title('Évolution du nombre d\'offres par jour')
        plt.xlabel('Date')
        plt.ylabel('Nombre d\'offres')
        plt.xticks(rotation=45)
        
        # 2. Évolution du nombre d'offres par mois
        plt.subplot(2, 2, 2)
        
        # Créer une colonne pour le mois
        temp_df['month'] = temp_df[date_col].dt.to_period('M')
        
        # Grouper par mois
        monthly_counts = temp_df.groupby('month').size()
        
        # Convertir les périodes en dates pour le tracé
        month_dates = [pd.to_datetime(str(period)) for period in monthly_counts.index]
        
        # Tracer l'évolution
        sns.barplot(x=[date.strftime('%Y-%m') for date in month_dates], y=monthly_counts.values)
        plt.title('Évolution du nombre d\'offres par mois')
        plt.xlabel('Mois')
        plt.ylabel('Nombre d\'offres')
        plt.xticks(rotation=45)
        
        # 3. Évolution des types de contrat au fil du temps
        if 'contract_type_std' in temp_df.columns:
            plt.subplot(2, 2, 3)
            
            # Limiter aux types de contrats les plus courants pour la lisibilité
            top_contracts = temp_df['contract_type_std'].value_counts().nlargest(5).index
            
            # Filtrer pour ne garder que les types de contrat significatifs
            filtered_temp_df = temp_df[temp_df['contract_type_std'].isin(top_contracts)]
            
            # Créer une colonne pour le trimestre
            filtered_temp_df['quarter'] = filtered_temp_df[date_col].dt.to_period('Q')
            
            # Grouper par trimestre et type de contrat
            contract_evolution = filtered_temp_df.groupby(['quarter', 'contract_type_std']).size().unstack()
            
            # Convertir les périodes en dates pour le tracé
            quarter_dates = [pd.to_datetime(str(period)) for period in contract_evolution.index]
            
            # Tracer l'évolution
            contract_evolution.plot(kind='line', marker='o', ax=plt.gca())
            plt.title('Évolution des types de contrat par trimestre')
            plt.xlabel('Trimestre')
            plt.ylabel('Nombre d\'offres')
            plt.xticks(rotation=45)
            plt.legend(title='Type de contrat')
        
        # 4. Évolution des technologies au fil du temps
        tech_columns = ['has_python', 'has_java', 'has_javascript', 'has_sql', 'has_aws', 'has_machine_learning']
        available_tech_columns = [col for col in tech_columns if col in temp_df.columns]
        
        if available_tech_columns:
            plt.subplot(2, 2, 4)
            
            # Créer une colonne pour le trimestre si ce n'est pas déjà fait
            if 'quarter' not in temp_df.columns:
                temp_df['quarter'] = temp_df[date_col].dt.to_period('Q')
            
            # Créer un DataFrame pour stocker les résultats
            tech_evolution_data = []
            
            for quarter in sorted(temp_df['quarter'].unique()):
                quarter_df = temp_df[temp_df['quarter'] == quarter]
                total_offers = len(quarter_df)
                
                for tech_col in available_tech_columns:
                    tech_name = tech_col.replace('has_', '').replace('_', ' ').title()
                    tech_count = quarter_df[tech_col].sum()
                    tech_proportion = tech_count / total_offers if total_offers > 0 else 0
                    
                    tech_evolution_data.append({
                        'Quarter': quarter,
                        'Technology': tech_name,
                        'Count': tech_count,
                        'Proportion': tech_proportion
                    })
            
            tech_evolution_df = pd.DataFrame(tech_evolution_data)
            
            # Créer un tableau croisé pour la visualisation
            pivot_df = tech_evolution_df.pivot(index='Quarter', columns='Technology', values='Proportion')
            
            # Convertir les périodes en dates pour le tracé
            quarter_dates = [pd.to_datetime(str(period)) for period in pivot_df.index]
            
            # Tracer l'évolution
            pivot_df.plot(kind='line', marker='o', ax=plt.gca())
            plt.title('Évolution de la proportion des technologies par trimestre')
            plt.xlabel('Trimestre')
            plt.ylabel('Proportion')
            plt.xticks(rotation=45)
            plt.legend(title='Technologie', bbox_to_anchor=(1.05, 1), loc='upper left')
        
        # Ajuster la mise en page
        plt.tight_layout()
        
        # Sauvegarder la figure
        os.makedirs("data/analysis/visualizations", exist_ok=True)
        output_path = "data/analysis/visualizations/temporal_evolution.png"
        plt.savefig(output_path, dpi=300, bbox_inches='tight')
        plt.close()
        
        # Générer des statistiques supplémentaires pour le logging
        date_range = temp_df[date_col].max() - temp_df[date_col].min()
        logger.info(f"Période couverte par l'analyse: {date_range.days} jours")
        logger.info(f"Date la plus ancienne: {temp_df[date_col].min()}")
        logger.info(f"Date la plus récente: {temp_df[date_col].max()}")
        
        # Trouver le jour avec le plus d'offres
        if len(daily_counts) > 0:
            max_day = daily_counts.idxmax()
            max_count = daily_counts.max()
            logger.info(f"Jour avec le plus d'offres: {max_day} ({max_count} offres)")
        
        logger.info(f"Analyse temporelle terminée, visualisation sauvegardée: {output_path}")
        return output_path
        
    except Exception as e:
        logger.error(f"Erreur lors de l'analyse temporelle: {e}")
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
    parser = argparse.ArgumentParser(description='Analyse temporelle des offres d\'emploi')
    parser.add_argument('--force', action='store_true',
                        help='Forcer l\'analyse même si les données existent déjà')
    return parser.parse_args()

def main():
    """
    Fonction principale qui orchestre l'analyse temporelle.
    """
    start_time = datetime.now()
    logger.info(f"=== Démarrage de l'analyse temporelle des offres d'emploi ===")
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
        
        logger.info(f"Analyse temporelle de {len(df)} offres d'emploi")
        
        # Analyse temporelle
        temporal_viz = analyze_temporal_evolution(df)
        
        # Fin de l'analyse
        end_time = datetime.now()
        duration = end_time - start_time
        
        logger.info(f"=== Analyse temporelle terminée ===")
        logger.info(f"Date et heure de fin: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info(f"Durée totale: {duration}")
        
        if temporal_viz:
            logger.info(f"Visualisation disponible à: {temporal_viz}")
        
        return 0
        
    except Exception as e:
        logger.exception(f"Erreur lors de l'analyse temporelle: {e}")
        return 1

if __name__ == "__main__":
    sys.exit(main())
