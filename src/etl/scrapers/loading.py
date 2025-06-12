#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Module de chargement de données pour le pipeline ETL.
Gère le chargement des données transformées vers différentes destinations.
"""

import os
import json
import logging
import boto3
import pandas as pd
import psycopg2
from datetime import datetime
from dotenv import load_dotenv

# Charger les variables d'environnement
load_dotenv()

# Configuration du logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
)
logger = logging.getLogger(__name__)

def load_to_s3(data, bucket_name, s3_path):
    """
    Charge des données vers S3.
    
    Args:
        data: Données à charger (DataFrame ou dict/list)
        bucket_name (str): Nom du bucket S3
        s3_path (str): Chemin dans S3
    
    Returns:
        bool: True si le chargement a réussi, False sinon
    """
    try:
        # Charger les identifiants AWS
        aws_access_key = os.getenv('KEY_ACCESS')
        aws_secret_key = os.getenv('KEY_SECRET')
        
        if not aws_access_key or not aws_secret_key:
            logger.error("Identifiants AWS manquants. Vérifiez les variables d'environnement KEY_ACCESS et KEY_SECRET.")
            return False
        
        # Créer le client S3
        s3_client = boto3.client(
            's3',
            aws_access_key_id=aws_access_key,
            aws_secret_access_key=aws_secret_key
        )
        
        # Convertir les données en JSON
        if isinstance(data, pd.DataFrame):
            json_data = data.to_json(orient='records', date_format='iso')
        else:
            json_data = json.dumps(data, ensure_ascii=False)
        
        # Charger les données vers S3
        s3_client.put_object(
            Bucket=bucket_name,
            Key=s3_path,
            Body=json_data.encode('utf-8'),
            ContentType='application/json'
        )
        
        logger.info(f"Données chargées avec succès vers s3://{bucket_name}/{s3_path}")
        return True
    
    except Exception as e:
        logger.error(f"Erreur lors du chargement vers S3: {e}")
        return False

def get_rds_connection():
    """
    Établit une connexion à la base de données RDS PostgreSQL.
    
    Returns:
        psycopg2.connection: Connexion à la base de données
    """
    try:
        # Récupérer les paramètres de connexion
        host = os.getenv('DB_HOST')
        port = os.getenv('DB_PORT')
        dbname = os.getenv('DB_NAME')
        user = os.getenv('DB_USER')
        password = os.getenv('DB_PASSWORD')
        
        # Établir la connexion
        connection = psycopg2.connect(
            host=host,
            port=port,
            dbname=dbname,
            user=user,
            password=password
        )
        
        logger.info(f"Connexion établie avec succès à la base de données {dbname} sur {host}")
        return connection
    except Exception as e:
        logger.error(f"Erreur lors de la connexion à la base de données RDS: {e}")
        return None

def load_jobs_to_rds(jobs_df):
    """
    Charge les offres d'emploi dans la base de données RDS.
    
    Args:
        jobs_df (pandas.DataFrame): DataFrame contenant les offres d'emploi
    
    Returns:
        bool: True si le chargement a réussi, False sinon
    """
    # Établir la connexion
    connection = get_rds_connection()
    if not connection:
        return False
    
    try:
        # Créer un curseur
        cursor = connection.cursor()
        
        # Créer la table si elle n'existe pas
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS jobs (
                id SERIAL PRIMARY KEY,
                title VARCHAR(255) NOT NULL,
                company VARCHAR(255),
                location VARCHAR(255),
                contract_type VARCHAR(50),
                description TEXT,
                url VARCHAR(512) UNIQUE,
                source VARCHAR(50),
                scraped_at TIMESTAMP,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Insérer les données
        inserted_count = 0
        for _, row in jobs_df.iterrows():
            try:
                cursor.execute("""
                    INSERT INTO jobs (title, company, location, contract_type, description, url, source, scraped_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (url) DO UPDATE
                    SET title = EXCLUDED.title,
                        company = EXCLUDED.company,
                        location = EXCLUDED.location,
                        contract_type = EXCLUDED.contract_type,
                        description = EXCLUDED.description,
                        source = EXCLUDED.source,
                        scraped_at = EXCLUDED.scraped_at
                """, (
                    row.get('title', ''),
                    row.get('company', ''),
                    row.get('location', ''),
                    row.get('contract_type', ''),
                    row.get('description', ''),
                    row.get('url', ''),
                    row.get('source', ''),
                    row.get('scraped_at', datetime.now().isoformat())
                ))
                inserted_count += 1
            except Exception as e:
                logger.error(f"Erreur lors de l'insertion de l'offre {row.get('url')}: {e}")
        
        # Valider les modifications
        connection.commit()
        
        logger.info(f"{inserted_count} offres d'emploi chargées avec succès dans la base de données")
        return True
    
    except Exception as e:
        logger.error(f"Erreur lors du chargement dans la base de données RDS: {e}")
        connection.rollback()
        return False
    
    finally:
        # Fermer la connexion
        if connection:
            connection.close()

def load_skills_to_rds(jobs_df):
    """
    Charge les compétences extraites dans la base de données RDS.
    
    Args:
        jobs_df (pandas.DataFrame): DataFrame contenant les offres d'emploi avec leurs compétences
    
    Returns:
        bool: True si le chargement a réussi, False sinon
    """
    # Établir la connexion
    connection = get_rds_connection()
    if not connection:
        return False
    
    try:
        # Créer un curseur
        cursor = connection.cursor()
        
        # Créer les tables si elles n'existent pas
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS skills (
                id SERIAL PRIMARY KEY,
                name VARCHAR(255) UNIQUE NOT NULL,
                category VARCHAR(50),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS job_skills (
                job_id INTEGER REFERENCES jobs(id),
                skill_id INTEGER REFERENCES skills(id),
                PRIMARY KEY (job_id, skill_id)
            )
        """)
        
        # Extraire toutes les compétences uniques
        all_skills = set()
        for skills_list in jobs_df['skills']:
            if isinstance(skills_list, list):
                all_skills.update(skills_list)
        
        # Insérer les compétences
        for skill in all_skills:
            try:
                cursor.execute("""
                    INSERT INTO skills (name)
                    VALUES (%s)
                    ON CONFLICT (name) DO NOTHING
                """, (skill,))
            except Exception as e:
                logger.error(f"Erreur lors de l'insertion de la compétence {skill}: {e}")
        
        # Récupérer les IDs des compétences
        cursor.execute("SELECT id, name FROM skills")
        skill_ids = {row[1]: row[0] for row in cursor.fetchall()}
        
        # Récupérer les IDs des offres d'emploi
        job_ids = {}
        for _, row in jobs_df.iterrows():
            url = row.get('url', '')
            if url:
                cursor.execute("SELECT id FROM jobs WHERE url = %s", (url,))
                result = cursor.fetchone()
                if result:
                    job_ids[url] = result[0]
        
        # Insérer les relations entre offres et compétences
        for _, row in jobs_df.iterrows():
            url = row.get('url', '')
            if url in job_ids:
                job_id = job_ids[url]
                skills_list = row.get('skills', [])
                if isinstance(skills_list, list):
                    for skill in skills_list:
                        if skill in skill_ids:
                            try:
                                cursor.execute("""
                                    INSERT INTO job_skills (job_id, skill_id)
                                    VALUES (%s, %s)
                                    ON CONFLICT (job_id, skill_id) DO NOTHING
                                """, (job_id, skill_ids[skill]))
                            except Exception as e:
                                logger.error(f"Erreur lors de l'insertion de la relation entre l'offre {job_id} et la compétence {skill}: {e}")
        
        # Valider les modifications
        connection.commit()
        
        logger.info(f"{len(all_skills)} compétences chargées avec succès dans la base de données")
        return True
    
    except Exception as e:
        logger.error(f"Erreur lors du chargement des compétences dans la base de données RDS: {e}")
        connection.rollback()
        return False
    
    finally:
        # Fermer la connexion
        if connection:
            connection.close()

def save_to_local(data, filename):
    """
    Sauvegarde des données dans un fichier local.
    
    Args:
        data: Données à sauvegarder (DataFrame ou dict/list)
        filename (str): Nom du fichier
    
    Returns:
        bool: True si la sauvegarde a réussi, False sinon
    """
    try:
        # Créer le dossier si nécessaire
        os.makedirs(os.path.dirname(filename), exist_ok=True)
        
        # Sauvegarder les données
        if isinstance(data, pd.DataFrame):
            if filename.endswith('.csv'):
                data.to_csv(filename, index=False, encoding='utf-8')
            elif filename.endswith('.json'):
                data.to_json(filename, orient='records', date_format='iso', force_ascii=False, indent=4)
            elif filename.endswith('.xlsx'):
                data.to_excel(filename, index=False)
            else:
                logger.error(f"Format de fichier non pris en charge: {filename}")
                return False
        else:
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=4)
        
        logger.info(f"Données sauvegardées avec succès dans {filename}")
        return True
    
    except Exception as e:
        logger.error(f"Erreur lors de la sauvegarde dans le fichier local: {e}")
        return False

if __name__ == "__main__":
    # Test de chargement
    from extraction import extract_welcome_jungle_data
    from transformation import transform_to_dataframe
    
    # Extraire les données
    jobs_data = extract_welcome_jungle_data()
    
    # Transformer les données
    df = transform_to_dataframe(jobs_data)
    
    # Sauvegarder en local
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    local_path = f"processed/jobs/processed_jobs_{timestamp}.json"
    save_to_local(df, local_path)
    
    # Charger vers S3
    bucket_name = os.getenv('data_lake_bucket', 'data-lake-brut')
    s3_path = f"processed/jobs/processed_jobs_{timestamp}.json"
    load_to_s3(df, bucket_name, s3_path)
    
    # Essayer de charger dans RDS
    try:
        load_jobs_to_rds(df)
        load_skills_to_rds(df)
    except Exception as e:
        logger.error(f"Erreur lors du chargement dans RDS: {e}")
