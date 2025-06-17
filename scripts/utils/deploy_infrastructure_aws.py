#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Script de déploiement et test de l'infrastructure AWS (S3, RDS, Lambda).
Permet de vérifier, configurer et déployer l'infrastructure nécessaire au projet.
"""

import os
import sys
import logging
import argparse
import boto3
from botocore.exceptions import ClientError
import psycopg2
import json
import time
import shutil
import tempfile
import zipfile
import subprocess
import base64
from datetime import datetime
from dotenv import load_dotenv

# Ajouter le répertoire parent au chemin Python pour importer les modules du projet
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Importer les modules d'infrastructure si nécessaire
# Nous implémentons directement les fonctions dans ce script

# Charger les variables d'environnement
load_dotenv()

# Configuration du logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f"logs/deploy_infrastructure_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Variables globales pour les identifiants AWS
AWS_ACCESS_KEY = os.getenv('KEY_ACCESS')
AWS_SECRET_KEY = os.getenv('KEY_SECRET')
S3_BUCKET_NAME = os.getenv('data_lake_bucket', 'data-lake-brut')
# Variables d'environnement pour RDS
RDS_HOST = os.getenv('DB_HOST')
RDS_PORT = int(os.getenv('DB_PORT', '5432'))
RDS_DBNAME = os.getenv('DB_NAME')
RDS_USER = os.getenv('DB_USER')
RDS_PASSWORD = os.getenv('DB_PASSWORD')

#
# Fonctions pour AWS S3
#

def test_s3_connection():
    """Teste la connexion à AWS S3."""
    try:
        s3_client = get_s3_client()
        if not s3_client:
            return False
        
        # Tester la connexion en listant les buckets
        response = s3_client.list_buckets()
        buckets = response.get('Buckets', [])
        
        logger.info(f"Connexion S3 réussie ! {len(buckets)} buckets trouvés.")
        return True
    
    except ClientError as e:
        logger.error(f"Erreur de connexion S3: {e}")
        return False
    
    except Exception as e:
        logger.error(f"Erreur inattendue: {e}")
        return False

def get_s3_client():
    """Crée et retourne un client S3 avec les identifiants AWS."""
    try:
        # Créer le client S3
        s3_client = boto3.client(
            's3',
            aws_access_key_id=AWS_ACCESS_KEY,
            aws_secret_access_key=AWS_SECRET_KEY
        )
        return s3_client
    except Exception as e:
        logger.error(f"Erreur lors de la création du client S3: {e}")
        return None

def setup_s3_bucket(bucket_name=S3_BUCKET_NAME, create_if_not_exists=True):
    """Configure le bucket S3 pour le projet."""
    try:
        s3_client = get_s3_client()
        if not s3_client:
            return False
        
        # Vérifier si le bucket existe
        try:
            s3_client.head_bucket(Bucket=bucket_name)
            logger.info(f"Le bucket '{bucket_name}' existe.")
            bucket_exists = True
        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == '404':
                logger.warning(f"Le bucket '{bucket_name}' n'existe pas.")
                bucket_exists = False
            else:
                logger.error(f"Erreur lors de la vérification du bucket: {e}")
                return False
        
        # Créer le bucket s'il n'existe pas
        if not bucket_exists and create_if_not_exists:
            try:
                s3_client.create_bucket(Bucket=bucket_name)
                logger.info(f"Bucket '{bucket_name}' créé avec succès !")
                bucket_exists = True
            except ClientError as e:
                logger.error(f"Erreur lors de la création du bucket: {e}")
                return False
        
        # Si le bucket existe, configurer les dossiers
        if bucket_exists:
            # Créer les dossiers nécessaires (les dossiers sont virtuels dans S3)
            folders = [
                'raw/welcome_jungle/',
                'raw/pole_emploi/',
                'processed/welcome_jungle/',
                'processed/pole_emploi/',
                'reports/'
            ]
            
            for folder in folders:
                try:
                    s3_client.put_object(Bucket=bucket_name, Key=folder)
                    logger.info(f"Dossier '{folder}' créé dans le bucket '{bucket_name}'.")
                except ClientError as e:
                    logger.error(f"Erreur lors de la création du dossier '{folder}': {e}")
            
            # Configurer la politique de cycle de vie (optionnel)
            try:
                lifecycle_config = {
                    'Rules': [
                        {
                            'ID': 'archive-raw-data',
                            'Status': 'Enabled',
                            'Prefix': 'raw/',
                            'Transitions': [
                                {
                                    'Days': 90,
                                    'StorageClass': 'STANDARD_IA'
                                }
                            ]
                        }
                    ]
                }
                s3_client.put_bucket_lifecycle_configuration(
                    Bucket=bucket_name,
                    LifecycleConfiguration=lifecycle_config
                )
                logger.info(f"Politique de cycle de vie configurée pour le bucket '{bucket_name}'.")
            except ClientError as e:
                logger.warning(f"Erreur lors de la configuration de la politique de cycle de vie: {e}")
            
            return True
        
        return False
    
    except Exception as e:
        logger.error(f"Erreur lors de la configuration du bucket S3: {e}")
        return False

def test_s3_upload(bucket_name=S3_BUCKET_NAME):
    """Teste l'upload d'un fichier vers S3."""
    try:
        s3_client = get_s3_client()
        if not s3_client:
            return False
        
        # Créer un fichier de test
        test_file = "test_s3_upload.json"
        test_data = {
            "test": True,
            "timestamp": datetime.now().isoformat(),
            "message": "Test de connexion S3 réussi !"
        }
        
        with open(test_file, 'w', encoding='utf-8') as f:
            json.dump(test_data, f, ensure_ascii=False, indent=4)
        
        # Uploader le fichier
        s3_key = f"tests/test_s3_upload_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        s3_client.upload_file(test_file, bucket_name, s3_key)
        
        # Supprimer le fichier local
        os.remove(test_file)
        
        logger.info(f"Fichier de test uploadé avec succès: s3://{bucket_name}/{s3_key}")
        return True
    
    except Exception as e:
        logger.error(f"Erreur lors de l'upload du fichier de test: {e}")
        return False

def list_s3_files(bucket_name=S3_BUCKET_NAME, prefix=""):
    """Liste les fichiers dans un bucket S3."""
    try:
        s3_client = get_s3_client()
        if not s3_client:
            return False
        
        # Lister les fichiers
        response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
        
        if 'Contents' not in response:
            logger.info(f"Aucun fichier trouvé dans s3://{bucket_name}/{prefix}")
            return True
        
        files = response['Contents']
        logger.info(f"Fichiers dans s3://{bucket_name}/{prefix} ({len(files)}):")
        
        for file in files:
            size_mb = file['Size'] / (1024 * 1024)
            logger.info(f"  - {file['Key']} ({size_mb:.2f} MB, modifié le {file['LastModified']})")
        
        return True
    
    except Exception as e:
        logger.error(f"Erreur lors de la liste des fichiers: {e}")
        return False

#
# Fonctions pour AWS RDS
#

def get_rds_connection(host=RDS_HOST, port=RDS_PORT, dbname=RDS_DBNAME, user=RDS_USER, password=RDS_PASSWORD, timeout=10):
    """Crée et retourne une connexion à la base de données RDS."""
    try:
        # Connexion à la base de données
        conn = psycopg2.connect(
            host=host,
            port=port,
            dbname=dbname,
            user=user,
            password=password,
            connect_timeout=timeout
        )
        return conn
    except Exception as e:
        logger.error(f"Erreur lors de la connexion à RDS: {e}")
        return None

def test_rds_connection(host=RDS_HOST, port=RDS_PORT, dbname=RDS_DBNAME, user=RDS_USER, password=RDS_PASSWORD, timeout=10):
    """Teste la connexion à la base de données PostgreSQL."""
    try:
        logger.info(f"Tentative de connexion à {host}:{port}/{dbname} avec l'utilisateur {user}...")
        
        # Connexion à la base de données
        conn = psycopg2.connect(
            host=host,
            port=port,
            dbname=dbname,
            user=user,
            password=password,
            connect_timeout=timeout
        )
        
        # Vérifier la connexion
        with conn.cursor() as cursor:
            cursor.execute("SELECT version();")
            version = cursor.fetchone()
            logger.info(f"Connexion RDS réussie ! Version PostgreSQL: {version[0]}")
            
            # Lister les tables
            cursor.execute("""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = 'public'
                ORDER BY table_name;
            """)
            tables = cursor.fetchall()
            
            if tables:
                logger.info("Tables existantes:")
                for table in tables:
                    logger.info(f"  - {table[0]}")
            else:
                logger.info("Aucune table n'existe dans le schéma public.")
        
        conn.close()
        return True
    
    except psycopg2.OperationalError as e:
        error_message = str(e)
        logger.error(f"Erreur de connexion RDS: {error_message}")
        
        if "timeout" in error_message.lower() or "timed out" in error_message.lower():
            logger.error("ERREUR: La base de données n'est pas accessible publiquement.")
            logger.info("Votre base de données RDS est configurée pour être accessible uniquement depuis le VPC AWS.")
            logger.info("Solutions possibles:")
            logger.info("1. Modifier les règles de sécurité de votre RDS pour autoriser l'accès public")
            logger.info("2. Utiliser un tunnel SSH via une instance EC2 dans le même VPC")
            logger.info("3. Utiliser AWS Systems Manager Session Manager pour accéder à la base de données")
            logger.info("4. Déployer votre application dans le même VPC que la base de données")
        elif "password authentication failed" in error_message.lower():
            logger.error("ERREUR: Authentification échouée. Vérifiez vos identifiants.")
        else:
            logger.info("Causes possibles:")
            logger.info("1. La base de données n'est pas accessible publiquement (problème de sécurité réseau)")
            logger.info("2. Les identifiants sont incorrects")
            logger.info("3. La base de données n'existe pas ou n'est pas démarrée")
            logger.info("4. Le port est bloqué par un pare-feu")
        return False
    
    except Exception as e:
        logger.error(f"Erreur inattendue: {e}")
        logger.error("Vérifiez que le mot de passe ne contient pas de caractères spéciaux qui pourraient causer des problèmes d'encodage.")
        return False

def setup_rds_database(host=RDS_HOST, port=RDS_PORT, dbname=RDS_DBNAME, user=RDS_USER, password=RDS_PASSWORD, timeout=10):
    """Configure la base de données RDS pour le projet."""
    try:
        # Connexion à la base de données
        conn = psycopg2.connect(
            host=host,
            port=port,
            dbname=dbname,
            user=user,
            password=password,
            connect_timeout=timeout
        )
        conn.autocommit = True
        
        # Créer les tables nécessaires
        with conn.cursor() as cursor:
            # Vérifier si les tables existent déjà
            cursor.execute("""
                SELECT table_name FROM information_schema.tables 
                WHERE table_schema = 'public' AND table_type = 'BASE TABLE'
            """)
            existing_tables = [row[0] for row in cursor.fetchall()]
            logger.info(f"Tables existantes: {existing_tables}")
            
            # Table france_travail_jobs
            if 'france_travail_jobs' not in existing_tables:
                logger.info("Création de la table france_travail_jobs...")
                cursor.execute("""
                    CREATE TABLE france_travail_jobs (
                        id VARCHAR(255) PRIMARY KEY,
                        title TEXT,
                        description TEXT,
                        company_name TEXT,
                        company_description TEXT,
                        contract_type TEXT,
                        contract_type_std TEXT,
                        contract_duration TEXT,
                        location TEXT,
                        city TEXT,
                        region TEXT,
                        country TEXT,
                        publication_date DATE,
                        experience_level TEXT,
                        salary TEXT,
                        has_python BOOLEAN,
                        has_java BOOLEAN,
                        has_javascript BOOLEAN,
                        has_csharp BOOLEAN,
                        has_cpp BOOLEAN,
                        has_php BOOLEAN,
                        has_ruby BOOLEAN,
                        has_sql BOOLEAN,
                        has_nosql BOOLEAN,
                        has_aws BOOLEAN,
                        has_azure BOOLEAN,
                        has_gcp BOOLEAN
                    );
                """)
                logger.info("Table france_travail_jobs créée avec succès !")
            else:
                logger.info("La table france_travail_jobs existe déjà.")
            
            # Table welcome_jungle_jobs
            if 'welcome_jungle_jobs' not in existing_tables:
                logger.info("Création de la table welcome_jungle_jobs...")
                cursor.execute("""
                    CREATE TABLE welcome_jungle_jobs (
                        id SERIAL PRIMARY KEY,
                        title TEXT,
                        company_name TEXT,
                        location TEXT,
                        city TEXT,
                        region TEXT,
                        country TEXT,
                        contract_type TEXT,
                        contract_type_std TEXT,
                        publication_date DATE,
                        description TEXT,
                        has_python BOOLEAN,
                        has_java BOOLEAN,
                        has_javascript BOOLEAN,
                        has_csharp BOOLEAN,
                        has_cpp BOOLEAN,
                        has_php BOOLEAN,
                        has_ruby BOOLEAN,
                        has_sql BOOLEAN,
                        has_nosql BOOLEAN,
                        has_aws BOOLEAN,
                        has_azure BOOLEAN,
                        has_gcp BOOLEAN
                    );
                """)
                logger.info("Table welcome_jungle_jobs créée avec succès !")
            else:
                logger.info("La table welcome_jungle_jobs existe déjà.")
            
            # Table job_skills
            if 'job_skills' not in existing_tables:
                logger.info("Création de la table job_skills...")
                cursor.execute("""
                    CREATE TABLE job_skills (
                        id SERIAL PRIMARY KEY,
                        job_id VARCHAR(255),
                        skill_name TEXT,
                        source VARCHAR(50),
                        FOREIGN KEY (job_id) REFERENCES france_travail_jobs(id)
                    );
                """)
                logger.info("Table job_skills créée avec succès !")
            else:
                logger.info("La table job_skills existe déjà.")
            
            # Créer des index pour améliorer les performances
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_ft_contract_type ON france_travail_jobs(contract_type_std);")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_ft_city ON france_travail_jobs(city);")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_wj_contract_type ON welcome_jungle_jobs(contract_type_std);")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_wj_city ON welcome_jungle_jobs(city);")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_skills_job_id ON job_skills(job_id);")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_skills_name ON job_skills(skill_name);")
            
            logger.info("Tables et index créés avec succès !")
        
        conn.close()
        return True
    
    except Exception as e:
        logger.error(f"Erreur lors de la configuration de la base de données: {e}")
        return False

#
# Fonction principale
#

#
# Fonctions pour AWS Lambda
#

def create_lambda_role(role_name="job-offers-lambda-role"):
    """Crée un rôle IAM pour la fonction Lambda avec les autorisations nécessaires."""
    try:
        # Créer un client IAM
        iam_client = boto3.client(
            'iam',
            aws_access_key_id=AWS_ACCESS_KEY,
            aws_secret_access_key=AWS_SECRET_KEY
        )
        
        # Vérifier si le rôle existe déjà
        try:
            response = iam_client.get_role(RoleName=role_name)
            logger.info(f"Le rôle IAM '{role_name}' existe déjà.")
            return response['Role']['Arn']
        except iam_client.exceptions.NoSuchEntityException:
            logger.info(f"Création du rôle IAM '{role_name}'...")
        
        # Document de politique d'approbation pour Lambda
        trust_policy = {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Principal": {"Service": "lambda.amazonaws.com"},
                    "Action": "sts:AssumeRole"
                }
            ]
        }
        
        # Créer le rôle
        response = iam_client.create_role(
            RoleName=role_name,
            AssumeRolePolicyDocument=json.dumps(trust_policy),
            Description="Rôle pour les fonctions Lambda de collecte d'offres d'emploi"
        )
        
        # Attacher les politiques nécessaires
        policies = [
            "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole",  # CloudWatch Logs
            "arn:aws:iam::aws:policy/AmazonS3FullAccess",  # S3
            "arn:aws:iam::aws:policy/AmazonRDSDataFullAccess"  # RDS
        ]
        
        for policy_arn in policies:
            iam_client.attach_role_policy(
                RoleName=role_name,
                PolicyArn=policy_arn
            )
            logger.info(f"Politique {policy_arn} attachée au rôle {role_name}")
        
        # Attendre que le rôle soit disponible (propagation IAM)
        logger.info("Attente de la propagation du rôle IAM (10 secondes)...")
        time.sleep(10)
        
        return response['Role']['Arn']
    
    except Exception as e:
        logger.error(f"Erreur lors de la création du rôle IAM: {e}")
        return None

def package_lambda_function(source_dir, output_path):
    """Crée un package de déploiement pour la fonction Lambda."""
    try:
        # Créer un répertoire temporaire
        with tempfile.TemporaryDirectory() as temp_dir:
            # Copier les fichiers source
            for item in os.listdir(source_dir):
                s = os.path.join(source_dir, item)
                d = os.path.join(temp_dir, item)
                if os.path.isdir(s):
                    shutil.copytree(s, d)
                else:
                    shutil.copy2(s, d)
            
            # Installer les dépendances dans le répertoire temporaire
            requirements_file = os.path.join(source_dir, 'requirements.txt')
            if os.path.exists(requirements_file):
                logger.info("Installation des dépendances pour Lambda...")
                subprocess.check_call([
                    sys.executable, '-m', 'pip', 'install',
                    '-r', requirements_file,
                    '--target', temp_dir,
                    '--quiet'
                ])
            
            # Créer l'archive ZIP
            with zipfile.ZipFile(output_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
                for root, _, files in os.walk(temp_dir):
                    for file in files:
                        file_path = os.path.join(root, file)
                        arcname = os.path.relpath(file_path, temp_dir)
                        zipf.write(file_path, arcname)
        
        logger.info(f"Package Lambda créé: {output_path}")
        return True
    
    except Exception as e:
        logger.error(f"Erreur lors de la création du package Lambda: {e}")
        return False

def deploy_lambda_function(function_name, zip_file, handler, role_arn, timeout=300, memory=256):
    """Déploie une fonction Lambda sur AWS."""
    try:
        # Lire le contenu du fichier ZIP
        with open(zip_file, 'rb') as f:
            zip_bytes = f.read()
        
        # Créer un client Lambda
        lambda_client = boto3.client(
            'lambda',
            aws_access_key_id=AWS_ACCESS_KEY,
            aws_secret_access_key=AWS_SECRET_KEY,
            region_name='eu-north-1'  # Région Stockholm
        )
        
        # Vérifier si la fonction existe déjà
        try:
            lambda_client.get_function(FunctionName=function_name)
            logger.info(f"La fonction Lambda '{function_name}' existe déjà. Mise à jour...")
            
            # Mettre à jour la fonction existante
            response = lambda_client.update_function_code(
                FunctionName=function_name,
                ZipFile=zip_bytes,
                Publish=True
            )
            
            # Mettre à jour la configuration
            lambda_client.update_function_configuration(
                FunctionName=function_name,
                Role=role_arn,
                Handler=handler,
                Timeout=timeout,
                MemorySize=memory
            )
        
        except lambda_client.exceptions.ResourceNotFoundException:
            logger.info(f"Création de la fonction Lambda '{function_name}'...")
            
            # Créer une nouvelle fonction
            response = lambda_client.create_function(
                FunctionName=function_name,
                Runtime='python3.9',
                Role=role_arn,
                Handler=handler,
                Code={'ZipFile': zip_bytes},
                Timeout=timeout,
                MemorySize=memory,
                Publish=True,
                Environment={
                    'Variables': {
                        'data_lake_bucket': S3_BUCKET_NAME,
                        'DB_HOST': RDS_HOST,
                        'DB_PORT': str(RDS_PORT),
                        'DB_NAME': RDS_DBNAME,
                        'DB_USER': RDS_USER,
                        'DB_PASSWORD': RDS_PASSWORD
                    }
                }
            )
        
        logger.info(f"Fonction Lambda '{function_name}' déployée avec succès !")
        logger.info(f"ARN: {response['FunctionArn']}")
        return response['FunctionArn']
    
    except Exception as e:
        logger.error(f"Erreur lors du déploiement de la fonction Lambda: {e}")
        return None

def test_lambda_invocation(function_name, payload={}):
    """Teste l'invocation d'une fonction Lambda."""
    try:
        # Créer un client Lambda
        lambda_client = boto3.client(
            'lambda',
            aws_access_key_id=AWS_ACCESS_KEY,
            aws_secret_access_key=AWS_SECRET_KEY,
            region_name='eu-north-1'  # Région Stockholm
        )
        
        # Invoquer la fonction
        logger.info(f"Invocation de la fonction Lambda '{function_name}'...")
        response = lambda_client.invoke(
            FunctionName=function_name,
            InvocationType='RequestResponse',  # Synchrone
            LogType='Tail',  # Inclure les logs
            Payload=json.dumps(payload)
        )
        
        # Décoder les logs
        logs = base64.b64decode(response['LogResult']).decode('utf-8')
        logger.info(f"Logs de l'invocation:\n{logs}")
        
        # Obtenir la réponse
        if 'Payload' in response:
            payload_stream = response['Payload']
            payload_bytes = payload_stream.read()
            payload_str = payload_bytes.decode('utf-8')
            logger.info(f"Réponse de la fonction Lambda:\n{payload_str}")
        
        return True
    
    except Exception as e:
        logger.error(f"Erreur lors de l'invocation de la fonction Lambda: {e}")
        return False

def deploy_welcome_jungle_lambda():
    """Déploie la fonction Lambda pour le scraper Welcome to the Jungle."""
    try:
        # Créer le rôle IAM
        role_name = "welcome-jungle-lambda-role"
        role_arn = create_lambda_role(role_name)
        if not role_arn:
            logger.error("Impossible de créer le rôle IAM pour Lambda.")
            return False
        
        # Chemin du code source
        src_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'src')
        
        # Créer le package de déploiement
        os.makedirs('build', exist_ok=True)
        zip_file = os.path.join('build', 'welcome_jungle_lambda.zip')
        if not package_lambda_function(src_dir, zip_file):
            logger.error("Impossible de créer le package de déploiement.")
            return False
        
        # Déployer la fonction Lambda
        function_name = "welcome-jungle-scraper"
        handler = "lambda_handlers.welcome_jungle_handler.handler"
        lambda_arn = deploy_lambda_function(function_name, zip_file, handler, role_arn)
        if not lambda_arn:
            logger.error("Impossible de déployer la fonction Lambda.")
            return False
        
        logger.info(f"Fonction Lambda '{function_name}' déployée avec succès !")
        return True
    
    except Exception as e:
        logger.error(f"Erreur lors du déploiement de la fonction Lambda: {e}")
        return False

def parse_arguments():
    """Parse les arguments de ligne de commande."""
    parser = argparse.ArgumentParser(description='Déploiement et test de l\'infrastructure AWS')
    
    # Options générales
    parser.add_argument('--service', choices=['s3', 'rds', 'lambda', 'all'], default='all',
                        help='Service AWS à tester ou déployer (s3, rds, lambda, all)')
    
    # Options S3
    parser.add_argument('--bucket-name', type=str, default=S3_BUCKET_NAME,
                        help='Nom du bucket S3 à utiliser ou créer')
    parser.add_argument('--create-bucket', action='store_true',
                        help='Créer le bucket S3 s\'il n\'existe pas')
    parser.add_argument('--test-upload', action='store_true',
                        help='Tester l\'upload d\'un fichier vers S3')
    parser.add_argument('--list-files', action='store_true',
                        help='Lister les fichiers dans le bucket S3')
    
    # Options RDS
    parser.add_argument('--rds-host', type=str, default=RDS_HOST,
                        help='Hôte de la base de données RDS')
    parser.add_argument('--rds-port', type=int, default=RDS_PORT,
                        help='Port de la base de données RDS')
    parser.add_argument('--rds-dbname', type=str, default=RDS_DBNAME,
                        help='Nom de la base de données RDS')
    parser.add_argument('--rds-user', type=str, default=RDS_USER,
                        help='Utilisateur de la base de données RDS')
    parser.add_argument('--rds-password', type=str, default=RDS_PASSWORD,
                        help='Mot de passe de la base de données RDS')
    parser.add_argument('--create-tables', action='store_true',
                        help='Créer les tables dans la base de données RDS')
    
    # Options Lambda
    parser.add_argument('--deploy-lambda', action='store_true',
                        help='Déployer la fonction Lambda pour le scraper Welcome to the Jungle')
    parser.add_argument('--test-lambda', action='store_true',
                        help='Tester l\'invocation de la fonction Lambda')
    parser.add_argument('--lambda-function', type=str, default='welcome-jungle-scraper',
                        help='Nom de la fonction Lambda à tester ou déployer')
    
    return parser.parse_args()

def main():
    """Fonction principale du script."""
    # Créer le dossier de logs s'il n'existe pas
    os.makedirs('logs', exist_ok=True)
    
    # Parser les arguments
    args = parse_arguments()
    
    # Afficher les informations de configuration
    logger.info("Déploiement et test de l'infrastructure AWS")
    logger.info(f"Service: {args.service}")
    
    # Vérifier les identifiants AWS
    if not AWS_ACCESS_KEY or not AWS_SECRET_KEY:
        logger.error("Identifiants AWS manquants. Vérifiez les variables d'environnement KEY_ACCESS et KEY_SECRET.")
        return
    
    logger.info(f"Identifiants AWS: {AWS_ACCESS_KEY[:5]}...{AWS_ACCESS_KEY[-5:]}")
    
    # Tester et déployer S3
    if args.service in ['s3', 'all']:
        logger.info("=== Configuration et test de S3 ===")
        
        # Tester la connexion S3
        if test_s3_connection():
            logger.info("Connexion S3 réussie !")
            
            # Configurer le bucket
            if args.create_bucket:
                logger.info(f"Configuration du bucket '{args.bucket_name}'...")
                if setup_s3_bucket(args.bucket_name, True):
                    logger.info(f"Bucket '{args.bucket_name}' configuré avec succès !")
            
            # Tester l'upload
            if args.test_upload:
                logger.info("Test d'upload vers S3...")
                test_s3_upload(args.bucket_name)
            
            # Lister les fichiers
            if args.list_files:
                logger.info("Liste des fichiers dans S3...")
                list_s3_files(args.bucket_name)
                
                # Lister les fichiers dans les dossiers spécifiques
                for prefix in ["raw/welcome_jungle/", "raw/pole_emploi/", "processed/"]:
                    list_s3_files(args.bucket_name, prefix)
        else:
            logger.error("Échec de la connexion S3. Vérifiez vos identifiants AWS.")
    
    # Tester et déployer RDS
    if args.service in ['rds', 'all']:
        logger.info("=== Configuration et test de RDS ===")
        
        # Tester la connexion RDS
        if test_rds_connection(args.rds_host, args.rds_port, args.rds_dbname, args.rds_user, args.rds_password):
            logger.info("Connexion RDS réussie !")
            
            # Créer les tables si demandé
            if args.create_tables:
                logger.info("Création des tables dans la base de données...")
                if setup_rds_database(args.rds_host, args.rds_port, args.rds_dbname, args.rds_user, args.rds_password):
                    logger.info("Tables créées ou mises à jour avec succès !")
                else:
                    logger.error("Échec de la création des tables.")
        else:
            logger.error("Échec de la connexion à la base de données.")
            logger.info("Suggestion: Vérifiez que votre base de données est accessible publiquement ou configurez un tunnel SSH.")
    
    # Tester et déployer Lambda
    if args.service in ['lambda', 'all']:
        logger.info("=== Configuration et test de Lambda ===")
        
        # Vérifier que S3 et RDS sont accessibles
        s3_ok = test_s3_connection()
        rds_ok = test_rds_connection(args.rds_host, args.rds_port, args.rds_dbname, args.rds_user, args.rds_password)
        
        if not s3_ok:
            logger.warning("La connexion S3 a échoué. Les fonctions Lambda pourraient ne pas fonctionner correctement.")
        
        if not rds_ok:
            logger.warning("La connexion RDS a échoué. Les fonctions Lambda pourraient ne pas fonctionner correctement.")
        
        # Déployer la fonction Lambda Welcome to the Jungle
        if args.deploy_lambda:
            logger.info("Déploiement de la fonction Lambda pour le scraper Welcome to the Jungle...")
            if deploy_welcome_jungle_lambda():
                logger.info("Fonction Lambda déployée avec succès !")
            else:
                logger.error("Échec du déploiement de la fonction Lambda.")
        
        # Tester l'invocation de la fonction Lambda
        if args.test_lambda:
            logger.info(f"Test d'invocation de la fonction Lambda '{args.lambda_function}'...")
            payload = {
                "max_pages": 1,  # Limiter à 1 page pour le test
                "save_to_s3": True,
                "bucket_name": args.bucket_name
            }
            if test_lambda_invocation(args.lambda_function, payload):
                logger.info("Invocation de la fonction Lambda réussie !")
            else:
                logger.error("Échec de l'invocation de la fonction Lambda.")
    
    logger.info("Déploiement et test terminés.")

if __name__ == "__main__":
    main()
