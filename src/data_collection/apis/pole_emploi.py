#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Module d'interaction avec l'API France Travail (ex-Pôle Emploi)
Permet l'authentification et la recherche d'offres d'emploi
"""

import os
import json
import requests
import logging
import urllib.parse
import time
from datetime import datetime
from dotenv import load_dotenv
import boto3

# Configuration du logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
)
logger = logging.getLogger(__name__)

# Configurations par défaut pour l'API France Travail - Configuration qui fonctionne d'après les tests
DEFAULT_AUTH_URL = "https://francetravail.io/connexion/oauth2/access_token?realm=%2Fpartenaire"
DEFAULT_API_URL = "https://api.francetravail.io/partenaire/offresdemploi/v2/offres/search"
DEFAULT_SCOPE = "o2dsoffre api_offresdemploiv2"

def get_access_token():
    """
    Récupère un token d'accès à l'API France Travail
    
    Returns:
        str: Token d'accès ou None en cas d'erreur
    """
    # Charger les variables d'environnement
    load_dotenv()
    
    client_id = os.getenv('POLE_EMPLOI_CLIENT_ID')
    client_secret = os.getenv('POLE_EMPLOI_CLIENT_SECRET')
    scope = os.getenv('POLE_EMPLOI_SCOPE') or DEFAULT_SCOPE
    auth_url = os.getenv('URL_POLE_EMPLOI') or DEFAULT_AUTH_URL
    
    if not all([client_id, client_secret]):
        logger.error("Variables d'environnement manquantes pour l'API France Travail")
        return None
    
    logger.info(f"Tentative d'authentification à l'API France Travail...")
    logger.info(f"URL: {auth_url}")
    logger.info(f"Scope: {scope}")
    logger.info(f"Client ID: {client_id[:10]}...")
    
    # Préparer les données pour la requête d'authentification
    payload = {
        'grant_type': 'client_credentials',
        'client_id': client_id,
        'client_secret': client_secret,
        'scope': scope
    }
    
    headers = {
        'Content-Type': 'application/x-www-form-urlencoded'
    }
    
    try:
        response = requests.post(auth_url, data=payload, headers=headers)
        
        logger.info(f"Statut de la réponse d'authentification: {response.status_code}")
        logger.info(f"Réponse complète: {response.text}")
        
        if response.status_code != 200:
            logger.error(f"Erreur d'authentification: {response.text}")
            return None
            
        data = response.json()
        access_token = data.get('access_token')
        token_type = data.get('token_type')
        expires_in = data.get('expires_in')
        
        if not access_token:
            logger.error("Token d'accès non trouvé dans la réponse")
            return None
            
        logger.info("Authentification réussie à l'API France Travail")
        logger.info(f"Type de token: {token_type}")
        logger.info(f"Expiration: {expires_in} secondes")
        
        return access_token
    except requests.exceptions.RequestException as e:
        logger.error(f"Erreur lors de l'authentification à l'API France Travail: {e}")
        if hasattr(e, 'response') and e.response:
            logger.error(f"Détails de l'erreur: {e.response.text}")
        return None

def search_jobs(access_token, keywords=None, location=None, distance=None, page=1, per_page=150):
    """
    Recherche des offres d'emploi via l'API France Travail
    
    Args:
        access_token (str): Token d'accès
        keywords (str, optional): Mots-clés de recherche
        location (str, optional): Localisation
        distance (int, optional): Distance en km
        page (int, optional): Numéro de page
        per_page (int, optional): Nombre d'offres par page
        
    Returns:
        dict: Résultats de la recherche ou None en cas d'erreur
    """
    # Charger les variables d'environnement
    load_dotenv()
    
    # Utiliser l'URL de l'API depuis les variables d'environnement ou la valeur par défaut
    api_url = DEFAULT_API_URL
    
    # Calculer la plage pour la pagination
    start = (page - 1) * per_page
    end = start + per_page - 1
    
    # Préparer les paramètres de recherche
    params = {
        'range': f"{start}-{end}"
    }
    
    # Ajouter les paramètres optionnels s'ils sont fournis
    if keywords:
        params['motsCles'] = keywords
    
    if location:
        params['commune'] = location
        
    if distance:
        params['distance'] = distance
    
    # Préparer les en-têtes avec le token d'authentification
    headers = {
        'Authorization': f'Bearer {access_token}',
        'Accept': 'application/json'
    }
    
    try:
        logger.info(f"Recherche d'offres d'emploi - Page {page}, {per_page} offres par page")
        logger.info(f"URL: {api_url}")
        logger.info(f"Paramètres: {params}")
        
        response = requests.get(api_url, params=params, headers=headers)
        
        # Enregistrer les en-têtes pour la pagination
        content_range = response.headers.get('Content-Range')
        
        # Accepter les codes 200 et 206 (Partial Content) comme succès
        if response.status_code in [200, 206]:
            data = response.json()
            
            # Ajouter les informations de pagination au résultat
            if content_range:
                data['Content-Range'] = content_range
                
            nb_resultats = len(data.get('resultats', []))
            logger.info(f"Requête réussie: {nb_resultats} offres trouvées (Code {response.status_code})")
            return data
        else:
            logger.error(f"Erreur lors de la recherche d'offres: Code {response.status_code}")
            
            # Sauvegarder la réponse d'erreur pour le débogage
            error_time = datetime.now().strftime("%Y%m%d_%H%M%S")
            error_filename = f"data/debug/api_error_{error_time}.txt"
            
            os.makedirs(os.path.dirname(error_filename), exist_ok=True)
            
            with open(error_filename, "w", encoding="utf-8") as f:
                f.write(f"URL: {api_url}\n")
                f.write(f"Paramètres: {params}\n")
                f.write(f"Code d'erreur: {response.status_code}\n")
                f.write(f"Contenu: {response.text}\n")
            
            logger.error(f"Détails de l'erreur sauvegardés dans {error_filename}")
            
            if response.text:
                logger.error(f"Détails: {response.text[:200]}...")
                
            # Pour l'erreur 403, afficher des informations supplémentaires
            if response.status_code == 403:
                logger.error("Erreur 403 Forbidden: Vous n'avez pas les autorisations nécessaires pour accéder à cette ressource.")
                logger.error("Vérifiez que le scope utilisé est correct et que votre application a les droits nécessaires.")
                logger.error(f"Scope utilisé: {scope if 'scope' in locals() else os.getenv('POLE_EMPLOI_SCOPE') or DEFAULT_SCOPE}")
                
            return None
    except requests.exceptions.RequestException as e:
        logger.error(f"Erreur lors de la recherche d'offres: {e}")
        if hasattr(e, 'response') and e.response:
            logger.error(f"Détails de l'erreur: {e.response.text}")
        return None

def display_job_results(results):
    """
    Affiche les résultats de la recherche d'offres d'emploi
    
    Args:
        results (dict): Résultats de la recherche
    """
    if not results:
        logger.error("Pas de résultats à afficher")
        return 0
        
    resultats = results.get('resultats', [])
    logger.info(f"Nombre d'offres trouvées: {len(resultats)}")
    
    # Afficher les 5 premières offres
    for i, offre in enumerate(resultats[:5]):
        logger.info(f"\nOffre {i+1}:")
        logger.info(f"ID: {offre.get('id')}")
        logger.info(f"Titre: {offre.get('intitule')}")
        logger.info(f"Entreprise: {offre.get('entreprise', {}).get('nom')}")
        logger.info(f"Lieu: {offre.get('lieuTravail', {}).get('libelle')}")
        logger.info(f"Type de contrat: {offre.get('typeContrat')}")
        logger.info(f"Date de publication: {offre.get('dateCreation')}")
        logger.info(f"URL: {offre.get('origineOffre', {}).get('urlOrigine')}")
    
    if len(resultats) > 5:
        logger.info(f"\n... et {len(resultats) - 5} autres offres")
        
    # Afficher les informations de pagination si disponibles
    if 'Content-Range' in results:
        logger.info(f"Pagination: {results.get('Content-Range')}")
        
    return len(resultats)

def save_jobs_to_file(data, keywords=None, page=1):
    """
    Sauvegarde les résultats de recherche dans un fichier local.
    
    Args:
        data (dict): Résultats de la recherche
        keywords (str, optional): Mots-clés utilisés pour la recherche
        page (int): Numéro de page
        
    Returns:
        str: Chemin du fichier sauvegardé ou None en cas d'erreur
    """
    if not data or 'resultats' not in data:
        logger.error("Pas de données à sauvegarder")
        return None
        
    # Créer le dossier de données s'il n'existe pas
    os.makedirs("data/raw/france_travail", exist_ok=True)
    
    # Créer un nom de fichier avec la date et l'heure actuelles
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    keywords_str = keywords.replace(" ", "_") if keywords else "all"
    filename = f"data/raw/france_travail/france_travail_{keywords_str}_{timestamp}_p{page}.json"
    
    try:
        with open(filename, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
            
        logger.info(f"Données sauvegardées dans {filename}")
        return filename
    except Exception as e:
        logger.error(f"Erreur lors de la sauvegarde des données: {e}")
        return None

def is_data_already_collected(date_str, keywords=None, bucket_name='data-lake-brut'):
    """
    Vérifie si des données ont déjà été collectées pour une date donnée
    
    Args:
        date_str (str): Date au format YYYYMMDD
        keywords (str, optional): Mots-clés spécifiques à vérifier
        bucket_name (str): Nom du bucket S3
        
    Returns:
        tuple: (bool, list) - (True si des données existent, liste des fichiers existants)
    """
    try:
        # Définir le pattern de recherche pour "france_travail_all" si pas de mots-clés spécifiés
        if keywords:
            clean_keywords = keywords.replace(' ', '_').lower()
        else:
            clean_keywords = "all"
        
        # Chercher dans le dossier local
        local_path = "data/raw/france_travail"
        local_files = []
        
        if os.path.exists(local_path):
            # Vérifier les fichiers qui contiennent la date ET "france_travail_all" (ou les mots-clés)
            for filename in os.listdir(local_path):
                if filename.startswith(f"france_travail_{clean_keywords}_") and date_str in filename:
                    local_files.append(filename)
            
            if local_files:
                logger.info(f"Données déjà collectées localement pour aujourd'hui ({len(local_files)} fichiers)")
                # Afficher les fichiers trouvés pour débuggage
                for f in local_files:
                    logger.info(f"  - {f}")
                return True, local_files
        
        # Si aucun fichier local n'est trouvé, vérifier sur S3
        try:
            s3_client = boto3.client(
                's3',
                aws_access_key_id=os.getenv('KEY_ACCESS'),
                aws_secret_access_key=os.getenv('KEY_SECRET')
            )
            
            # Liste tous les objets dans le bucket avec la structure de dossier appropriée
            prefix = "raw/france_travail/"
            response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
            
            s3_files = []
            if 'Contents' in response:
                # Filtrer pour ne garder que les fichiers correspondant à cette date et mot-clé
                search_pattern = f"france_travail_{clean_keywords}_{date_str}"
                for obj in response['Contents']:
                    key = obj['Key']
                    if search_pattern in key:
                        s3_files.append(key)
            
            if s3_files:
                logger.info(f"Données déjà collectées sur S3 pour aujourd'hui ({len(s3_files)} fichiers)")
                # Afficher les fichiers S3 trouvés pour débuggage
                for f in s3_files:
                    logger.info(f"  - s3://{bucket_name}/{f}")
                return True, s3_files
                
        except Exception as e:
            logger.warning(f"Erreur lors de la vérification sur S3: {e}")
        
        # Aucune donnée existante trouvée
        logger.info(f"Aucune donnée existante trouvée pour la date {date_str} avec motclés '{clean_keywords}'")
        return False, []
        
    except Exception as e:
        logger.error(f"Erreur lors de la vérification des données existantes: {e}")
        return False, []

def upload_to_s3(file_path, bucket_name='data-lake-brut'):
    """
    Télécharge un fichier vers S3
    
    Args:
        file_path (str): Chemin du fichier à télécharger
        bucket_name (str): Nom du bucket S3
        
    Returns:
        bool: True si le téléchargement a réussi, False sinon
    """
    try:
        # Vérifier si le fichier existe
        if not os.path.exists(file_path):
            logger.error(f"Fichier introuvable: {file_path}")
            return False
            
        # Initialiser le client boto3
        try:
            s3_client = boto3.client(
                's3',
                aws_access_key_id=os.getenv('KEY_ACCESS'),
                aws_secret_access_key=os.getenv('KEY_SECRET')
            )
        except Exception as e:
            logger.error(f"Erreur lors de l'initialisation du client S3: {e}")
            return False
            
        # Déterminer la clé S3
        # Si le fichier est dans data/raw/france_travail/, conserver la structure dans S3
        if 'data/raw/france_travail/' in file_path:
            s3_path = file_path.replace('data/', '')
        else:
            basename = os.path.basename(file_path)
            s3_path = f"raw/france_travail/{basename}"
            
        # Télécharger le fichier
        logger.info(f"Téléchargement vers S3: {file_path} -> s3://{bucket_name}/{s3_path}")
        
        s3_client.upload_file(
            Filename=file_path,
            Bucket=bucket_name,
            Key=s3_path
        )
        
        logger.info(f"Fichier téléchargé avec succès vers S3: s3://{bucket_name}/{s3_path}")
        return True
        
    except Exception as e:
        logger.error(f"Erreur lors du téléchargement vers S3: {e}")
        return False

def collect_jobs(keywords=None, location=None, distance=None, max_pages=10, upload_s3=True):
    """
    Collecte les offres d'emploi et les sauvegarde localement et sur S3.
    
    Args:
        keywords (str, optional): Mots-clés de recherche
        location (str, optional): Localisation
        distance (int, optional): Distance en km
        max_pages (int): Nombre maximum de pages à récupérer
        upload_s3 (bool): Si True, upload les données vers S3
            
    Returns:
        int: Nombre total d'offres collectées
    """
    # Obtenir un token d'accès
    access_token = get_access_token()
    
    if not access_token:
        logger.error("Impossible d'obtenir un token d'accès. Arrêt de la collecte.")
        return 0
    
    # Récupérer le nom du bucket
    bucket_name = os.getenv('data_lake_bucket', 'data-lake-brut')
    
    # Récupérer les offres d'emploi par lots
    total_offres = 0
    saved_files = []
    
    for page in range(1, max_pages + 1):
        logger.info(f"\n=== Collecte des offres - Page {page}/{max_pages} ===")
        
        # Rechercher les offres
        results = search_jobs(
            access_token=access_token,
            keywords=keywords,
            location=location,
            distance=distance,
            page=page,
            per_page=150
        )
        
        if not results:
            logger.error(f"Aucun résultat pour la page {page}. Arrêt de la pagination.")
            break
            
        # Compter les offres
        nb_offres = len(results.get('resultats', []))
        
        # Ne compter les offres que s'il y en a
        if nb_offres > 0:
            total_offres += nb_offres
            logger.info(f"Nombre d'offres trouvées: {nb_offres}")
            
            # Sauvegarder les résultats localement
            local_file_path = save_jobs_to_file(results, keywords, page)
            
            if local_file_path:
                saved_files.append(local_file_path)
                # Télécharger vers S3 si demandé
                if upload_s3:
                    upload_to_s3(local_file_path, bucket_name)
        else:
            logger.warning(f"Page {page}: Aucune offre trouvée dans les résultats.")
        
        if nb_offres < 150:
            logger.info("Moins de 150 offres récupérées, fin de la pagination.")
            break
            
        logger.info("Attente de 2 secondes avant la prochaine requête...")
        time.sleep(2)  # Attendre 2 secondes entre les requêtes pour éviter de surcharger l'API
    
    logger.info(f"\n=== Total des offres collectées: {total_offres} ===")
    return total_offres, saved_files
