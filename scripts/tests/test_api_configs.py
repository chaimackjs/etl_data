import os
import requests
import logging
import json
import time
from dotenv import load_dotenv
from datetime import datetime

# Configuration du logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Créer les répertoires nécessaires
os.makedirs("data/debug", exist_ok=True)

def get_access_token(scope, auth_url):
    """
    Récupère un token d'accès à l'API France Travail avec un scope et une URL spécifiques
    
    Args:
        scope (str): Scope à utiliser
        auth_url (str): URL d'authentification
        
    Returns:
        str: Token d'accès ou None en cas d'erreur
    """
    # Charger les variables d'environnement
    load_dotenv()
    
    client_id = os.getenv('POLE_EMPLOI_CLIENT_ID')
    client_secret = os.getenv('POLE_EMPLOI_CLIENT_SECRET')
    
    if not all([client_id, client_secret]):
        logger.error("Variables d'environnement manquantes pour l'API France Travail")
        return None
    
    logger.info(f"Tentative d'authentification avec:")
    logger.info(f"URL: {auth_url}")
    logger.info(f"Scope: {scope}")
    
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
        
        if response.status_code != 200:
            logger.error(f"Erreur d'authentification: {response.text}")
            return None
            
        data = response.json()
        access_token = data.get('access_token')
        
        if not access_token:
            logger.error("Token d'accès non trouvé dans la réponse")
            return None
            
        logger.info("Authentification réussie")
        logger.info(f"Type de token: {data.get('token_type')}")
        logger.info(f"Expiration: {data.get('expires_in')} secondes")
        
        return access_token
        
    except Exception as e:
        logger.error(f"Erreur lors de l'authentification: {e}")
        return None

def test_api_access(access_token, api_url):
    """
    Teste l'accès à une URL d'API spécifique
    
    Args:
        access_token (str): Token d'accès
        api_url (str): URL de l'API à tester
        
    Returns:
        bool: True si l'accès est réussi, False sinon
    """
    logger.info(f"Test d'accès à l'API: {api_url}")
    
    headers = {
        'Authorization': f'Bearer {access_token}',
        'Accept': 'application/json'
    }
    
    params = {
        'range': '0-10'  # Juste quelques offres pour tester
    }
    
    try:
        response = requests.get(api_url, params=params, headers=headers)
        
        logger.info(f"Statut de la réponse: {response.status_code}")
        
        if response.status_code == 200:
            logger.info("✅ Accès à l'API réussi!")
            
            # Sauvegarder la réponse pour analyse
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"data/debug/api_success_{timestamp}.json"
            
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(response.json(), f, ensure_ascii=False, indent=4)
                
            logger.info(f"Réponse sauvegardée dans {filename}")
            return True
        else:
            logger.error(f"Erreur {response.status_code}: {response.reason}")
            
            # Sauvegarder la réponse d'erreur pour analyse
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            error_filename = f"data/debug/api_error_{response.status_code}_{timestamp}.txt"
            
            with open(error_filename, 'w', encoding='utf-8') as f:
                f.write(f"Status: {response.status_code}\n")
                f.write(f"Headers: {dict(response.headers)}\n")
                f.write(f"Response: {response.text}\n")
            
            logger.error(f"Détails de l'erreur sauvegardés dans {error_filename}")
            return False
            
    except Exception as e:
        logger.error(f"Exception lors du test de l'API: {e}")
        return False

def test_all_configurations():
    """Teste différentes configurations d'authentification et d'API"""
    # URLs d'authentification à tester
    auth_urls = [
        "https://entreprise.pole-emploi.fr/connexion/oauth2/access_token?realm=/partenaire",
        "https://entreprise.pole-emploi.fr/connexion/oauth2/access_token",
        "https://francetravail.io/connexion/oauth2/access_token?realm=%2Fpartenaire"
    ]
    
    # Scopes à tester
    scopes = [
        "api_offresdemploiv2",
        "o2dsoffre api_offresdemploiv2",
        "api_offresdemploiv2 o2dsoffre",
        "application_PAR_dataanalyse_7a35816bfdfa9a889ff2c8c3786217cbd397e99f7f1d1056bb3568f17ec115c0"
    ]
    
    # URLs d'API à tester
    api_urls = [
        "https://api.francetravail.io/partenaire/offresdemploi/v2/offres/search",
        "https://api.pole-emploi.io/partenaire/offresdemploi/v2/offres/search"
    ]
    
    # Tester chaque combinaison d'URL d'authentification et de scope
    for auth_url in auth_urls:
        for scope in scopes:
            logger.info(f"\n=== Test avec URL d'authentification: {auth_url} et scope: {scope} ===")
            
            access_token = get_access_token(scope, auth_url)
            
            if not access_token:
                logger.error("Impossible d'obtenir un token d'accès avec cette configuration")
                continue
            
            # Tester chaque URL d'API avec ce token
            for api_url in api_urls:
                logger.info(f"\n--- Test avec URL d'API: {api_url} ---")
                
                if test_api_access(access_token, api_url):
                    logger.info(f"\n✅ CONFIGURATION FONCTIONNELLE TROUVÉE!")
                    logger.info(f"URL d'authentification: {auth_url}")
                    logger.info(f"Scope: {scope}")
                    logger.info(f"URL d'API: {api_url}")
                    
                    # Sauvegarder la configuration fonctionnelle dans un fichier
                    config = {
                        "auth_url": auth_url,
                        "scope": scope,
                        "api_url": api_url
                    }
                    
                    with open("data/france_travail_config.json", "w") as f:
                        json.dump(config, f, indent=4)
                        
                    logger.info("Configuration sauvegardée dans data/france_travail_config.json")
                    return True
                
                # Attendre un peu entre les requêtes pour éviter de surcharger l'API
                time.sleep(2)
    
    logger.error("❌ Aucune configuration n'a fonctionné")
    return False

if __name__ == "__main__":
    test_all_configurations()
