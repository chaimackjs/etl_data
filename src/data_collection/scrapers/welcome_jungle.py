#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Scraper amélioré pour Welcome to the Jungle.
Extrait les offres d'emploi depuis welcometothejungle.com avec tous les champs classiques :
- titre, entreprise, lieu, type de contrat
- date de publication, description, compétences, niveau d'expérience, secteur, salaire
Et les sauvegarde au format JSON.
"""

import os
import time
import json
import re
import boto3
import traceback
import pandas as pd
from datetime import datetime, timedelta
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
from webdriver_manager.chrome import ChromeDriverManager
import argparse

def upload_to_s3(data, filename):
    """
    Upload des données au format JSON vers un bucket S3
    """
    try:
        import boto3
        from botocore.exceptions import NoCredentialsError, ClientError
        
        # Récupération des variables d'environnement AWS
        aws_access_key = os.environ.get('KEY_ACCESS')
        aws_secret_key = os.environ.get('KEY_SECRET')
        bucket_name = os.environ.get('DATA_LAKE_BUCKET')  # Utilisation du bucket par défaut si non défini
        
        # Vérification des identifiants AWS
        if not aws_access_key or not aws_secret_key:
            print("\nErreur: Variables d'environnement AWS_ACCESS_KEY_ID ou AWS_SECRET_ACCESS_KEY manquantes")
            print("Tentative d'utilisation des valeurs par défaut...")
            # Utilisation des valeurs par défaut stockées dans le projet
            aws_access_key = "AKIAS2VS4EK2UIF56F5O"
            aws_secret_key = "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"
        
        print(f"\nTentative d'upload vers le bucket S3: {bucket_name}")
        print(f"Chemin du fichier S3: {filename}")
            
        # Création du client S3
        s3_client = boto3.client(
            's3',
            region_name='eu-north-1',  # Spécification de la région AWS
            aws_access_key_id=aws_access_key,
            aws_secret_access_key=aws_secret_key
        )
        
        # Conversion des données en JSON
        json_data = json.dumps(data, ensure_ascii=False)
        
        # Upload vers S3
        s3_client.put_object(
            Body=json_data,
            Bucket=bucket_name,
            Key=filename,
            ContentType='application/json'
        )
        
        print(f"\nUpload réussi vers S3: s3://{bucket_name}/{filename}")
        return True
        
    except ImportError:
        print("\nErreur: Module boto3 non installé. Installez-le avec 'pip install boto3'")
        return False
    except NoCredentialsError:
        print("\nErreur: Identifiants AWS invalides ou insuffisants")
        return False
    except ClientError as e:
        print(f"\nErreur AWS: {str(e)}")
        return False
    except Exception as e:
        print(f"\nErreur lors de l'upload vers S3: {str(e)}")
        return False


def print_job_summary(job):
    """
    Affiche un résumé formaté d'une offre d'emploi
    """
    print("\n" + "=" * 80)
    print(f"TITRE: {job.get('title', 'Non disponible')}")
    print(f"ENTREPRISE: {job.get('company', 'Non disponible')}")
    print(f"LIEU: {job.get('location', 'Non disponible')}")
    print(f"TYPE DE CONTRAT: {job.get('contract_type', 'Non disponible')}")
    print(f"SALAIRE: {job.get('salary', 'Non disponible')}")
    print(f"CATÉGORIE: {job.get('category', 'Non disponible')}")
    print(f"EXPÉRIENCE: {job.get('experience', 'Non disponible')}")
    print(f"DATE: {job.get('date', 'Non disponible')}")
    print(f"URL: {job.get('url', 'Non disponible')}")
    
    # Afficher un extrait de la description
    description = job.get('description', 'Non disponible')
    if description != 'Description non disponible':
        # Limiter l'affichage à 200 caractères pour la lisibilité
        print(f"\nDESCRIPTION (extrait): {description[:200]}...")
        print(f"Longueur totale de la description: {len(description)} caractères")
    else:
        print("\nDESCRIPTION: Non disponible")
    print("=" * 80)


def clean_salary(salary_text):
    """Nettoie et extrait le salaire à partir du texte brut.
    Gère correctement les formats comme "1,5K" (1500€) ou "1.5K" (1500€).
    """
    if not salary_text or salary_text == "Salaire non disponible":
        return "Salaire non disponible"
    
    # Conserver le texte original pour le retourner à la fin
    original_text = salary_text.strip()
    
    # Normaliser le texte pour faciliter l'extraction
    salary_text = salary_text.lower()
    
    # Recherche spécifique pour le format "Salaire : X à Y € par mois/an"
    salary_match = re.search(r'salaire\s*:\s*(\d[\d\s.,]*(?:\s*[àa]\s*\d[\d\s.,]*)?\s*(?:k\s*€|k€|€|euros)(?:\s*(?:brut|net|par mois|par an|annuel|mensuel))?)', salary_text)
    
    if salary_match:
        extracted_salary = salary_match.group(1).strip()
        return normalize_salary_format(extracted_salary)
    
    # Recherche de montants avec symbole monétaire (fourchette)
    amount_match = re.search(r'([\d\s,.]+(?:,\d+)?\s*(?:[àa]|-)\s*[\d\s,.]+(?:,\d+)?\s*(?:k\s*€|k€|€|euros|k))', salary_text)
    if amount_match:
        extracted_salary = amount_match.group(1).strip()
        return normalize_salary_format(extracted_salary)
    
    # Recherche de montants simples avec symbole monétaire
    amount_match = re.search(r'([\d\s,.]+(?:,\d+)?\s*(?:k\s*€|k€|€|euros|k))', salary_text)
    if amount_match:
        extracted_salary = amount_match.group(1).strip()
        return normalize_salary_format(extracted_salary)
    
    # Si aucun pattern spécifique ne correspond, retourner le texte original
    return original_text


def normalize_salary_format(salary_text):
    """Normalise le format du salaire pour gérer correctement les valeurs comme 1,5K.
    Ne transforme que les formats avec K/k, conserve les autres formats tels quels.
    """
    if not salary_text:
        return "Salaire non disponible"
    
    # Détecter le format avec K/k pour le convertir correctement
    # Mais seulement s'il n'y a pas de fourchette (comme "800 à 1 000 € par mois")
    if " à " in salary_text.lower() or "-" in salary_text or "entre" in salary_text.lower():
        # C'est une fourchette de salaire, on la laisse telle quelle
        return salary_text
    
    # Détecter le format avec K/k pour le convertir correctement
    k_match = re.search(r'(\d+[\s.,]?\d*)\s*k', salary_text.lower())
    if k_match:
        # Récupérer la valeur numérique
        value_str = k_match.group(1).strip()
        
        # Remplacer la virgule par un point pour la conversion
        value_str = value_str.replace(',', '.')
        
        try:
            # Convertir en nombre et multiplier par 1000
            value = float(value_str) * 1000
            # Formater avec le symbole €
            return f"{int(value)} €"
        except ValueError:
            # En cas d'erreur de conversion, retourner le texte original
            return salary_text
    
    return salary_text


def normalize_contract_type(contract_type):
    """Normalise le type de contrat pour assurer la cohérence des données."""
    if not contract_type:
        return "Type de contrat non spécifié"
        
    contract_type = contract_type.lower()
    
    # Normalisation des types de contrats
    if any(term in contract_type for term in ["cdi", "permanent", "indéterminé", "indetermine", "indefinite"]):
        return "CDI"
    elif any(term in contract_type for term in ["cdd", "fixed", "déterminé", "determine", "temporary"]):
        return "CDD"
    elif any(term in contract_type for term in ["stage", "intern", "internship"]):
        return "Stage"
    elif any(term in contract_type for term in ["freelance", "indépendant", "independant", "contractor"]):
        return "Freelance"
    elif any(term in contract_type for term in ["alternance", "apprentissage", "apprenticeship", "alternating"]):
        return "Alternance"
    elif any(term in contract_type for term in ["temps partiel", "part time", "part-time"]):
        return "Temps partiel"
    elif any(term in contract_type for term in ["vdi", "vendeur", "indépendant"]):
        return "VDI"
    else:
        return contract_type.capitalize()  # Conserver le type d'origine si aucune correspondance


def human_delay(min_delay=1, max_delay=3):
    """Ajoute un délai aléatoire pour simuler un comportement humain"""
    time.sleep(random.uniform(min_delay, max_delay))


def scrape_welcome_jungle(query="", location="Paris", max_pages=10, headless=False, upload_to_aws=False):
    """Scrape les offres d'emploi sur Welcome to the Jungle de manière optimisée"""
    os.makedirs("data/raw/welcome_jungle", exist_ok=True)
    
    # Configuration du navigateur avec options essentielles
    options = Options()
    options.add_argument("--start-maximized")
    options.add_argument("--disable-notifications")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option("useAutomationExtension", False)
    options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36")
    if headless:
        options.add_argument("--headless")
        options.add_argument("--disable-gpu")
    
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
    driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
    
    all_jobs = []
    
    try:
        # Construction de l'URL de recherche
        base_url = "https://www.welcometothejungle.com/fr/jobs"
        url_params = []
        if query:
            url_params.append(f"query={query.replace(' ', '%20')}")
        if location:
            url_params.append(f"aroundQuery={location.replace(' ', '%20')}")
        
        search_url = f"{base_url}?{('&'.join(url_params) + '&' if url_params else '')}page=1"
        
        # Fonctions auxiliaires
        def load_page_with_retry(url, max_retries=3, wait_time=3):
            for retry in range(max_retries):
                try:
                    driver.get(url)
                    WebDriverWait(driver, 10).until(
                        lambda d: d.execute_script('return document.readyState') == 'complete'
                    )
                    time.sleep(wait_time)
                    return True
                except Exception as e:
                    print(f"Erreur chargement page {url}: {e}. Tentative {retry+1}/{max_retries}")
                    time.sleep(wait_time * (retry + 1))
            return False
        
        def extract_element_text(selectors, default="Non disponible"):
            for selector in selectors:
                try:
                    xpath = selector.startswith("//")
                    elements = driver.find_elements(By.XPATH if xpath else By.CSS_SELECTOR, selector)
                    for elem in elements:
                        if elem and elem.text.strip():
                            return elem.text.strip()
                except Exception:
                    continue
            return default
        
        def extract_from_url(url, pattern, transform=lambda x: x.replace('-', ' ').title()):
            try:
                if pattern in url:
                    parts = url.split('/')
                    idx = parts.index(pattern)
                    if idx + 1 < len(parts):
                        return transform(parts[idx + 1])
            except Exception:
                pass
            return None
        
        # Chargement de la première page et détermination du nombre de pages
        if not load_page_with_retry(search_url):
            print("Impossible de charger la première page")
            return all_jobs
        
        # Détection simplifiée du nombre de pages
        pages_to_scrape = max_pages
        try:
            pagination_elements = driver.find_elements(By.CSS_SELECTOR, "[class*='Pagination'], [class*='pagination']")
            if pagination_elements:
                page_numbers = [int(e.text) for e in pagination_elements if e.text.strip().isdigit()]
                if page_numbers:
                    pages_to_scrape = min(max_pages, max(page_numbers))
        except Exception as e:
            print(f"Erreur détection pagination: {e}")
        
        print(f"Scraping de {pages_to_scrape} pages")
        
        # Scraping des pages
        for page in range(1, pages_to_scrape + 1):
            page_url = f"{base_url}?{('&'.join(url_params) + '&' if url_params else '')}page={page}"
            print(f"Page {page}: {page_url}")
            
            if page > 1:  # La page 1 est déjà chargée
                if not load_page_with_retry(page_url):
                    continue
            
            # Extraction des URLs d'offres avec gestion des éléments périmés
            try:
                # Attendre que les cartes soient chargées
                WebDriverWait(driver, 10).until(
                    lambda d: len(d.find_elements(By.XPATH, "//a[contains(@href, '/companies/') and contains(@href, '/jobs/')]")) > 0
                )
                
                # Récupérer les URLs avec gestion des erreurs
                job_cards = driver.find_elements(By.XPATH, "//a[contains(@href, '/companies/') and contains(@href, '/jobs/')]")
                job_urls = []
                
                for card in job_cards:
                    try:
                        href = card.get_attribute("href")
                        if href:
                            job_urls.append(href.split('?')[0])
                    except Exception:
                        # Ignorer les cartes qui posent problème
                        continue
                
                # Éliminer les doublons
                job_urls = list(set(job_urls))
                print(f"{len(job_urls)} offres trouvées sur la page {page}")
            except Exception as e:
                print(f"Erreur lors de l'extraction des URLs sur la page {page}: {e}")
                job_urls = []
            
            # Traitement de chaque offre
            for url in job_urls:
                job_info = {
                    "url": url,
                    "title": "Titre non disponible",
                    "company": "Entreprise non disponible",
                    "location": "Lieu non disponible",
                    "contract_type": "Type de contrat non disponible",
                    "description": "Description non disponible",
                    "category": "Secteur non disponible",
                    "experience": "Expérience non spécifiée",
                    "salary": "Salaire non disponible",
                    "publication_date": "Date non disponible"
                }
                
                if not load_page_with_retry(url, max_retries=2, wait_time=2):
                    continue
                
                # Extraction des informations de l'offre
                # Titre - Sélecteurs améliorés
                title_text = extract_element_text([
                    "h1[data-testid='job-title']", 
                    "h1.job-title", 
                    "h1.sc-dmqHEX",
                    "h1.wui-heading--lg",
                    "//h1[contains(@class, 'JobViewTitle')]"
                ], "Titre non disponible")
                
                if title_text != "Titre non disponible":
                    job_info["title"] = title_text
                else:
                    # Si le titre n'est pas trouvé avec les sélecteurs, essayer de l'extraire de l'URL
                    url_parts = url.split("/")
                    if len(url_parts) > 0:
                        # Le dernier segment de l'URL contient souvent le titre
                        last_segment = url_parts[-1].split("_")[0]
                        # Remplacer les tirets par des espaces et capitaliser
                        if last_segment:
                            job_info["title"] = " ".join(word.capitalize() for word in last_segment.split("-"))
                
                # Entreprise
                job_info["company"] = extract_element_text([
                    "div[data-testid='job-company-name']", "a[data-testid='company-link']", 
                    "div.sc-beqWaB a", "div.company-name", "a[href*='/companies/']"
                ], "Entreprise non disponible")
                
                if job_info["company"] == "Entreprise non disponible":
                    company_from_url = extract_from_url(url, "companies")
                    if company_from_url:
                        job_info["company"] = company_from_url
                
                # Localisation - Sélecteurs améliorés
                location_text = extract_element_text([
                    "div[data-testid='job-location']", 
                    "div.location", 
                    "div[class*='Location']",
                    ".sc-dmqHEX div:nth-child(1)",
                    ".wui-text--subtitle:nth-child(1)",
                    "//div[contains(text(), 'Lieu')]/following-sibling::div",
                    "//p[contains(text(), 'Lieu')]/following-sibling::p",
                    "//div[contains(@class, 'JobViewInfos')]/div[1]"
                ], "Lieu non disponible")
                
                # Vérifier si la localisation extraite est valide
                if location_text != "Lieu non disponible":
                    # Vérifier que ce n'est pas un code court ou une chaîne non pertinente
                    if len(location_text) > 2 and not any(c.isdigit() for c in location_text) \
                       and not any(keyword in location_text.lower() for keyword in ["cdi", "cdd", "stage", "alternance", "type de contrat"]):
                        job_info["location"] = location_text
                
                # Liste étendue des villes françaises
                cities = ["paris", "lyon", "marseille", "toulouse", "nice", "nantes", "strasbourg", 
                        "montpellier", "bordeaux", "lille", "rennes", "reims", "toulon", "grenoble", 
                        "dijon", "angers", "nimes", "villeurbanne", "clermont-ferrand", "le-mans", 
                        "aix-en-provence", "saint-etienne", "saint-denis", "versailles", "montreuil",
                        "nancy", "caen", "tourcoing", "nanterre", "avignon", "poitiers", "dunkerque",
                        "saint-quentin-fallavier", "niort", "la-plaine-saint-denis"]
                
                # Si la localisation n'a pas été extraite ou est invalide
                if job_info["location"] == "Lieu non disponible":
                    # Méthode 1: Extraire directement de l'URL
                    for city in cities:
                        if f"_{city}" in url.lower() or f"/{city}" in url.lower():
                            job_info["location"] = city.replace("-", " ").title()
                            break
                    
                    # Méthode 2: Extraire du dernier segment de l'URL
                    if job_info["location"] == "Lieu non disponible":
                        url_parts = url.split("/")
                        if len(url_parts) > 0:
                            last_part = url_parts[-1].split("_")[0]  # Prendre la partie avant le premier underscore
                            for city in cities:
                                if city in last_part.lower():
                                    job_info["location"] = city.replace("-", " ").title()
                                    break
                    
                    # Méthode 3: Chercher dans le texte complet de la page
                    if job_info["location"] == "Lieu non disponible":
                        try:
                            page_text = driver.find_element(By.TAG_NAME, "body").text.lower()
                            # Rechercher des motifs comme "Lieu : Paris" ou "Localisation : Lyon"
                            location_patterns = [r"lieu\s*:?\s*([^\n\r,]+)", r"localisation\s*:?\s*([^\n\r,]+)"]
                            for pattern in location_patterns:
                                match = re.search(pattern, page_text)
                                if match:
                                    location_value = match.group(1).strip()
                                    # Vérifier si c'est une ville valide
                                    for city in cities:
                                        if city in location_value.lower() or city.replace("-", " ") in location_value.lower():
                                            job_info["location"] = city.replace("-", " ").title()
                                            break
                                    if job_info["location"] != "Lieu non disponible":
                                        break
                        except Exception:
                            pass
                    
                    # Méthode 4: Extraire du titre du poste
                    if job_info["location"] == "Lieu non disponible" and "_" in job_info["title"]:
                        title_parts = job_info["title"].split("_")
                        for part in title_parts:
                            part_lower = part.lower()
                            for city in cities:
                                if city == part_lower or city.replace("-", "") == part_lower:
                                    job_info["location"] = city.replace("-", " ").title()
                                    break
                        
                        # Si toujours pas trouvé, chercher dans les autres segments de l'URL
                        if job_info["location"] == "Lieu non disponible":
                            for part in url_parts:
                                for city in cities:
                                    if city == part.lower():
                                        job_info["location"] = city.replace("-", " ").title()
                                        break
                
                # Traiter les cas spéciaux comme "Saint Quentin Fallavier"
                if job_info["title"] == "Saint Quentin Fallavier":
                    job_info["location"] = job_info["title"]
                    # Essayer de récupérer le vrai titre depuis l'URL
                    url_parts = job_url.split("/")
                    if len(url_parts) > 0:
                        job_title_part = url_parts[-1].split("_")[0]
                        if job_title_part:
                            job_info["title"] = " ".join(word.capitalize() for word in job_title_part.split("-"))
                
                # Type de contrat - Sélecteurs améliorés
                contract_text = extract_element_text([
                    "div[data-testid='job-contract-type']", 
                    "div.contract-type", 
                    "div[class*='ContractType']",
                    ".sc-dmqHEX div:nth-child(2)",
                    ".wui-text--subtitle:nth-child(2)",
                    "//div[contains(text(), 'Type de contrat')]/following-sibling::div",
                    "//p[contains(text(), 'Type de contrat')]/following-sibling::p",
                    "//div[contains(@class, 'JobViewInfos')]/div[2]"
                ], "Type de contrat non disponible")
                
                # Vérifier si le texte extrait est valide et n'est pas une autre information
                if contract_text != "Type de contrat non disponible" and not any(keyword in contract_text.lower() for keyword in ["prise de poste", "lieu", "salaire", "expérience"]):
                    job_info["contract_type"] = normalize_contract_type(contract_text)
                
                # Si le type de contrat n'a pas été trouvé ou est invalide
                if job_info["contract_type"] == "Type de contrat non disponible":
                    # Chercher dans le texte de la page complète
                    try:
                        page_text = driver.find_element(By.TAG_NAME, "body").text.lower()
                        if "type de contrat" in page_text:
                            # Essayer de trouver le type de contrat après "Type de contrat :"
                            match = re.search(r"type de contrat\s*:?\s*([^\n\r]+)", page_text)
                            if match:
                                contract_value = match.group(1).strip()
                                # Vérifier que c'est un type de contrat valide
                                if any(keyword in contract_value.lower() for keyword in ["cdi", "cdd", "stage", "alternance", "freelance", "intérim", "apprentissage"]):
                                    job_info["contract_type"] = normalize_contract_type(contract_value)
                    except Exception:
                        pass
                
                # Si toujours pas trouvé, essayer d'extraire du titre ou de l'URL
                if job_info["contract_type"] == "Type de contrat non disponible":
                    # Essayer d'extraire du titre
                    title_lower = job_info["title"].lower()
                    url_lower = url.lower()
                    
                    # Rechercher dans le titre et l'URL
                    for text in [title_lower, url_lower]:
                        if "alternance" in text or "apprentissage" in text:
                            job_info["contract_type"] = "Alternance"
                            break
                        elif "stage" in text:
                            job_info["contract_type"] = "Stage"
                            break
                        elif "cdd" in text:
                            job_info["contract_type"] = "CDD"
                            break
                        elif "cdi" in text:
                            job_info["contract_type"] = "CDI"
                            break
                        elif "freelance" in text or "indépendant" in text:
                            job_info["contract_type"] = "Freelance"
                            break
                        elif "intérim" in text:
                            job_info["contract_type"] = "Intérim"
                            break
                
                # Corriger les cas où le type de contrat est dans le champ location
                if job_info["location"].startswith("Type de contrat"):
                    # Extraire le type de contrat du champ location
                    match = re.search(r"Type de contrat\s*:?\s*([^\n\r]+)", job_info["location"])
                    if match:
                        contract_value = match.group(1).strip()
                        job_info["contract_type"] = normalize_contract_type(contract_value)
                        # Réinitialiser le champ location
                        job_info["location"] = "Lieu non disponible"
                
                # Description (simplifiée)
                try:
                    description_elements = driver.find_elements(By.CSS_SELECTOR, 
                        "div[class*='DescriptionContent'], div[class*='JobDescription'], div[data-testid='job-description'], div.sc-beqWaB, div.wui-rich-text")
                    if description_elements:
                        job_info["description"] = description_elements[0].text.strip()
                except Exception:
                    pass
                
                # Catégorie/Secteur - Sélecteurs améliorés
                try:
                    # Essayer d'abord de trouver les éléments avec des labels explicites
                    category_elements = driver.find_elements(By.XPATH, "//div[contains(text(), 'Secteur') or contains(text(), 'Catégorie')]/following-sibling::div")
                    if category_elements:
                        job_info["category"] = category_elements[0].text.strip()
                    else:
                        # Sinon utiliser les sélecteurs CSS
                        job_info["category"] = extract_element_text([
                            "div[data-testid='job-category']", 
                            "div.category",
                            ".sc-dmqHEX div:nth-child(2)",
                            ".wui-text--subtitle:nth-child(2)",
                            "//div[contains(@class, 'JobViewInfos')]/div[2]"
                        ], "Secteur non disponible")
                except Exception:
                    job_info["category"] = "Secteur non disponible"
                
                # Expérience - Sélecteurs améliorés
                try:
                    # Essayer d'abord de trouver les éléments avec des labels explicites
                    exp_elements = driver.find_elements(By.XPATH, "//div[contains(text(), 'Expérience')]/following-sibling::div")
                    if exp_elements:
                        exp_text = exp_elements[0].text.strip()
                        # Formater l'expérience
                        if "junior" in exp_text.lower() or "débutant" in exp_text.lower() or "< 1 an" in exp_text.lower():
                            job_info["experience"] = "Junior (< 1 an)"
                        elif "1 an" in exp_text.lower() or "1-2" in exp_text.lower():
                            job_info["experience"] = "1-2 ans"
                        elif "2 ans" in exp_text.lower() or "2-3" in exp_text.lower():
                            job_info["experience"] = "2-3 ans"
                        elif "3 ans" in exp_text.lower() or "3-5" in exp_text.lower():
                            job_info["experience"] = "3-5 ans"
                        elif "5 ans" in exp_text.lower() or "5-10" in exp_text.lower():
                            job_info["experience"] = "5-10 ans"
                        elif "10 ans" in exp_text.lower() or "> 10" in exp_text.lower() or "senior" in exp_text.lower():
                            job_info["experience"] = "Senior (> 10 ans)"
                        else:
                            job_info["experience"] = exp_text
                    else:
                        # Sinon utiliser les sélecteurs CSS
                        job_info["experience"] = extract_element_text([
                            "div[data-testid='job-experience']", 
                            "div.experience",
                            ".sc-dmqHEX div:nth-child(3)",
                            ".wui-text--subtitle:nth-child(3)",
                            "//div[contains(@class, 'JobViewInfos')]/div[3]"
                        ], "Expérience non spécifiée")
                except Exception:
                    job_info["experience"] = "Expérience non spécifiée"
                
                # Salaire - Sélecteurs améliorés
                salary_text = extract_element_text([
                    "div[data-testid='job-salary']", 
                    "div.salary", 
                    "div[class*='Salary']",
                    ".sc-dmqHEX div:nth-child(3)",
                    ".wui-text--subtitle:nth-child(3)",
                    "//div[contains(text(), 'Salaire')]/following-sibling::div",
                    "//p[contains(text(), 'Salaire')]/following-sibling::p",
                    "//div[contains(@class, 'JobViewInfos')]/div[3]"
                ], "Salaire non disponible")
                
                # Nettoyage du salaire
                if salary_text != "Salaire non disponible":
                    job_info["salary"] = clean_salary(salary_text)
                else:
                    # Rechercher le salaire dans le texte complet de la page
                    try:
                        page_text = driver.find_element(By.TAG_NAME, "body").text
                        
                        # Motifs de recherche pour les salaires
                        salary_patterns = [
                            r"salaire\s*:?\s*([^\n\r,\.]+(?:\.[0-9]+)?[€k\s]*)",
                            r"r[ée]mun[ée]ration\s*:?\s*([^\n\r,\.]+(?:\.[0-9]+)?[€k\s]*)",
                            r"([0-9]+[\s,\.]*[0-9]*\s*[€k]\s*(?:brut|net)?)\s*(?:par|/|\\)\s*(?:mois|an|annuel)",
                            r"([0-9]+[\s,\.]*[0-9]*\s*[€k]\s*(?:à|-)\s*[0-9]+[\s,\.]*[0-9]*\s*[€k])"
                        ]
                        
                        for pattern in salary_patterns:
                            matches = re.findall(pattern, page_text, re.IGNORECASE)
                            if matches:
                                # Prendre le premier match qui semble valide
                                for match in matches:
                                    if isinstance(match, tuple):
                                        match = match[0]  # Prendre le premier groupe si c'est un tuple
                                    if match and len(match) > 3 and any(c.isdigit() for c in match):
                                        job_info["salary"] = clean_salary(match.strip())
                                        break
                            if job_info["salary"] != "Salaire non disponible":
                                break
                    except Exception:
                        pass
                
                # Date de publication - Sélecteurs améliorés
                try:
                    # Essayer d'abord avec l'attribut datetime qui est le plus précis
                    date_elem = driver.find_element(By.TAG_NAME, "time")
                    datetime_attr = date_elem.get_attribute("datetime")
                    if datetime_attr:
                        job_info["publication_date"] = datetime_attr
                except Exception:
                    # Si échec, essayer avec d'autres sélecteurs
                    date_text = extract_element_text([
                        "div[data-testid='job-publication-date']", 
                        "div.publication-date",
                        ".sc-dmqHEX time",
                        ".wui-text--caption",
                        "//div[contains(text(), 'Publiée')]", 
                        "//div[contains(text(), 'Postée')]",
                        "//time"
                    ], "Date non disponible")

                    if date_text != "Date non disponible":
                        # Essayer de convertir le texte de date en format ISO
                        try:
                            # Traiter les dates relatives (ex: "il y a 2 jours")
                            if "il y a" in date_text.lower():
                                match = re.search(r"il y a (\d+) (jour|jours|semaine|semaines|mois|an|ans)", date_text.lower())
                                if match:
                                    quantity = int(match.group(1))
                                    unit = match.group(2)
                                    
                                    if unit == "jour" or unit == "jours":
                                        delta = timedelta(days=quantity)
                                    elif unit == "semaine" or unit == "semaines":
                                        delta = timedelta(weeks=quantity)
                                    elif unit == "mois":
                                        delta = timedelta(days=quantity*30)  # approximation
                                    else:  # an ou ans
                                        delta = timedelta(days=quantity*365)  # approximation
                                    
                                    pub_date = datetime.now() - delta
                                    job_info["publication_date"] = pub_date.strftime("%Y-%m-%dT%H:%M:%SZ")
                            else:
                                # Essayer plusieurs formats de date
                                job_info["publication_date"] = date_text
                        except Exception:
                            job_info["publication_date"] = date_text
                
                # Nettoyer le titre pour enlever les codes et parties inutiles
                if "_" in job_info["title"]:
                    title_parts = job_info["title"].split("_")
                    
                    # Identifier les parties à conserver (généralement le début du titre)
                    clean_parts = []
                    for i, part in enumerate(title_parts):
                        # Conserver uniquement les parties qui ressemblent au titre du poste
                        # Ignorer les parties qui ressemblent à des codes ou des identifiants
                        if any(keyword in part.lower() for keyword in ["f h", "h f", "m f", "f m", "thale", "gwdq"]):
                            continue
                        # Ignorer les parties qui sont trop courtes et contiennent des chiffres/lettres mélangés
                        if len(part) < 10 and any(c.isdigit() for c in part):
                            continue
                        # Ignorer les parties qui sont des villes (déjà extraites dans location)
                        if job_info["location"] != "Lieu non disponible" and part.lower() == job_info["location"].lower():
                            continue
                        # Conserver cette partie
                        clean_parts.append(part)
                    
                    # Si on a trouvé des parties à conserver, reconstruire le titre
                    if clean_parts:
                        job_info["title"] = " ".join(clean_parts)
                    # Sinon, garder juste la première partie qui est généralement le titre du poste
                    elif len(title_parts) > 0:
                        job_info["title"] = title_parts[0]
                
                # Cas spécial: si le titre est juste une ville (comme "Paris")
                cities = ["paris", "lyon", "marseille", "toulouse", "nice", "nantes", "strasbourg", 
                         "montpellier", "bordeaux", "lille", "rennes", "reims", "toulon", "grenoble", 
                         "dijon", "angers", "nimes", "villeurbanne", "clermont", "limoges", "tours"]
                
                if job_info["title"].lower() in cities:
                    # Sauvegarder la ville comme localisation si elle n'est pas déjà définie
                    if job_info["location"] == "Lieu non disponible":
                        job_info["location"] = job_info["title"]
                    
                    # Essayer d'extraire le vrai titre depuis l'URL
                    url_parts = url.split("/")
                    if len(url_parts) > 0:
                        last_part = url_parts[-1].split("_")[0]  # Prendre la partie avant le premier underscore
                        if last_part and last_part.lower() != job_info["title"].lower():
                            # Convertir les tirets en espaces et capitaliser chaque mot
                            job_info["title"] = " ".join(word.capitalize() for word in last_part.split("-"))
                
                # Corriger les cas où les informations de poste sont dans le champ location
                if job_info["location"].startswith("Type de contrat :") or job_info["location"].startswith("Prise de poste :"):
                    # Extraire le type de contrat du champ location
                    contract_match = re.search(r"Type de contrat\s*:?\s*([^\n\r]+)", job_info["location"])
                    if contract_match and job_info["contract_type"] == "Type de contrat non disponible":
                        contract_value = contract_match.group(1).strip()
                        job_info["contract_type"] = normalize_contract_type(contract_value)
                    
                    # Réinitialiser le champ location et essayer de l'extraire de l'URL
                    job_info["location"] = "Lieu non disponible"
                    
                    # Essayer d'extraire la localisation de l'URL
                    url_parts = url.split("/")
                    if len(url_parts) > 0:
                        last_part = url_parts[-1].split("_")[0]
                        for city in cities:
                            if city in last_part.lower():
                                job_info["location"] = city.replace("-", " ").title()
                                break
                
                # Salaire - Sélecteurs améliorés
                salary_text = extract_element_text([
                    "div[data-testid='job-salary']", 
                    "div.salary", 
                    "div[class*='Salary']",
                    ".sc-dmqHEX div:last-child",
                    ".wui-text--subtitle:last-child",
                    "//div[contains(text(), 'Salaire')]/following-sibling::div",
                    "//div[contains(text(), 'Rémunération')]/following-sibling::div",
                    "//p[contains(text(), 'Salaire')]/following-sibling::p",
                    "//div[contains(@class, 'JobViewInfos')]/div[4]"
                ], "Salaire non disponible")
                
                if salary_text != "Salaire non disponible":
                    job_info["salary"] = clean_salary(salary_text)
                
                # Date de publication - Sélecteurs améliorés
                try:
                    # Essayer d'abord avec l'attribut datetime qui est le plus précis
                    date_elem = driver.find_element(By.TAG_NAME, "time")
                    datetime_attr = date_elem.get_attribute("datetime")
                    if datetime_attr:
                        job_info["publication_date"] = datetime_attr
                except Exception:
                    # Si échec, essayer avec d'autres sélecteurs
                    date_text = extract_element_text([
                        "div[data-testid='job-publication-date']", 
                        "div.publication-date",
                        ".sc-dmqHEX time",
                        ".wui-text--caption",
                        "//div[contains(text(), 'Publiée')]", 
                        "//div[contains(text(), 'Postée')]",
                        "//time"
                    ], "Date non disponible")
                    
                    if date_text != "Date non disponible":
                        # Essayer de convertir le texte de date en format ISO
                        try:
                            # Gestion des formats comme "il y a X jours"
                            if "il y a" in date_text.lower():
                                days_ago = re.search(r'\d+', date_text)
                                if days_ago:
                                    days = int(days_ago.group())
                                    pub_date = datetime.now() - timedelta(days=days)
                                    job_info["publication_date"] = pub_date.strftime("%Y-%m-%dT%H:%M:%SZ")
                            else:
                                job_info["publication_date"] = date_text
                        except Exception:
                            job_info["publication_date"] = date_text
                
                # Description du poste - Sélecteurs améliorés
                try:
                    # Essayer d'abord avec les sélecteurs spécifiques pour la description
                    description_selectors = [
                        "div[data-testid='job-description']", 
                        "div.job-description",
                        "div[class*='Description']",
                        ".sc-dmqHEX div.MarkdownText",
                        ".wui-text--body div.MarkdownText",
                        "//div[contains(@class, 'MarkdownText')]",
                        "//div[contains(@class, 'JobDescription')]",
                        "//div[contains(@class, 'job-sections')]"
                    ]
                    
                    for selector in description_selectors:
                        try:
                            if selector.startswith("//"):
                                elements = driver.find_elements(By.XPATH, selector)
                            else:
                                elements = driver.find_elements(By.CSS_SELECTOR, selector)
                                
                            if elements:
                                # Extraire le texte de tous les éléments trouvés
                                description_text = "\n".join([elem.text for elem in elements if elem.text.strip()])
                                if description_text and len(description_text) > 50:  # Vérifier que c'est une description valide
                                    job_info["description"] = description_text
                                    break
                        except Exception:
                            continue
                    
                    # Si aucune description n'a été trouvée, essayer une approche plus générale
                    if job_info["description"] == "Description non disponible":
                        # Chercher les sections principales du job qui pourraient contenir la description
                        main_sections = driver.find_elements(By.CSS_SELECTOR, "main section")
                        for section in main_sections:
                            section_text = section.text
                            # Vérifier si cette section ressemble à une description (texte long)
                            if len(section_text) > 200 and not section_text.startswith("Offres similaires"):
                                job_info["description"] = section_text
                                break
                except Exception as e:
                    print(f"Erreur lors de l'extraction de la description: {e}")
                
                all_jobs.append(job_info)
                print(f"Offre extraite: {job_info['title']} - {job_info['company']}")
                
                
                # Pas de sauvegarde périodique pour éviter les duplications
                # La sauvegarde se fera uniquement à la fin du scraping
        
        # Sauvegarde finale des données
        if all_jobs:
            save_data(all_jobs, query, location, upload_to_aws, save_locally=True)
        
        return all_jobs
    
    except Exception as e:
        print(f"Erreur lors du scraping: {e}")
        traceback.print_exc()
        return all_jobs
    
    finally:
        try:
            driver.quit()
        except:
            pass

def save_data(jobs, query, location, upload_to_aws=True, save_locally=True):
    """Sauvegarde les données vers AWS S3 et/ou localement"""
    if not jobs:
        print("Aucune offre d'emploi trouvée. Rien à sauvegarder.")
        return
        
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # Amélioration du nommage de fichier pour éviter les underscores vides
    if query and location:
        filename = f"welcome_jungle_{query.replace(' ', '_')}_{location.replace(' ', '_')}_{timestamp}.json"
    elif query:
        filename = f"welcome_jungle_{query.replace(' ', '_')}_all_locations_{timestamp}.json"
    elif location:
        filename = f"welcome_jungle_all_jobs_{location.replace(' ', '_')}_{timestamp}.json"
    else:
        filename = f"welcome_jungle_all_jobs_all_locations_{timestamp}.json"
        
    # Préparation des données pour la sauvegarde
    result = {
        "metadata": {
            "source": "Welcome to the Jungle",
            "query": query,
            "location": location,
            "timestamp": datetime.now().isoformat(),
            "total_jobs": len(jobs)
        },
        "jobs": jobs
    }
        
    # Convertir les données en JSON
    json_data = json.dumps(result, ensure_ascii=False, indent=2)
        
    # Sauvegarde locale si demandée
    if save_locally:
        # Créer le répertoire de sortie s'il n'existe pas
        output_dir = os.path.join(os.getcwd(), "data", "raw", "welcome_jungle")
        os.makedirs(output_dir, exist_ok=True)
            
        # Chemin complet du fichier local
        local_file_path = os.path.join(output_dir, filename)
            
        try:
            with open(local_file_path, 'w', encoding='utf-8') as f:
                f.write(json_data)
            print(f"{len(jobs)} offres sauvegardées localement: {local_file_path}")
        except Exception as e:
            print(f"Erreur lors de la sauvegarde locale: {e}")
        
    # Upload vers AWS S3 si demandé
    if upload_to_aws:
        try:
            # Créer le client S3
            s3_client = boto3.client(
                's3',
                aws_access_key_id=os.environ.get('KEY_ACCESS'),
                aws_secret_access_key=os.environ.get('KEY_SECRET'),
                region_name='eu-north-1'
            )
                
            # Upload direct vers S3
            s3_client.put_object(
                Bucket='data-lake-brut',
                Key=f"raw/welcome_jungle/{filename}",
                Body=json_data.encode('utf-8'),
                ContentType='application/json'
            )
                
            print(f"{len(jobs)} offres uploadées vers S3: data-lake-brut/raw/welcome_jungle/{filename}")
        except Exception as e:
            print(f"Erreur lors de l'upload vers S3: {e}")
        
    return True



def main():
    parser = argparse.ArgumentParser(description='Scraper Welcome to the Jungle')
    parser.add_argument('--query', type=str, default="", help='Terme de recherche (vide pour toutes les offres)')
    parser.add_argument('--location', type=str, default="", help='Localisation (vide pour toutes les localisations)')
    parser.add_argument('--pages', type=int, default=10, help='Nombre maximum de pages à scraper')
    parser.add_argument('--headless', action='store_true', help='Exécuter en mode headless')
    parser.add_argument('--no-upload', action='store_true', help='Ne pas uploader les résultats vers AWS S3')
    args = parser.parse_args()

    query_msg = f"'{args.query}'" if args.query else "toutes les offres"
    location_msg = f"à '{args.location}'" if args.location else "toutes localisations"
    print(f"Scraping de {query_msg} {location_msg} sur {args.pages} pages...")
    
    # Exécution du scraper
    jobs = scrape_welcome_jungle(
        query=args.query,
        location=args.location,
        max_pages=args.pages,
        headless=args.headless,
        upload_to_aws=not args.no_upload  # Par défaut, on upload vers AWS sauf si --no-upload est spécifié
    )
    
    # Normalisation des types de contrats pour fusionner "Stage" et "Offres de stage"
    for job in jobs:
        if job.get("contract_type"):
            contract_lower = job["contract_type"].lower()
            if "stage" in contract_lower or "offres de stage" in contract_lower or "offre de stage" in contract_lower:
                job["contract_type"] = "Stage"
    
    # La sauvegarde est déjà effectuée dans la fonction scrape_welcome_jungle
    # Pas besoin de sauvegarder à nouveau ici

if __name__ == "__main__":
    main()
