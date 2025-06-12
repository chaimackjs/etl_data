# Projet de Collecte et Analyse d'Offres d'Emploi

## Description
Ce projet vise à collecter, traiter et analyser des offres d'emploi provenant de différentes sources (Welcome to the Jungle, Pôle Emploi) pour constituer une base de données d'offres d'emploi. Les données sont extraites, transformées et chargées dans un pipeline ETL complet, puis stockées dans AWS S3 et RDS PostgreSQL.

## Architecture du Projet

```
/projet_offres_emploi
│
├── src/
│   ├── data_collection/
│   │   ├── __init__.py
│   │   ├── scrapers/
│   │   │   ├── __init__.py
│   │   │   ├── welcome_jungle_improved.py  # Scraper pour Welcome to the Jungle
│   │   │   └── pole_emploi.py              # Client API pour Pôle Emploi
│   │
│   ├── etl/
│   │   ├── __init__.py
│   │   ├── extraction.py                   # Extraction des données depuis S3 ou local
│   │   ├── transformation.py               # Nettoyage et standardisation des données
│   │   ├── loading.py                      # Chargement vers S3 et RDS
│   │   └── etl_pipeline.py                 # Pipeline ETL complet
│   │
│   ├── infrastructure/
│   │   ├── __init__.py
│   │   ├── s3_setup.py                     # Configuration et gestion des buckets S3
│   │   ├── rds_setup.py                    # Configuration et connexion à RDS PostgreSQL
│   │   └── lambda_setup.py                 # Déploiement de fonctions AWS Lambda
│   │
│   └── analysis/
│       ├── __init__.py
│       └── job_analysis.py                 # Analyse des offres d'emploi
│
├── scripts/
│   ├── analyze_job_page.py                 # Analyse des pages d'offres pour le scraping
│   └── deploy_lambda.py                    # Script de déploiement des fonctions Lambda
│
├── data/
│   ├── raw/                                # Données brutes scrapées
│   │   ├── welcome_jungle/
│   │   └── pole_emploi/
│   └── processed/                          # Données transformées
│       ├── welcome_jungle/
│       └── pole_emploi/
│
├── logs/                                   # Fichiers de logs
│
├── notebooks/                              # Notebooks Jupyter pour l'analyse
│
├── .env                                    # Variables d'environnement (non versionné)
├── .gitignore                              # Fichiers à ignorer par Git
├── README.md                               # Documentation du projet
└── requirements.txt                        # Dépendances Python
```

## Configuration

### Variables d'environnement
Créez un fichier `.env` à la racine du projet avec les variables suivantes :

```
# AWS Credentials
KEY_ACCESS=votre_access_key
KEY_SECRET=votre_secret_key

# S3 Configuration
data_lake_bucket=nom_de_votre_bucket

# RDS Configuration
RDS_HOST=votre_endpoint_rds
RDS_PORT=5432
RDS_DBNAME=nom_de_votre_db
RDS_USER=votre_utilisateur
RDS_PASSWORD=votre_mot_de_passe

# Pôle Emploi API
POLE_EMPLOI_CLIENT_ID=votre_client_id
POLE_EMPLOI_CLIENT_SECRET=votre_client_secret
```

## Installation

```bash
# Cloner le dépôt
git clone <url_du_depot>
cd projet_offres_emploi

# Installer les dépendances
pip install -r requirements.txt
```

## Utilisation

### Scraper Welcome to the Jungle
```bash
python -m src.data_collection.scrapers.welcome_jungle_improved
```

### Exécuter le pipeline ETL complet
```bash
python -m src.etl.etl_pipeline --source all
```

### Options du pipeline ETL
```
--source {welcome_jungle,pole_emploi,all}  # Source des données
--search-terms TERM1 TERM2                # Termes de recherche pour Welcome to the Jungle
--max-pages N                             # Nombre maximum de pages à scraper
--no-local                                # Ne pas sauvegarder en local
--no-s3                                   # Ne pas uploader vers S3
--no-rds                                  # Ne pas charger dans RDS
```

## Infrastructure AWS

### S3
- Bucket: `data-lake-brut`
- Structure:
  - `/raw/welcome_jungle/` - Données brutes de Welcome to the Jungle
  - `/raw/pole_emploi/` - Données brutes de Pôle Emploi
  - `/processed/welcome_jungle/` - Données transformées de Welcome to the Jungle
  - `/processed/pole_emploi/` - Données transformées de Pôle Emploi

### RDS PostgreSQL
- Endpoint: [défini dans .env]
- Port: [défini dans .env]
- Database: [défini dans .env]
- Tables:
  - `jobs` - Offres d'emploi
  - `skills` - Compétences extraites
  - `job_skills` - Relation entre offres et compétences

### Lambda
- Fonctions pour l'exécution automatique des scrapers

## Déploiement de l'Infrastructure AWS

Le script `deploy_infrastructure.py` permet de tester et déployer l'infrastructure AWS complète (S3, RDS, Lambda) en une seule commande.

### Utilisation

```bash
# Déployer et tester toute l'infrastructure
python scripts/deploy_infrastructure.py --service all

# Tester uniquement la connexion S3
python scripts/deploy_infrastructure.py --service s3

# Tester la connexion RDS et créer les tables
python scripts/deploy_infrastructure.py --service rds --create-tables

# Déployer et tester la fonction Lambda pour Welcome to the Jungle
python scripts/deploy_infrastructure.py --service lambda --deploy-lambda --test-lambda
```

### Options disponibles

```
# Options générales
--service {s3,rds,lambda,all}            # Service AWS à tester ou déployer

# Options S3
--bucket-name NOM_BUCKET                # Nom du bucket S3 à utiliser
--create-bucket                         # Créer le bucket s'il n'existe pas
--test-upload                           # Tester l'upload d'un fichier
--list-files                            # Lister les fichiers dans le bucket

# Options RDS
--rds-host HOTE                         # Hôte de la base de données
--rds-port PORT                         # Port de la base de données
--rds-dbname NOM_DB                     # Nom de la base de données
--rds-user UTILISATEUR                  # Utilisateur de la base de données
--rds-password MOT_DE_PASSE             # Mot de passe de la base de données
--create-tables                         # Créer les tables dans la base de données

# Options Lambda
--deploy-lambda                         # Déployer la fonction Lambda
--test-lambda                           # Tester l'invocation de la fonction
--lambda-function NOM_FONCTION          # Nom de la fonction Lambda à tester
```
