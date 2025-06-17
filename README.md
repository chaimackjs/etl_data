# Plateforme d'Analyse des Offres d'Emploi Tech

Ce projet implémente une plateforme complète d'extraction, transformation, chargement (ETL) et analyse des offres d'emploi tech provenant de multiples sources : France Travail (anciennement Pôle Emploi) et Welcome to the Jungle. La plateforme est optimisée pour traiter efficacement de grands volumes de données, les stocker dans une base de données PostgreSQL sur AWS RDS, et générer des visualisations et analyses pertinentes pour comprendre les tendances du marché de l'emploi tech en France. Le pipeline ETL unifié permet une collecte, transformation et analyse cohérente des données provenant de ces différentes sources.

## Architecture

La plateforme est structurée en quatre composants principaux :

1. **Extraction** : Récupération des données depuis les sources (API France Travail, scraping Welcome to the Jungle)
2. **Transformation** : Nettoyage, structuration et enrichissement des données brutes
3. **Chargement** : Stockage des données transformées dans une base de données PostgreSQL sur AWS RDS
4. **Analyse et Visualisation** : Génération de visualisations et tableaux de bord interactifs pour l'analyse des données

![Architecture du Projet](docs/images/architecture_diagram.png)

L'architecture s'appuie sur AWS pour l'infrastructure cloud (S3 pour le stockage des données brutes, RDS PostgreSQL pour la base de données) et utilise des scripts Python modulaires pour chaque étape du processus.

## Fonctionnalités

### Pipeline ETL

- Extraction des offres d'emploi par date ou de toutes les données disponibles
- Nettoyage et normalisation des données textuelles
- Extraction structurée des informations (type de contrat, localisation, etc.)
- Analyse des technologies mentionnées dans les descriptions
- Extraction et normalisation des villes à partir des libellés de localisation
- Catégorisation des types de contrat (CDI, CDD, STAGE, ALTERNANCE)
- Chargement optimisé dans une base de données PostgreSQL avec gestion des types de données
- Vérification des données déjà chargées pour éviter les doublons
- Gestion des erreurs et logging détaillé
- Variables d'environnement sécurisées via fichier .env
- Option `--force` pour forcer l'exécution du pipeline même si les données existent déjà
- Correction des incompatibilités de types entre les tables pour les jointures SQL
- Structure modulaire avec scripts d'orchestration unifiés

### Analyse et Visualisation

#### France Travail
- Distribution des types de contrat (CDI, CDD, etc.)
- Distribution géographique des offres d'emploi
- Technologies les plus demandées dans les offres
- Durée des contrats CDD
- Tableau de bord HTML interactif

#### Welcome to the Jungle
- Distribution des types de contrat
- Distribution géographique avec gestion intelligente des localisations non spécifiées ("Télétravail / Remote")
- Analyse des niveaux d'expérience avec catégorisation claire ("Tous niveaux d'expérience", "Junior", "Senior")
- Distribution des salaires
- Technologies et compétences demandées
- Nuage de mots des compétences
- Tableau de bord HTML interactif avec visualisations
- Rapport JSON avec statistiques détaillées

#### Analyses Avancées
- **Analyse des corrélations** : Corrélation entre technologies et types de contrat avec visualisations heatmap et graphiques à barres empilées
- **Analyse géographique avancée** : Distribution géographique détaillée des offres avec visualisations cartographiques
- **Analyse des salaires** : Distribution des salaires par technologie et expérience
- **Analyse temporelle** : Évolution des offres d'emploi dans le temps avec détection de tendances

## Prérequis

- Python 3.8+
- Bibliothèques Python :
  - Traitement de données : pandas, numpy
  - Base de données : sqlalchemy, psycopg2
  - Web scraping : requests, beautifulsoup4, selenium
  - Visualisation : matplotlib, seaborn, wordcloud
  - AWS : boto3
  - Utilitaires : python-dotenv, tqdm, logging
- Accès à AWS (RDS PostgreSQL, S3)
- Navigateur Chrome (pour le scraping Welcome to the Jungle)
- ChromeDriver compatible avec votre version de Chrome

## Installation

1. Cloner le dépôt
   ```bash
   git clone https://github.com/votre-utilisateur/projet-analyse-emploi-tech.git
   cd projet-analyse-emploi-tech
   ```

2. Installer les dépendances :
   ```bash
   pip install -r requirements.txt
   ```

3. Configurer les variables d'environnement :
   ```bash
   cp .env.example .env
   # Modifier le fichier .env avec vos informations
   ```

4. Télécharger ChromeDriver et l'ajouter au PATH (pour le scraping Welcome to the Jungle)

## Utilisation

### Script d'orchestration principal

Le script principal `main.py` permet d'exécuter l'ensemble du pipeline ETL et des analyses pour toutes les sources de données :

```bash
python main.py --action all --source all --force
```

#### Options disponibles dans main.py

- `--action` : Action à exécuter (`collect`, `etl`, `analyze`, `all`)
- `--source` : Source de données (`france_travail`, `welcome_jungle`, `all`)
- `--force` : Forcer l'exécution même si les données existent déjà
- `--save-locally` : Sauvegarder les données localement en plus de les envoyer sur S3

### Pipeline ETL France Travail

#### Exécution du pipeline France Travail

```bash
python scripts/pipeline/run_etl_france_travail.py --all
```

#### Options disponibles

- `--extract` : Exécuter uniquement l'étape d'extraction
- `--transform` : Exécuter uniquement l'étape de transformation
- `--load` : Exécuter uniquement l'étape de chargement
- `--all` : Exécuter toutes les étapes (extraction, transformation, chargement)
- `--force` : Forcer l'exécution même si les données existent déjà
- `--save-locally` : Sauvegarder les données localement en plus de les envoyer sur S3
- `--force` : Forcer l'exécution du pipeline même si les données sont déjà chargées

#### Exemples d'utilisation

Extraction seule :
```bash
python scripts/pipeline/run_etl.py --mode extract --all-data --output-csv
```

Transformation seule :
```bash
python scripts/pipeline/run_etl.py --mode transform --output-csv
```

Chargement seul :
```bash
python scripts/pipeline/run_etl.py --mode load
```

Forcer l'exécution même si les données sont déjà chargées :
```bash
python scripts/pipeline/run_etl.py --all-data --force
```

### Pipeline ETL Welcome to the Jungle

#### Extraction des données

```bash
python src/data_collection/scrapers/welcome_jungle.py
```

#### Transformation des données

```bash
python src/etl/scrapers/transformation.py
```

#### Visualisation des données brutes

```bash
python src/pipeline/scrapers/view_data.py
```

### Analyse des données

#### Analyse France Travail

```bash
python scripts/analyse/analyze_job_france_travai.py
```

#### Analyse Welcome to the Jungle

```bash
python src/analyse/run_welcome_jungle_analysis.py
```

Les visualisations et le tableau de bord HTML seront générés dans le dossier `data/analysis/visualizations/welcome_jungle/`

## Structure du projet

```
|── data/
|   |── raw/
|   |   |── france_travail/    # Données brutes extraites de l'API France Travail
|   |   |── welcome_jungle/    # Données brutes extraites de Welcome to the Jungle
|   |── processed/
|   |   |── france_travail/    # Données transformées France Travail
|   |   |── welcome_jungle/    # Données transformées Welcome to the Jungle
|   |── analysis/
|   |   |── visualizations/    # Visualisations générées
|   |   |── reports/           # Rapports JSON générés
|── logs/                   # Fichiers de log
|── main.py                # Script principal d'orchestration
|── scripts/
|   |── analysis/              # Scripts d'analyse avancée
|   |   |── run_etl_france_travail.py    # Pipeline ETL France Travail
|   |   |── run_etl_welcome_jungle.py    # Pipeline ETL Welcome to the Jungle
|   |── scrap_extract/         # Scripts de collecte de données
|   |   |── collect_all_france_travail_jobs.py  # Collecte France Travail
|   |   |── collect_welcome_jungle_jobs.py      # Collecte Welcome to the Jungle
|── src/
|   |── pipeline/
|   |   |── api/              # Modules pour l'API France Travail
|   |   |   |── extraction.py
|   |   |   |── transformation.py
|   |   |   |── loading.py
|   |   |   |── dotenv_utils.py
|   |   |── scrapers/          # Modules pour le scraping Welcome to the Jungle
|   |   |   |── init.py          # Script principal Welcome to the Jungle
|   |   |   |── extraction.py    # Extraction des données Welcome to the Jungle
|   |   |   |── transformation.py # Transformation des données Welcome to the Jungle
|   |   |   |── loading.py       # Chargement des données Welcome to the Jungle
|   |   |   |── view_data.py     # Visualisation des données brutes
|   |── data_collection/      # Modules de collecte de données
|   |   |── apis/
|   |   |   |── pole_emploi.py   # API France Travail
|   |   |── scrapers/
|   |   |   |── welcome_jungle.py # Scraper Welcome to the Jungle
|   |── analysis/            # Modules d'analyse de données
|   |   |── job_analysis.py   # Analyse des données France Travail
|   |   |── welcome_jungle_analysis.py # Analyse des données Welcome to the Jungle
|── .env                     # Fichier de variables d'environnement (non versionné)
│   │── tests/                 # Scripts de test
│   │   │── test_loading.py
│   │   │── test_transformation.py
│   │   └── test_api_configs.py
│   └── utils/                 # Scripts utilitaires
│       │── recreate_table.py  # Recréation de la table avec le bon schéma
│       └── deploy_infrastructure_aws.py
│── src/
│   │── pipeline/
│   │   │── api/              # Modules pour l'API France Travail
│   │   │   │── extraction.py
│   │   │   │── transformation.py
│   │   │   │── loading.py
│   │   │   └── dotenv_utils.py
│   │   └── scrapers/          # Modules pour le scraping Welcome to the Jungle
│   │       │── extraction.py    # Extraction des données Welcome to the Jungle
│   │       │── transformation.py # Transformation des données Welcome to the Jungle
│   │       │── loading.py       # Chargement des données Welcome to the Jungle
│   │       │── view_data.py     # Visualisation des données brutes
│   │       └── init.py          # Script principal Welcome to the Jungle
│   │── data_collection/      # Modules de collecte de données
│   │   │── apis/
│   │   └── scrapers/
│   │       └── welcome_jungle.py # Scraper Welcome to the Jungle
│   │── analysis/            # Modules d'analyse de données
│   │   │── job_analysis.py   # Analyse des données France Travail
│   │   │── welcome_jungle_analysis.py # Analyse des données Welcome to the Jungle
│   │   └── run_welcome_jungle_analysis.py # Script d'exécution de l'analyse
│   │── databases/           # Modules de gestion de base de données
│   └── infrastructure/      # Modules de gestion d'infrastructure
│── .env                     # Fichier de variables d'environnement (non versionné)
└── requirements.txt         # Dépendances Python
```

## Documentation

Le projet est accompagné d'une documentation complète pour chaque composant :

### Configuration de l'environnement

Le projet utilise un fichier `.env` pour stocker les variables d'environnement sensibles. Créez un fichier `.env` à la racine du projet avec les informations suivantes :

```
# Configuration AWS
KEY_ACCESS=votre_cle_acces_aws
KEY_SECRET=votre_cle_secrete_aws
DATA_LAKE_BUCKET=data-lake-brut

# Configuration PostgreSQL RDS
DB_HOST=votre_endpoint_rds.region.rds.amazonaws.com
DB_PORT=5432
DB_NAME=datawarehouses
DB_USER=admin
DB_PASSWORD=votre_mot_de_passe
```

### Documentation technique

Le projet comprend plusieurs documents de documentation technique détaillés :

- **[welcome_jungle_etl_documentation.md](docs/welcome_jungle_etl_documentation.md)** : Documentation technique du pipeline ETL Welcome to the Jungle
- **[welcome_jungle_analysis_documentation.md](docs/welcome_jungle_analysis_documentation.md)** : Documentation technique du module d'analyse Welcome to the Jungle
- **[DOCUMENTATION_API_FRANCE_TRAVAIL.md](docs/DOCUMENTATION_API_FRANCE_TRAVAIL.md)** : Documentation de l'API France Travail
- **[transformation_documentation.md](docs/transformation_documentation.md)** : Documentation des processus de transformation

### Documentation d'analyse et interprétation

- **[welcome_jungle_analysis_interpretation.md](docs/welcome_jungle_analysis_interpretation.md)** : Interprétation des résultats d'analyse Welcome to the Jungle
- **[analyse_donnees.md](docs/analyse_donnees.md)** : Méthodologie d'analyse des données France Travail
- **[interpretation_dashboard.md](docs/interpretation_dashboard.md)** : Guide d'interprétation du tableau de bord

## Pipelines ETL

### Pipeline France Travail

Le pipeline ETL France Travail est composé de trois étapes principales :

1. **Extraction** : Les données sont extraites depuis le bucket S3 AWS ou directement via l'API France Travail. Les fichiers JSON bruts sont stockés dans `data/raw/france_travail/`.

2. **Transformation** : Les données brutes sont nettoyées, structurées et enrichies. Les principales transformations incluent :
   - Normalisation des types de contrat (CDI, CDD, STAGE, ALTERNANCE)
   - Extraction des villes à partir des libellés de localisation
   - Analyse des technologies mentionnées dans les descriptions
   - Gestion des valeurs manquantes

3. **Chargement** : Les données transformées sont chargées dans une base de données PostgreSQL sur AWS RDS.

### Pipeline Welcome to the Jungle

Le pipeline ETL Welcome to the Jungle comprend également trois étapes :

1. **Extraction** : Les données sont extraites par scraping du site Welcome to the Jungle à l'aide de Selenium. Les offres d'emploi sont stockées au format JSON dans `data/raw/welcome_jungle/`.

2. **Transformation** : Les données brutes sont nettoyées et enrichies :
   - Normalisation des types de contrat
   - Extraction des niveaux d'expérience avec gestion intelligente des valeurs manquantes ("Tous niveaux d'expérience")
   - Normalisation des localisations avec gestion des valeurs non spécifiées ("Télétravail / Remote")
   - Extraction des technologies mentionnées
   - Analyse des salaires

3. **Chargement** : Les données transformées sont stockées dans la base de données PostgreSQL.

## Visualisations et Analyses

### Tableaux de bord

Le projet génère deux tableaux de bord HTML interactifs :

1. **Tableau de bord France Travail** : `data/analysis/visualizations/france_travail/dashboard.html`
   - Distribution des types de contrat
   - Distribution géographique
   - Technologies les plus demandées
   - Durée des contrats CDD

2. **Tableau de bord Welcome to the Jungle** : `data/analysis/visualizations/welcome_jungle/dashboard.html`
   - Distribution des types de contrat
   - Top 15 des localisations
   - Distribution des niveaux d'expérience
   - Distribution des salaires
   - Nuage de mots des compétences

### Rapports JSON

Des rapports JSON détaillés sont également générés pour permettre l'intégration avec d'autres outils d'analyse :

- `data/analysis/reports/france_travail_analysis.json`
- `data/analysis/reports/welcome_jungle_analysis.json`

## Base de données

La base de données PostgreSQL sur AWS RDS contient les tables suivantes :

- `france_travail_jobs` : Offres d'emploi de France Travail avec les champs suivants :
  - `id` : Identifiant unique de l'offre
  - `title` : Titre de l'offre
  - `description` : Description complète du poste
  - `contract_type` : Type de contrat normalisé (CDI, CDD, STAGE, ALTERNANCE)
  - `location` : Localisation du poste
  - `company_name` : Nom de l'entreprise
  - `publication_date` : Date de publication
  - `technologies` : Technologies mentionnées dans l'offre
  - Colonnes booléennes pour les technologies (has_python, has_java, etc.)

- `welcome_jungle_jobs` : Offres d'emploi de Welcome to the Jungle avec les champs suivants :
  - `id` : Identifiant unique de l'offre
  - `title` : Titre de l'offre
  - `description` : Description complète du poste
  - `contract_type` : Type de contrat normalisé
  - `lieu_travail` : Localisation du poste (avec gestion des valeurs manquantes)
  - `company_name` : Nom de l'entreprise
  - `publication_date` : Date de publication
  - `experience_level` : Niveau d'expérience requis (avec gestion des valeurs manquantes)
  - `min_salary` et `max_salary` : Fourchette de salaire
  - `technologies` : Technologies mentionnées dans l'offre

- `job_skills` : Table de liaison pour les compétences associées aux offres

## Améliorations récentes

### Corrections du pipeline ETL

- **Résolution des problèmes d'import** : Correction des chemins d'importation de `src.etl` vers `src.pipeline` pour résoudre les erreurs `ModuleNotFoundError`
- **Correction des chemins d'importation** : Mise à jour de tous les imports pour refléter la nouvelle structure du projet avec le module `pipeline` au lieu de `etl`
- **Correction des incompatibilités de types SQL** : Ajout d'un cast explicite (`CAST(j.id AS VARCHAR)`) dans les jointures entre tables pour résoudre les erreurs de type entre `integer` et `varchar`
- **Option `--force`** : Ajout d'une option pour forcer l'exécution du pipeline même si les données existent déjà
- **Orchestration unifiée** : Scripts d'orchestration dans le dossier `scripts/pipeline` permettant d'exécuter toutes les étapes du pipeline ETL
- **Gestion des erreurs améliorée** : Meilleure détection et signalement des erreurs dans le pipeline ETL

### Analyses avancées

- **Modules d'analyse spécialisés** : Création de modules dédiés dans `scripts/analyse/` pour différents types d'analyses (corrélations, géographie, salaires, temporel)
- **Visualisations interactives** : Génération de tableaux de bord HTML avec visualisations interactives
- **Analyses croisées** : Corrélation entre technologies et types de contrat pour identifier les tendances du marché
- **Analyse géographique améliorée** : Correction de l'analyse géographique pour mieux gérer les données de localisation et attribuer correctement les régions

## Optimisations et Bonnes Pratiques

- **Gestion cohérente des valeurs manquantes** : Remplacement logique des valeurs manquantes ("Télétravail / Remote" pour les localisations non spécifiées, "Tous niveaux d'expérience" pour l'expérience non précisée)
- **Vérification des données déjà chargées** : Évite les doublons et optimise les performances
- **Logging détaillé** : Suivi précis de chaque étape du pipeline
- **Structure modulaire** : Organisation claire du code pour faciliter la maintenance
- **Documentation complète** : Documentation technique et d'interprétation pour chaque composant
- **Gestion sécurisée des identifiants** : Variables d'environnement stockées dans un fichier `.env`

## Conclusion et Perspectives

Cette plateforme d'analyse des offres d'emploi tech fournit une vision complète du marché de l'emploi en France à travers deux sources complémentaires : France Travail et Welcome to the Jungle. Le pipeline ETL unifié permet une collecte, transformation et analyse cohérente des données, avec une gestion robuste des erreurs et des options flexibles pour l'exécution.

Les analyses avancées générées permettent d'identifier les tendances du marché, les compétences les plus demandées, et la distribution géographique des opportunités. Les visualisations interactives et les tableaux de bord HTML facilitent l'interprétation des données pour les décideurs.

Les récentes améliorations ont permis de résoudre les problèmes d'import, les incompatibilités de types dans les jointures SQL, et d'ajouter des options comme `--force` pour une meilleure flexibilité d'utilisation.

Perspectives d'évolution :

1. **Intégration d'autres sources de données** : LinkedIn, Indeed, Glassdoor
2. **Analyse temporelle avancée** : Modèles prédictifs pour anticiper les tendances du marché
3. **Prédiction des salaires** : Modèles de machine learning pour prédire les salaires en fonction des compétences et de l'expérience
4. **API REST** : Exposition des données et analyses via une API
5. **Interface utilisateur web** : Développement d'une interface web interactive pour explorer les données
6. **Automatisation complète** : Mise en place d'un pipeline ETL automatisé avec exécution périodique

## Auteur

Projet développé dans le cadre du BLOC 2 - Architecture AI
