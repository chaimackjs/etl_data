# Documentation ETL Welcome to the Jungle

## Vue d'ensemble

Ce document décrit le pipeline ETL (Extract, Transform, Load) pour les données de Welcome to the Jungle. Le pipeline extrait les offres d'emploi depuis le site welcometothejungle.com via un scraper, transforme les données pour les normaliser et les enrichir, puis les charge dans une base de données PostgreSQL AWS RDS (Data Warehouse) pour analyse ultérieure.

## Architecture du pipeline

Le pipeline ETL est composé de trois étapes principales :

1. **Extraction** : Scraping des données depuis welcometothejungle.com et stockage dans AWS S3 (Data Lake)
2. **Transformation** : Nettoyage et enrichissement des données
3. **Chargement** : Insertion des données transformées dans PostgreSQL AWS RDS (Data Warehouse)

Cette architecture s'intègre dans l'infrastructure globale de données du projet qui utilise AWS S3 comme Data Lake pour stocker les données brutes et AWS RDS PostgreSQL comme Data Warehouse pour l'analyse.

## Structure du projet

```
Projet/
├── src/
│   ├── data_collection/
│   │   └── scrapers/
│   │       └── welcome_jungle.py          # Script de scraping
│   ├── pipeline/
│   │   └── scrapers/
│   │       ├── extraction.py              # Module d'extraction
│   │       ├── transformation.py          # Module de transformation
│   │       └── loading.py                 # Module de chargement
│   └── analysis/
│       └── welcome_jungle_analysis.py     # Analyse des données
├── scripts/
│   └── pipeline/
│       └── run_etl_welcome_jungle.py      # Script d'exécution du pipeline
├── data/
│   ├── raw/
│   │   └── welcome_jungle/                # Données brutes
│   └── processed/
│       └── welcome_jungle/                # Données transformées
└── docs/
    └── welcome_jungle_etl_documentation.md # Cette documentation
```

## Configuration requise

- Python 3.8+
- Selenium et ChromeDriver pour le scraping
- Accès à AWS S3
- Base de données PostgreSQL
- Variables d'environnement configurées dans un fichier `.env`

### Variables d'environnement

Le fichier `.env` doit contenir les variables suivantes :

```
# AWS Credentials
KEY_ACCESS= 
KEY_SECRET= 

# S3 Configuration
DATA_LAKE_BUCKET=

# PostgreSQL Configuration
DB_HOST= 
DB_PORT=
DB_NAME=
DB_USER=
DB_PASSWORD=
```

Le script `setup_env.py` dans le dossier `scripts/utils/` peut être utilisé pour configurer facilement ces variables d'environnement.

## Étape d'extraction

L'extraction des données est réalisée par le script `welcome_jungle.py` qui utilise Selenium pour scraper les offres d'emploi depuis le site Welcome to the Jungle.

### Fonctionnalités principales

- Scraping des offres d'emploi avec Selenium et ChromeDriver
- Extraction de données détaillées pour chaque offre :
  - Titre, entreprise, lieu, type de contrat
  - Date de publication, description, compétences
  - Niveau d'expérience, secteur, salaire
- Sauvegarde locale des données au format JSON
- Upload des données vers AWS S3 (Data Lake)
- Correction de la duplication des fichiers de sauvegarde
- Amélioration du nommage des fichiers pour éviter les underscores vides

### Options de ligne de commande

```bash
python src/data_collection/scrapers/welcome_jungle.py --query "data scientist" --location "Paris" --pages 5 --headless
```

- `--query` : Terme de recherche (vide pour toutes les offres)
- `--location` : Localisation (vide pour toutes les localisations)
- `--pages` : Nombre maximum de pages à scraper (défaut: 10)
- `--headless` : Exécuter en mode headless (sans interface graphique)
- `--no-upload` : Ne pas uploader les résultats vers AWS S3

### Structure des données extraites

Les données sont sauvegardées au format JSON avec la structure suivante :

```json
{
  "metadata": {
    "source": "Welcome to the Jungle",
    "query": "data scientist",
    "location": "Paris",
    "timestamp": "2025-06-15T14:05:57",
    "total_jobs": 42
  },
  "jobs": [
    {
      "title": "Data Scientist Senior",
      "company": "Exemple Entreprise",
      "location": "Paris, France",
      "contract_type": "CDI",
      "date": "2025-06-10",
      "url": "https://www.welcometothejungle.com/fr/companies/exemple/jobs/data-scientist-senior",
      "description": "Description détaillée du poste...",
      "category": "Tech",
      "experience": "3-5 ans",
      "salary": "45K€ - 60K€"
    },
    // ... autres offres
  ]
}
```

### Optimisations implémentées

1. **Correction de la duplication des fichiers**
   - Problème : Les données étaient sauvegardées deux fois (une fois dans la fonction `scrape_welcome_jungle` et une seconde fois dans la fonction `main`)
   - Solution : Suppression de la sauvegarde redondante dans la fonction `main`

2. **Amélioration du nommage des fichiers**
   - Problème : Les noms de fichiers contenaient des underscores vides quand `query` et `location` n'étaient pas spécifiés
   - Solution : Génération de noms de fichiers plus propres et descriptifs

3. **Gestion des erreurs et retries**
   - Implémentation d'un système de retry en cas d'échec du chargement d'une page
   - Gestion des exceptions lors de l'extraction des éléments

4. **Simulation de comportement humain**
   - Ajout de délais aléatoires entre les actions pour éviter la détection du scraping
   - Utilisation de mouvements de souris aléatoires

## Étape de transformation

La transformation des données est réalisée par le module `transformation.py` qui nettoie et enrichit les données extraites pour les préparer au chargement.

### Fonctionnalités principales

1. **Détection des technologies**
   - Analyse des descriptions d'offres pour détecter les technologies mentionnées
   - Création de colonnes booléennes pour chaque technologie (has_python, has_java, etc.)

2. **Normalisation des types de contrat**
   - Standardisation des types de contrat (CDI, CDD, STAGE, ALTERNANCE, FREELANCE, OTHER)
   - Fusion des catégories similaires (ex: "Stage" et "Offres de stage")

3. **Extraction des informations de salaire**
   - Analyse et normalisation des formats de salaire (45K€ - 60K€, etc.)
   - Extraction des valeurs minimales et maximales

4. **Normalisation des lieux de travail**
   - Extraction des villes et pays à partir des chaînes de localisation
   - Standardisation des formats

5. **Extraction de l'expérience requise**
   - Analyse des descriptions pour déterminer les années d'expérience requises
   - Catégorisation par niveau d'expérience

### Algorithmes clés

#### Détection des technologies

```python
def detect_technologies(text):
    if not text or not isinstance(text, str):
        return {}
    
    # Liste des technologies à détecter
    technologies = {
        'has_python': ['python'],
        'has_java': ['java'],
        'has_javascript': ['javascript', 'js', 'node', 'react', 'angular', 'vue'],
        # ... autres technologies
    }
    
    # Détection
    result = {}
    for tech_key, keywords in technologies.items():
        result[tech_key] = any(keyword in text.lower() for keyword in keywords)
    
    return result
```

#### Normalisation des types de contrat

```python
def normalize_contract_type(contract_type, title=None):
    if not contract_type:
        return "OTHER"
    
    contract_lower = contract_type.lower()
    
    if any(term in contract_lower for term in ["cdi", "permanent", "indéterminé"]):
        return "CDI"
    elif any(term in contract_lower for term in ["cdd", "déterminé", "temporary"]):
        return "CDD"
    elif any(term in contract_lower for term in ["stage", "intern", "internship"]):
        return "STAGE"
    elif any(term in contract_lower for term in ["alternance", "apprentissage"]):
        return "ALTERNANCE"
    elif any(term in contract_lower for term in ["freelance", "indépendant"]):
        return "FREELANCE"
    else:
        return "OTHER"
```

### Structure des données transformées

Les données transformées sont sauvegardées au format CSV avec les colonnes suivantes :

| Colonne | Description |
|---------|-------------|
| id | Identifiant unique de l'offre |
| title | Titre de l'offre d'emploi |
| company | Nom de l'entreprise |
| location | Localisation brute |
| city | Ville extraite |
| country | Pays extrait |
| contract_type | Type de contrat brut |
| contract_type_std | Type de contrat normalisé |
| date | Date de publication |
| url | URL de l'offre |
| description | Description de l'offre |
| category | Catégorie de l'offre |
| experience | Expérience requise |
| min_experience | Années d'expérience minimales |
| max_experience | Années d'expérience maximales |
| experience_level | Niveau d'expérience (Junior, Mid, Senior) |
| salary | Information de salaire brute |
| min_salary | Salaire minimum extrait |
| max_salary | Salaire maximum extrait |
| salary_currency | Devise du salaire |
| salary_period | Période du salaire (annuel, mensuel) |
| has_python | Indique si Python est mentionné |
| has_java | Indique si Java est mentionné |
| has_javascript | Indique si JavaScript est mentionné |
| ... | Autres technologies détectées |

### Utilisation

```python
from src.pipeline.scrapers.transformation import transform_welcome_jungle_data, save_transformed_data

# Transformer les données
transformed_df = transform_welcome_jungle_data(df)

# Sauvegarder les données transformées
output_file = save_transformed_data(transformed_df)
```

## Étape de chargement

Le chargement des données est réalisé par le module `loading.py` qui insère les données transformées dans une base de données PostgreSQL hébergée sur AWS RDS (Data Warehouse).

### Fonctionnalités principales

- Connexion à la base de données PostgreSQL avec SQLAlchemy
- Création de la table `welcome_jungle_jobs` si elle n'existe pas
- Vérification des données existantes avant chargement pour éviter les duplications
- Utilisation du type `Text` pour les colonnes textuelles longues
- Optimisation des requêtes avec `sqlalchemy.text()`

### Schéma de la table

La table `welcome_jungle_jobs` est créée avec le schéma suivant :

```sql
CREATE TABLE IF NOT EXISTS welcome_jungle_jobs (
    id TEXT PRIMARY KEY,
    title TEXT,
    company TEXT,
    location TEXT,
    city TEXT,
    country TEXT,
    contract_type TEXT,
    contract_type_std TEXT,
    date DATE,
    url TEXT,
    description TEXT,
    category TEXT,
    experience TEXT,
    min_experience INTEGER,
    max_experience INTEGER,
    experience_level TEXT,
    salary TEXT,
    min_salary FLOAT,
    max_salary FLOAT,
    salary_currency TEXT,
    salary_period TEXT,
    has_python BOOLEAN,
    has_java BOOLEAN,
    has_javascript BOOLEAN,
    has_typescript BOOLEAN,
    has_php BOOLEAN,
    has_ruby BOOLEAN,
    has_csharp BOOLEAN,
    has_cpp BOOLEAN,
    has_c BOOLEAN,
    has_go BOOLEAN,
    has_sql BOOLEAN,
    has_nosql BOOLEAN,
    has_aws BOOLEAN,
    has_azure BOOLEAN,
    has_gcp BOOLEAN,
    has_docker BOOLEAN,
    has_kubernetes BOOLEAN,
    has_html BOOLEAN,
    has_css BOOLEAN,
    has_react BOOLEAN,
    has_angular BOOLEAN,
    has_vue BOOLEAN,
    has_datascience BOOLEAN,
    has_git BOOLEAN,
    has_agile BOOLEAN
)
```

### Optimisations implémentées

1. **Vérification des données déjà chargées**
   - Le script vérifie si la table `welcome_jungle_jobs` existe et contient déjà des données
   - Si les données sont déjà présentes, un message est affiché à l'utilisateur
   - L'option `--force` permet de forcer le chargement même si les données existent déjà

2. **Utilisation de types de données optimisés**
   - Utilisation du type `Text` au lieu de `String(255)` pour les colonnes textuelles qui peuvent contenir des valeurs longues
   - Utilisation de types appropriés pour chaque colonne (BOOLEAN, INTEGER, FLOAT, etc.)

3. **Gestion des erreurs de connexion**
   - Implémentation de retries en cas d'échec de connexion à la base de données
   - Gestion des exceptions lors de l'insertion des données

### Utilisation

```python
from src.pipeline.scrapers.loading import get_db_connection, load_welcome_jungle_data

# Obtenir une connexion à la base de données
engine = get_db_connection()

# Charger les données
success = load_welcome_jungle_data(transformed_df, engine)
```

## Exécution du pipeline complet

Le script `run_etl_welcome_jungle.py` dans le dossier `scripts/etl/` permet d'exécuter le pipeline ETL complet en une seule commande.

### Fonctionnalités principales

- Orchestration des étapes d'extraction, transformation et chargement
- Options de ligne de commande pour personnaliser l'exécution
- Gestion des erreurs et logging
- Vérification des données existantes avant exécution

### Options de ligne de commande

```bash
python scripts/pipeline/run_etl_welcome_jungle.py --query "data scientist" --location "Paris" --pages 5 --headless --force
```

- `--query` : Terme de recherche pour l'extraction (vide pour toutes les offres)
- `--location` : Localisation pour l'extraction (vide pour toutes les localisations)
- `--pages` : Nombre maximum de pages à scraper (défaut: 10)
- `--headless` : Exécuter le scraper en mode headless (sans interface graphique)
- `--no-upload` : Ne pas uploader les résultats vers AWS S3
- `--force` : Forcer l'exécution même si les données existent déjà
- `--extract-only` : Exécuter uniquement l'étape d'extraction
- `--transform-only` : Exécuter uniquement l'étape de transformation
- `--load-only` : Exécuter uniquement l'étape de chargement

### Exemple d'exécution complète

```bash
# Configuration des variables d'environnement
python scripts/utils/setup_env.py

# Exécution du pipeline complet pour les offres de data scientist à Paris
python scripts/etl/run_etl_welcome_jungle.py --query "data scientist" --location "Paris" --headless

# Exécution forcée du pipeline complet pour toutes les offres (sans filtrage)
python scripts/pipeline/run_etl_welcome_jungle.py --force --headless --pages 20
```

### Structure du script principal

Le script principal `run_etl_welcome_jungle.py` est structuré comme suit :

```python
def run_pipeline(args):
    """Exécute le pipeline ETL Welcome to the Jungle complet"""
    # Étape 1: Vérification des données existantes
    if not args.force:
        # Vérifier si les données existent déjà
        # Si oui, afficher un message et terminer
    
    # Étape 2: Extraction
    if args.extract_only or not (args.transform_only or args.load_only):
        # Exécuter l'extraction
    
    # Étape 3: Transformation
    if args.transform_only or not (args.extract_only or args.load_only):
        # Exécuter la transformation
    
    # Étape 4: Chargement
    if args.load_only or not (args.extract_only or args.transform_only):
        # Exécuter le chargement
```

## Intégration avec le Data Lake et le Data Warehouse

### Data Lake (AWS S3)

- Les données brutes sont stockées dans le bucket S3 `data-lake-brut`
- Structure des fichiers dans le Data Lake :
  ```
  data-lake-brut/
  ├── welcome_jungle/
  │   ├── raw/
  │   │   ├── jobs_data_scientist_paris_2025-06-15.json
  │   │   ├── jobs_all_2025-06-10.json
  │   │   └── ...
  │   ├── processed/
  │       ├── jobs_data_scientist_paris_2025-06-15.csv
  │       ├── jobs_all_2025-06-10.csv
  │       └── ...
  └── france_travail/
      ├── raw/
      └── processed/
  ```

### Data Warehouse (AWS RDS PostgreSQL)

- Les données transformées sont stockées dans la base de données PostgreSQL hébergée sur AWS RDS
- Point de terminaison : `datawarehouses.c32ygg4oyapa.eu-north-1.rds.amazonaws.com`
- Tables principales :
  - `welcome_jungle_jobs` : Offres d'emploi Welcome to the Jungle
  - `france_travail_jobs` : Offres d'emploi France Travail
  - `job_skills` : Compétences associées aux offres d'emploi

## Analyse des données

Les données chargées dans le Data Warehouse peuvent être analysées pour obtenir des insights sur le marché de l'emploi :

1. **Distribution des types de contrat**
   ```sql
   SELECT contract_type_std, COUNT(*) as count
   FROM welcome_jungle_jobs
   GROUP BY contract_type_std
   ORDER BY count DESC;
   ```

2. **Distribution géographique des offres**
   ```sql
   SELECT city, COUNT(*) as count
   FROM welcome_jungle_jobs
   WHERE city IS NOT NULL
   GROUP BY city
   ORDER BY count DESC
   LIMIT 10;
   ```

3. **![Technologies les Plus Demandées](../images/welcome_jungle/top_technologies.png)**
   ```sql
   SELECT
     SUM(CASE WHEN has_python THEN 1 ELSE 0 END) as python_count,
     SUM(CASE WHEN has_java THEN 1 ELSE 0 END) as java_count,
     SUM(CASE WHEN has_javascript THEN 1 ELSE 0 END) as javascript_count,
     SUM(CASE WHEN has_sql THEN 1 ELSE 0 END) as sql_count
   FROM welcome_jungle_jobs;
   ```

4. **Salaires moyens par niveau d'expérience**
   ```sql
   SELECT experience_level, AVG(min_salary) as avg_min_salary, AVG(max_salary) as avg_max_salary
   FROM welcome_jungle_jobs
   WHERE min_salary IS NOT NULL AND max_salary IS NOT NULL
   GROUP BY experience_level;
   ```

## Maintenance et dépannage

### Problèmes connus

1. **Erreurs de scraping**
   - **Problème** : Le site Welcome to the Jungle peut changer sa structure HTML, causant des erreurs de scraping
   - **Solution** : Mettre à jour les sélecteurs CSS/XPath dans le script `welcome_jungle.py`

2. **Erreurs de connexion RDS**
   - **Problème** : Connection timeout - Base de données non accessible publiquement
   - **Solution** : Vérifier les paramètres de sécurité RDS et s'assurer que l'adresse IP est autorisée

3. **Erreurs d'authentification AWS**
   - **Problème** : Clés d'accès AWS expirées ou invalides
   - **Solution** : Mettre à jour les variables d'environnement avec des clés valides

### Scripts utilitaires

1. **Recréation de la table**
   ```bash
   python scripts/utils/recreate_table.py --table welcome_jungle_jobs
   ```

2. **Vérification de la structure de la table**
   ```bash
   python scripts/utils/check_table_structure.py --table welcome_jungle_jobs
   ```

3. **Vérification des données de salaire**
   ```bash
   python scripts/utils/check_salary_data.py
   ```

## Prochaines étapes

1. **Améliorations du scraper**
   - Implémentation d'un système de proxy rotation pour éviter les blocages
   - Optimisation du scraping parallèle pour améliorer les performances

2. **Améliorations de la transformation**
   - Amélioration de la détection des technologies avec NLP
   - Normalisation plus précise des lieux avec une API de géocodage

3. **Améliorations du chargement**
   - Implémentation d'un système de chargement incrémental
   - Optimisation des performances de la base de données avec des index

4. **Analyses avancées**
   - Création d'un tableau de bord interactif pour visualiser les données
   - Implémentation d'analyses prédictives sur l'évolution du marché de l'emploi
