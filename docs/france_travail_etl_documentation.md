# Documentation ETL France Travail

## Vue d'ensemble

Ce document décrit le pipeline ETL (Extract, Transform, Load) pour les données de France Travail (anciennement Pôle Emploi). Le pipeline extrait les offres d'emploi depuis des fichiers JSON stockés dans un bucket AWS S3 (Data Lake), transforme les données pour les normaliser et les enrichir, puis les charge dans une base de données PostgreSQL AWS RDS (Data Warehouse) pour analyse ultérieure.

## Architecture du pipeline

Le pipeline ETL est composé de trois étapes principales :

1. **Extraction** : Récupération des données brutes depuis AWS S3 (Data Lake)
2. **Transformation** : Nettoyage et enrichissement des données
3. **Chargement** : Insertion des données transformées dans PostgreSQL AWS RDS (Data Warehouse)

Cette architecture s'intègre dans l'infrastructure globale de données du projet qui utilise AWS S3 comme Data Lake pour stocker les données brutes et AWS RDS PostgreSQL comme Data Warehouse pour l'analyse.

## Structure du projet

```
src/pipeline/
├── api/
│   ├── __pycache__/
│   ├── dotenv_utils.py            # Utilitaires pour les variables d'environnement
│   ├── extraction.py              # Module d'extraction des données
│   ├── france_travail_etl.py      # Point d'entrée du pipeline ETL France Travail
│   ├── job_skills_loader.py       # Chargement des compétences
│   ├── loading.py                 # Module de chargement des données
│   ├── skills_extraction.py       # Extraction des compétences
│   ├── transformation.py          # Module de transformation des données
│   └── verify_db_load.py          # Vérification du chargement en base
├── __pycache__/
└── run_etl_france_travail.py      # Script principal d'exécution
```

## Configuration requise

- Python 3.8+
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

L'extraction récupère les fichiers JSON depuis AWS S3 (Data Lake) et les convertit en DataFrame pandas.

### Fonctionnalités principales

- Connexion au bucket S3 avec authentification AWS
- Téléchargement des fichiers JSON contenant les offres d'emploi
- Mise en cache local des fichiers pour éviter des téléchargements répétés
- Support pour l'extraction de toutes les données sans filtrage par date
- Vérification de l'existence des données en base avant extraction

### Statistiques d'extraction

- Nombre d'offres extraites : 9429 offres d'emploi
- Source des données : Bucket S3 AWS `data-lake-brut`

### Utilisation

```python
from src.pipeline.api.extraction import extract_france_travail_data

# Extraire toutes les données
df = extract_france_travail_data(all_data=True)

# Forcer l'extraction même si les données existent déjà en base
df = extract_france_travail_data(all_data=True, force=True)
```

## Étape de transformation

La transformation nettoie et enrichit les données extraites pour les préparer au chargement.

### Fonctionnalités principales

1. **Extraction et normalisation des données de localisation**
   - Extraction des villes à partir des libellés de localisation (format "XX - NOM_VILLE")
   - Gestion des pays et régions
   - Traitement des valeurs manquantes pour les villes

2. **Catégorisation des types de contrat**
   - Normalisation en catégories standard (CDI, CDD, STAGE, ALTERNANCE)
   - Analyse du texte des offres pour détecter les types de contrat

3. **Analyse des technologies mentionnées**
   - Détection des langages de programmation (Python, Java, JavaScript, etc.)
   - Détection des technologies cloud (AWS, Azure, GCP)
   - Détection des bases de données (SQL, NoSQL)

4. **Gestion des valeurs manquantes**
   - Stratégies de remplacement pour les champs obligatoires
   - Filtrage des offres incomplètes

5. **Validation de la qualité des données**
   - Vérification de la cohérence des données transformées
   - Génération de statistiques sur les données transformées

### Statistiques de transformation

- Nombre d'offres transformées : 3150
- Distribution des types de contrat : 1493 CDI, 632 CDD, 1025 contrats divers
- Principales villes : Toulouse, Montpellier, Bordeaux, Avignon
- Offres avec technologies : 3 Python, 1 Java, 2 JavaScript, 4 SQL

### Algorithmes clés

#### Extraction des villes

La fonction `extract_city` analyse les chaînes de localisation au format "XX - NOM_VILLE" pour extraire le nom de la ville :

```python
def extract_city(location_str):
    if not location_str or not isinstance(location_str, str):
        return None
    
    # Format typique: "XX - NOM_VILLE"
    if ' - ' in location_str:
        return location_str.split(' - ')[1].strip()
    return location_str.strip()
```

#### Normalisation des types de contrat

La fonction `normalize_contract_type` utilise un dictionnaire de mots-clés pour classifier les types de contrat :

```python
def normalize_contract_type(contract_type):
    if not contract_type:
        return "OTHER"
    
    contract_type = contract_type.lower()
    
    if any(term in contract_type for term in ["cdi", "indéterminé", "permanent"]):
        return "CDI"
    elif any(term in contract_type for term in ["cdd", "déterminé", "temporary"]):
        return "CDD"
    elif any(term in contract_type for term in ["stage", "internship"]):
        return "STAGE"
    elif any(term in contract_type for term in ["alternance", "apprentissage"]):
        return "ALTERNANCE"
    else:
        return "OTHER"
```

#### Détection des technologies

La fonction `detect_technologies` analyse les descriptions d'offres pour détecter les technologies mentionnées :

```python
def detect_technologies(description):
    tech_columns = {}
    tech_keywords = {
        'python': ['python'],
        'java': ['java', 'java ee', 'java se'],
        'javascript': ['javascript', 'js', 'node.js', 'nodejs'],
        # ... autres technologies
    }
    
    for tech, keywords in tech_keywords.items():
        column_name = f'has_{tech}'
        tech_columns[column_name] = False
        
        if description and any(keyword in description.lower() for keyword in keywords):
            tech_columns[column_name] = True
            
    return tech_columns
```

### Utilisation

```python
from src.pipeline.api.transformation import transform_france_travail_data

# Transformer les données
transformed_df = transform_france_travail_data(df)

# Sauvegarder les données transformées
from src.pipeline.api.transformation import save_transformed_data
output_file = save_transformed_data(transformed_df, "france_travail")
```

## Étape de chargement

Le chargement insère les données transformées dans une base de données PostgreSQL hébergée sur AWS RDS (Data Warehouse). Le processus vérifie d'abord si les données existent déjà pour éviter les duplications.

### Fonctionnalités principales

- Connexion à la base de données PostgreSQL avec SQLAlchemy
- Création de la table si elle n'existe pas
- Vérification des données existantes avant chargement
- Utilisation de `sqlalchemy.text()` pour les requêtes brutes
- Optimisation avec le type `Text` pour les colonnes textuelles longues

### Schéma de la table

| Colonne | Type | Description |
|---------|------|-------------|
| id | VARCHAR(255) | Identifiant unique (clé primaire) |
| title | TEXT | Titre de l'offre d'emploi |
| description | TEXT | Description de l'offre |
| company_name | TEXT | Nom de l'entreprise |
| company_description | TEXT | Description de l'entreprise |
| contract_type | TEXT | Type de contrat brut |
| contract_type_std | TEXT | Type de contrat normalisé |
| contract_duration | TEXT | Durée du contrat |
| location | TEXT | Localisation brute |
| city | TEXT | Ville extraite |
| region | TEXT | Région extraite |
| country | TEXT | Pays extrait |
| publication_date | DATE | Date de publication |
| experience_level | TEXT | Niveau d'expérience requis |
| salary | TEXT | Information de salaire |
| has_python | BOOLEAN | Indique si Python est mentionné |
| has_java | BOOLEAN | Indique si Java est mentionné |
| has_javascript | BOOLEAN | Indique si JavaScript est mentionné |
| has_csharp | BOOLEAN | Indique si C# est mentionné |
| has_cpp | BOOLEAN | Indique si C++ est mentionné |
| has_php | BOOLEAN | Indique si PHP est mentionné |
| has_ruby | BOOLEAN | Indique si Ruby est mentionné |
| has_sql | BOOLEAN | Indique si SQL est mentionné |
| has_nosql | BOOLEAN | Indique si NoSQL est mentionné |
| has_aws | BOOLEAN | Indique si AWS est mentionné |
| has_azure | BOOLEAN | Indique si Azure est mentionné |
| has_gcp | BOOLEAN | Indique si GCP est mentionné |

### Statistiques de chargement

- Nombre d'offres chargées : 9430
- Base de données cible : PostgreSQL sur AWS RDS
- Table : `france_travail_jobs`

### Utilisation

```python
from src.pipeline.api.loading import get_db_connection, load_france_travail_data

# Obtenir une connexion à la base de données
engine = get_db_connection()

# Charger les données
success = load_france_travail_data(transformed_df, engine)
```

## Exécution du pipeline complet

Le pipeline complet peut être exécuté via le script `run_etl_france_travail.py`.

### Options de ligne de commande

- `--extract-only` : Exécuter uniquement l'étape d'extraction
- `--transform-only` : Exécuter uniquement l'étape de transformation
- `--load-only` : Exécuter uniquement l'étape de chargement
- `--all` : Exécuter toutes les étapes du pipeline (par défaut)
- `--force` : Forcer l'exécution du pipeline même si les données sont déjà chargées
- `--all-data` : Extraire toutes les données sans filtrage par date

### Exemples d'utilisation

```bash
# Exécuter tout le pipeline
python scripts/pipeline/run_etl_france_travail.py

# Exécuter uniquement l'étape d'extraction
python scripts/pipeline/run_etl_france_travail.py --extract-only

# Exécuter uniquement l'étape de transformation
python scripts/pipeline/run_etl_france_travail.py --transform-only

# Exécuter uniquement l'étape de chargement
python scripts/pipeline/run_etl_france_travail.py --load-only

# Forcer l'exécution même si les données existent déjà
python scripts/pipeline/run_etl_france_travail.py --force

# Extraire toutes les données sans filtrage par date
python scripts/pipeline/run_etl_france_travail.py --all-data
```

## Optimisations implémentées

1. **Vérification des données déjà chargées**
   - Le script vérifie si la table `france_travail_jobs` existe et contient des données
   - Si les données sont déjà présentes, un message est affiché à l'utilisateur
   - L'option `--force` permet de forcer l'exécution du pipeline même si les données sont déjà chargées

2. **Extraction sans filtrage par date**
   - Le script a été modifié pour extraire toutes les données disponibles sans filtrage par date par défaut
   - Cela résout le problème où aucune donnée n'était trouvée lors du filtrage par date

3. **Optimisation du stockage en base de données**
   - Utilisation du type `Text` au lieu de `String(255)` pour les colonnes textuelles qui peuvent contenir des valeurs trop longues
   - Correction de l'exécution SQL avec `sqlalchemy.text()` pour les requêtes brutes

4. **Gestion des valeurs manquantes**
   - Stratégies robustes pour traiter les valeurs manquantes, notamment pour les villes

## Intégration avec le Data Lake et le Data Warehouse

### Data Lake (AWS S3)

Les données brutes et transformées sont stockées dans le bucket S3 `data-lake-brut` avec la structure suivante :

```
data-lake-brut/
└── france_travail/
    ├── raw/
    │   └── [fichiers JSON des offres d'emploi]
    └── processed/
        └── [fichiers CSV transformés]
```

### Data Warehouse (AWS RDS PostgreSQL)

Les données transformées sont chargées dans la table `france_travail_jobs` de la base de données PostgreSQL hébergée sur AWS RDS. Cette table fait partie d'un modèle de données plus large qui inclut également les tables `welcome_jungle_jobs` et `job_skills`.

## Analyse des données

Un script d'analyse et de visualisation des données (`scripts/analysis/analyze_job_data.py`) a été créé pour :

1. **Distribution des types de contrat**
   - Analyse de la colonne `contract_type_std`
   - Visualisation de la répartition des CDI, CDD, stages, etc.

2. **Distribution géographique des offres**
   - Extraction des villes depuis la colonne `city`
   - Cartographie des offres par région

3. **Technologies les plus demandées**
   - Analyse des colonnes `has_python`, `has_java`, etc.
   - Visualisation des technologies les plus recherchées

4. **Durée des contrats CDD**
   - Analyse de la colonne `contract_duration` pour les CDD
   - Visualisation de la distribution des durées de contrat

Les résultats sont présentés dans un tableau de bord HTML interactif.

## Maintenance et résolution des problèmes

### Recréation des tables

En cas de problème avec la structure des tables, le script `recreate_table.py` permet de recréer les tables avec la structure correcte.

```bash
python scripts/utils/recreate_table.py
```

### Problèmes connus

1. **Authentification API France Travail**
   - Des erreurs `invalid_client` peuvent survenir lors de l'authentification à l'API
   - Solution temporaire : utilisation des données déjà stockées dans S3

2. **Connexion RDS**
   - La base de données `nom_base_de_donnees.c32ygg4oyapa.eu-north-1.rds.amazonaws.com` n'est pas accessible publiquement
   - Solution : utilisation de la base de données `nom_base_de_donnees.c32ygg4oyapa.eu-north-1.rds.amazonaws.com`

## Prochaines étapes

1. **Enrichissement des données** : Ajouter des sources de données supplémentaires pour enrichir les offres d'emploi
2. **Amélioration de la détection des technologies** : Étendre la liste des technologies détectées
3. **Analyse prédictive** : Développer des modèles pour prédire les tendances du marché de l'emploi
4. **Automatisation** : Mettre en place une exécution automatique périodique du pipeline
5. **Monitoring** : Ajouter des métriques et des alertes pour surveiller la santé du pipeline

## Conclusion

Le pipeline ETL France Travail est maintenant complet et opérationnel avec les trois étapes fonctionnelles : extraction, transformation et chargement. Les données sont extraites du Data Lake (AWS S3), transformées et chargées de manière fiable dans le Data Warehouse (AWS RDS PostgreSQL).

Les optimisations implémentées permettent d'éviter les rechargements inutiles et de gérer efficacement les grandes quantités de données textuelles. Le pipeline est prêt à être utilisé pour l'analyse des données d'offres d'emploi et à être intégré dans un système plus large d'analyse du marché de l'emploi.
