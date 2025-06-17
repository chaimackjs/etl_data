# Welcome to the Jungle Scraper

Ce scraper permet d'extraire les offres d'emploi depuis Welcome to the Jungle avec des fonctionnalités avancées pour éviter la détection et assurer une extraction fiable des données.

## Structure du projet

```
src/data_collection/scrapers/
├── wttj_scraper.py      # Fonctions de base pour le scraping avec Selenium
├── wttj_job_extraction.py # Fonctions d'extraction des données des offres
├── wttj_storage.py      # Fonctions de sauvegarde des données
├── wttj_main.py         # Script principal
└── Doc_scraping.md           # Documentation
```

## Prérequis

1. Python 3.8 ou supérieur
2. Chrome ou Chromium installé
3. Les packages Python suivants :
```bash
pip install selenium webdriver_manager pandas boto3
```

## Installation



1. Installez les dépendances :
```bash
pip install -r requirements.txt
```

## Utilisation

### Syntaxe de base

### variable d'environnement 

```bash
# pour créer l'environnement virtuel
python -m venv env

# pour activer l'environnement virtuel
env/Scripts/activate

# pour desactiver l'environnement virtuel
env/Scripts/deactivate
```

```bash
 Chemin/vers/le/repertoire/Projet/ python src/data_collection/scrapers/wttj_main.py --query "data engineer" --location "Paris" --max-pages 5
```

### Options disponibles

- `--query` : Terme de recherche (défaut: "data engineer")
- `--location` : Localisation (défaut: "Paris")
- `--max-pages` : Nombre maximum de pages à scraper (défaut: 3)
- `--no-headless` : Désactive le mode headless (permet de voir le navigateur)
- `--no-stealth` : Désactive les techniques anti-détection
- `--no-csv` : Ne pas sauvegarder en CSV (sauvegarde uniquement en JSON)
- `--no-aws` : Ne pas uploader vers AWS S3

### Exemples d'utilisation

1. Recherche simple avec paramètres par défaut :
```bash
python wttj_main.py
```

2. Recherche spécifique avec visualisation du navigateur :
```bash
python wttj_main.py --query "développeur python" --location "Lyon" --no-headless
```

3. Scraping approfondi avec plus de pages :
```bash
python wttj_main.py --query "data scientist" --location "Paris" --max-pages 10
```

4. Sauvegarde locale uniquement (pas d'upload AWS) :
```bash
python wttj_main.py --query "devops" --no-aws
```

## Structure des données

Les données sont sauvegardées dans le dossier `data/raw/welcome_jungle/` sous deux formats :

1. **JSON** : `welcome_jungle_[QUERY]_[TIMESTAMP].json`
2. **CSV** : `welcome_jungle_[QUERY]_[TIMESTAMP].csv`

### Format des données

Chaque offre d'emploi contient les champs suivants :
- `title` : Titre du poste
- `company` : Nom de l'entreprise
- `location` : Localisation
- `contract_type` : Type de contrat
- `description` : Description complète du poste
- `url` : URL de l'offre

## Logs et Monitoring

- Les logs sont sauvegardés dans `welcome_jungle_scraper.log`
- Des statistiques sont générées à la fin de chaque session de scraping
- En cas d'erreur, consultez les logs pour plus de détails

## Dépannage

1. **Le scraper est détecté comme bot** :
   - Utilisez l'option `--no-headless` pour voir ce qui se passe
   - Augmentez les délais entre les requêtes
   - Vérifiez que votre IP n'est pas bloquée

2. **Données manquantes** :
   - Vérifiez les logs pour voir quels sélecteurs ont échoué
   - Les sélecteurs sont dans `wttj_job_extraction.py`
   - Mettez à jour les sélecteurs si le site a changé

3. **Erreurs de connexion** :
   - Vérifiez votre connexion internet
   - Augmentez le nombre de tentatives dans `load_page_with_retry`

## Maintenance

Pour mettre à jour les sélecteurs en cas de changement du site :
1. Ouvrez `wttj_job_extraction.py`
2. Localisez la section des sélecteurs
3. Ajoutez ou modifiez les sélecteurs selon les nouveaux éléments HTML
