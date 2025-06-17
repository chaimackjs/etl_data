# Welcome to the Jungle Scraper

Ce module permet de collecter des offres d'emploi depuis le site Welcome to the Jungle en utilisant une architecture modulaire.

## Architecture

Le scraper est divisé en plusieurs modules Python pour améliorer la maintenabilité et la réutilisabilité du code :

1. **wttj_job_extraction.py** : Extraction des données des offres d'emploi
2. **wttj_storage.py** : Gestion du stockage (JSON, CSV) et upload vers AWS S3
3. **wttj_scraper.py** : Configuration du driver Selenium et navigation sur le site
4. **multi_search.py** : Exécution de recherches multiples avec différentes combinaisons de mots-clés et lieux
5. **wttj_main.py** : Script principal avec interface en ligne de commande

## Prérequis

- Python 3.6+
- Selenium
- Chrome WebDriver
- Accès AWS (pour l'upload S3)

## Variables d'environnement

Les variables d'environnement suivantes doivent être configurées pour l'upload vers AWS S3 :

```
KEY_ACCESS=AKIAS2VS4EK2UIF56F5O
KEY_SECRET=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
S3_BUCKET=data-lake-brut
```

## Utilisation

### Mode simple (une seule recherche)

```bash
python wttj_main.py simple --keyword "data engineer" --location "Paris" --max-pages 3
```

### Mode multi-recherche

```bash
python wttj_main.py multi -k "data engineer" "data scientist" -l "Paris" "Lyon" -p 2
```

## Fonctionnalités

- Extraction des offres d'emploi avec détails complets (titre, entreprise, lieu, contrat, description, etc.)
- Sauvegarde au format JSON et CSV
- Upload automatique vers AWS S3
- Gestion de la pagination
- Mode headless pour exécution sans interface graphique
- Fusion des résultats de plusieurs recherches
- Logs détaillés pour le suivi et le débogage

## Structure des données

Chaque offre d'emploi est extraite sous forme d'un dictionnaire contenant les champs suivants :

- `url` : URL de l'offre
- `title` : Titre de l'offre
- `company` : Nom de l'entreprise
- `location` : Lieu de travail
- `contract_type` : Type de contrat
- `description` : Description complète de l'offre
- `salary` : Salaire (si disponible)
- `experience` : Niveau d'expérience requis
- `category` : Catégorie de l'offre
- `publication_date` : Date de publication (format ISO)
- `date_extraction` : Date d'extraction des données

## Notes

- Le scraper utilise des techniques pour éviter la détection (user-agent aléatoire, délais variables)
- Les fichiers sont sauvegardés dans un dossier `data` créé automatiquement
- Les logs sont enregistrés dans le fichier `welcome_jungle_scraper.log`
