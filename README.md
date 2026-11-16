# Binance Futures Scanner

Scanner local de contrats Binance USDⓈ-M Futures. Il utilise uniquement les données publiques et ne passe aucun ordre.

## Prérequis

- Windows 10/11
- Python 3.11 ou plus récent
- Node.js 20 ou plus récent avec npm
- PostgreSQL 16 ou plus récent, installé comme service Windows, cluster local, ou Docker Desktop avec Compose

## Installation

Depuis la racine du projet, préparez l'environnement Python et ses dépendances :

```powershell
py -3 -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r backend\requirements.txt
```

`START.cmd` installe les dépendances frontend déclarées dans `frontend/package.json` lors du premier lancement si elles ne sont pas déjà présentes. Pour les installer séparément :

```powershell
cd frontend
npm.cmd install
cd ..
```

## Configuration

Le backend lit `backend/.env`. Si ce fichier est absent, `START.cmd` le crée depuis `backend/.env.example`. Vérifiez `DATABASE_URL` et adaptez l'utilisateur, le mot de passe, l'hôte et le nom de base à votre PostgreSQL. La base doit déjà exister ; au premier démarrage, le backend crée la table `signals` si l'utilisateur a les droits nécessaires.

Avec le service Docker Compose fourni, la base `scanner` et l'utilisateur `scanner` sont initialisés automatiquement avec les paramètres de développement présents dans `docker-compose.yml`. Ces paramètres sont réservés à un usage local ; remplacez-les pour toute instance exposée sur un réseau. Gardez `backend/.env` privé et ne le partagez pas. `REST_OPERATOR_TOKEN` est facultatif et active les routes locales de reprise HTTP 418 lorsqu'il est défini.

## Démarrage et arrêt

Lancez `START.cmd` depuis l'Explorateur ou un terminal. Le script vérifie Python et Node.js, démarre PostgreSQL si nécessaire, puis lance le backend et le frontend. Il réutilise les services déjà disponibles et évite de lancer un second exemplaire sur les mêmes ports.

Si PostgreSQL est un service Windows arrêté, son démarrage peut nécessiter des droits administrateur. Le backend et le frontend ne requièrent pas de droits administrateur. Le script affiche une erreur si PostgreSQL ne peut pas démarrer ou si la base est indisponible.

Ouvrez ensuite <http://127.0.0.1:3000>. L'API est disponible sur <http://127.0.0.1:8000>, son état sur `/api/health` et sa documentation sur `/docs`. Le chargement initial récupère l'historique public des contrats admissibles sur quatre timeframes et peut prendre quelques minutes selon Binance et le réseau.

Pour arrêter, lancez `STOP.cmd`. Il arrête proprement le backend et le frontend lancés par `START.cmd`, puis arrête le cluster local ou le service PostgreSQL uniquement si ce lancement l'avait démarré. Si PostgreSQL a été démarré par Docker Compose, le conteneur reste actif ; ses données sont conservées dans le volume Compose. L'arrêt de PostgreSQL ne supprime ni la base ni ses fichiers de données. Les services déjà actifs avant le lancement restent actifs. Journaux et marqueurs de processus se trouvent dans `.runtime/`.

## Interface

Les snapshots arrivent par WebSocket. L'interface propose des filtres par timeframe, symbole, statut confirmé/pré-signal et direction. La sélection d'une ligne ouvre son graphique ; les bougies et volumes sont actualisés périodiquement depuis l'API locale. Les signaux confirmés utilisent des bougies clôturées et les pré-signaux la bougie en formation. Ces indicateurs ne constituent pas un conseil financier.

## Reprise opérateur après HTTP 418

Pour activer les routes de reprise manuelle, définissez un secret privé dans `backend/.env` sous `REST_OPERATOR_TOKEN`, puis redémarrez le backend. Les routes n'acceptent que les connexions loopback. Envoyez le jeton dans l'en-tête `X-REST-Operator-Token`, jamais dans l'URL.
