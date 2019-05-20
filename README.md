Ce projet vise à gérer le rafraichissement automatique de vues matérialisées PostgreSQL via des scripts Python et une table de gestion sur un serveur Debian.

# Installation

Tout d'abord, vous devez récupérer l'archive sur votre machine :
```bash
wget https://github.com/CyprienAn/pgMV_refresh/archive/master.zip
unzip pgmv_refresh-master.zip
mv pgmv_refresh-master/ pgmv_refresh/
rm pgmv_refresh-master.zip
```

Les scripts Python nécessite l'utilisation de `psycopg2` pour pouvoir se connecter à votre base de données. Si ce paquet n'est pas présent sur votre serveur, vous devez l'ajouter :
```bash
sudo apt-get install python3-psycopg2
```

Il faut ensuite adapter le fichier `database.ini` à votre contexte :
```bash
cd pgmv_refresh/
cp database.ini.sample database.ini
nano database.ini
```

Afin de créer la table `public.list_materialized_view` dans PostgreSQL qui permettra la gestion du rafraichissement, il faut lancer le script `createMVList.py` :
```bash
python3 createMVList.py
```

Une fois la table créée, vous devez aller modifier le contenu de celle-ci afin de définir pour chaque vue matérialisée :
- si elle doit-être rafraichit en remplaçant `FALSE` par `TRUE` dans le champs `auto_refresh`,
- la périodicité du rafraichissement en indiquant `Daily`, `Weekly`, `Monthly` dans la champs `periodicity`.

Dernière étape, il faut créer une nouvelle tâche `Cron` qui exécutera le script `refreshMV.py`:
```bash
crontab -e
```
Et ajouter en adaptant : `00 22 * * * cd /monChemin/versLeDossier/pgmv_refresh &&  python3 refreshMV.py >>refreshMV.log`.

Le script `refreshMV.py`, vérifie que la table `public.list_materialized_view` contient bien toutes les vues matérialisées de la base de données et ajoute les manquantes. Attention : celles-ci ne seront pas rafraichit tant que vous ne l'aurez pas indiqué manuellement dans la table. Il va ensuite rafraichir les vues matérialisées en fonction des paramètres `auto_refresh` et `periodicity` de chacune.
