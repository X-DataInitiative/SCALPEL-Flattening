# Flattening

C'est une étape de dénormalisation de la donnée initiale qui provient d'une base de donnée SQL. C'est une jointure entre les différentes table. Par exemple, DCIR contient plusieurs tables comme ER_PHA_F ou ER_PRS_F que nous recevons dans des fichiers CSV distincts. L'aplatissement va produire une seule table à partir de ces fichiers. le résultat est sauvegardé sur HDFS au format parquet. L'avantage d'une telle approche est de nous permettre d'utiliser la donnée beaucoup plus rapidement par la suite. En effet, faire des filtres avec Spark est beaucoup plus rapide que de faire des jointures.

## Input Data
C'est une étape de dénormalisation de la donnée initiale qui provient d'une base de donnée SQL. C'est une jointure entre les différentes table. Par exemple, DCIR contient plusieurs tables comme ER_PHA_F ou ER_PRS_F que nous recevons dans des fichiers CSV distincts. L'aplatissement va produire une seule table à partir de ces fichiers. le résultat est sauvegardé sur HDFS au format parquet. L'avantage d'une telle approche est de nous permettre d'utiliser la donnée beaucoup plus rapidement par la suite. En effet, faire des filtres avec Spark est beaucoup plus rapide que de faire des jointures.

## Processing and Parameters
Deux ensembles de paramètres sont requis pour cette étape:
* Les schémas des tables d'entrées;
* Les clés nécessaires pour faire les jointures (ainsi que le nom de la table centrale).

Ces paramètres sont nécessaires car le format CSV ne permet pas de conserver de métadonnée ou d’information sur le type des colonnes. Nous avons donc besoin d’un fichier de configuration qui va contenir les types de colonnes et le format de dates. Par ailleurs nous avons besoin de préciser le format des jointures car il ne peut être inféré. Nous avons donc besoin d’un autre fichier de configuration pour les clés de jointures et la table principale.

On procède donc en deux temps. Tout d’abord on applique le schéma aux tables (en vérifiant sa cohérence avec la donnée), puis on calcule la jointure des différentes tables.

Pendant la jointure, pour chaque ligne de la table centrale (ER_PRS_F dans le cas de DCIR), les colonnes additionnelles des autres tables sont étalées sur une seule ligne dont le résultat est une grande table plate. La dénomination technique de ce type de jointure est "left_outer". Le processus qui permet d'arriver à ce résultat est une dénormalisation. Le diagramme ci-dessus représente la dénormalisation de DCIR.

Cependant, la dénormalisation peut provoquer un effet d'expansion de la donnée. For example, while flattening DCIR, let's assume one entry in the central table (ER_PRS_F) contains one corresponding in ER_CAM_F, and 3 corresponding entry in the table ER_PHA_F, the resulting flat table will have two extra lines.  En effet les tables peuvent être liées entre elle par des relation OneToMany et, de ce fait, pousser à la réplication des lignes afin de posséder l'exhaustivité des combinaisons possibles. On peut donc se retrouver avec un nombre de ligne largement plus grand à la sortie de cette étape.

Cette expansion n'est pas un soucis car Spark nous permet d'effectuer des filtrages de façon très efficace. Néanmoins nous pourrons éventuellement faire évoluer cette stratégie ultérieurement et utiliser des colonnes d'ensembles imbriquées (on pourrait alors stocker une liste de valeur dans une seule colonne).

## Output Data
Une fois que la transformation est finie, les tables dénormalisées sont sauvegardées sur HDFS au format parquet. On sauvegarde aussi chaque table source au format parquet pour les recherches ultérieures sur la donnée brute. Le schéma de la donnée finale est le même que celui de la donnée initiale. Il utilise les types fournis dans le fichier de configuration.

Les tables actuellement dénormalisées sont les suivantes :

| Tables transformées | Tables sources                                    |
|---------------------|---------------------------------------------------|
| DCIR                | ER_PRS_F, ER_PHA_F, ER_CAM_F                      |
| PMSI_MCO            | T_MCOXXC, T_MCOXXA, T_MCOXXB, T_MCOXXD, T_MCOXXUM |

---

# Validation Statistics

The repository also contains a module for computing statistics on the flat data in order to validate its results. This module can be run to compare the result of the newest flattening with the old one, as well as to compare statistics of the result of the flattening with the individual tables.
 
**Configuration**

A configuration file is needed for indicating the paths for the input and the output. The format and default values for each environment can be found in [this directory](https://github.com/X-DataInitiative/SNIIRAM-flattening-2.0/tree/master/src/main/resources/statistics).

**Usage**

The statistics main contains a main class called `StatisticsMain` which can be invoked via spark-submit. An example of code that can be used is shown below.

```bash
spark-submit \
  --executor-memory 100G \
  --class fr.polytechnique.cmap.cnam.statistics.StatisticsMain \
  /path/to/SNIIRAM-flattening-2.0-assembly-1.0.jar env=cmap conf=config_file.conf
```

Please note that the items in the file `config_file.conf` will override the default ones for the given environment.

**Results**

For each flat table present in the configuration file, the results will be written as parquet files under the path specified in `output_stat_path`. Three tables will be written for each flat table:

* `${output_stat_path}/flat_table`: Contains the statistics for the flat table
* `${output_stat_path}/single_tables`: Contains the statistics for the columns of all listed single tables
* `${output_stat_path}/diff`: Contains the rows from the flat table statistics which have different values in the single_tables statistics (ideally it has to be empty).