[![CircleCI](https://circleci.com/gh/X-DataInitiative/SCALPEL-Flattening.svg?style=shield&circle-token=1c2d54e464ad11d11d5515221b75f644d1c6fb5a)](https://circleci.com/gh/X-DataInitiative/SCALPEL-Flattening)
[![codecov](https://codecov.io/gh/X-DataInitiative/SCALPEL-Flattening/branch/master/graph/badge.svg?token=GWYM6JLi0z)](https://codecov.io/gh/X-DataInitiative/SCALPEL-Flattening)
[![License](https://img.shields.io/badge/License-BSD%203--Clause-blue.svg)](https://opensource.org/licenses/BSD-3-Clause)
![Version](https://img.shields.io/github/v/release/X-DataInitiative/SCALPEL-Flattening?include_prereleases)

# SCALPEL-Flattening

SCALPEL-Flattening is a library part of the SCALPEL3 framework,
 resulting from a research Partnership between [École Polytechnique](https://www.polytechnique.edu/en) & 
 [Caisse Nationale d'Assurance Maladie](https://assurance-maladie.ameli.fr/qui-sommes-nous/fonctionnement/organisation/cnam-tete-reseau)
 started in 2015 by [Emmanuel Bacry](http://www.cmap.polytechnique.fr/~bacry/) and [Stéphane Gaïffas](https://stephanegaiffas.github.io/).
 Since then, many research engineers and PhD students developed and used this framework to do
 research on SNDS data, the full list of contributors is available in [CONTRIBUTORS.md](CONTRIBUTORS.md).
 This library, based on [Apache Spark](https://spark.apache.org/), denormalizes [Système National des Données de Santé (SNDS)](https://www.snds.gouv.fr/SNDS/Accueil)
 data to accelerate concept extraction when using
 [SCALPEL-Extraction](https://github.com/X-DataInitiative/SCALPEL-Extraction).
 Denormalization consists of several join operations and data compression,
 resulting in a big table representing SNDS databases, such as DCIR or PMSI.

Raw data issued by [Caisse Nationale d'Assurance Maladie (CNAM)](https://www.ameli.fr/)
 (the data producer and maintainer), from their SQL databases, should be in CSV
 format, each CSV file representing a table. This library converts such tables
 to [Apache Parquet](https://parquet.apache.org/) files, compressing the data
 and adding schema information, and then joins them to product big compressed
 tables representing SNDS databases, such as DCIR or PMSI.

For example, DCIR contains several tables such as ER_PHA_F (drug deliverances)
 or ER_PRS_F (claims) stored as distinct CSV files. The flattening parses these
 files to produce a table for each of them, and then save them as Parquet files,
 to finally join them into a flat table, also saved as a Parquet file.

It is meant to be used on a cluster running [Apache Spark](https://spark.apache.org/)
 and [Apache Hadoop](https://hadoop.apache.org/) when working on large SNDS
 datasets. When working on smaller datasets, it can also be used in standalone
 mode, on a single server running [Apache Spark](https://spark.apache.org/),
 in which case it will use the local file system.

> **Important remark** : This software is currently in alpha stage. It should be fairly stable,
> but the API might still change and the documentation is partial. We are currently doing our best
> to improve documentation coverage as quickly as possible. 

## Overview

### Configuration

Since the CSV format used for data delivery do not track any metadata or
 information about the types of the columns, two sets of parameters are required
 to run the Flattening:

* Input tables schemas, to know beforehand which type of information is
 contained in their columns, and the date format, to parse it correctly. Schemas 
 for DCIR and PMSI MCO databases are already available in this [folder](src/main/resources/schema)
* The join keys for table join, as well as the names of the tables to be joined,
 the order in which they should be joined. As the joins are always left outer
 joins, it is recommended to put the largest table first in the list of tables
 to be joined.

### Processing

The flattening performs two kinds of jobs:

* Convert: applies the schema to the input CSV files, checking their consistency
 with the data, performing eventual type conversions and finally processing
 columns formats if necessary(for example: addMoleculeCombinationColumn in
 IR_PHA_R).
* Join: performs the joins the many input tables to produce a single big table
 per database. The joins are performed over by time slices to ensure the
 scalability of this process. When all the slices are joined, they are
 concatenated to result in the desired flat table. The size of the time slice
 can be configured.

Due to the many left outer joins, denormalization is very likely to expand the
 data, resulting in tables with many lines. This is not an issue,
 as [Apache Parquet](https://parquet.apache.org/) data model allows us to store
 this kind of data efficiently, and Spark allows us to perform filtering very
 efficiently to fetch relevant information.

The Flattening process can be launched as separate Spark jobs, meaning that the
 user can launch Convert or Join individually, or Convert and Join together.
 This can be useful to avoid repeating jobs.

### Output

Once a job is finished, the converted tables or the denormalized tables are
 saved in the file system (local file system or HDFS) in Parquet format or ORC format.
 
Using Apache ORC instead of Parquet could also lead to performance improvements. Parquet was initially chosen
 over ORC because of better integration with Spark. ORC is now well-integrated in it since spark 2.3 and has been
 reported to have better performances and a higher compression factor on non-nested data.
 
`file_format` is used to select a data format between Parquet or ORC in the configuration files([examples](src/main/resources/flattening/config/template/template-env.conf))
 and add a special configuration in the spark-submit command `spark.sql.orc.impl=native`
  
 
The schema of the flat tables corresponds to the data types provided in the
 schema configuration files.

**Note:** if you are not sure, DO NOT change the table schema

## Usage

### Build

To build a JAR from this repo, you will need SBT v. 0.13.15 (Scala Build Tool)
 & the assembly plugin for SBT. To build a JAR, just run the following commands:

```bash
git clone git@github.com:X-DataInitiative/SCALPEL-Flattening.git
cd SCALPEL-Flattening
sbt assembly
```

### Configuration and environments

Configuration files are used to declare table schemas and to set up the joins
 structure. We provide a set of several default environments in the flattening
 (fall, cmap, test, etc) which can be used as starting points or examples.
 When running the flattening on your dataset, you can create a new environment
 containing your configuration files and override default settings bypassing the
 `env=custom_env` when submitting the Spark job (see [example](#Running)).  

The default environments and their configuration files
 [here](src/main/resources/flattening/config),
 as well as configuration files [templates](src/main/resources/flattening/config/template).

You can add your own set of settings in the default
 [configuration](src/main/resources/flattening/config)
 older and then register it in the
 [main.conf](src/main/resources/flattening/config/main.conf)
 configuration file.

A good [example](src/main/resources/flattening/config/test)
 can be found in the tests, illustrating how DCIR flattening can be customized.

Using the default configurations of fall, which is the environment that we
 recommend to use in production, the final denormalized tables are built as
 follows :

| Flat Table          | Single Table joined together                                                    |
|---------------------|---------------------------------------------------------------------------------|
| DCIR                | ER_PRS_F, ER_PHA_F, ER_CAM_F, ER_ETE_F, ER_UCD_F, (IR_PHA_R, IR_BEN_R, IR_IMB_R)|
| PMSI_MCO            | MCO_C, MCO_A, MCO_B, MCO_D, MCO_UM                                              |
| PMSI_MCO_CE         | MCO_CSTC, MCO_FMSTC, MCO_FASTC                                                  |

**Note:** Reference tables, such as IR_PHA_R, IR_BEN_R, IR_IMB_R, containing
 code mappings (for drugs, acts, diagnoses, etc) are not like other single
 tables accumulated year by year and are not forced to be included in the
 flattening for now. For example: IR_PHA_R can optionally be added to
 flattening. You can find an example configuration adding this table to the
 flattening [Convert](src/main/resources/flattening/config/test/single_tables/IR_PHA_R.conf)
 and [Join](src/main/resources/flattening/config/test/test-ref-join.conf)

### Running

```bash
spark-submit \
  --driver-memory 40G \
  --executor-memory 110G \
  --class fr.polytechnique.cmap.cnam.flattening.FlatteningMain \
  --conf spark.sql.shuffle.partitions=720 \
  --conf spark.task.maxFailures=20 \
  --conf spark.driver.maxResultSize=20G \
  --conf spark.sql.broadcastTimeout=3600 \
  --conf spark.locality.wait=30s \
  --conf spark.sql.orc.impl=native \
  /path/to/SCALPEL-Flattening/target/scala-2.11/SCALPEL-Flattening-assembly-1.1.jar env=fall conf=/path/config_file.conf meta_bin=/path/meta.bin
  ```

`env` is used to select an environments (fall, cmap, test) containing default
configuration parameters. This parameter is required.

`conf` can be used to override some settings from the environment with your own
 values. This parameter is optional. If no configuration file is used, the
 Flattening will take default configurations set in env.

`meta_bin` should indicate a path used to store a binary log file containing
information produced by the `Convert` job, which is then used by the `Join` job.
It is optional when running `FlatteningMain`, but it is required when launching
the two jobs separatly (see note below).

**Note:** To run only the CSV to Parquet conversion, just replace
 `FlatteningMain` by `FlatteningMainConvert` in this command. Same with
 `FlatteningMainJoin` if you want to run only the joins. In these 2 cases,
 using `meta_bin` is necessary, as it stores the intermediary information
 resulting from `Convert`, and `Join` MUST have this information to run.
 To run `FlatteningMain`, you don't need a `meta_bin`.

We recommend the spark parameters as below in order to improve the performance
 of Flattening, especially when flattening DCIR.

spark.driver.memory(--driver-memory) >= 40G

spark.executor.memory(--executor-memory) >= 110G

spark.driver.maxResultSize >= 20G

spark.sql.shuffle.partitions = (3-4) times of your cores

In the case of incremental flattening, before running CSV to Parquet conversion,
 you could just created a single table config including only one year and then
 only put it to your `singletablerawset` config; before running joins, you could
 set only new year to `only_output` in your flat table config. In this way,
 adding a new year data does not touch any other year data.  

## Citation

If you use a library part of _SCALPEL3_ in a scientific publication, we would appreciate citations. You can use the following bibtex entry:

    @article{bacry2020scalpel3,
      title={SCALPEL3: a scalable open-source library for healthcare claims databases},
      author={Bacry, Emmanuel and Gaiffas, St{\'e}phane and Leroy, Fanny and Morel, Maryan and Nguyen, Dinh-Phong and Sebiat, Youcef and Sun, Dian},
      journal={International Journal of Medical Informatics},
      pages={104203},
      year={2020},
      publisher={Elsevier}
    }

## Statistics

> **Deprecated:** this functionality is currently being moved to
[SCALPEL-Analysis](https://github.com/X-DataInitiative/SCALPEL-Analysis)

SCALPEL-Flattening provides tools to compute a set of statistics used to control
 the quality of the flattening result. It produces two types of statistics.

* Descriptive, which helps to validate the flattening process.
* Exploratory, which helps to validate the correctness of the data itself by
 looking at the distribution of data in the flat table per patient ID and per
 month-year.

### Descriptive/Validation Statistics

It compares column values between two tables using a custom `describe` method
 producing statistics such as min, max, count, count_distinct, sum,
 sum_distinct, and avg. These statistics can be used to compare the results of
 two flattening jobs, as well as to compare statistics of the result of the
 flattening with the individual tables.

#### Descriptive statistics configuration

The format and default values of the configuration files for each environment
 can be found in [this directory](src/main/resources/statistics).

#### Descriptive statistics ssage

The run method in the `*statistics.descriptive.StatisticsMain` class
 orchestrates the descriptive statistics process. It can be invoked via
 spark-submit:

```bash
spark-submit \
  --executor-memory 100G \
  --class fr.polytechnique.cmap.cnam.statistics.descriptive.StatisticsMain \
  /path/to/SNIIRAM-flattening-assembly-1.0.jar env=fall conf=config_file.conf
```

Please note that the items in the file `config_file.conf` will override the
 default ones for a given environment.

#### Results

For each flat table present in the configuration file, the results will be
 written as parquet files under the path specified in `output_stat_path` in the
 configuration file. Three tables will be written for each flat table:

* `${output_stat_path}/flat_table`: Contains the statistics for the flat table
* `${output_stat_path}/single_tables`: Contains the statistics for the columns
 of all listed single tables
* `${output_stat_path}/diff`: Contains the rows from the flat table statistics
 which have different values in the single_tables statistics
 (ideally it has to be empty).

### Exploratory Statistics

It provides some useful numbers on the flattened table such as the number of
 lines and events (distinct on the event date, patient Id) per patient per
 month-year. It also checks the consistency of columns values between different
 tables such as patient Ids, birth, and death dates on DCIR, MCO, IR_BEN_R, and
 IR_IMB_R.

#### Exploratory Statistics Configuration

The `*statistics.exploratory.StatisticsMain` class takes values of the input and
 output path root as command-line arguments and extracts the needed path as
 follows.

| Input Table         | Derived path                                      |
|---------------------|---------------------------------------------------|
| DCIR                | inputPathRoot + "/flat_table/DCIR"                |
| PMSI_MCO            | inputPathRoot + "/flat_table/MCO"                 |
| IR_BEN_R            | inputPathRoot + "/single_table/IR_BEN_R"          |
| IR_IMB_R            | inputPathRoot + "/single_table/IR_IMB_R"          |

| Output Table                            | Derived path                                          |
|-----------------------------------------|-------------------------------------------------------|
| code consistency                        | outputPathRoot + "/codeConsistency"                   |
| dcir count by patient                   | outputPathRoot + "/dcirCountByPatient"                |
| dcir count by patient and month         | outputPathRoot + "/dcirCountByPatientAndMonth"        |
| dcir purchase count by month            | outputPathRoot + "/dcirPurchaseCountByMonth"          |
| dcir purchase count by patient and month| outputPathRoot + "/dcirPurchaseCountByPatientAndMonth"|
| mco count by patient                    | outputPathRoot + "/mcoCountByPatient"                 |
| mco count by patient and month          | outputPathRoot + "/mcoCountByPatientAndMonth"         |
| mco diag count by month                 | outputPathRoot + "/mcoDiagCountByMonth"               |
| mco diag count by patient and month     | outputPathRoot + "/mcoDiagCountByPatientAndMonth"     |

#### Exploratory Statistics Usage

The snippet below shows an invocation example

```bash
spark-submit \
  --executor-memory 100G \
  --class fr.polytechnique.cmap.cnam.statistics.exploratory.StatisticsMain \
  /path/to/SNIIRAM-flattening-assembly-1.0.jar inputPathRoot=/shared/Observapur/staging/Flattening \
  outputPathRoot=/shared/Observapur/staging/Flattening/statistics/exploratory
```

Note that the variables `env` and `conf` are not used by this main class.

#### Exploratory Statistics Results

The results of the exploratory statistics can be visualized using this
 [python notebook](https://github.com/X-DataInitiative/SCALPEL-Flattening/blob/master/scripts/descriptiveStatistics.ipynb) under `scripts/descriptiveStatistics.ipynb`
