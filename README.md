[![CircleCI](https://circleci.com/gh/X-DataInitiative/SNIIRAM-flattening.svg?style=shield&circle-token=1c2d54e464ad11d11d5515221b75f644d1c6fb5a)](https://circleci.com/gh/X-DataInitiative/SNIIRAM-flattening)
[![codecov](https://codecov.io/gh/X-DataInitiative/SNIIRAM-flattening/branch/master/graph/badge.svg?token=GWYM6JLi0z)](https://codecov.io/gh/X-DataInitiative/SNIIRAM-flattening)

# Flattening
This is an stage of denormalisation of original data issued from a SQL database. There will be a join among the different tables. For example, DCIR contains several tables like:  ER_PHA_F or ER_PRS_F from distinct CSV files. The flattening will produce a single table from these files and save it in HDFS in the format of Parquet. The advantage of this approach allows us to use the data much faster for analytics later in the following pipeline.     

## Processing and Parameters
2 parts of parameters are required for this stage:
* The schemas of input tables
* The join keys for table join (as well the center table name)

These parameters are necessary since the csv format is not allowed to keep the metadata or information about the types of columns. Hence, we need a configuration file including the types of columns and the format of date. Besides, we need to precise the type of join as it cannot be inferered. So, we need another configuration file for the join keys and the main table.

We proceed in two stages. Firstly, we apply the schéma in the tables (in checking their coherence with the data), then we join different tables.

During the join, for every line of the main table (For example: ER_PRS_F in the DCIR), the additional columns of other tables are spread sur une single line, in which le result is a large plate table. The type de join is "left_outer". The process qui allows to reach the result is a denormalisation. Le diagram below show the processing of denormalisation.

However, This denormalisation may provoke an effect of expansion of data. For example, while flattening DCIR, let's assume one entry in the central table (ER_PRS_F) contains one corresponding in ER_CAM_F, and 3 corresponding entry in the table ER_PHA_F, the resulting flat table will have two extra lines. In fact, the tables can be joint in the relation of OneToMany, therefore, push for line replication to have the completeness of possible combinations. We can therefore end up with a much greater number of line at the end. 

This expansion is not a problem because Spark allows us to perform filtering very efficiently.

## Output Data
Once the transformation is finished, the denormalized tables are saved in HDFS in the format of Parquet. We also save every original table in the format of Parquet in order to search the raw data in the later. The schema of the final data is the same as the original data. It uses the data types provided by configuration files.
The final denormalized tables as follow :

| Transformed Tables  | Tables sources                                    |
|---------------------|---------------------------------------------------|
| DCIR                | ER_PRS_F, ER_PHA_F, ER_CAM_F, ER_ETE_F            |
| PMSI_MCO            | T_MCOXXC, T_MCOXXA, T_MCOXXB, T_MCOXXD, T_MCOXXUM |
| PMSI_MCO_CE         | T_MCOXXCSTC, T_MCOXXFMSTC, T_MCOXXFASTC           |
---

**Configuration**

A configuration file is needed for indicating all the parameters of single and flat tables. The format and default values for each environment can be found in [this directory](https://github.com/X-DataInitiative/SNIIRAM-flattening/tree/master/src/main/resources/flattening/config).

**Usage**

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
  --conf spark.sql.autoBroadcastJoinThreshold=104857600  \
  /path/to/SNIIRAM-flattening/target/scala-2.11/SNIIRAM-flattening-assembly-1.1.jar env=cmap conf=/path/conf
  ```

# Statistics
This repository contains two types of statistics.
* Descriptive - That helps to validate the flattening process.
* Exploratory - That helps to validate the correctness of the data itself by understanding the distribution of data in the flat table per patientId and per month-year.

## Descriptive/Validation Statistics

It compares column values between two tables using custom describe method that incorporates statistics such min, max, count, count_distinct, sum, sum_distinct and avg.
This statistics can be run to compare the results of the newest flattening with the old one, as well as to compare statistics of the result of the flattening with the individual tables.
 
**Configuration**

A configuration file is needed for indicating all the parameters for th. The format and default values for each environment can be found in [this directory](https://github.com/X-DataInitiative/SNIIRAM-flattening/tree/master/src/main/resources/statistics).

**Usage**

The run method in the `*statistics.descriptive.StatisticsMain` class orchestrate the descriptive statistics process, which can be invoked via spark-submit. 
An example of code that can be used is shown below.

```bash
spark-submit \
  --executor-memory 100G \
  --class fr.polytechnique.cmap.cnam.statistics.descriptive.StatisticsMain \
  /path/to/SNIIRAM-flattening-assembly-1.0.jar env=cmap conf=config_file.conf
```

Please note that the items in the file `config_file.conf` will override the default ones for the given environment.

**Results**

For each flat table present in the configuration file, the results will be written as parquet files under the path specified in `output_stat_path`. Three tables will be written for each flat table:

* `${output_stat_path}/flat_table`: Contains the statistics for the flat table
* `${output_stat_path}/single_tables`: Contains the statistics for the columns of all listed single tables
* `${output_stat_path}/diff`: Contains the rows from the flat table statistics which have different values in the single_tables statistics (ideally it has to be empty).

## Exploratory Statistics

It provides some useful numbers on the flattened table such as the number of lines and events (distinct on the event date, patient Id) per patient per month-year. 
It also verifies the consistency of columns values between different tables such as patient Ids, birth and death dates on DCIR, MCO, IR_BEN_R and IR_IMB_R.

**Configuration**

The `*statistics.exploratory.StatisticsMain` class takes values of the input and output path root as command-line arguments and extracts the needed path as follows.


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


**Usage**

The snippet below shows a sample invocation

```bash
spark-submit \
  --executor-memory 100G \
  --class fr.polytechnique.cmap.cnam.statistics.exploratory.StatisticsMain \
  /path/to/SNIIRAM-flattening-assembly-1.0.jar inputPathRoot=/shared/Observapur/staging/Flattening \
  outputPathRoot=/shared/Observapur/staging/Flattening/statistics/exploratory
```

Note that it is not useful to pass the variables `env` and `conf`.

**Results**

The results of the exploratory statistics can be visualized using the python notebook under `scripts/descriptiveStatistics.ipynb`
