## Spark

|     Feature     |                           Config                            | Write Option |   Default    | Note                                                                         |
|:---------------:|:-----------------------------------------------------------:|:------------:|:------------:|:-----------------------------------------------------------------------------|
|    OPTIMIZE     |        `spark.databricks.delta.optimize.maxFileSize`        |              |    `1gb`     |                                                                              |
| Optimized Write |       `spark.databricks.delta.optimizeWrite.enabled`        |              |    `None`    |                                                                              |
|                 |       `spark.databricks.delta.optimizeWrite.binSize`        |              |   `512MiB`   |                                                                              |
|                 |   `spark.databricks.delta.optimizeWrite.numShuffleBlocks`   |              | `50.000.000` | maximum number of shuffle blocks to target                                   |
|                 | `spark.databricks.delta.optimizeWrite.maxShufflePartitions` |              |   `2.000`    | max number of output buckets (reducers) that can be used by optimized writes |
|      MERGE      |      `spark.databricks.delta.schema.autoMerge.enable`       | `autoMerge`  |              | merge schema                                                                 |
|                 |                                                             |              |              |                                                                              |
|                 |                                                             |              |              |                                                                              |

## Delta

|     Feature     |                 Key                  | Default  | Note |
|:---------------:|:------------------------------------:|:--------:|:-----|
| Optimized Write |  `delta.autoOptimize.optimizeWrite`  |  `None`  |      |
|                 |     `delta.enableChangeDataFeed`     |          |      |
|                 |    `delta.enableDeletionVectors`     |          |      |
|     Vacuum      | `delta.deletedFileRetentionDuration` | `7 days` |      |

## Combine for purposes

|      Feature       |                                          Key                                          |        Value        | Note        |
|:------------------:|:-------------------------------------------------------------------------------------:|:-------------------:|:------------|
| Prevent small file | `delta.autoOptimize.optimizeWrite` <br/> `spark.databricks.delta.autoCompact.enabled` | `true` <br/> `true` |             |
|       Vacuum       |                         `delta.deletedFileRetentionDuration`                          |      `30 days`      | should same |
|                    |                                                                                       |                     |             |

