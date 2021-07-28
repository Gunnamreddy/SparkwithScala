def mergeFiles(spark: SparkSession, grouped: ListBuffer[ListBuffer[String]], targetDirectory: String): Unit = {
    val startedAt = System.currentTimeMillis()
    val forkJoinPool = new ForkJoinPool(grouped.size)
    val parllelBatches = grouped.par
    parllelBatches.tasksupport = new ForkJoinTaskSupport(forkJoinPool)
    parllelBatches foreach (batch => {
      logger.debug(s"Merging ${batch.size} files into one")
      try {
        spark.read.parquet(batch.toList: _*).coalesce(1).write.mode("append").parquet(targetDirectory.stripSuffix("/") + "/")
      } catch {
        case e: Exception => logger.error(s"Error while processing batch $batch : ${e.getMessage}")
      }
    })
    logger.debug(s"Total Time taken to merge this directory: ${(System.currentTimeMillis() - startedAt) / (1000 * 60)} mins")
  }
