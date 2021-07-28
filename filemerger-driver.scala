val (inputBucket, prefix) = getBucketNameAndPrefix(args(1))
val targetDirectory = args(2)
val maxIndividualMergedFileSize = args(3).toLong

val inputDirs = listDirectoriesInS3(inputBucket, prefix).map(prefix => "s3://" + inputBucket + "/" + prefix)
logger.info(s"Total directories found : ${inputDirs.size}")
val startedAt = System.currentTimeMillis()
//You may want to tweak the following to set how many input directories to process in parallel
val forkJoinPool = new ForkJoinPool(inputDirs.size)
val parallelBatches = inputDirs.par
parallelBatches.tasksupport = new ForkJoinTaskSupport(forkJoinPool)
parallelBatches.foreach(dir => {
  val (srcBkt, prefix) = getBucketNameAndPrefix(dir)
  logger.info(s"Working on ::=> SourceBkt : $srcBkt,Prefix:$prefix")
  //Step 1 : Get file sizes
  val fileSizesMap = getFileSizes(inputBucket, prefix)
  
  //Step 2 : Group them based on target file size
  val grouped = makeMergeBatches(fileSizesMap, maxIndividualMergedFileSize)
  val sizedStripped = stripSizesFromFileNames(grouped)
  val finalSourceFileNames = addS3BucketNameToFileNames(sizedStripped, bucketName = inputBucket)

  //Step 3 : Merge files and write them out
  mergeFiles(spark, finalSourceFileNames, targetDirectory)
})