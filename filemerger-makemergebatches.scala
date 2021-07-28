def makeMergeBatches(fileSizesMap: scala.collection.immutable.Map[String, Long], maxTargetFileSize: Long): ListBuffer[ListBuffer[String]] = {
    val sortedFileSizes = fileSizesMap.toSeq.sortBy(_._2)
    val groupedFiles = ListBuffer[ListBuffer[String]]()    
    groupedFiles += ListBuffer[String]()
    for (aFile <- smallerFiles) {
      val lastBatch = groupedFiles.last
      if ((sizeOfThisBatch(lastBatch) + aFile._2) < maxTargetFileSize) {
        lastBatch += aFile._1 + "|" + aFile._2.toString
      } else {
        val newBatch = ListBuffer[String]()
        newBatch += aFile._1 + "|" + aFile._2.toString
        groupedFiles += newBatch
      }
    }
    groupedFiles
  }
