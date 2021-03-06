def getFileSizes(bucketName: String, prefix: String): scala.collection.immutable.Map[String, Long] = {
    val s3 = AmazonS3ClientBuilder.standard().withRegion(Regions.US_EAST_1).build()
    val listing = s3.listObjectsV2(bucketName, prefix)
    val files = listing.getObjectSummaries.asScala.map(_.getKey).filter(!_.split("/").last.startsWith("_"))
    val filesSizeMap = collection.mutable.Map[String, Long]()
    files.foreach(obj => {
      val meta = s3.getObjectMetadata(new GetObjectMetadataRequest(bucketName, obj))
      filesSizeMap += (obj -> meta.getContentLength)
    })
    filesSizeMap.toMap
  }
