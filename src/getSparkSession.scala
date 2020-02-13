import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.serializer.{ KryoRegistrator, KryoSerializer }

object Utils_SC {
  
  def Spark_Context(): SparkSession = {
      val spark = SparkSession.builder().config("spark.driver.cores", "12").config("spark.driver.memory", "32g").config("spark.cores.max", "100").config("spark.driver.maxResultSize", "2g")
          .config("spark.memory.offHeap.enabled", "true").config("spark.memory.offHeap.size", "17g")  
          .config("spark.executor.memory", "21g").config("spark.executor.cores", "10").config("spark.executor.instances", "17") 
          .config("spark.sql.shuffle.partitions", "332").config("spark.default.parallelism", "332").config("spark.memory.fraction", "0.6")
          .config("spark.driver.memoryOverhead","1024").config("spark.executor.memoryOverhead","1024")
          //.config("spark.shuffle.service.enabled", "true").config("spark.dynamicAllocation.enabled", "true")
          //.config("spark.dynamicAllocation.minExecutors", "8").config("spark.dynamicAllocation.initialExecutors", "8")
          //.config("spark.dynamicAllocation.executorIdleTimeout", "360s").config("spark.dynamicAllocation.cachedExecutorIdleTimeout", "300s")
          .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer").config("spark.kryo.registrationRequired", "true").config(KryoClasses)
          .config("spark.kryoserializer.buffer.max", "1600m").config("spark.kryoserializer.buffer", "1024k")
          .appName("Spark APP").enableHiveSupport().getOrCreate()
          return spark
  }
  
  private def KryoClasses = {

    val conf = new SparkConf()
    conf.registerKryoClasses(
      Array(
        // Scala/Java classes
        classOf[scala.Array[scala.collection.Seq[_]]],
        classOf[scala.Array[Int]],
        classOf[scala.Array[Long]],
        classOf[scala.Array[String]],
        classOf[scala.Array[Option[_]]],
        classOf[scala.Array[scala.Array[Byte]]],
        classOf[scala.Array[java.lang.Integer]],
        classOf[scala.Array[java.lang.Long]],
        classOf[scala.Array[java.lang.Object]],
        classOf[scala.collection.mutable.WrappedArray.ofRef[_]],
        classOf[org.apache.spark.util.collection.BitSet],
        Class.forName("org.apache.spark.internal.io.FileCommitProtocol$TaskCommitMessage"),
        Class.forName("scala.collection.immutable.Set$EmptySet$"),
        Class.forName("scala.reflect.ClassTag$$anon$1"),
        Class.forName("java.lang.Class"),
        Class.forName("[[B"),
        // Spark SQL classes
        classOf[org.apache.spark.sql.types.StructType],
        classOf[Array[org.apache.spark.sql.types.StructType]],
        classOf[Array[org.apache.spark.sql.types.StructField]],
        classOf[org.apache.spark.sql.types.StructField],
        classOf[org.apache.spark.sql.types.Metadata],
        classOf[org.apache.spark.sql.types.MapType],
        classOf[org.apache.spark.sql.types.ArrayType],
        classOf[org.apache.spark.sql.catalyst.InternalRow],
        classOf[Array[org.apache.spark.sql.catalyst.InternalRow]],
        classOf[org.apache.spark.sql.types.DataType],
        classOf[Array[org.apache.spark.sql.types.DataType]],
        classOf[org.apache.spark.sql.catalyst.expressions.UnsafeRow],
        Class.forName("org.apache.spark.sql.execution.datasources.FileFormatWriter$WriteTaskResult"),
        Class.forName("org.apache.spark.sql.execution.datasources.BasicWriteTaskStats"),
        Class.forName("org.apache.spark.sql.execution.datasources.ExecutedWriteSummary"),
        Class.forName("org.apache.spark.sql.types.BooleanType$"),
        Class.forName("org.apache.spark.sql.types.DoubleType$"),
        Class.forName("org.apache.spark.sql.types.FloatType$"),
        Class.forName("org.apache.spark.sql.types.IntegerType$"),
        Class.forName("org.apache.spark.sql.types.LongType$"),
        Class.forName("org.apache.spark.sql.types.StringType$"),
        Class.forName("org.apache.spark.sql.types.NullType$"),
        Class.forName("org.apache.spark.sql.types.TimestampType$"),
        Class.forName("org.apache.spark.sql.execution.joins.UnsafeHashedRelation"),
        Class.forName("org.apache.spark.sql.execution.joins.LongHashedRelation"),
        Class.forName("org.apache.spark.sql.execution.joins.LongToUnsafeRowMap"),
        Class.forName("org.apache.spark.sql.execution.columnar.CachedBatch"),
        Class.forName("org.apache.spark.sql.catalyst.expressions.GenericInternalRow"),
        Class.forName("org.apache.spark.unsafe.types.UTF8String")))
  }
