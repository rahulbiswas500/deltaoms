package odpmetrics.unified

/*
models
 */

import java.time.Instant
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.delta.DeltaLog
import org.apache.spark.sql.types.StructType

case class DeltaLocationDetails(deltaLocation: String,
                                deltaLocationIdentifier: String,
                                wcPath: String,
                                wcPathIdentifier: String,
                                extraConfig: Map[String, String] = Map.empty[String, String],
                                //                      automated: Boolean = true,
                                fullTableName: Option[String] = None,
                                commitVersion: Long,
                                ignore: Boolean = false,
                                updatedOn: Instant = Instant.now()) {
  def getDeltaLog(spark: SparkSession): DeltaLog = {
    DeltaLog.forTable(spark, deltaLocation)
  }
}

case class MasterConfig(deltaLocation: String, ignore: Boolean = false,
                        extraConfig: Map[String, String] = Map.empty[String, String])

case class ProcessedHistory(tableName: String, latestVersion: Long,
                            updatedOn: Instant = Instant.now())

case class TableDefinition(
                            tableName: String,
                            databaseName: String = "default",
                            schema: StructType,
                            path: String,
                            comment: Option[String] = None,
                            properties: Map[String, String] = Map.empty[String, String],
                            partitionColumnNames: Seq[String] = Seq.empty[String],
                            version: Long = 0) {
  assert(path.nonEmpty & tableName.nonEmpty, "Table Name and Path is required")
}

case class DatabaseDefinition(databaseName: String,
                              location: Option[String],
                              comment: Option[String] = None,
                              properties: Map[String, String] = Map.empty) {
  assert(databaseName.nonEmpty, "Database Name is required")
}

case class StreamTargetInfo(path: String, checkpointPath: String,
                            wcPathIdentifier: Option[String] = None, pathIdentifier: Option[String] = None)

/***********************************************************************************************************************/

/*
ODPMetricsConfig
 */

case class ODPMetricsConfig(baseLocation: Option[String] = None,
                            dbName: Option[String] = None,
                            checkpointBase: Option[String] = None,
                            checkpointSuffix: Option[String] = None,
                            unrefinedTable: String = "unrefined_metrics",
                            masterTable: String = "master",
                            deltaLocationDetailsTable: String = "delta_location_details",
                            processedLogTable: String = "processed_log",
                            refinedCommitMetricsTable: String = "refined_commit_metrics",
                            //                            refinedFileLevelInfoTable: String = "refined_file_level_info",
                            mergeWildcardPaths: Boolean = true,
                            resetDeltaLocationDetails: Boolean = false,
                            doNotUpdateDeltaLocationDetails: Boolean = false,
                            doNotInitializeODPMetrics: Boolean = false,
                            useAutoloader: Boolean = true,
                            srcDatabases: Option[String] = None,
                            tablePattern: Option[String] = None,
                            triggerInterval: Option[String] = None,
                            maxFilesPerTrigger : String = "1024",
                            streamLower: Int = 1,
                            streamUpper: Int = 50)

/************************************************************************************************************************/

/*
Schemas
 */

import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.delta.actions.SingleAction
import org.apache.spark.sql.types._

trait Schemas {
  final val PATH = "deltaLocation"
  final val PATHID = "deltaLocationIdentifier"
  final val WILCARDID = "wcPathIdentifier"
  final val FULL_TABLE_NAME = "fullTableName"
  final val UPDATE_TS = "updatedOn"
  final val COMMIT_DATE = "commitShortDate"
  final val COMMIT_TS = "committedOn"
  final val COMMIT_VERSION = "commitVersion"
  final val FILE_NAME = "file_name"
  final val WILDCARD_LEVEL = "wildCardLevel"
  final val WILDCARD_PATH = "wcPath"
  final val EXTRA_CONFIG = "extraConfig"
  final val SKIP_PROCESSING = "ignore"
  final val ODP_METRICS_VERSION = "1.0"
  final val ENTITY_NAME = "odpmetrics"

  final val unrefined = ScalaReflection.schemaFor[SingleAction].dataType.asInstanceOf[StructType]
    .add(StructField(FILE_NAME, StringType))
    .add(StructField(PATH, StringType))
    .add(StructField(PATHID, StringType))
    .add(StructField(COMMIT_VERSION, LongType))
    .add(StructField(UPDATE_TS, TimestampType))
    .add(StructField(COMMIT_TS, TimestampType))
    .add(StructField(COMMIT_DATE, DateType))
  final val pathDetails = ScalaReflection.schemaFor[DeltaLocationDetails].dataType.asInstanceOf[StructType]
  final val masterConfig = ScalaReflection.schemaFor[MasterConfig].dataType.asInstanceOf[StructType]
  final val processedHistory = ScalaReflection.schemaFor[ProcessedHistory].dataType
    .asInstanceOf[StructType]
}

object Schemas extends Schemas

/**********************************************************************************************************************/

/*
SparkSettings
 */

import org.apache.spark.sql.SparkSession

trait SparkSettings extends Serializable with ConfigurationSettings {
  protected val sparkSession: SparkSession = environment match {
    case InBuilt => SparkSession.builder()
      .master("local")
      .config("spark.driver.host", "localhost")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .appName("ODP_METRICS_CORE").getOrCreate()
    case _ => val spark = SparkSession.builder().appName("ODP Metrics").getOrCreate()
      spark.conf.set("spark.databricks.delta.properties.defaults.enableChangeDataFeed",
        value = true)
      spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", value = true)
      spark.conf.set("spark.databricks.delta.autoCompact.enabled", value = true)
      spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", value = true)
      spark.conf.set("spark.odpmetrics.version", value = Schemas.ODP_METRICS_VERSION)
      spark
  }
  def spark: SparkSession = sparkSession

}

/**********************************************************************************************************************/

/*
ODPMetricsSparkConf
 */

trait ODPMetricsSparkConf extends Serializable with SparkSettings {

  def buildConfKey(key: String): String = s"odpmetrics.${key}"

  // Mandatory Configurations
  val DB_LOCATION = buildConfKey("db.location")
  val DB_NAME = buildConfKey("db.name")
  val CHECKPOINT_LOCATION = buildConfKey("checkpoint.location")
  val CHECKPOINT_SUFFIX = buildConfKey("checkpoint.suffix")

  // Optional Configuration
  val UNREFINED_TABLE = buildConfKey("unrefined.table")
  val MASTER_TABLE = buildConfKey("master.table")
  val DELTA_LOCATION_DETAILS_TABLE = buildConfKey("delta.location.details.table")
  val PROCESSED_LOG_TABLE = buildConfKey("processed.log.table")
  val REFINED_COMMIT_METRICS_TABLE = buildConfKey("refined.commit.metrics.table")
//  val REFINED_FILE_LEVEL_INFO_TABLE = buildConfKey("refined.file.level.info.table")
  val MERGE_WILDCARD_PATHS = buildConfKey("merge.wildcard.paths")
  val RESET_DELTA_LOCATION_DETAILS = buildConfKey("reset.delta.location.details")
  val DO_NOT_UPDATE_DELTA_LOCATION_DETAILS = buildConfKey("omit.delta.location.details")
  val SKIP_INITIALIZE = buildConfKey("omit.initialize")
  val SRC_DATABASES = buildConfKey("src.databases")
  val TABLE_PATTERN = buildConfKey("table.pattern")
  val TRIGGER_INTERVAL = buildConfKey("trigger.interval")
  val TRIGGER_MAX_FILES = buildConfKey("trigger.max.files")
  val STREAM_LOWER = buildConfKey("stream.lower")
  val STREAM_UPPER = buildConfKey("stream.upper")
  val USE_AUTOLOADER = buildConfKey("use.autoloader")


  val configFields = Seq(DB_LOCATION, DB_NAME, CHECKPOINT_LOCATION, CHECKPOINT_SUFFIX,
    UNREFINED_TABLE, MASTER_TABLE, DELTA_LOCATION_DETAILS_TABLE, PROCESSED_LOG_TABLE,
    REFINED_COMMIT_METRICS_TABLE,
//    REFINED_FILE_LEVEL_INFO_TABLE,
    MERGE_WILDCARD_PATHS,
    RESET_DELTA_LOCATION_DETAILS, DO_NOT_UPDATE_DELTA_LOCATION_DETAILS, SKIP_INITIALIZE, SRC_DATABASES,
    TABLE_PATTERN, TRIGGER_INTERVAL, STREAM_LOWER, STREAM_UPPER, USE_AUTOLOADER,
    TRIGGER_MAX_FILES)

  /**
   * Consolidates configuration values from the Spark session's configuration into the provided
   * ODPMetricsConfig object.
   *
   * This method iterates through a list of predefined configuration fields and updates the
   * corresponding fields in the input ODPMetricsConfig object based on the configuration values
   * present in the Spark session's configuration. If a configuration value is not present, the
   * corresponding field in the ODPMetricsConfig object remains unchanged.
   *
   * @param config The input ODPMetricsConfig object to be updated with configuration values.
   * @return An updated ODPMetricsConfig object with fields populated from the Spark session's configuration.
   */
  def consolidateODPMetricsConfigFromSparkConf(config: ODPMetricsConfig): ODPMetricsConfig = {
    configFields.foldLeft(config) {
      (odpMetricsSparkConfig, configValue) => {
        configValue match {
          case DB_LOCATION => spark.conf.getOption(DB_LOCATION).
            fold(odpMetricsSparkConfig) { scv => odpMetricsSparkConfig.copy(baseLocation = Some(scv))}
          case DB_NAME => spark.conf.getOption(DB_NAME).
            fold(odpMetricsSparkConfig) { scv => odpMetricsSparkConfig.copy(dbName = Some(scv))}
          case CHECKPOINT_LOCATION => spark.conf.getOption(CHECKPOINT_LOCATION).
            fold(odpMetricsSparkConfig) { scv => odpMetricsSparkConfig.copy(checkpointBase = Some(scv))}
          case CHECKPOINT_SUFFIX => spark.conf.getOption(CHECKPOINT_SUFFIX).
            fold(odpMetricsSparkConfig) { scv => odpMetricsSparkConfig.copy(checkpointSuffix = Some(scv))}
          case UNREFINED_TABLE => spark.conf.getOption(UNREFINED_TABLE).
            fold(odpMetricsSparkConfig) { scv => odpMetricsSparkConfig.copy(unrefinedTable = scv)}
          case MASTER_TABLE => spark.conf.getOption(MASTER_TABLE).
            fold(odpMetricsSparkConfig) { scv => odpMetricsSparkConfig.copy(masterTable = scv)}
          case DELTA_LOCATION_DETAILS_TABLE => spark.conf.getOption(DELTA_LOCATION_DETAILS_TABLE).
            fold(odpMetricsSparkConfig) { scv => odpMetricsSparkConfig.copy(deltaLocationDetailsTable = scv)}
          case PROCESSED_LOG_TABLE => spark.conf.getOption(PROCESSED_LOG_TABLE).
            fold(odpMetricsSparkConfig) { scv => odpMetricsSparkConfig.copy(processedLogTable = scv)}
          case REFINED_COMMIT_METRICS_TABLE => spark.conf.getOption(REFINED_COMMIT_METRICS_TABLE).
            fold(odpMetricsSparkConfig) { scv => odpMetricsSparkConfig.copy(refinedCommitMetricsTable = scv)}
//          case REFINED_FILE_LEVEL_INFO_TABLE => spark.conf.getOption(REFINED_FILE_LEVEL_INFO_TABLE).
//            fold(omsSparkConfig) { scv => omsSparkConfig.copy(refinedFileLevelInfoTable = scv)}
          case MERGE_WILDCARD_PATHS => spark.conf.getOption(MERGE_WILDCARD_PATHS).
            fold(odpMetricsSparkConfig) {
              scv => odpMetricsSparkConfig.copy(mergeWildcardPaths = scv.toBoolean)}
          case RESET_DELTA_LOCATION_DETAILS => spark.conf.getOption(RESET_DELTA_LOCATION_DETAILS).
            fold(odpMetricsSparkConfig) {
              scv => odpMetricsSparkConfig.copy(resetDeltaLocationDetails = scv.toBoolean)}
          case DO_NOT_UPDATE_DELTA_LOCATION_DETAILS => spark.conf.getOption(DO_NOT_UPDATE_DELTA_LOCATION_DETAILS).
            fold(odpMetricsSparkConfig) {
              scv => odpMetricsSparkConfig.copy(doNotUpdateDeltaLocationDetails = scv.toBoolean)}
          case SKIP_INITIALIZE => spark.conf.getOption(SKIP_INITIALIZE).
            fold(odpMetricsSparkConfig) {
              scv => odpMetricsSparkConfig.copy(doNotInitializeODPMetrics = scv.toBoolean)}
          case SRC_DATABASES => spark.conf.getOption(SRC_DATABASES).
            fold(odpMetricsSparkConfig) { scv => odpMetricsSparkConfig.copy(srcDatabases = Some(scv))}
          case TABLE_PATTERN => spark.conf.getOption(TABLE_PATTERN).
            fold(odpMetricsSparkConfig) { scv => odpMetricsSparkConfig.copy(tablePattern = Some(scv))}
          case TRIGGER_INTERVAL => spark.conf.getOption(TRIGGER_INTERVAL).
            fold(odpMetricsSparkConfig) { scv => odpMetricsSparkConfig.copy(triggerInterval = Some(scv))}
          case STREAM_LOWER => spark.conf.getOption(STREAM_LOWER).
            fold(odpMetricsSparkConfig) { scv => odpMetricsSparkConfig.copy(streamLower = scv.toInt)}
          case STREAM_UPPER => spark.conf.getOption(STREAM_UPPER).
            fold(odpMetricsSparkConfig) { scv => odpMetricsSparkConfig.copy(streamUpper = scv.toInt)}
          case USE_AUTOLOADER => spark.conf.getOption(USE_AUTOLOADER).
            fold(odpMetricsSparkConfig) {
              scv => odpMetricsSparkConfig.copy(useAutoloader = scv.toBoolean)}
          case TRIGGER_MAX_FILES => spark.conf.getOption(TRIGGER_MAX_FILES).
            fold(odpMetricsSparkConfig) {
              scv => odpMetricsSparkConfig.copy(maxFilesPerTrigger = scv)}
        }
      }
    }
  }
}

object ODPMetricsSparkConf extends ODPMetricsSparkConf

/**********************************************************************************************************************/

/*
Environment
 */

sealed trait Environment

case object InBuilt extends Environment

case object Empty extends Environment

object EnvironmentResolver {
  def fetchEnvironment(envStr: String): Environment = {
    if (envStr.contains("empty")) Empty
    else InBuilt
  }
}

/**********************************************************************************************************************/

/*
ConfigurationSettings
 */

import org.apache.spark.internal.Logging

trait ConfigurationSettings extends Serializable with Logging {
  def odpmetricsConfig: ODPMetricsConfig = odpmetricsConfigSource

  def odpmetricsConfigSource: ODPMetricsConfig = environment match {
    case Empty => ODPMetricsConfig()
    case InBuilt => ODPMetricsConfig(baseLocation = Some("/tmp/spark-warehouse/odp_metrics.db"),
      dbName = Some("odp_metrics_inbuilt"),
      checkpointBase = Some("/tmp/_odp_metrics_checkpoints/"),
      checkpointSuffix = Some("_1"),
      unrefinedTable = "unrefined_metrics",
      masterTable = "master",
      deltaLocationDetailsTable = "delta_location_details",
      processedLogTable = "processed_log",
      refinedCommitMetricsTable = "refined_commit_metrics")
//      refinedFileLevelInfoTable = "refined_file_level_info")
  }

  def environment: Environment = EnvironmentResolver.fetchEnvironment(environmentType)

  def environmentType: String =
    sys.props.getOrElse("ODP_METRICS_ENV",
      sys.env.getOrElse("ODP_METRICS_ENV", "empty")).toLowerCase
}

object ConfigurationSettings extends ConfigurationSettings

/**********************************************************************************************************************/

/*
Utils
 */

import org.apache.spark.internal.Logging

trait Utils extends Serializable with Logging with Schemas {

  def getODPMetricsDBPath(config: ODPMetricsConfig): String =
    s"${config.baseLocation.get}/${config.dbName.get}"
  def getUnrefinedTableName(config: ODPMetricsConfig): String =
    s"${config.dbName.get}.${config.unrefinedTable}"
  def getUnrefinedTablePath(config: ODPMetricsConfig): String =
    s"${getODPMetricsDBPath(config)}/${config.unrefinedTable}/"
  def getDeltaLocationDetailsTablePath(config: ODPMetricsConfig): String =
    s"${getODPMetricsDBPath(config)}/${config.deltaLocationDetailsTable}/"
  def getMasterConfigTablePath(config: ODPMetricsConfig): String =
    s"${getODPMetricsDBPath(config)}/${config.masterTable}/"
  def getProcessedHistoryTablePath(config: ODPMetricsConfig): String =
    s"${getODPMetricsDBPath(config)}/${config.processedLogTable}/"
  def getRefinedCommitMetricsTablePath(config: ODPMetricsConfig): String =
    s"${getODPMetricsDBPath(config)}/${config.refinedCommitMetricsTable}/"
  def getRefinedCommitMetricsTableName(config: ODPMetricsConfig): String =
    s"${config.dbName.get}.${config.refinedCommitMetricsTable}"
//  def getActionSnapshotTablePath(config: ODPMetricsConfig): String =
//    s"${getODPMetricsDBPath(config)}/${config.refinedFileLevelInfoTable}/"
//  def getActionSnapshotTableName(config: ODPMetricsConfig): String =
//    s"${config.dbName.get}.${config.refinedFileLevelInfoTable}"

  val pathidCommitDatePartitions = Seq(PATHID, COMMIT_DATE)

  private val odpMetricsProperties = Map("entity" -> s"$ENTITY_NAME", "odpmetrics.version" -> s"$ODP_METRICS_VERSION")

  def pathDetailsTableDefinition(ODPMetricsConfig: ODPMetricsConfig): TableDefinition = {
    TableDefinition(ODPMetricsConfig.deltaLocationDetailsTable,
      ODPMetricsConfig.dbName.get,
      pathDetails,
      getDeltaLocationDetailsTablePath(ODPMetricsConfig),
      Some("Metrics Aggregator Path Details Table"),
      odpMetricsProperties
    )
  }

  def masterConfigDefinition(ODPMetricsConfig: ODPMetricsConfig): TableDefinition = {
    TableDefinition(ODPMetricsConfig.masterTable,
      ODPMetricsConfig.dbName.get,
      masterConfig,
      getMasterConfigTablePath(ODPMetricsConfig),
      Some("Metrics Aggregator Master Configuration Table"),
      odpMetricsProperties
    )
  }

  def unrefinedTableDefinition(ODPMetricsConfig: ODPMetricsConfig): TableDefinition = {
    TableDefinition(ODPMetricsConfig.unrefinedTable,
      ODPMetricsConfig.dbName.get,
      unrefined,
      getUnrefinedTablePath(ODPMetricsConfig),
      Some("Metrics Aggregator Unrefined Metrics Table"),
      odpMetricsProperties,
      pathidCommitDatePartitions)
  }

  def processedHistoryTableDefinition(ODPMetricsConfig: ODPMetricsConfig): TableDefinition = {
    TableDefinition(ODPMetricsConfig.processedLogTable,
      ODPMetricsConfig.dbName.get,
      processedHistory,
      getProcessedHistoryTablePath(ODPMetricsConfig),
      Some("Metrics Aggregator Processed Log Table"),
      odpMetricsProperties
    )
  }

  def odpMetricsDatabaseDefinition(ODPMetricsConfig: ODPMetricsConfig): DatabaseDefinition = {
    DatabaseDefinition(ODPMetricsConfig.dbName.get,
      Some(getODPMetricsDBPath(ODPMetricsConfig)),
      Some("Metrics Aggregator Database"),
      odpMetricsProperties
    )
  }
}

object Utils extends Utils

/*********************************************************************************************************************/


/*
ToolkitOps
 */
import java.net.URI
import scala.collection.mutable.ListBuffer
import scala.util.{Failure, Success, Try}
import io.delta.tables.DeltaTable
import org.apache.hadoop.fs.{FileSystem, Path, RemoteIterator}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.delta.{DeltaTableIdentifier, DeltaTableUtils}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.util.SerializableConfiguration

trait ToolkitOps extends Serializable with Logging {
  /**
   * Fetches Delta table identifiers from the metastore based on the specified databases and table patterns.
   *
   * @param databases Optional list of databases to filter by.
   * @param pattern   Optional pattern to filter table names by.
   * @return A sequence of DeltaTableIdentifier objects.
   */
  def fetchMetaStoreDeltaTables(databases: Option[String], pattern: Option[String])
  : Seq[DeltaTableIdentifier] = {
    val srcDatabases = if (databases.isDefined && databases.get.trim.nonEmpty) {
      databases.get.trim.split("[,;:]").map(_.trim).toSeq
    } else {
      Seq.empty[String]
    }
    val tablePattern: String = pattern.map {
      _.trim
    }.filterNot {
      _.isEmpty
    }.getOrElse("*")
    getDeltaTablesFromMetastore(srcDatabases, tablePattern)
  }

  /**
   * Retrieves Delta table identifiers from the metastore based on the specified databases and table patterns.
   *
   * @param databases Optional list of databases to filter by.
   * @param pattern   Optional pattern to filter table names by.
   * @return A sequence of DeltaTableIdentifier objects.
   */
  def getDeltaTablesFromMetastore(databases: Seq[String] = Seq.empty[String],
                                  pattern: String = "*"): Seq[DeltaTableIdentifier] = {
    val spark = SparkSession.active
    val sessionCatalog = spark.sessionState.catalog
    val databaseList = if (databases.nonEmpty) {
      sessionCatalog.listDatabases.filter(databases.contains(_))
    } else {
      sessionCatalog.listDatabases
    }
    val allTables = databaseList.flatMap(dbName =>
      sessionCatalog.listTables(dbName, pattern, includeLocalTempViews = false))
    allTables.flatMap(tableIdentifierToDeltaTableIdentifier)
  }

  /**
   * Validates the Delta location defined in a MasterConfig instance.
   *
   * @param sourceConfig The MasterConfig instance containing the path and extraConfig.
   * @return A sequence of tuples containing (Delta table name, path, extraConfig).
   */
  def validateDeltaLocation(sourceConfig: MasterConfig): Seq[(Option[String], String,
    Map[String, String])] = {
    val spark = SparkSession.active
    val sessionCatalog = spark.sessionState.catalog

    if (!sourceConfig.deltaLocation.contains("/") && !sourceConfig.deltaLocation.contains(".")
      && sessionCatalog.databaseExists(sourceConfig.deltaLocation)) {
      val dbTables = sessionCatalog.listTables(sourceConfig.deltaLocation, "*",
        includeLocalTempViews = false)
      val dbDeltaTableIds = dbTables.flatMap(tableIdentifierToDeltaTableIdentifier)
      dbDeltaTableIds.map(ddt => (Some(ddt.unquotedString), ddt.getPath(spark).toString,
        sourceConfig.extraConfig))
    } else {
      val pathDTableTry = Try {
        DeltaTable.forPath(spark, sourceConfig.deltaLocation)
      }
      pathDTableTry match {
        case Success(_) => Seq((None, sourceConfig.deltaLocation, sourceConfig.extraConfig))
        case Failure(e) =>
          val nameDTableTry = Try {
            DeltaTable.forName(spark, sourceConfig.deltaLocation)
          }
          nameDTableTry match {
            case Success(_) =>
              val tableId = spark.sessionState.sqlParser.parseTableIdentifier(sourceConfig.deltaLocation)
              Seq((Some(sourceConfig.deltaLocation),
                new Path(spark.sessionState.catalog.getTableMetadata(tableId).location).toString,
                sourceConfig.extraConfig))
            case Failure(ex) =>
              logError(s"Error while accessing Delta location $sourceConfig." +
                s"It should be a valid database, table path or fully qualified table name.\n " +
                s"Exception thrown: $ex")
              throw ex
          }
      }
    }
  }

  /**
   * Converts a regular table identifier to a Delta table identifier.
   *
   * @param identifier The regular table identifier.
   * @return An optional DeltaTableIdentifier.
   */
  def tableIdentifierToDeltaTableIdentifier(identifier: TableIdentifier)
  : Option[DeltaTableIdentifier] = {
    val spark = SparkSession.active
    DeltaTableIdentifier(spark, identifier)
  }

  /**
   * Creates a database if it doesn't exist.
   *
   * @param dbDefn The definition of the database.
   */
  def createDatabaseIfAbsent(dbDefn: DatabaseDefinition): Unit = {
    val spark = SparkSession.active
    val dBCreateSQL = new StringBuilder(s"CREATE DATABASE IF NOT EXISTS ${dbDefn.databaseName} ")
    if (dbDefn.comment.nonEmpty) {
      dBCreateSQL.append(s"COMMENT '${dbDefn.comment.get}' ")
    }
    if (dbDefn.location.nonEmpty) {
      dBCreateSQL.append(s"LOCATION '${dbDefn.location.get}' ")
    }
    if (dbDefn.properties.nonEmpty) {
      val tableProperties = dbDefn.properties.map(_.productIterator.mkString("'", "'='", "'"))
        .mkString(",")
      dBCreateSQL.append(s"WITH DBPROPERTIES($tableProperties) ")
    }
    logDebug(s"CREATING DATABASE using SQL => ${dBCreateSQL.toString()}")
    Try {
      spark.sql(dBCreateSQL.toString())
    } match {
      case Success(value) => logInfo(s"Successfully created the database ${dbDefn.databaseName}")
      case Failure(exception) =>
        throw new RuntimeException(s"Unable to create the Database: $exception")
    }
  }

  /**
   * Creates a table if it doesn't exist.
   *
   * @param tableDefn The definition of the table.
   */
  def createTableIfAbsent(tableDefn: TableDefinition): Unit = {
    val spark = SparkSession.active
    val fqTableName = s"${tableDefn.databaseName}.${tableDefn.tableName}"
    val tableId = spark.sessionState.sqlParser.parseTableIdentifier(s"${fqTableName}")
    if (!DeltaTable.isDeltaTable(tableDefn.path) && !DeltaTableUtils.isDeltaTable(spark, tableId)) {
      val emptyDF = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], tableDefn.schema)
      val dataFrameCreateWriter = emptyDF
        .writeTo(fqTableName)
        .tableProperty("location", tableDefn.path)
      val dataFrameCreateWriterWithComment = tableDefn.comment.foldLeft(dataFrameCreateWriter) {
        (dfw, c) => dfw.tableProperty("comment", c)
      }
      val dataFrameCreateWriterWithProperties = tableDefn.properties.toList
        .foldLeft(dataFrameCreateWriterWithComment) {
          (dfw, kv) => dfw.tableProperty(kv._1, kv._2)
        }
      if (tableDefn.partitionColumnNames.nonEmpty) {
        val partitionColumns = tableDefn.partitionColumnNames.map(col)
        dataFrameCreateWriterWithProperties
          .using("delta")
          .partitionedBy(partitionColumns.head, partitionColumns.tail: _*)
          .createOrReplace()
      } else {
        dataFrameCreateWriterWithProperties
          .using("delta")
          .createOrReplace()
      }
    }
  }

  /**
   * Deletes a directory using Hadoop's FileSystem API.
   *
   * @param dirName The name of the directory to delete.
   * @return True if the directory was successfully deleted, otherwise false.
   */
  def deleteDirectory(dirName: String): Boolean = {
    val fileSystem = FileSystem.get(new URI(dirName),
      SparkSession.active.sparkContext.hadoopConfiguration)
    fileSystem.delete(new Path(dirName), true)
  }

  /**
   * Drops a database in Spark SQL, including its contents.
   *
   * @param dbName The name of the database to drop.
   * @return A DataFrame representing the result of the drop operation.
   */
  def dropDatabase(dbName: String): DataFrame = {
    SparkSession.active.sql(s"DROP DATABASE IF EXISTS $dbName CASCADE")
  }

  /**
   * Drops a database in Spark SQL, including its contents.
   *
   * @param dbName The name of the database to drop.
   * @return A DataFrame representing the result of the drop operation.
   */
  def resolveWildCardPath(filePath: String, wildCardLevel: Int) : String = {
    assert(wildCardLevel == -1 || wildCardLevel == 0 || wildCardLevel == 1,
      "WildCard Level should be -1, 0 or 1")
    val modifiedPath = if (wildCardLevel == 0) {
      (filePath.split("/").dropRight(1):+"*")
    } else if (wildCardLevel == 1) {
      (filePath.split("/").dropRight(2):+"*":+"*")
    } else {
      filePath.split("/")
    }
    modifiedPath.mkString("/") + "/_delta_log/*.json"
  }

  /**
   * Returns a Spark UserDefinedFunction for resolving wildcard paths.
   *
   * @return The UserDefinedFunction.
   */
  def getDeltaWildCardPathUDF(): UserDefinedFunction = {
    udf((filePath: String, wildCardLevel: Int) => resolveWildCardPath(filePath, wildCardLevel))
  }

  /**
   * Consolidates a list of wildcard paths, filtering out duplicates and overlaps.
   * The `consolidateWildCardPaths` method takes a sequence of tuples containing two strings each: `(String, String)`.
   * Each tuple represents a wildcard path where the first string is the wildcard path itself, and the second string is some associated value (which isn't directly relevant to the method's operation). The goal of this method is to consolidate and filter these wildcard paths based on certain criteria, resulting in a new sequence of `(String, String)` tuples.
   *
   *  Here's an elaboration of how the method works:
   *
   *  1. The method starts by defining an empty mutable list buffer, `ListBuffer.empty[(String, String)]`,
   *  which will be used to store the consolidated wildcard paths.
   *
   *  2. The `foldLeft` function is then applied on the `wildCardPaths` sequence using the initial value of the empty list buffer.
   *  The `foldLeft` function iterates through each element of `wildCardPaths` while maintaining the `ListBuffer` to hold the consolidated paths.
   *
   *  3. Within the `foldLeft` block, the code checks if the list buffer `l` (containing previously processed paths) is non-empty.
   *  If it's empty, it directly adds the current wildcard path tuple `a` to the list buffer `l`.
   *
   *  4. If the list buffer `l` is non-empty, the method proceeds to consolidate the wildcard paths:
   *  - For each wildcard path tuple `b` in the list buffer,
   *  it extracts the initial part of the wildcard path (before the first '*') and stores it as `split_b`.
   *  - Similarly, the initial part of the current wildcard path tuple `a` is extracted and stored as `split_a`.
   *  - If the `split_b` contains the `split_a` (meaning that the initial parts of `b` and `a` match),
   *  the method removes the tuple `b` from the list buffer `l` and checks if the list buffer still contains tuple `a`.
   *  If not, it adds tuple `a` to the list buffer `l`.
   *  - If the `split_a` doesn't contain `split_b`, and tuple `a` is not already present in the list buffer `l`,
   *  it adds tuple `a` to the list buffer `l`.
   *
   *  5. After all elements in the `wildCardPaths` sequence are processed, the `foldLeft` function returns the consolidated list buffer.
   *
   *  6. Finally, the method converts the list buffer to a list using the `.toList` method and returns the resulting list of consolidated wildcard path tuples.
   *
   *  In summary, the `consolidateWildCardPaths` method takes a sequence of wildcard path tuples,
   *  consolidates the paths based on certain criteria (initial parts of the paths), and returns a new sequence of consolidated wildcard path tuples.
   *  This consolidation ensures that only non-overlapping paths are retained in the result.
   *
   * @param wildCardPaths The list of wildcard paths.
   * @return The consolidated list of wildcard paths.
   */
//  def consolidateWildCardPaths(wildCardPaths: Seq[(String, String)]): Seq[(String, String)] = {
//    wildCardPaths.foldLeft(ListBuffer.empty[(String, String)]) { (l, a) =>
//      if (l.nonEmpty) {
//        val split_a = a._1.split("\\*")(0)
//        for (b <- l.toList) {
//          val split_b = b._1.split("\\*")(0)
//          if (split_b contains split_a) {
//            l -= b
//            if (! l.contains(a)) {
//              l += a
//            }
//          } else {
//            if (!(split_a contains split_b) && ! l.contains(a)) {
//              l += a
//            }
//          }
//        }
//        l
//      } else {
//        l += a
//      }
//    }.toList
//  }
  def consolidateWildCardPaths(wildCardPaths: Seq[(String, String)]): Seq[(String, String)] = {
    val wildCardPathsReverseSorted = wildCardPaths.sortBy(_._1)(Ordering[String].reverse)
    val consolidatedListBuffer = wildCardPathsReverseSorted.foldLeft(ListBuffer.empty[(String, String)]) { (l, a) =>
      if (l.isEmpty) {
        l += a
      } else {
        val split_a = a._1.split("\\*")(0)  // Extract initial part of a
        val matchingPaths = l.filter { b =>
          val split_b = b._1.split("\\*")(0)  // Extract initial part of b
          split_b.contains(split_a) || split_a.contains(split_b)
        }

        if (matchingPaths.nonEmpty) {
          l --= matchingPaths
          if (!l.contains(a)) {
            l += a
          }
        } else if (!l.contains(a)) {
          l += a
        }
      }
      l
    }

    consolidatedListBuffer.toList
  }


  /**
   * Lists subdirectories of the provided path, returning them as an array of SourceConfig instances.
   *
   * @param sourceConfig The SourceConfig instance.
   * @param conf         The SerializableConfiguration instance.
   * @return An array of SourceConfig instances representing subdirectories.
   */
  def listSubDirectories(sourceConfig: MasterConfig, conf: SerializableConfiguration):
  Array[MasterConfig] = {
    val skipProcessing = sourceConfig.ignore
    val extraConfig = sourceConfig.extraConfig
    val subDirectories = listSubDirectories(sourceConfig.deltaLocation, conf)
    subDirectories.map(d => MasterConfig(d, skipProcessing, extraConfig))
  }

  /**
   * Lists subdirectories of the provided path.
   *
   * @param path The path to list subdirectories from.
   * @param conf The SerializableConfiguration instance.
   * @return An array of strings representing subdirectory paths.
   */
  def listSubDirectories(path: String, conf: SerializableConfiguration): Array[String] = {
    val fs = new Path(path).getFileSystem(conf.value)
    fs.listStatus(new Path(path)).filter(_.isDirectory).map(_.getPath.toString)
  }

  /**
   * Recursively lists Delta table paths in subdirectories.
   *
   * @param sourceConfig The SourceConfig instance.
   * @param conf         The SerializableConfiguration instance.
   * @return A set of SourceConfig instances representing Delta table paths.
   */
  def recursiveListDeltaTablePaths(sourceConfig: MasterConfig, conf: SerializableConfiguration):
  Set[MasterConfig] = {
    val skipProcessing = sourceConfig.ignore
    val extraConfig = sourceConfig.extraConfig
    recursiveListDeltaTablePaths(sourceConfig.deltaLocation, conf)
      .map(d => MasterConfig(d, skipProcessing, extraConfig))
  }

  /**
   * Recursively lists Delta table paths in subdirectories.
   *
   * @param path The path to start the recursive listing from.
   * @param conf The SerializableConfiguration instance.
   * @return A set of strings representing Delta table paths.
   */
  def recursiveListDeltaTablePaths(path: String, conf: SerializableConfiguration): Set[String] = {
    implicit def remoteIteratorToIterator[A](ri: RemoteIterator[A]): Iterator[A] =
      new Iterator[A] {
        override def hasNext: Boolean = ri.hasNext
        override def next(): A = ri.next()
      }
    val fs = new Path(path).getFileSystem(conf.value)
    fs.listFiles(new Path(path), true)
      .map(_.getPath.toString)
      .filter(x => x.contains("_delta_log") && x.endsWith(".json"))
      .map(_.split("/").dropRight(2).mkString("/"))
      .toSet
  }
}
object ToolkitOps extends ToolkitOps

/**********************************************************************************************************************************************************/

/*
CommandLineParser
 */

object CommandLineParser {

  final val SWITCH_PATTERN = "(--[^=]+)".r
  final val KEY_VALUE_PATTERN = "(--[^=]+)=(.+)".r
  final val DO_NOT_UPDATE_DELTA_LOCATION_DETAILS = "--omitUpdateDeltaLocationDetails"
  final val DO_NOT_INITIALIZE_ODP_METRICS = "--omitInitialization"
  final val SKIP_WILDCARD_PATHS_MERGING = "--omitMergeWildcardPaths"
  final val DB_NAME = "--dbName"
  final val BASE_LOCATION = "--baseLocation"
  final val CHECKPOINT_LOCATION = "--checkpointLocation"
  final val CHECKPOINT_SUFFIX = "--checkpointSuffix"
  final val STREAM_LOWER = "--streamLower"
  final val STREAM_UPPER = "--streamUpper"


  def consolidateAndValidateODPMetricsConfig(args: Array[String], fileODPMetricsConfig: ODPMetricsConfig,
                                             isBatch: Boolean = true): ODPMetricsConfig = {
    val consolidatedODPMetricsConfig = parseCommandArgsAndConsolidateODPMetricsConfig(args, fileODPMetricsConfig)
    validateODPMetricsConfig(consolidatedODPMetricsConfig, isBatch)
    consolidatedODPMetricsConfig
  }

  def parseCommandArgs(args: Array[String]): Array[(String, String)] = {
    args.map(arg => arg match {
      case SWITCH_PATTERN(k) => (k, "true")
      case KEY_VALUE_PATTERN(k, v) => (k, v)
      case _ => throw new RuntimeException("Malformed Metrics Aggregator Command Line Options")
    })
  }

  def parseCommandArgsAndConsolidateODPMetricsConfig(args: Array[String], fileODPMetricsConfig: ODPMetricsConfig):
  ODPMetricsConfig = {
    val parsedCommandArgs: Array[(String, String)] = parseCommandArgs(args)
    parsedCommandArgs.foldLeft(fileODPMetricsConfig) {
      (odpMetricsCommandArgsConfig, argOptionValue) => {
        argOptionValue match {
          case (option, value) =>
            option match {
              case DO_NOT_UPDATE_DELTA_LOCATION_DETAILS => odpMetricsCommandArgsConfig.copy(doNotUpdateDeltaLocationDetails = true)
              case DO_NOT_INITIALIZE_ODP_METRICS => odpMetricsCommandArgsConfig.copy(doNotInitializeODPMetrics = true)
              case SKIP_WILDCARD_PATHS_MERGING =>
                odpMetricsCommandArgsConfig.copy(mergeWildcardPaths = false)
              case DB_NAME => odpMetricsCommandArgsConfig.copy(dbName = Some(value))
              case BASE_LOCATION => odpMetricsCommandArgsConfig.copy(baseLocation = Some(value))
              case CHECKPOINT_LOCATION => odpMetricsCommandArgsConfig.copy(checkpointBase = Some(value))
              case CHECKPOINT_SUFFIX => odpMetricsCommandArgsConfig.copy(checkpointSuffix = Some(value))
              case STREAM_LOWER => odpMetricsCommandArgsConfig.copy(streamLower = value.toInt)
              case STREAM_UPPER => odpMetricsCommandArgsConfig.copy(streamUpper = value.toInt)

            }
        }
      }
    }
  }

  def validateODPMetricsConfig(ODPMetricsConfig: ODPMetricsConfig, isBatch: Boolean = true): Unit = {
    assert(ODPMetricsConfig.baseLocation.isDefined,
      "MUST PROVIDE configuration Base Location. " +
        "Provide through Command line argument --baseLocation or " +
        "through config file parameter base-location")
    assert(ODPMetricsConfig.dbName.isDefined,
      "MUST PROVIDE configuration DB Name. " +
        "Provide through Command line argument --dbName or " +
        "through config file parameter db-name")
    if(!isBatch) {
      assert(ODPMetricsConfig.checkpointBase.isDefined,
        "MUST PROVIDE configuration Checkpoint Base Location. " +
          "Provide through Command line argument --checkpointLocation or " +
          "through config file parameter checkpoint-base")
      assert(ODPMetricsConfig.checkpointSuffix.isDefined,
        "MUST PROVIDE configuration Checkpoint Suffix. " +
          "Provide through Command line argument --checkpointSuffix or " +
          "through config file parameter checkpoint-suffix")
    }
  }
}

/*********************************************************************************************************************/

/*
Initializer
 */

import scala.util.{Failure, Success, Try}
import org.apache.spark.internal.Logging

trait Initializer extends Serializable with Logging {

  def initializeODPMetrics(config: ODPMetricsConfig, dropAndRecreate: Boolean = false): Unit = {
    if (dropAndRecreate) {
      cleanupODPMetrics(config)
    }
    createODPMetricsDB(config)
    createODPMetricsTables(config)
    /* Uncomment to add the new created OMS Database to be monitored by OMS
    if (dropAndRecreate) {
      populateOMSSourceConfigTableWithSelf(config.dbName, config.sourceConfigTable)
    } */
  }

  def createODPMetricsDB(config: ODPMetricsConfig): Unit = {
    logInfo("Generating the Metrics Aggregator Database on Delta Lake")
    ToolkitOps.createDatabaseIfAbsent(Utils.odpMetricsDatabaseDefinition(config))
  }

  def createODPMetricsTables(config: ODPMetricsConfig): Unit = {
    logInfo("Generating the EXTERNAL Master Configuration table on Metrics Aggregator Delta Lake")
    ToolkitOps.createTableIfAbsent(Utils.masterConfigDefinition(config))
    logInfo("Generating the INTERNAL Paths Details table on Metrics Aggregator Delta Lake")
    createPathConfigTables(config)
    logInfo("Generating the Affected Files Info table on Metrics Aggregator Delta Lake")
    ToolkitOps.createTableIfAbsent(Utils.unrefinedTableDefinition(config))
    logInfo("Generating the Processing log table on Metrics Aggregator Delta Lake")
    ToolkitOps.createTableIfAbsent(Utils.processedHistoryTableDefinition(config))
  }

  def createPathConfigTables(config: ODPMetricsConfig): Unit = {
    logInfo("Generating the Delta Table Paths Details Table on Metrics Aggregator")
    ToolkitOps.createTableIfAbsent(Utils.pathDetailsTableDefinition(config))
  }

  def cleanupODPMetrics(config: ODPMetricsConfig): Unit = {
    val deleteDBPath = Try {
      ToolkitOps.deleteDirectory(Utils.getODPMetricsDBPath(config))
    }
    deleteDBPath match {
      case Success(value) => logInfo(s"Successfully deleted the directory ${Utils.getODPMetricsDBPath(config)}")
      case Failure(exception) => throw exception
    }
    ToolkitOps.dropDatabase(config.dbName.get)
  }
}
/**********************************************************************************************************************/

/*
ODPMetricsOperations
 */
import java.time.Instant

import scala.util.{Failure, Success, Try}
import io.delta.tables._

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.delta.actions.SingleAction
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{StreamingQuery, Trigger}
import org.apache.spark.sql.types.{LongType, StructType}
import org.apache.spark.util.SerializableConfiguration

trait ODPMetricsOperations extends Serializable with SparkSettings with Logging with Schemas {
  val implicits = spark.implicits

  import implicits._

  def updateODPMetricsDeltaLocationDetailsFromMasterConfig(config: ODPMetricsConfig): Unit = {
    // Fetch the latest tables configured
    val configuredSources: Array[MasterConfig] = fetchMasterConfigForProcessing(config)
    // Update the ODP Metrics Path Details
    updateODPMetricsDeltaLocationDetailsFromList(configuredSources.toSeq,
      Utils.getDeltaLocationDetailsTablePath(config),
      config.resetDeltaLocationDetails)
  }

  def processWildcardDirectories(sourceConfigs: DataFrame): Dataset[MasterConfig] = {
    val spark = SparkSession.active
    val hadoopConf = new SerializableConfiguration(spark.sessionState.newHadoopConf())

    val nonWildCardSourcePaths = sourceConfigs
      .filter(substring(col(PATH), -2, 2) =!= "**").as[MasterConfig]
    val wildCardSourcePaths = sourceConfigs
      .filter(substring(col(PATH), -2, 2) === "**")
      .selectExpr(s"substring($PATH,1,length($PATH)-2) as $PATH",
        s"$SKIP_PROCESSING", s"$EXTRA_CONFIG").as[MasterConfig]
    val wildCardSubDirectories = wildCardSourcePaths.flatMap(ToolkitOps.listSubDirectories(_, hadoopConf))
    val wildCardTablePaths = wildCardSubDirectories.repartition(32)
      .flatMap(ToolkitOps.recursiveListDeltaTablePaths(_, hadoopConf))
    wildCardTablePaths.unionByName(nonWildCardSourcePaths)
  }

  def fetchMasterConfigForProcessing(config: ODPMetricsConfig): Array[MasterConfig] = {
    val spark = SparkSession.active
    val sourceConfigs = spark.read.format("delta").load(Utils.getMasterConfigTablePath(config))
      .where(s"$SKIP_PROCESSING <> true").select(PATH, SKIP_PROCESSING, EXTRA_CONFIG)
    val expandedSourceConfigs = processWildcardDirectories(sourceConfigs).collect()
    expandedSourceConfigs.foreach(sc => assert(sc.extraConfig.contains(WILDCARD_LEVEL),
      s"Master Config $sc missing Wild Card Level Parameter"))
    expandedSourceConfigs
  }

  def updateODPMetricsDeltaLocationDetailsFromList(sourceConfigs: Seq[MasterConfig],
                                                   pathConfigTablePath: String,
                                                   truncate: Boolean = false)
  : Unit = {
    val tablePaths: DataFrame = sourceConfigs.flatMap(ToolkitOps.validateDeltaLocation)
      .toDF(FULL_TABLE_NAME, PATH, EXTRA_CONFIG)
    updateDeltaLocationDetailsToODPMetrics(tablePathToDeltaLocationDetails(tablePaths),
      pathConfigTablePath,
      truncate)
  }

  def updateODPMetricsDeltaLocationDetailsFromMetaStore(config: ODPMetricsConfig, truncate: Boolean = false): Unit = {
    val metaStoreDeltaTables = ToolkitOps.fetchMetaStoreDeltaTables(config.srcDatabases,
      config.tablePattern)
    val tablePaths = metaStoreDeltaTables.map(mdt => (mdt.unquotedString,
      mdt.getPath(spark).toString, Map(WILDCARD_LEVEL -> 1)))
      .toDF(FULL_TABLE_NAME, PATH, EXTRA_CONFIG)
    updateDeltaLocationDetailsToODPMetrics(tablePathToDeltaLocationDetails(tablePaths),
      Utils.getDeltaLocationDetailsTablePath(config), truncate)
  }

  def tablePathToDeltaLocationDetails(tablePaths: DataFrame): Dataset[DeltaLocationDetails] = {
    val deltaWildCardPath = ToolkitOps.getDeltaWildCardPathUDF()
    tablePaths
      .withColumn(PATHID, substring(sha1($"deltaLocation"), 0, 7))
      .withColumn("wcPath",
        deltaWildCardPath(col(s"$PATH"), col(s"${EXTRA_CONFIG}.${WILDCARD_LEVEL}")))
      .withColumn(WILCARDID, substring(sha1($"wcPath"), 0, 7))
//      .withColumn("automated", lit(false))
      .withColumn(COMMIT_VERSION, lit(0L))
      .withColumn("ignore", lit(false))
      .withColumn(UPDATE_TS, lit(Instant.now())).as[DeltaLocationDetails]
  }

  def updateDeltaLocationDetailsToODPMetrics(pathConfigs: Dataset[DeltaLocationDetails],
                                             deltaLocationDetailsTablePath: String,
                                             truncate: Boolean = false): Unit = {
    val pathConfigOMSDeltaTable = Try {
      DeltaTable.forPath(deltaLocationDetailsTablePath)
    }
    pathConfigOMSDeltaTable match {
      case Success(pct) =>
        if (truncate) pct.delete()
        pct.as("pathconfig")
          .merge(pathConfigs.toDF().as("pathconfig_updates"),
            s"""pathconfig.$PATHID = pathconfig_updates.$PATHID and
               |pathconfig.$WILCARDID = pathconfig_updates.$WILCARDID
               |""".stripMargin)
          .whenMatched.updateExpr(Map(s"$UPDATE_TS" -> s"pathconfig_updates.$UPDATE_TS"))
          .whenNotMatched.insertAll().execute()
      case Failure(ex) => throw new RuntimeException(s"Unable to update the Delta Location Details table. $ex")
    }
  }

  def insertRawDeltaLogs(rawActionsTablePath: String)(newDeltaLogDF: DataFrame, batchId: Long):
  Unit = {
    newDeltaLogDF.cache()

    val pathids = newDeltaLogDF.select(PATHID).distinct().as[String].collect()
      .mkString("'", "','", "'")
    val commitDates = newDeltaLogDF.select(COMMIT_DATE).distinct().as[String].collect()
      .mkString("'", "','", "'")
    val rawActionsTable = Try {
      DeltaTable.forPath(rawActionsTablePath)
    }
    rawActionsTable match {
      case Success(rat) =>
        rat.as("raw_actions")
          .merge(newDeltaLogDF.as("raw_actions_updates"),
            s"""raw_actions.$PATHID = raw_actions_updates.$PATHID and
               |raw_actions.$PATHID in ($pathids) and
               |raw_actions.$COMMIT_DATE in ($commitDates) and
               |raw_actions.$COMMIT_DATE = raw_actions_updates.$COMMIT_DATE and
               |raw_actions.$COMMIT_VERSION = raw_actions_updates.$COMMIT_VERSION
               |""".stripMargin)
          .whenNotMatched.insertAll().execute()
      case Failure(ex) => throw new RuntimeException(s"Unable to insert new data into " +
        s"Unrefined table. $ex")
    }
    newDeltaLogDF.unpersist()
  }

  def processDeltaLogStreams(streamTargetAndLog: (DataFrame, StreamTargetInfo),
                             rawActionsTablePath: String,
                             triggerIntervalOption: Option[String],
                             appendMode: Boolean = false): (String, StreamingQuery) = {
    val readStream = streamTargetAndLog._1
    val targetInfo = streamTargetAndLog._2
    assert(targetInfo.wcPathIdentifier.isDefined, "Metrics Readstreams should be associated with WildcardPath")
    val triggerInterval = triggerIntervalOption.getOrElse("availableNow")
    val trigger = if (triggerInterval.equalsIgnoreCase("availableNow") || triggerInterval.equalsIgnoreCase("once")) { // scalastyle:ignore
      Trigger.AvailableNow()
    } else {
      Trigger.ProcessingTime(triggerInterval)
    }
    val wuid = targetInfo.wcPathIdentifier.get
    val poolName = "pool_" + wuid
    val queryName = "query_" + wuid

    spark.sparkContext.setLocalProperty("spark.scheduler.pool", poolName)
    if(!appendMode) {
      (wuid, readStream
        .writeStream
        .format("delta")
        .queryName(queryName)
        .foreachBatch(insertRawDeltaLogs(rawActionsTablePath) _)
        .outputMode("update")
        .option("checkpointLocation", targetInfo.checkpointPath)
        .trigger(trigger)
        .start(targetInfo.path))
    } else {
      (wuid, readStream
        .writeStream
        .queryName(queryName)
        .partitionBy(Utils.pathidCommitDatePartitions: _*)
        .outputMode("append")
        .format("delta")
        .option("checkpointLocation", targetInfo.checkpointPath)
        .trigger(trigger)
        .start(targetInfo.path))
    }
  }

  def streamingUpdateUnrefinedToODPMetrics(config: ODPMetricsConfig): Unit = {
    val uniquePaths = if (config.mergeWildcardPaths) {
      ToolkitOps.consolidateWildCardPaths(
        fetchPathForStreamProcessing(Utils.getDeltaLocationDetailsTablePath(config),
          startingStream = config.streamLower, endingStream = config.streamUpper))
    } else {
      fetchPathForStreamProcessing(Utils.getDeltaLocationDetailsTablePath(config),
        startingStream = config.streamLower, endingStream = config.streamUpper)
    }
    val logReadStreams = uniquePaths.flatMap(p =>
      fetchStreamTargetAndDeltaLogForPath(p,
        config.checkpointBase.get,
        config.checkpointSuffix.get,
        Utils.getUnrefinedTablePath(config), config.useAutoloader, config.maxFilesPerTrigger))
    val logWriteStreamQueries = logReadStreams
      .map(lrs => processDeltaLogStreams(lrs,
        Utils.getUnrefinedTablePath(config),
        config.triggerInterval))
    spark.streams.addListener(new ODPMetricsStreamingQueryListener())
    logWriteStreamQueries.foreach(x => x._2.status.prettyJson)
    spark.streams.awaitAnyTermination()
  }


  def fetchPathForStreamProcessing(pathConfigTablePath: String,
                                   useWildCardPath: Boolean = true, startingStream: Int = 1, endingStream: Int = 50):
  Seq[(String, String)] = {
    if (useWildCardPath) {
      val wildcard_window = Window.orderBy(WILCARDID)
      fetchPathConfigForProcessing(pathConfigTablePath)
        .select(WILDCARD_PATH, WILCARDID)
        .distinct()
        .withColumn("wildcard_row_id", row_number().over(wildcard_window))
        .where($"wildcard_row_id".between(startingStream, endingStream))
        .drop("wildcard_row_id")
        .as[(String, String)].collect()
    } else {
      val path_window = Window.orderBy(PATHID)
      fetchPathConfigForProcessing(pathConfigTablePath)
        .select(concat(col(PATH), lit("/_delta_log/*.json")).as(PATH), col(PATHID))
        .distinct()
        .withColumn("path_row_id", row_number().over(path_window))
        .where($"path_row_id".between(startingStream, endingStream))
        .drop("path_row_id")
        .as[(String, String)].collect()
    }
  }

  def fetchPathConfigForProcessing(pathConfigTablePath: String): Dataset[DeltaLocationDetails] = {
    val spark = SparkSession.active
    spark.read.format("delta").load(pathConfigTablePath).as[DeltaLocationDetails]
  }

  def fetchStreamTargetAndDeltaLogForPath(pathInfo: (String, String),
                                          checkpointBaseDir: String, checkpointSuffix: String, rawActionsTablePath: String,
                                          useAutoLoader: Boolean, maxFilesPerTrigger: String):
  Option[(DataFrame, StreamTargetInfo)] = {
    val wildCardPath = pathInfo._1
    val wuid = pathInfo._2
    val checkpointPath = checkpointBaseDir + "/_odp_metrics_checkpoints/unprocessed_log_" +
      wuid + checkpointSuffix

    val readPathStream = fetchStreamingDeltaLogForPath(wildCardPath, useAutoLoader,
      maxFilesPerTrigger)
    if(readPathStream.isDefined) {
      Some(readPathStream.get,
        StreamTargetInfo(path = rawActionsTablePath, checkpointPath = checkpointPath,
          wcPathIdentifier = Some(wuid)))
    } else {
      None
    }
  }

  def fetchStreamingDeltaLogForPath(path: String, useAutoloader: Boolean = true,
                                    maxFilesPerTrigger: String = "1024")
  : Option[DataFrame] = {
    val actionSchema: StructType = ScalaReflection.schemaFor[SingleAction].dataType
      .asInstanceOf[StructType]
    val regex_str = "^(.*)\\/_delta_log\\/(.*)\\.json$"
    val deltaLogDFOpt = if (useAutoloader) {
      Some(spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.maxFilesPerTrigger", maxFilesPerTrigger)
        .option("cloudFiles.useIncrementalListing", "true")
        .schema(actionSchema)
        .load(path).select("*", "_metadata"))
    } else {
      getDeltaLogs(actionSchema, path, maxFilesPerTrigger)
    }
    if (deltaLogDFOpt.nonEmpty) {
      val deltaLogDF = deltaLogDFOpt.get
        .withColumn(FILE_NAME, col("_metadata.file_path"))
        .withColumn(COMMIT_TS, col("_metadata.file_modification_time"))
      Some(deltaLogDF
        .withColumn(PATH, regexp_extract(col(s"$FILE_NAME"), regex_str, 1))
        .withColumn(PATHID, substring(sha1(col(s"$PATH")), 0, 7))
        .withColumn(COMMIT_VERSION, regexp_extract(col(s"$FILE_NAME"),
          regex_str, 2).cast(LongType))
        .withColumn(UPDATE_TS, lit(Instant.now()))
        .withColumn(COMMIT_DATE, to_date(col(s"$COMMIT_TS")))
        .drop("_metadata"))
    } else {
      None
    }
  }

  def getCurrentUnrefinedVersion(unrefinedTablePath: String): Long = {
    spark.sql(s"describe history delta.`$unrefinedTablePath`")
      .select(max("version").as("max_version")).as[Long].head()
  }

  def getLastProcessedUnrefinedVersion(processedHistoryTablePath: String,
                                       unrefinedTable: String): Long = {
    Try {
      spark.read.format("delta")
        .load(processedHistoryTablePath)
        .where(s"tableName='${unrefinedTable}'")
        .select("latestVersion").as[Long].head()
    }.getOrElse(0L)
  }

//  def getLatestUnrefinedVersion(unrefined: DataFrame): Long = {
//    Try {
//      unrefined.select(max(s"$COMMIT_VERSION")).as[Long].head()
//    }.getOrElse(0L)
//  }


  def updateLastProcessedUnrefined(latestVersion: Long,
                                   unrefinedTable: String,
                                   processedHistoryTablePath: String ): Unit = {
    val updatedRawActionsLastProcessedVersion =
      Seq((unrefinedTable, latestVersion, Instant.now()))
        .toDF("tableName", "latestVersion", "updatedOn")

    val processedHistoryTable = Try {
      DeltaTable.forPath(processedHistoryTablePath)
    }
    processedHistoryTable match {
      case Success(pht) =>
        pht.as("processed_history")
          .merge(updatedRawActionsLastProcessedVersion.as("processed_history_updates"),
            """processed_history.tableName = processed_history_updates.tableName""".stripMargin)
          .whenMatched().updateAll()
          .whenNotMatched.insertAll().execute()
      case Failure(ex) => throw new RuntimeException(s"Unable to update the " +
        s"Processed History table. $ex")
    }
  }

  def getUpdatedUnrefined(lastProcessedVersion: Long, unrefinedTablePath: String): DataFrame = {
    spark.read.format("delta")
      .option("readChangeFeed", "true")
      .option("startingVersion", lastProcessedVersion + 1)
      .load(s"$unrefinedTablePath")
      .filter("""_change_type IN ("insert", "update_postimage")""")
  }

  def processCommitInfoFromUnrefined(rawActions: DataFrame,
                                     commitSnapshotTablePath: String,
                                     commitSnapshotTableName: String): Unit = {
    val commitInfo = rawActions.where(col("commitInfo.operation").isNotNull)
      .selectExpr(COMMIT_VERSION, s"current_timestamp() as $UPDATE_TS",
        COMMIT_TS, FILE_NAME, PATH,
        PATHID, COMMIT_DATE, "commitInfo.*").drop("version", "timestamp")

    val commitSnapshotExists = DeltaTable.isDeltaTable(commitSnapshotTablePath)
    if (!commitSnapshotExists) {
      commitInfo.write
        .mode("overwrite")
        .format("delta")
        .partitionBy(Utils.pathidCommitDatePartitions: _*)
        .option("path", commitSnapshotTablePath)
        .saveAsTable(commitSnapshotTableName)
    } else {
      val commitInfoSnapshotTable = Try {
        DeltaTable.forPath(commitSnapshotTablePath)
      }
      commitInfoSnapshotTable match {
        case Success(cst) =>
          cst.as("commitinfo_snap")
            .merge(commitInfo.as("commitinfo_snap_updates"),
              s"""commitinfo_snap.$PATHID = commitinfo_snap_updates.$PATHID and
                 |commitinfo_snap.$COMMIT_DATE = commitinfo_snap_updates.$COMMIT_DATE and
                 |commitinfo_snap.$COMMIT_VERSION = commitinfo_snap_updates.$COMMIT_VERSION
                 |""".stripMargin)
            .whenNotMatched.insertAll().execute()
        case Failure(ex) => throw new RuntimeException(s"Unable to update the Refined Commit " +
          s"Metrics table. $ex")
      }
    }
  }

//  def processActionSnapshotsFromUnrefined(unrefined: DataFrame,
//                                          actionSnapshotTablePath: String,
//                                          actionSnapshotTableName: String): Unit = {
//    val actionSnapshotExists = DeltaTable.isDeltaTable(actionSnapshotTablePath)
//    val actionSnapshots = computeActionSnapshotFromUnrefined(unrefined,
//      actionSnapshotExists,
//      actionSnapshotTablePath)
//    if (!actionSnapshotExists) {
//      actionSnapshots.write
//        .mode("overwrite")
//        .format("delta")
//        .partitionBy(Utils.pathidCommitDatePartitions: _*)
//        .option("overwriteSchema", "true")
//        .option("path", actionSnapshotTablePath)
//        .saveAsTable(actionSnapshotTableName)
//    } else {
//      val actionSnapshotTable = Try {
//        DeltaTable.forPath(actionSnapshotTablePath)
//      }
//      actionSnapshotTable match {
//        case Success(ast) =>
//          ast.as("action_snap")
//            .merge(actionSnapshots.as("action_snap_updates"),
//              s"""action_snap.$PATHID = action_snap_updates.$PATHID and
//                 |action_snap.$COMMIT_DATE = action_snap_updates.$COMMIT_DATE and
//                 |action_snap.$COMMIT_VERSION = action_snap_updates.$COMMIT_VERSION
//                 |""".stripMargin)
//            .whenNotMatched.insertAll().execute()
//        case Failure(ex) => throw new RuntimeException(s"Unable to update the " +
//          s"Action Snapshot table. $ex")
//      }
//    }
//  }

//  def computeActionSnapshotFromUnrefined(rawActions: org.apache.spark.sql.DataFrame,
//                                         snapshotExists: Boolean, actionSnapshotTablePath: String): DataFrame = {
//    val addRemoveFileActions = prepareAddRemoveActionsFromRawActions(rawActions)
//    val cumulativeAddRemoveFiles = if (snapshotExists) {
//      val previousSnapshot = spark.read.format("delta").load(actionSnapshotTablePath)
//      val previousSnapshotMaxCommitVersion = previousSnapshot.groupBy(PATHID)
//        .agg(max(COMMIT_VERSION).as(COMMIT_VERSION))
//      val previousSnapshotMaxAddRemoveFileActions = previousSnapshot
//        .join(previousSnapshotMaxCommitVersion, Seq(PATHID, COMMIT_VERSION))
//        .withColumn("remove_file", lit(null: StructType))
//      val cumulativeAddRemoveFileActions =
//        computeCumulativeFilesFromAddRemoveActions(
//          addRemoveFileActions.unionByName(previousSnapshotMaxAddRemoveFileActions))
//      cumulativeAddRemoveFileActions
//        .join(previousSnapshotMaxCommitVersion, Seq(PATHID, COMMIT_VERSION), "leftanti")
//        .select(cumulativeAddRemoveFileActions("*"))
//    } else {
//      computeCumulativeFilesFromAddRemoveActions(addRemoveFileActions)
//    }
//    deriveActionSnapshotFromCumulativeActions(cumulativeAddRemoveFiles)
//  }

//  def prepareAddRemoveActionsFromRawActions(rawActions: org.apache.spark.sql.DataFrame)
//  : DataFrame = {
//    val addFileActions = rawActions
//      .where(col("add.path").isNotNull)
//      .selectExpr("add", "remove", PATHID, s"$PATH as data_path",
//        COMMIT_VERSION, COMMIT_TS, COMMIT_DATE)
//
//    val duplicateAddWindow =
//      Window.partitionBy(col(PATHID), col("add.path"))
//        .orderBy(col(COMMIT_VERSION).desc_nulls_last)
//    // Duplicate AddFile actions could be present under rare circumstances
//    val rankedAddFileActions = addFileActions
//      .withColumn("rank", rank().over(duplicateAddWindow))
//    val dedupedAddFileActions = rankedAddFileActions
//      .where("rank = 1").drop("rank")
//
//    val removeFileActions = rawActions
//      .where(col("remove.path").isNotNull)
//      .selectExpr("add", "remove", PATHID, s"$PATH as data_path", COMMIT_VERSION,
//        COMMIT_TS, COMMIT_DATE)
//
//    val addRemoveFileActions = dedupedAddFileActions.unionByName(removeFileActions)
//      .select(col(PATHID), col("data_path"), col(COMMIT_VERSION), col(COMMIT_TS),
//        col(COMMIT_DATE),
//        col("add").as("add_file"), col("remove").as("remove_file"))
//    addRemoveFileActions
//  }

//  def computeCumulativeFilesFromAddRemoveActions(addRemoveActions: org.apache.spark.sql.DataFrame)
//  : DataFrame = {
//    val commitVersions = addRemoveActions.select(PATHID, COMMIT_VERSION, COMMIT_TS, COMMIT_DATE)
//      .distinct()
//    val cumulativeAddRemoveFiles = addRemoveActions.as("arf")
//      .join(commitVersions.as("cv"), col("arf.pathIdentifier") === col("cv.pathIdentifier")
//        && col(s"arf.$COMMIT_VERSION") <= col(s"cv.$COMMIT_VERSION"))
//      .select(col(s"cv.$COMMIT_VERSION"), col(s"cv.$COMMIT_TS"),
//        col(s"cv.$COMMIT_DATE"),
//        col(s"arf.$PATHID"), col("arf.data_path"),
//        col("arf.add_file"), col("arf.remove_file"))
//    cumulativeAddRemoveFiles
//  }

//  def deriveActionSnapshotFromCumulativeActions(
//                                                 cumulativeAddRemoveFiles: org.apache.spark.sql.DataFrame): DataFrame = {
//    val cumulativeAddFiles = cumulativeAddRemoveFiles
//      .where(col("add_file.path").isNotNull)
//      .drop("remove_file")
//    val cumulativeRemoveFiles = cumulativeAddRemoveFiles
//      .where(col("remove_file.path").isNotNull)
//      .drop("add_file")
//    val snapshotInputFiles = cumulativeAddFiles.as("ca")
//      .join(cumulativeRemoveFiles.as("cr"),
//        col(s"ca.$PATHID") === col(s"cr.$PATHID")
//          && col(s"ca.$COMMIT_VERSION") === col(s"cr.$COMMIT_VERSION")
//          && col("ca.add_file.path") ===
//          col("cr.remove_file.path"), "leftanti").selectExpr("ca.*")
//    snapshotInputFiles
//  }

  def getDeltaLogs(schema: StructType, path: String,
                   maxFilesPerTrigger: String = "1024"): Option[DataFrame] = {
    val deltaLogTry = Try {
      spark.readStream.schema(schema)
        .option("maxFilesPerTrigger", maxFilesPerTrigger)
        .json(path).select("*", "_metadata")
    }
    deltaLogTry match {
      case Success(value) => Some(value)
      case Failure(exception) =>
        logError(s"Exception while loading Delta log at $path: $exception")
        None
    }
  }
}

object ODPMetricsOperations extends ODPMetricsOperations

/**********************************************************************************************************************/

/*
ODPMetricsRunner
 */

import org.apache.spark.internal.Logging
import org.apache.http.impl.client.HttpClients
import org.apache.http.client.methods.{CloseableHttpResponse, HttpGet}
import org.apache.http.util.EntityUtils
import org.apache.http.client.config.RequestConfig
import com.databricks.dbutils_v1.DBUtilsHolder.dbutils

trait ODPMetricsRunner extends Serializable
  with SparkSettings
  with Initializer
  with ODPMetricsOperations
  with Logging {

  logInfo(s"Loading configuration from : ${environmentType}")
  logInfo(s"Environment set to : ${environment}")
  logInfo(s"Metrics Aggregator Config from configuration file : ${odpmetricsConfig}")

  // Add Usage tracking calls

  def setTrackingHeader(): Unit = {
    val ntbCtx = dbutils.notebook.getContext()
    val apiUrl = ntbCtx.apiUrl.get
    val apiToken = ntbCtx.apiToken.get
    val clusterId = ntbCtx.clusterId.get

    val trackingHeaders = Seq[(String, String)](
      ("Content-Type", "application/json"),
      ("Charset", "UTF-8"),
      ("User-Agent", s"Optra_Data_Platform_Metrics/${ODP_METRICS_VERSION}"),
      ("Authorization", s"Bearer ${apiToken}"))

    val timeout = 30 * 1000

    val httpClient = HttpClients.createDefault()
    val getClusterByIdGet = new HttpGet(s"${apiUrl}/api/2.0/clusters/get?cluster_id=${clusterId}")
    val requestConfig: RequestConfig = RequestConfig.custom
      .setConnectionRequestTimeout(timeout)
      .setConnectTimeout(timeout)
      .setSocketTimeout(timeout)
      .build

    trackingHeaders.foreach(hdr => getClusterByIdGet.addHeader(hdr._1, hdr._2))
    getClusterByIdGet.setConfig(requestConfig)
    val response = httpClient.execute(getClusterByIdGet)
    logInfo(EntityUtils.toString(response.getEntity, "UTF-8"))
  }

  scala.util.control.Exception.ignoring(classOf[Throwable]) { setTrackingHeader() }

  def consolidateAndValidateODPMetricsConfig(args: Array[String], config: ODPMetricsConfig): ODPMetricsConfig

  def fetchConsolidatedODPMetricsConfig(args: Array[String]) : ODPMetricsConfig = {
    val sparkOMSConfig = consolidateODPMetricsConfigFromSparkConf(odpmetricsConfig)
    consolidateAndValidateODPMetricsConfig(args, sparkOMSConfig)
  }

  def consolidateODPMetricsConfigFromSparkConf(config: ODPMetricsConfig): ODPMetricsConfig = {
    ODPMetricsSparkConf.consolidateODPMetricsConfigFromSparkConf(config)
  }
}

trait BatchODPMetricsRunner extends ODPMetricsRunner {
  def consolidateAndValidateODPMetricsConfig(args: Array[String], config: ODPMetricsConfig): ODPMetricsConfig = {
    CommandLineParser.consolidateAndValidateODPMetricsConfig(args, odpmetricsConfig)
  }
}

trait StreamODPMetricsRunner extends ODPMetricsRunner{
  def consolidateAndValidateODPMetricsConfig(args: Array[String], config: ODPMetricsConfig): ODPMetricsConfig = {
    CommandLineParser.consolidateAndValidateODPMetricsConfig(args, odpmetricsConfig, isBatch = false)
  }
}

/**********************************************************************************************************************/

/*
ODPMetricsStreamingQueryListener
 */

import org.apache.spark.internal.Logging
import org.apache.spark.sql.streaming.StreamingQueryListener
import org.apache.spark.sql.streaming.StreamingQueryListener.{QueryProgressEvent,
  QueryStartedEvent, QueryTerminatedEvent}

class ODPMetricsStreamingQueryListener extends StreamingQueryListener with Logging {

  override def onQueryStarted(queryStarted: QueryStartedEvent): Unit = {
    logInfo(s"Query=${queryStarted.name}:QueryId=${queryStarted.id}:STARTED:" +
      s"RunId=${queryStarted.runId}:Timestamp=${queryStarted.timestamp}" )
  }
  override def onQueryTerminated(queryTerminated: QueryTerminatedEvent): Unit = {
    logInfo(s"QueryId=${queryTerminated.id}:RunId=${queryTerminated.runId}:TERMINATED:" +
      s"Exception: ${queryTerminated.exception.toString}"
    )
  }
  override def onQueryProgress(queryProgress: QueryProgressEvent): Unit = {
    if (queryProgress.progress.numInputRows > 0) {
      logInfo(s"Query Progress: ${queryProgress.progress.prettyJson}")
    }
  }
}
