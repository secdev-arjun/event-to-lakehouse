from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, BooleanType, DoubleType,
    IntegerType, ArrayType
)
from pyspark.sql.functions import (
    col, lit, current_timestamp, to_json, struct,
    concat_ws, lower, trim, sha2
)

# ------------------------------------------------------------------------------
# Spark session (your docker spark-submit provides Iceberg + s3a configs)
# ------------------------------------------------------------------------------
spark = SparkSession.builder.appName("Bronze Assets -> Iceberg Silver assets").getOrCreate()

TARGET_TABLE = "iceberg.silver.assets"

RAPID7_SOURCE = "s3a://bronze/topics/rapid7.assets.raw/"
FORTI_SOURCE  = "s3a://bronze/topics/fortisiem.devices.raw/"

RAPID7_CKPT = "s3a://warehouse/checkpoints/silver_assets_rapid7/"
FORTI_CKPT  = "s3a://warehouse/checkpoints/silver_assets_fortisiem/"

# JSON reader hardening:
# - multiLine: true for pretty/indented JSON objects
# - mode: PERMISSIVE keeps going on malformed records
# - columnNameOfCorruptRecord: capture bad JSON into a column (requires schema to include it)
JSON_OPTIONS = {
    "multiLine": "true",
    "mode": "PERMISSIVE",
    "columnNameOfCorruptRecord": "_corrupt_record"
}

TRIGGER_INTERVAL = "30 seconds"


# ------------------------------------------------------------------------------
# Rapid7 schema
# ------------------------------------------------------------------------------
rapid7_schema = StructType([
    StructField("_corrupt_record", StringType(), True),

    StructField("id", IntegerType(), True),
    StructField("ip", StringType(), True),
    StructField("hostName", StringType(), True),

    StructField("addresses", ArrayType(StructType([
        StructField("ip", StringType(), True)
    ])), True),

    StructField("assessedForPolicies", BooleanType(), True),
    StructField("assessedForVulnerabilities", BooleanType(), True),

    StructField("os", StringType(), True),
    StructField("osCertainty", StringType(), True),  # sample shows string

    StructField("osFingerprint", StructType([
        StructField("architecture", StringType(), True),
        StructField("family", StringType(), True),
        StructField("vendor", StringType(), True),
        StructField("product", StringType(), True),
        StructField("cpe", StructType([
            StructField("version", StringType(), True),
        ]), True),
    ]), True),

    StructField("riskScore", DoubleType(), True),
    StructField("rawRiskScore", DoubleType(), True),

    StructField("vulnerabilities", StructType([
        StructField("total", IntegerType(), True),
        StructField("critical", IntegerType(), True),
        StructField("severe", IntegerType(), True),
        StructField("moderate", IntegerType(), True),
        StructField("exploits", IntegerType(), True),
        StructField("malwareKits", IntegerType(), True),
    ]), True),
])

rapid7_raw = (
    spark.readStream.format("json")
    .schema(rapid7_schema)
    .options(**JSON_OPTIONS)
    .load(RAPID7_SOURCE)
)

# If JSON is corrupt, many fields will be null and _corrupt_record will be populated.
# We drop corrupt records from the silver table by filtering them out here.
rapid7_clean = rapid7_raw.filter(col("_corrupt_record").isNull())

rapid7_norm = (
    rapid7_clean
    .withColumn("source_system", lit("rapid7"))
    .withColumn("ingest_ts", current_timestamp())
    .withColumn("rapid7_id", col("id").cast("string"))
    .withColumn("fortisiem_id", lit(None).cast("string"))

    .withColumn("asset_name", col("hostName"))
    .withColumn("primary_hostname", col("hostName"))

    .withColumn("primary_ip", col("ip"))
    .withColumn("access_ip", lit(None).cast("string"))

    .withColumn("natural_id", lit(None).cast("string"))
    .withColumn("approved", lit(None).cast("boolean"))
    .withColumn("unmanaged", lit(None).cast("boolean"))

    .withColumn("device_vendor", lit(None).cast("string"))
    .withColumn("device_model", lit(None).cast("string"))
    .withColumn("device_version", lit(None).cast("string"))

    .withColumn("os_name", col("os"))
    .withColumn("os_family", col("osFingerprint.family"))
    .withColumn("os_vendor", col("osFingerprint.vendor"))
    .withColumn("os_product", col("osFingerprint.product"))
    .withColumn("os_version", col("osFingerprint.cpe.version"))
    .withColumn("os_architecture", col("osFingerprint.architecture"))
    .withColumn("os_certainty", col("osCertainty").cast("double"))

    .withColumn("assessed_for_policies", col("assessedForPolicies"))
    .withColumn("assessed_for_vulnerabilities", col("assessedForVulnerabilities"))
    .withColumn("risk_score", col("riskScore").cast("double"))
    .withColumn("raw_risk_score", col("rawRiskScore").cast("double"))

    .withColumn("vuln_total", col("vulnerabilities.total").cast("int"))
    .withColumn("vuln_critical", col("vulnerabilities.critical").cast("int"))
    .withColumn("vuln_severe", col("vulnerabilities.severe").cast("int"))
    .withColumn("vuln_moderate", col("vulnerabilities.moderate").cast("int"))
    .withColumn("vuln_exploits", col("vulnerabilities.exploits").cast("int"))
    .withColumn("vuln_malware_kits", col("vulnerabilities.malwareKits").cast("int"))

    # Store original JSON (from the cleaned DF columns, excluding _corrupt_record)
    .withColumn(
        "raw_json",
        to_json(struct([col(c) for c in rapid7_clean.columns if c != "_corrupt_record"]))
    )

    .withColumn(
        "asset_uid",
        sha2(
            concat_ws(
                "|",
                lower(trim(col("primary_hostname"))),
                lower(trim(col("primary_ip"))),
                col("rapid7_id")
            ),
            256
        )
    )
    .select(
        "asset_uid", "source_system", "ingest_ts",
        "rapid7_id", "fortisiem_id",
        "asset_name", "primary_hostname",
        "primary_ip", "access_ip",
        "natural_id", "approved", "unmanaged",
        "device_vendor", "device_model", "device_version",
        "os_name", "os_family", "os_vendor", "os_product", "os_version", "os_architecture", "os_certainty",
        "assessed_for_policies", "assessed_for_vulnerabilities",
        "risk_score", "raw_risk_score",
        "vuln_total", "vuln_critical", "vuln_severe", "vuln_moderate", "vuln_exploits", "vuln_malware_kits",
        "raw_json"
    )
)


# ------------------------------------------------------------------------------
# FortiSIEM schema
# ------------------------------------------------------------------------------
forti_schema = StructType([
    StructField("_corrupt_record", StringType(), True),

    StructField("_id", StructType([
        StructField("$oid", StringType(), True)
    ]), True),

    StructField("accessIp", StringType(), True),
    StructField("name", StringType(), True),
    StructField("naturalId", StringType(), True),
    StructField("approved", BooleanType(), True),
    StructField("unmanaged", BooleanType(), True),

    StructField("deviceType", StructType([
        StructField("vendor", StringType(), True),
        StructField("model", StringType(), True),
        StructField("version", StringType(), True),
    ]), True),
])

forti_raw = (
    spark.readStream.format("json")
    .schema(forti_schema)
    .options(**JSON_OPTIONS)
    .load(FORTI_SOURCE)
)

forti_clean = forti_raw.filter(col("_corrupt_record").isNull())

forti_norm = (
    forti_clean
    .withColumn("source_system", lit("fortisiem"))
    .withColumn("ingest_ts", current_timestamp())
    .withColumn("rapid7_id", lit(None).cast("string"))
    .withColumn("fortisiem_id", col("_id.$oid").cast("string"))

    .withColumn("asset_name", col("name"))
    .withColumn("primary_hostname", col("name"))

    .withColumn("primary_ip", lit(None).cast("string"))
    .withColumn("access_ip", col("accessIp"))

    .withColumn("natural_id", col("naturalId"))
    .withColumn("approved", col("approved"))
    .withColumn("unmanaged", col("unmanaged"))

    .withColumn("device_vendor", col("deviceType.vendor"))
    .withColumn("device_model", col("deviceType.model"))
    .withColumn("device_version", col("deviceType.version"))

    .withColumn("os_name", lit(None).cast("string"))
    .withColumn("os_family", lit(None).cast("string"))
    .withColumn("os_vendor", lit(None).cast("string"))
    .withColumn("os_product", lit(None).cast("string"))
    .withColumn("os_version", lit(None).cast("string"))
    .withColumn("os_architecture", lit(None).cast("string"))
    .withColumn("os_certainty", lit(None).cast("double"))

    .withColumn("assessed_for_policies", lit(None).cast("boolean"))
    .withColumn("assessed_for_vulnerabilities", lit(None).cast("boolean"))
    .withColumn("risk_score", lit(None).cast("double"))
    .withColumn("raw_risk_score", lit(None).cast("double"))

    .withColumn("vuln_total", lit(None).cast("int"))
    .withColumn("vuln_critical", lit(None).cast("int"))
    .withColumn("vuln_severe", lit(None).cast("int"))
    .withColumn("vuln_moderate", lit(None).cast("int"))
    .withColumn("vuln_exploits", lit(None).cast("int"))
    .withColumn("vuln_malware_kits", lit(None).cast("int"))

    .withColumn(
        "raw_json",
        to_json(struct([col(c) for c in forti_clean.columns if c != "_corrupt_record"]))
    )

    .withColumn(
        "asset_uid",
        sha2(
            concat_ws(
                "|",
                lower(trim(col("primary_hostname"))),
                lower(trim(col("access_ip"))),
                col("fortisiem_id")
            ),
            256
        )
    )
    .select(
        "asset_uid", "source_system", "ingest_ts",
        "rapid7_id", "fortisiem_id",
        "asset_name", "primary_hostname",
        "primary_ip", "access_ip",
        "natural_id", "approved", "unmanaged",
        "device_vendor", "device_model", "device_version",
        "os_name", "os_family", "os_vendor", "os_product", "os_version", "os_architecture", "os_certainty",
        "assessed_for_policies", "assessed_for_vulnerabilities",
        "risk_score", "raw_risk_score",
        "vuln_total", "vuln_critical", "vuln_severe", "vuln_moderate", "vuln_exploits", "vuln_malware_kits",
        "raw_json"
    )
)


# ------------------------------------------------------------------------------
# Write function + streaming queries
# ------------------------------------------------------------------------------
def write_append(batch_df, batch_id: int):
    # Append micro-batch to Iceberg table
    batch_df.writeTo(TARGET_TABLE).append()


rapid7_q = (
    rapid7_norm.writeStream
    .outputMode("append")
    .option("checkpointLocation", RAPID7_CKPT)
    .trigger(processingTime=TRIGGER_INTERVAL)
    .foreachBatch(write_append)
    .start()
)

forti_q = (
    forti_norm.writeStream
    .outputMode("append")
    .option("checkpointLocation", FORTI_CKPT)
    .trigger(processingTime=TRIGGER_INTERVAL)
    .foreachBatch(write_append)
    .start()
)

# Keep the container running (returns/throws if any query terminates)
spark.streams.awaitAnyTermination()
