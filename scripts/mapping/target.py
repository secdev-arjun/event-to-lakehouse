from pyspark.sql import functions as F
from pyspark.sql.functions import col, lit, to_json, struct, concat_ws, sha2
from pyspark.sql.types import (
    StructField,
    StringType,
    BooleanType,
    DoubleType,
    IntegerType,
    ArrayType,
    LongType,
    TimestampType,
)

SCHEMA_VERSION = "silver.asset_observation.v2"

ORG_NAME_NORMALIZATION_RULES = [
    (r"(?i)^cced windows quarter$", "CCED"),
    (r"(?i)^ccedorg$", "CCED"),
    (r"(?i)^cc energy development oman$", "CCED"),
    (r"(?i)^abraj$", "Abraj Energy"),
    (r"(?i)^abraj[\s-]+.+$", "Abraj Energy"),
    (r"(?i)^securado$", "Securado"),
    (r"(?i)^securado hq$", "Securado"),
    (r"(?i)^securado - hq$", "Securado"),
    (r"(?i)^securado ho$", "Securado"),
    (r"(?i)^securado - ho$", "Securado"),
    (r"(?i)^securado headoffice$", "Securado"),
    (r"(?i)^securado - head office$", "Securado"),
    (r"(?i)^securado head office$", "Securado"),
    (r"(?i)^securado-in$", "Securado"),
]

MANUAL_SITE_ORG_OVERRIDES = [
    ("abraj energy - linux new", "Abraj", "Linux"),
    ("abraj energy - network new", "Abraj", "Network"),
    ("abraj energy - windows new", "Abraj", "Windows"),
    ("abraj firewall devices monthly", "Abraj", "Firewall"),
    ("abraj linux scan monthly", "Abraj", "Linux"),
    ("abraj switch and network devices monthly scan", "Abraj", "Network"),
    ("abraj windows devices monthly", "Abraj", "Windows"),
    ("abraj energy - endpoint", "Abraj", "Endpoint"),
    ("abraj", "Abraj", "Endpoint"),
    ("cced firewalls quarter", "CCED", "Firewall"),
    ("cced scan assistant", "CCED", "Scan assistant"),
    ("cced sw quarter", "CCED", "SW QUARTER"),
    ("cced windows quarter", "CCED", "Windows"),
    ("ccedorg", "CCED", "All"),
    ("cc energy development oman", "CCED", "All"),
    ("cced", "CCED", "All"),
    ("d2c-vascan", "D2C", "Vascan"),
    ("discovery", "Discovery", "Discovery"),
    ("hamdan exchange", "Hamdan Exchange", "Hamdan Exchange"),
    ("hr vapt", "HR", "VAPT"),
    ("hr web vapt", "HR", "Web VAPT"),
    ("ibnsina", "Ibn Sina Pharmacy", "All"),
    ("ibn sina pharmacy", "Ibn Sina Pharmacy", "All"),
    ("ibn sina pharmacy - control", "Ibn Sina Pharmacy", "Control"),
    ("ibnsina-oci-linux", "Ibn Sina Pharmacy", "Linux"),
    ("ibnsina-oci-windows", "Ibn Sina Pharmacy", "Windows"),
    ("icv new", "ICV", "New"),
    ("linuxscan assistant test 111", "LinuxScan Assistant Test 111", "LinuxScan Assistant Test 111"),
    ("mdc d2c scan assistant", "MDC", "Scan Assistant"),
    ("mdc firewall va quarter", "MDC", "Firewall"),
    ("mdc linux quarter", "MDC", "Linux"),
    ("mdc switchs quarter", "MDC", "Switch"),
    ("mdc test 2025", "MDC", "Testing"),
    ("mdc-windows quarter", "MDC", "Windows"),
    ("mdc", "MDC", "All"),
    ("mazoon dairy company saoc", "MDC", "All"),
    ("mog firewalls quarter", "MOG", "Firewall"),
    ("mog linux quarter", "MOG", "Linux"),
    ("mog switch quarter", "MOG", "Switch"),
    ("mog troubleshooting", "MOG", "Troubleshooting"),
    ("mog windows quarter", "MOG", "Windows"),
    ("mog-discovery_for_all_assets", "MOG", "Discovery"),
    ("mog", "MOG", "All"),
    ("musandam exchange", "Musandam Exchange", "All"),
    ("oib url scanning", "OIB", "url scanning"),
    ("phoenix power network", "PhoenixPower", "Network"),
    ("phoenix power server", "PhoenixPower", "Server"),
    ("phoenixpower test", "PhoenixPower", "Testing"),
    ("phoenixpower-blackbox", "PhoenixPower", "Blackbox"),
    ("phoenixpower", "PhoenixPower", "All"),
    ("ridge", "Ridge", "All"),
    ("securado", "Securado", "All"),
    ("securado - all active ip discovery", "Securado", "All"),
    ("securado-in", "Securado", "All"),
    ("securado discovry", "Securado", "Discovery"),
    ("securado-windows server", "Securado", "Windows"),
    ("sonar-test", "Sonar-Test", "Sonar-Test"),
    ("surtest", "surtest", "surtest"),
    ("test", "Test", "Test"),
    ("test 4", "test 4", "test 4"),
    ("test aisha", "test aisha", "test aisha"),
    ("test phone\\", "test phone\\", "test phone\\"),
    ("test, telnet", "TEST, Telnet", "TEST, Telnet"),
    ("test_131", "TEST_131", "TEST_131"),
    ("test1212132123", "test1212132123", "test1212132123"),
    ("testingr", "TestingR", "TestingR"),
    ("testwin-securado", "TestWin-Securado", "TestWin-Securado"),
    ("url scanning", "url scanning", "url scanning"),
    ("windows", "windows", "windows"),
    ("x-labs discovery", "Securado", "Discovery"),
    ("x-labs testingr", "Securado", "Testing"),
    ("x-labs-cis", "Securado", "CIS"),
    ("x-labs", "Securado", "All"),
    ("ajx", "AJX", "All"),
    ("edoman", "EDOMAN", "All"),
    ("globalmoneyexchange", "GlobalMoneyExchange", "All"),
    ("gulfoverseasexchange", "GulfOverseasExchange", "All"),
    ("lakhoos", "Lakhoos", "All"),
    ("lakhoos_exchange", "Lakhoos", "All"),
    ("mipp", "MIPP", "All"),
    ("mpc", "MPC", "All"),
    ("national_university", "National_University", "All"),
    ("smnpower", "SMNPOWER", "All"),
    ("ufc", "UFC", "All"),
    ("unimoni", "UNIMONI", "All"),
    ("ajit khimji group", "Ajit Khimji Group", "All"),
    ("al siraj holdings", "Al Siraj Holdings", "All"),
    ("asyad corporate", "Asyad", "Corporate"),
    ("corporate/odp - corporate", "Asyad", "Corporate"),
    ("corporate/odp - corprate", "Asyad", "Corporate"),
    ("corporate/odp - cloud", "Asyad", "Corporate"),
    ("corporate/odp - odp", "Asyad", "Corporate"),
    ("asyad drydock", "Asyad", "Drydock"),
    ("drydock", "Asyad", "Drydock"),
    ("drydock - drydock", "Asyad", "Drydock"),
    ("drydock - odp", "Asyad", "Drydock"),
    ("asyad ports", "Asyad", "Ports"),
    ("port - port", "Asyad", "Ports"),
    ("port - odp", "Asyad", "Ports"),
    ("asyad shipping", "Asyad", "Shipping"),
    ("mwasalat", "Asyad", "Mwasalat"),
    ("mwasalat - mwasalat az", "Asyad", "Mwasalat"),
    ("mwasalat - mwasalat gh", "Asyad", "Mwasalat"),
    ("oman post & asyad express", "Asyad", "Oman Post & Express"),
    ("omanpost", "Asyad", "Oman Post & Express"),
    ("omanpost - omanpost", "Asyad", "Oman Post & Express"),
    ("omanship - corporate", "Asyad", "Oman Post & Express"),
    ("omanship - omanship", "Asyad", "Oman Post & Express"),
    ("salalah free zone", "Asyad", "Salalah Free Zone"),
    ("salalah freezone", "Asyad", "Salalah Free Zone"),
    ("salalah freezone - odp", "Asyad", "Salalah Free Zone"),
    ("salalah freezone - sfz", "Asyad", "Salalah Free Zone"),
    ("asyad", "Asyad", "All"),
    ("atd", "ATD", "All"),
    ("sfs", "SFS", "All"),
    ("shifahospital", "ShifaHospital", "All"),
    ("sof - airport heights", "SOF", "Airport Heights"),
    ("united engineering projects company", "United Engineering Projects Company", "All"),
    ("bank_dhofar", "BankDhofar", "All"),
]

FILESHARE_ORG_LOCATION_OVERRIDES = [
    ("Abraj", "Adam Base", "Adam Base", "Abraj"),
    ("Abraj", "Cementing", "Cementing", "Abraj"),
    ("Abraj", "DR", "DR", "Abraj"),
    ("Abraj", "Head Office - Date Center", "Head Office - Date Center", "Abraj"),
    ("Abraj", "IT 3rd Floor IDF", "IT 3rd Floor IDF", "Abraj"),
    ("Abraj", "IT 6th Floor IDF", "IT 6th Floor IDF", "Abraj"),
    ("Abraj", "IT Data Center", "IT Data Center", "Abraj"),
    ("Abraj", "Training Center", "Training Center", "Abraj"),
    ("Abraj", "TrainingCenter", "Training Center", "Abraj"),
    ("Abraj", "Warehouse", "Warehouse", "Abraj"),
    ("Abraj", "Warehouse, Control Room", "Warehouse", "Abraj"),
    ("Asyad", "Corporate/ODP - Cloud", "Coporate", "Asyad"),
    ("Asyad", "Corporate/ODP - Corporate", "Coporate", "Asyad"),
    ("Asyad", "Corporate/ODP - Corprate", "Coporate", "Asyad"),
    ("Asyad", "Corporate/ODP - ODP", "Coporate", "Asyad"),
    ("Asyad", "DryDock", "Drydock", "Asyad"),
    ("Asyad", "DryDock - DryDock", "Drydock", "Asyad"),
    ("Asyad", "DryDock - ODP", "Drydock", "Asyad"),
    ("Asyad", "Mwasalat", "Mwasalat", "Asyad"),
    ("Asyad", "Mwasalat - Mwasalat AZ", "Mwasalat", "Asyad"),
    ("Asyad", "Mwasalat - Mwasalat GH", "Mwasalat", "Asyad"),
    ("Asyad", "OmanPost", "Oman Post", "Asyad"),
    ("Asyad", "OmanPost - Omanpost", "Oman Post", "Asyad"),
    ("Asyad", "Omanship - Corporate", "Shipping", "Asyad"),
    ("Asyad", "Omanship - Omanship", "Shipping", "Asyad"),
    ("Asyad", "Port - ODP", "Ports", "Asyad"),
    ("Asyad", "Port - Port", "Ports", "Asyad"),
    ("Asyad", "Salalah FreeZone", "Salalah Free Zone", "Asyad"),
    ("Asyad", "Salalah FreeZone - ODP", "Salalah Free Zone", "Asyad"),
    ("Asyad", "Salalah FreeZone - SFZ", "Salalah Free Zone", "Asyad"),
    ("Bank_Dhofar", "Assets-1", "All", "Bank Dhofar"),
    ("Bank_Dhofar", "DB_IP_Details", "Database", "Bank Dhofar"),
    ("Bank_Dhofar", "Primary DMZ_Live_IP", "Primary DMZ", "Bank Dhofar"),
    ("Bank_Dhofar", "Primary Data Center_Live_IP", "Primary Data Center", "Bank Dhofar"),
    ("Bank_Dhofar", "Primary Network Devices_Live_IPs", "Primary Network Devices", "Bank Dhofar"),
    ("Bank_Dhofar", "Primary Routers_Live_Ip", "Primary Routers", "Bank Dhofar"),
    ("Bank_Dhofar", "Public IPs - updated v1.1", "Public IPs", "Bank Dhofar"),
    ("Bank_Dhofar", "Secondary Data Center_Live_IP", "Secondary Data Center", "Bank Dhofar"),
    ("Bank_Dhofar", "Swift_Live_IP", "Swift", "Bank Dhofar"),
    ("CCED", "NULL", "All", "CCED"),
    ("CCED", "FARHA", "FARHA", "CCED"),
    ("CCED", "HQ", "HQ", "CCED"),
    ("CCED", "Muscat", "Muscat", "CCED"),
    ("CCED", "QARN ALAM", "QARN ALAM", "CCED"),
    ("CCED", "SAIWAN", "SAIWAN", "CCED"),
    ("CCED", "SHAHD", "SHAHD", "CCED"),
    ("CCED", "ULFA", "ULFA", "CCED"),
    ("Lakhoos_Exchange", "NULL", "All", "Lakhoos Exchange"),
    ("MDC", "NULL", "All", "MDC"),
    ("MOG", "Muscat", "All", "MOG"),
    ("MPC", "NULL", "All", "MPC"),
    ("PhoenixPower", "NULL", "All", "Phoenix Power"),
    ("PhoenixPower", "Sur", "Sur", "Phoenix Power"),
    ("PhoenixPower", "Sur ODP", "Oman Data Park", "Phoenix Power"),
    ("Securado", "NULL", "All", "Securado"),
    ("SMNPower", "NULL", "All", "SMN Power"),
    ("Sohar_Port_Freezone", "NULL", "All", "Sohar Port Freezone"),
    ("Unimoni", "ODP (Hosted)", "Oman Data Park", "Unimoni"),
    ("Unimoni", "Oman Data Park (DC)", "Oman Data Park (DC)", "Unimoni"),
]


def _normalize_manual_mapping_key(value):
    if value is None:
        return "__null__"
    normalized = " ".join(str(value).strip().split()).lower()
    if normalized in {"", "null", "none"}:
        return "__null__"
    return normalized


def _normalized_mapping_key_expr(col_expr):
    cleaned = F.lower(
        F.trim(
            F.regexp_replace(
                F.coalesce(clean_string(col_expr), lit("")),
                r"\s+",
                " ",
            )
        )
    )
    return F.when(cleaned.isin("", "null", "none"), lit("__null__")).otherwise(cleaned)


def _as_timestamp(col_name: str):
    return F.to_timestamp(col(col_name))



def clean_string(col_expr):
    casted = col_expr.cast("string")
    trimmed = F.trim(casted)
    return F.when(casted.isNull() | (trimmed == ""), lit(None).cast("string")).otherwise(trimmed)



def clean_string_array(arr_col):
    as_array = arr_col.cast("array<string>")
    trimmed = F.transform(F.filter(as_array, lambda x: x.isNotNull()), lambda x: F.trim(x))
    filtered = F.filter(trimmed, lambda x: x != "")
    cleaned = F.array_sort(F.array_distinct(filtered))
    return F.when(as_array.isNull(), lit(None).cast("array<string>")).otherwise(cleaned)



def _array_sort_nullable(arr_col):
    return F.when(arr_col.isNull(), arr_col).otherwise(F.array_sort(arr_col))



def first_array_value(arr_col):
    return F.when(arr_col.isNotNull() & (F.size(arr_col) > 0), arr_col.getItem(0)).otherwise(
        lit(None).cast("string")
    )



def normalize_mac_string(col_expr):
    cleaned = clean_string(col_expr)
    lowered = F.lower(cleaned)
    dashed = F.regexp_replace(lowered, "-", ":")
    compact = F.regexp_replace(dashed, r"[^0-9a-f:]", "")
    return clean_string(compact)



def normalize_mac_array(arr_col):
    as_array = arr_col.cast("array<string>")
    macs = F.transform(F.filter(as_array, lambda x: x.isNotNull()), lambda x: normalize_mac_string(x))
    normalized = clean_string_array(macs)
    return F.when(as_array.isNull(), lit(None).cast("array<string>")).otherwise(normalized)



def filter_matching_ip_addresses(arr_col):
    cleaned = clean_string_array(arr_col)
    empty = F.array().cast("array<string>")
    filtered = F.filter(
        F.coalesce(cleaned, empty),
        lambda x: (~x.rlike(r"^127\\.")) & (~x.rlike(r"^169\\.254\\.")),
    )
    return F.when(cleaned.isNull(), lit(None).cast("array<string>")).otherwise(filtered)



def valid_hardware_serial(col_expr):
    serial = clean_string(col_expr)
    lowered = F.lower(F.coalesce(serial, lit("")))
    placeholder = (
        lowered.rlike(r"^vmware-")
        | lowered.isin(
            "to be filled by o.e.m.",
            "default string",
            "n/a",
            "na",
            "unknown",
            "null",
        )
    )
    return F.when(serial.isNull() | placeholder, lit(None).cast("string")).otherwise(serial)



def normalize_org_name(col_expr):
    expr = clean_string(col_expr)
    for pattern, replacement in ORG_NAME_NORMALIZATION_RULES:
        expr = F.regexp_replace(expr, pattern, replacement)
    return clean_string(expr)


def split_site_name_and_org(site_col_expr):
    """
    Manual site/org split rules for known naming conventions.
    Returns a tuple of Columns: (site_name, normalised_org_name).

    Fallback:
    - site_name keeps the cleaned raw site value
    - normalised_org_name uses normalize_org_name(raw_site_value)
    """
    raw_site = clean_string(site_col_expr)
    site_name = raw_site
    org_name = normalize_org_name(raw_site)

    key = F.lower(F.trim(F.regexp_replace(F.coalesce(raw_site, lit("")), r"\s+", " ")))

    manual_org_entries = []
    manual_site_entries = []
    for raw_key, mapped_org, mapped_site in MANUAL_SITE_ORG_OVERRIDES:
        manual_org_entries.extend([lit(raw_key), lit(mapped_org).cast("string")])
        manual_site_entries.extend([lit(raw_key), lit(mapped_site).cast("string")])

    manual_org = F.element_at(F.create_map(*manual_org_entries), key)
    manual_site = F.element_at(F.create_map(*manual_site_entries), key)
    has_manual_mapping = manual_org.isNotNull()

    site_name = F.when(has_manual_mapping, manual_site).otherwise(site_name)
    org_name = F.when(has_manual_mapping, manual_org).otherwise(org_name)

    # MOG-* / MOG * => org=MOG, site=<suffix>
    mog_match = (~has_manual_mapping) & raw_site.rlike(r"(?i)^mog[\s-]+.+$")
    mog_site = clean_string(F.regexp_extract(raw_site, r"(?i)^mog[\s-]+(.+)$", 1))
    site_name = F.when(mog_match, mog_site).otherwise(site_name)
    org_name = F.when(mog_match, lit("MOG")).otherwise(org_name)

    # Securado exact => org=Securado, site=NULL
    securado_exact = (~has_manual_mapping) & raw_site.rlike(r"(?i)^securado$")
    site_name = F.when(securado_exact, lit(None).cast("string")).otherwise(site_name)
    org_name = F.when(securado_exact, lit("Securado")).otherwise(org_name)

    # Securado-* / Securado * => org=Securado, site=<suffix>
    securado_match = (~has_manual_mapping) & raw_site.rlike(r"(?i)^securado[\s-]+.+$")
    securado_site = clean_string(F.regexp_extract(raw_site, r"(?i)^securado[\s-]+(.+)$", 1))
    site_name = F.when(securado_match, securado_site).otherwise(site_name)
    org_name = F.when(securado_match, lit("Securado")).otherwise(org_name)

    # X-Labs-* / X-Labs * => org=Securado, site=<suffix>
    xlabs_match = (~has_manual_mapping) & raw_site.rlike(r"(?i)^x-labs[\s-]+.+$")
    xlabs_site = clean_string(F.regexp_extract(raw_site, r"(?i)^x-labs[\s-]+(.+)$", 1))
    site_name = F.when(xlabs_match, xlabs_site).otherwise(site_name)
    org_name = F.when(xlabs_match, lit("Securado")).otherwise(org_name)

    # Abraj-* / Abraj * => org=Abraj Energy, site=<suffix>
    abraj_match = (~has_manual_mapping) & raw_site.rlike(r"(?i)^abraj[\s-]+.+$")
    abraj_site = clean_string(F.regexp_extract(raw_site, r"(?i)^abraj[\s-]+(.+)$", 1))
    site_name = F.when(abraj_match, abraj_site).otherwise(site_name)
    org_name = F.when(abraj_match, lit("Abraj Energy")).otherwise(org_name)

    return site_name, org_name



def split_fileshare_site_and_org(location_col_expr, organization_col_expr):
    """
    Fileshare-specific site/org split keyed by (organization_name, location).
    Returns a tuple of Columns: (site_name, normalised_org_name).

    Fallback:
    - site_name keeps cleaned location value
    - normalised_org_name keeps cleaned organization_name (or derives from location if missing)
    """
    raw_location = clean_string(location_col_expr)
    raw_org = clean_string(organization_col_expr)
    site_name = raw_location
    org_name = F.coalesce(raw_org, normalize_org_name(raw_location))

    pair_key = F.concat_ws(
        "||",
        _normalized_mapping_key_expr(raw_org),
        _normalized_mapping_key_expr(raw_location),
    )

    manual_org_entries = []
    manual_site_entries = []
    for raw_org_key, raw_location_key, mapped_site, mapped_org in FILESHARE_ORG_LOCATION_OVERRIDES:
        normalized_key = (
            f"{_normalize_manual_mapping_key(raw_org_key)}"
            f"||{_normalize_manual_mapping_key(raw_location_key)}"
        )
        manual_org_entries.extend([lit(normalized_key), lit(mapped_org).cast("string")])
        manual_site_entries.extend([lit(normalized_key), lit(mapped_site).cast("string")])

    manual_org = F.element_at(F.create_map(*manual_org_entries), pair_key)
    manual_site = F.element_at(F.create_map(*manual_site_entries), pair_key)
    has_manual_mapping = manual_org.isNotNull()

    site_name = F.when(has_manual_mapping, manual_site).otherwise(site_name)
    org_name = F.when(has_manual_mapping, manual_org).otherwise(org_name)
    return site_name, org_name


def org_map_matched(raw_col_expr, normalized_col_expr):
    raw_norm = F.lower(clean_string(raw_col_expr))
    mapped_norm = F.lower(clean_string(normalized_col_expr))
    return (
        F.when(raw_norm.isNull() | mapped_norm.isNull(), lit(False))
        .otherwise(raw_norm != mapped_norm)
        .cast("boolean")
    )



def add_common_fields(df, topic_name: str, vendor_id_col, source_updated_at_col):
    return (
        df.withColumn("schema_version", lit(SCHEMA_VERSION))
        .withColumn("topic_name", lit(topic_name))
        .withColumn("vendor_id", vendor_id_col.cast("string"))
        .withColumn("entity_key_str", concat_ws("|", col("topic_name"), col("vendor_id")))
        .withColumn("source_updated_at", source_updated_at_col)
        .withColumn("first_seen_at", lit(None).cast("timestamp"))
        .withColumn("last_seen_at", lit(None).cast("timestamp"))
    )



def ensure_columns(df, fields):
    for field in fields:
        if field.name not in df.columns:
            df = df.withColumn(field.name, lit(None).cast(field.dataType))
    return df.select([col(f.name) for f in fields])



def col_if_exists(df, name: str):
    return col(name) if name in df.columns else lit(None)



def nested_col_if_exists(df, path: str):
    root = path.split(".")[0]
    return col(path) if root in df.columns else lit(None)



def drop_corrupt_if_present(df):
    if "_corrupt_record" in df.columns:
        return df.filter(col("_corrupt_record").isNull())
    return df


TARGET_FIELDS = [
    StructField("schema_version", StringType(), True),
    StructField("source", StringType(), True),
    StructField("entity_id", StringType(), True),
    StructField("entity_key_str", StringType(), True),
    StructField("payload_hash", StringType(), True),
    StructField("topic_name", StringType(), True),
    StructField("vendor_id", StringType(), True),
    StructField("ingest_ts", TimestampType(), True),
    StructField("first_seen_at", TimestampType(), True),
    StructField("last_seen_at", TimestampType(), True),
    StructField("source_updated_at", TimestampType(), True),
    StructField("event_time", TimestampType(), True),
    StructField("asset_uid", StringType(), True),
    StructField("source_system", StringType(), True),
    StructField("source_record_id", StringType(), True),
    StructField("source_natural_id", StringType(), True),
    StructField("source_site_ref_id", StringType(), True),
    StructField("source_display_name", StringType(), True),
    StructField("asset_name", StringType(), True),
    StructField("primary_hostname", StringType(), True),
    StructField("hostnames", ArrayType(StringType()), True),
    StructField("host_domain", StringType(), True),
    StructField("primary_ip", StringType(), True),
    StructField("ip_addresses", ArrayType(StringType()), True),
    StructField("ip_addresses_raw", ArrayType(StringType()), True),
    StructField("ipv6_addresses", ArrayType(StringType()), True),
    StructField("primary_mac", StringType(), True),
    StructField("mac_addresses", ArrayType(StringType()), True),
    StructField("gateway_mac_addresses", ArrayType(StringType()), True),
    StructField("virtual_mac_addresses", ArrayType(StringType()), True),
    StructField("serial_number", StringType(), True),
    StructField("access_ip", StringType(), True),
    StructField("external_ip", StringType(), True),
    StructField("approved", BooleanType(), True),
    StructField("unmanaged", BooleanType(), True),
    StructField("tags", ArrayType(StringType()), True),
    StructField("site_id", StringType(), True),
    StructField("site_name", StringType(), True),
    StructField("normalised_org_name", StringType(), True),
    StructField("account_id", StringType(), True),
    StructField("account_name", StringType(), True),
    StructField("org_map_matched", BooleanType(), True),
    StructField("site_description", StringType(), True),
    StructField("site_type", StringType(), True),
    StructField("site_importance", StringType(), True),
    StructField("site_assets_count", LongType(), True),
    StructField("site_risk_score", DoubleType(), True),
    StructField("site_last_scan_time", TimestampType(), True),
    StructField("site_scan_engine", LongType(), True),
    StructField("site_scan_template", StringType(), True),
    StructField("site_vuln_total", LongType(), True),
    StructField("site_vuln_critical", LongType(), True),
    StructField("site_vuln_severe", LongType(), True),
    StructField("site_vuln_moderate", LongType(), True),
    StructField("platform_version", StringType(), True),
    StructField("asset_type", StringType(), True),
    StructField("os_name", StringType(), True),
    StructField("os_family", StringType(), True),
    StructField("os_vendor", StringType(), True),
    StructField("os_product", StringType(), True),
    StructField("os_version", StringType(), True),
    StructField("os_architecture", StringType(), True),
    StructField("os_edition", StringType(), True),
    StructField("os_certainty", DoubleType(), True),
    StructField("cpu_count", IntegerType(), True),
    StructField("memory_bytes", LongType(), True),
    StructField("system_uptime", LongType(), True),
    StructField("assessed_for_policies", BooleanType(), True),
    StructField("assessed_for_vulnerabilities", BooleanType(), True),
    StructField("risk_score", DoubleType(), True),
    StructField("raw_risk_score", DoubleType(), True),
    StructField("vuln_total", IntegerType(), True),
    StructField("vuln_critical", IntegerType(), True),
    StructField("vuln_severe", IntegerType(), True),
    StructField("vuln_moderate", IntegerType(), True),
    StructField("vuln_exploits", IntegerType(), True),
    StructField("vuln_malware_kits", IntegerType(), True),
    StructField("posture_is_active", BooleanType(), True),
    StructField("posture_firewall_enabled", BooleanType(), True),
    StructField("posture_network_quarantine_enabled", BooleanType(), True),
    StructField("posture_active_threats", IntegerType(), True),
    StructField("scan_status", StringType(), True),
    StructField("operational_state", StringType(), True),
    StructField("operational_state_expiration", StringType(), True),
    StructField("network_status", StringType(), True),
    StructField("ranger_status", StringType(), True),
    StructField("mitigation_mode", StringType(), True),
    StructField("mitigation_mode_suspicious", StringType(), True),
    StructField("machine_type", StringType(), True),
    StructField("group_id", StringType(), True),
    StructField("group_name", StringType(), True),
    StructField("last_logged_in_user_name", StringType(), True),
    StructField("active_protection_modes", ArrayType(StringType()), True),
    StructField("missing_permissions", ArrayType(StringType()), True),
    StructField("user_actions_needed", ArrayType(StringType()), True),
    StructField("location_names", ArrayType(StringType()), True),
    StructField("discover_method", StringType(), True),
    StructField("event_log_status", StringType(), True),
    StructField("perf_mon_status", StringType(), True),
    StructField("update_method", StringType(), True),
    StructField("services_count", IntegerType(), True),
    StructField("software_count", IntegerType(), True),
    # ── SentinelOne: Posture & Threat (new) ──────────────────
    StructField("infected", BooleanType(), True),
    StructField("threat_reboot_required", BooleanType(), True),
    StructField("detection_state", StringType(), True),
    StructField("apps_vulnerability_status", StringType(), True),
    StructField("encrypted_applications", BooleanType(), True),
    # ── SentinelOne: Agent Config & Status (new) ─────────────
    StructField("allow_remote_shell", BooleanType(), True),
    StructField("in_remote_shell_session", BooleanType(), True),
    StructField("remote_profiling_state", StringType(), True),
    StructField("console_migration_status", StringType(), True),
    StructField("is_hyper_automate", StringType(), True),
    StructField("is_decommissioned", BooleanType(), True),
    StructField("is_uninstalled", BooleanType(), True),
    StructField("is_pending_uninstall", BooleanType(), True),
    StructField("is_up_to_date", BooleanType(), True),
    StructField("is_ad_connector", BooleanType(), True),
    StructField("has_containerized_workload", BooleanType(), True),
    StructField("containerized_workload_counts", StringType(), True),
    StructField("license_key", StringType(), True),
    StructField("installer_type", StringType(), True),
    StructField("ranger_version", StringType(), True),
    StructField("show_alert_icon", BooleanType(), True),
    StructField("proxy_console", BooleanType(), True),
    StructField("proxy_deep_visibility", StringType(), True),
    # ── SentinelOne: Hardware (new) ──────────────────────────
    StructField("model_name", StringType(), True),
    StructField("cpu_id", StringType(), True),
    StructField("core_count", IntegerType(), True),
    StructField("storage_name", StringType(), True),
    StructField("storage_type", StringType(), True),
    StructField("machine_sid", StringType(), True),
    # ── SentinelOne: User / AD (new) ─────────────────────────
    StructField("os_username", StringType(), True),
    StructField("ad_computer_dn", StringType(), True),
    StructField("ad_user_principal_name", StringType(), True),
    StructField("ad_computer_member_of", ArrayType(StringType()), True),
    StructField("ad_last_user_dn", StringType(), True),
    StructField("ad_last_user_member_of", ArrayType(StringType()), True),
    # ── SentinelOne: Timestamps (new) ────────────────────────
    StructField("scan_started_at", TimestampType(), True),
    StructField("scan_finished_at", TimestampType(), True),
    StructField("scan_aborted_at", TimestampType(), True),
    StructField("full_disk_scan_last_updated_at", TimestampType(), True),
    StructField("first_full_mode_time", TimestampType(), True),
    StructField("last_successful_scan_date", TimestampType(), True),
    StructField("created_at", TimestampType(), True),
    StructField("registered_at", TimestampType(), True),
    StructField("last_active_date", TimestampType(), True),
    # ── SentinelOne: Location (new) ──────────────────────────
    StructField("location_type", StringType(), True),
    StructField("location_enabled", BooleanType(), True),
    # ── Rapid7: OS detail (new) ──────────────────────────────
    StructField("os_system_name", StringType(), True),
    StructField("os_description", StringType(), True),
    StructField("os_cpe_v2_3", StringType(), True),
    # ── FileShare: Ownership & Asset Detail (new) ────────────
    StructField("asset_owner_name", StringType(), True),
    StructField("asset_owner_contact", StringType(), True),
    StructField("asset_owner_email", StringType(), True),
    StructField("priority_business_impact", StringType(), True),
    StructField("role_function", StringType(), True),
    StructField("notes", StringType(), True),
    StructField("zone", StringType(), True),
    StructField("applications", StringType(), True),
    StructField("database", StringType(), True),
    StructField("services_enabled", StringType(), True),
    # ── FortiSIEM: Device Detail (new) ───────────────────────
    StructField("bios", StringType(), True),
    StructField("build_number", StringType(), True),
    StructField("hw_vendor", StringType(), True),
    StructField("hw_serial_num", StringType(), True),
    StructField("os_serial_num", StringType(), True),
    StructField("device_description", StringType(), True),
    StructField("creation_method", StringType(), True),
    StructField("shallow_discovery", BooleanType(), True),
    StructField("param", StringType(), True),
    StructField("discover_time", TimestampType(), True),
    StructField("primary_contact_user", LongType(), True),
    StructField("secondary_contact_user", LongType(), True),
    # ── Raw data ─────────────────────────────────────────────
    StructField("raw_payload", StringType(), True),
    StructField("raw_json", StringType(), True),
]


PAYLOAD_HASH_EXCLUDE = {
    "schema_version",
    "source",
    "entity_id",
    "entity_key_str",
    "payload_hash",
    "topic_name",
    "vendor_id",
    "ingest_ts",
    "first_seen_at",
    "last_seen_at",
    "source_updated_at",
    "event_time",
    "raw_payload",
    "raw_json",
}
PAYLOAD_HASH_COLUMNS = [f.name for f in TARGET_FIELDS if f.name not in PAYLOAD_HASH_EXCLUDE]


def add_payload_hash(df):
    ordered_cols = [col(c) for c in PAYLOAD_HASH_COLUMNS]
    return df.withColumn("payload_hash", sha2(to_json(struct(*ordered_cols)), 256))
