from pyspark.sql.types import StructType, StructField, TimestampType, IntegerType, DecimalType, DateType, BooleanType

silver_schema = StructType(
    [
        StructField('timestamp', TimestampType()),
        StructField('turbine_id', IntegerType()),
        StructField('wind_speed', DecimalType(4, 2)),
        StructField('wind_direction', IntegerType()),
        StructField('power_output', DecimalType(4, 2)),
        StructField('load_timestamp', TimestampType()),
        StructField('modified_timestamp', TimestampType())
        ]
    )

gold_schema = StructType(
    [
        StructField('date', DateType()),
        StructField('turbine_id', IntegerType()),
        StructField('min_power_output', DecimalType(4, 2)),
        StructField('max_power_output', DecimalType(4, 2)),
        StructField('avg_power_output', DecimalType(4, 2)),
        StructField('anomaly', BooleanType())
        ]
    )
