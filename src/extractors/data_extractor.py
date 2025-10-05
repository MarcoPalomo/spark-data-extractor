import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import input_file_name, current_timestamp

class DataExtractor:
    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.logger = logging.getLogger(__name__)
    
    def extract_from_source(self, source_config):
        """Extract data from configured source"""
        source_type = source_config.get('type')
        
        if source_type == 'csv':
            return self._extract_csv(source_config)
        elif source_type == 'json':
            return self._extract_json(source_config)
        elif source_type == 'parquet':
            return self._extract_parquet(source_config)
        elif source_type == 'jdbc':
            return self._extract_jdbc(source_config)
        elif source_type == 's3':
            return self._extract_s3(source_config)
        else:
            raise ValueError(f"Unsupported source type: {source_type}")
    
    def _extract_csv(self, config):
        """Extract CSV files"""
        path = config['path']
        options = config.get('options', {})
        
        df = self.spark.read \
            .option('header', options.get('header', True)) \
            .option('inferSchema', options.get('inferSchema', True)) \
            .option('delimiter', options.get('delimiter', ',')) \
            .csv(path)
        
        # Add source file metadata
        df = df.withColumn('source_file', input_file_name()) \
               .withColumn('extraction_time', current_timestamp())
        
        self.logger.info(f"Extracted {df.count()} rows from CSV: {path}")
        return df
    
    def _extract_json(self, config):
        """Extract JSON files"""
        path = config['path']
        options = config.get('options', {})
        
        df = self.spark.read \
            .option('multiLine', options.get('multiLine', False)) \
            .option('mode', options.get('mode', 'PERMISSIVE')) \
            .json(path)
        
        self.logger.info(f"Extracted {df.count()} rows from JSON: {path}")
        return df
    
    def _extract_parquet(self, config):
        """Extract Parquet files"""
        path = config['path']
        df = self.spark.read.parquet(path)
        
        self.logger.info(f"Extracted {df.count()} rows from Parquet: {path}")
        return df
    
    def _extract_jdbc(self, config):
        """Extract from database via JDBC"""
        jdbc_url = config['jdbc_url']
        table = config['table']
        options = config.get('options', {})
        
        properties = {
            'user': config['user'],
            'password': config['password'],
            'driver': config['driver']
        }
        
        # Add performance options
        if 'fetchsize' in options:
            properties['fetchsize'] = str(options['fetchsize'])
        
        # Read with partitioning if configured
        if 'partitionColumn' in options:
            df = self.spark.read.jdbc(
                url=jdbc_url,
                table=table,
                column=options['partitionColumn'],
                lowerBound=options['lowerBound'],
                upperBound=options['upperBound'],
                numPartitions=options['numPartitions'],
                properties=properties
            )
        else:
            df = self.spark.read.jdbc(
                url=jdbc_url,
                table=table,
                properties=properties
            )
        
        self.logger.info(f"Extracted {df.count()} rows from JDBC: {table}")
        return df
    
    def _extract_s3(self, config):
        """Extract from S3"""
        bucket = config['bucket']
        key = config['key']
        file_type = config.get('file_type', 'parquet')
        
        s3_path = f"s3a://{bucket}/{key}"
        
        if file_type == 'csv':
            df = self.spark.read.csv(s3_path, header=True, inferSchema=True)
        elif file_type == 'json':
            df = self.spark.read.json(s3_path)
        elif file_type == 'parquet':
            df = self.spark.read.parquet(s3_path)
        else:
            raise ValueError(f"Unsupported S3 file type: {file_type}")
        
        self.logger.info(f"Extracted {df.count()} rows from S3: {s3_path}")
        return df
    
    def extract_with_schema(self, source_config, schema):
        """Extract with predefined schema"""
        source_type = source_config['type']
        path = source_config['path']
        
        if source_type == 'csv':
            df = self.spark.read.schema(schema).csv(path, header=True)
        elif source_type == 'json':
            df = self.spark.read.schema(schema).json(path)
        else:
            raise ValueError(f"Schema enforcement not supported for: {source_type}")
        
        return df
    
    def extract_incremental(self, source_config, last_updated_column, last_value):
        """Extract only new/updated records"""
        if source_config['type'] != 'jdbc':
            raise ValueError("Incremental extraction only supported for JDBC")
        
        table = source_config['table']
        query = f"""
        (SELECT * FROM {table} 
         WHERE {last_updated_column} > '{last_value}'
        ) AS incremental_data
        """
        
        source_config['table'] = query
        return self.extract_from_source(source_config)