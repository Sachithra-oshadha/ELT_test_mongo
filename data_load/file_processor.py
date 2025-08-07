import pandas as pd
import os
from pymongo.errors import OperationFailure
from datetime import datetime

class FileProcessor:
    def __init__(self, db, s3_client, temp_dir, logger):
        self.db = db
        self.s3_client = s3_client
        self.temp_dir = temp_dir
        self.logger = logger

    def read_data(self, file_path):
        try:
            ext = os.path.splitext(file_path)[1].lower()
            if ext in ['.xlsx', '.xls']:
                df = pd.read_excel(file_path, engine='openpyxl')
            elif ext == '.csv':
                df = pd.read_csv(file_path)
            else:
                raise ValueError(f"Unsupported file format: {ext}")
            self.logger.info(f"Successfully read file: {file_path}")
            return df
        except Exception as e:
            self.logger.error(f"Failed to read file: {e}")
            raise

    def is_file_processed(self, s3_key):
        try:
            result = self.db.find_one('processed_files', {'fileName': os.path.basename(s3_key)})
            return result is not None
        except Exception as e:
            self.logger.error(f"Failed to check processed files: {e}")
            raise

    def mark_file_processed(self, s3_key):
        try:
            file_name = os.path.basename(s3_key)
            document = {
                'fileName': file_name,
                's3Path': s3_key,
                'processedAt': datetime.now()
            }
            self.db.insert_one('processed_files', document)
            self.logger.info(f"Marked file as processed: {s3_key}")
        except Exception as e:
            self.logger.error(f"Failed to mark file as processed: {e}")
            raise

    def insert_customers(self, df):
        customers = df[['CUSTOMER_REF']].drop_duplicates().dropna()
        customer_data = [
            {
                '_id': int(row['CUSTOMER_REF']),
                'customerRef': int(row['CUSTOMER_REF']),
                'firstName': None,  # Not provided in data
                'lastName': None,   # Not provided in data
                'email': None,  # Generate unique email
                'createdAt': datetime.now(),
                'updatedAt': datetime.now(),
                'model': {},
                'predictions': []
            } for _, row in customers.iterrows()
        ]
        try:
            existing_refs = set(
                doc['customerRef'] for doc in self.db.db['customers'].find(
                    {'customerRef': {'$in': [doc['customerRef'] for doc in customer_data]}},
                    {'customerRef': 1}
                )
            )
            new_customers = [doc for doc in customer_data if doc['customerRef'] not in existing_refs]
            if new_customers:
                self.db.insert_many('customers', new_customers)
            self.logger.info(f"Inserted {len(new_customers)} customers")
        except Exception as e:
            self.logger.error(f"Failed to insert customers: {e}")
            raise

    def insert_meters(self, df):
        meters = df[['SERIAL', 'CUSTOMER_REF']].drop_duplicates().dropna()
        meter_data = [
            {
                '_id': int(row['SERIAL']),
                'customerRef': int(row['CUSTOMER_REF']),
                'createdAt': datetime.now(),
                'updatedAt': datetime.now()
            } for _, row in meters.iterrows()
        ]
        try:
            existing_serials = set(
                doc['_id'] for doc in self.db.db['meters'].find(
                    {'_id': {'$in': [doc['_id'] for doc in meter_data]}},
                    {'_id': 1}
                )
            )
            new_meters = [doc for doc in meter_data if doc['_id'] not in existing_serials]
            if new_meters:
                self.db.insert_many('meters', new_meters)
            self.logger.info(f"Inserted {len(new_meters)} meters")
        except Exception as e:
            self.logger.error(f"Failed to insert meters: {e}")
            raise

    def insert_measurements(self, df):
        measurement_cols = [
            'SERIAL', 'DATE', 'TIME', 'OBIS', 'AVG._IMPORT_KW (kW)', 'IMPORT_KWH (kWh)',
            'AVG._EXPORT_KW (kW)', 'EXPORT_KWH (kWh)', 'AVG._IMPORT_KVA (kVA)',
            'AVG._EXPORT_KVA (kVA)', 'IMPORT_KVARH (kvarh)', 'EXPORT_KVARH (kvarh)',
            'POWER_FACTOR', 'AVG._CURRENT (V)', 'AVG._VOLTAGE (V)',
            'PHASE_A_INST._CURRENT (A)', 'PHASE_A_INST._VOLTAGE (V)', 'INST._POWER_FACTOR',
            'PHASE_B_INST._CURRENT (A)', 'PHASE_B_INST._VOLTAGE (V)',
            'PHASE_C_INST._CURRENT (A)', 'PHASE_C_INST._VOLTAGE (V)'
        ]

        df_measurements = df[measurement_cols].copy()
        df_measurements.columns = [
            'serial', 'date', 'time', 'obis', 'avg_import_kw', 'import_kwh',
            'avg_export_kw', 'export_kwh', 'avg_import_kva', 'avg_export_kva',
            'import_kvarh', 'export_kvarh', 'power_factor', 'avg_current', 'avg_voltage',
            'phase_a_inst_current', 'phase_a_inst_voltage', 'inst_power_factor',
            'phase_b_inst_current', 'phase_b_inst_voltage',
            'phase_c_inst_current', 'phase_c_inst_voltage'
        ]

        # Combine DATE and TIME
        df_measurements['timestamp'] = pd.to_datetime(
            df_measurements['date'] + ' ' + df_measurements['time'],
            format='%Y-%m-%d %H:%M:%S',
            errors='coerce'
        )

        # Drop invalid timestamps
        invalid_rows = df_measurements[df_measurements['timestamp'].isna()]
        if not invalid_rows.empty:
            self.logger.warning(f"Dropped {len(invalid_rows)} rows due to invalid DATE/TIME format")
            df_measurements = df_measurements.dropna(subset=['timestamp'])

        # Ensure numeric types
        df_measurements['serial'] = df_measurements['serial'].astype(int)
        df_measurements['obis'] = df_measurements['obis'].astype(str)
        df_measurements['power_factor'] = df_measurements['power_factor'].clip(lower=-1, upper=1)

        # Convert to list of documents
        measurement_data = []
        for _, row in df_measurements.iterrows():
            doc = {
                'timestamp': row['timestamp'],
                'metadata': {
                    'serial': row['serial'],
                    'obis': row['obis']
                },
                'avg_import_kw': row['avg_import_kw'] if pd.notnull(row['avg_import_kw']) else None,
                'import_kwh': row['import_kwh'] if pd.notnull(row['import_kwh']) else None,
                'avg_export_kw': row['avg_export_kw'] if pd.notnull(row['avg_export_kw']) else None,
                'export_kwh': row['export_kwh'] if pd.notnull(row['export_kwh']) else None,
                'avg_import_kva': row['avg_import_kva'] if pd.notnull(row['avg_import_kva']) else None,
                'avg_export_kva': row['avg_export_kva'] if pd.notnull(row['avg_export_kva']) else None,
                'import_kvarh': row['import_kvarh'] if pd.notnull(row['import_kvarh']) else None,
                'export_kvarh': row['export_kvarh'] if pd.notnull(row['export_kvarh']) else None,
                'power_factor': row['power_factor'] if pd.notnull(row['power_factor']) else None,
                'avg_current': row['avg_current'] if pd.notnull(row['avg_current']) else None,
                'avg_voltage': row['avg_voltage'] if pd.notnull(row['avg_voltage']) else None,
                'phases': {
                    'A': {
                        'instCurrent': row['phase_a_inst_current'] if pd.notnull(row['phase_a_inst_current']) else None,
                        'instVoltage': row['phase_a_inst_voltage'] if pd.notnull(row['phase_a_inst_voltage']) else None,
                        'instPowerFactor': row['inst_power_factor'] if pd.notnull(row['inst_power_factor']) else None
                    },
                    'B': {
                        'instCurrent': row['phase_b_inst_current'] if pd.notnull(row['phase_b_inst_current']) else None,
                        'instVoltage': row['phase_b_inst_voltage'] if pd.notnull(row['phase_b_inst_voltage']) else None,
                        'instPowerFactor': None
                    },
                    'C': {
                        'instCurrent': row['phase_c_inst_current'] if pd.notnull(row['phase_c_inst_current']) else None,
                        'instVoltage': row['phase_c_inst_voltage'] if pd.notnull(row['phase_c_inst_voltage']) else None,
                        'instPowerFactor': None
                    }
                }
            }
            measurement_data.append(doc)

        try:
            # Check for duplicates efficiently
            if not measurement_data:
                self.logger.info("No measurements to insert.")
                return

            # Extract lookup keys: (serial, timestamp_iso)
            lookup_keys = [
                (doc['metadata']['serial'], doc['timestamp'].isoformat())
                for doc in measurement_data
            ]

            # Query MongoDB for existing documents
            serials = list(set(key[0] for key in lookup_keys))
            timestamps = [key[1] for key in lookup_keys]

            existing_docs = self.db.db['measurements'].find(
                {
                    'metadata.serial': {'$in': serials},
                    'timestamp': {'$in': [pd.to_datetime(ts) for ts in timestamps]}
                },
                {'metadata.serial': 1, 'timestamp': 1}
            )

            existing_keys = {
                (doc['metadata']['serial'], doc['timestamp'].isoformat())
                for doc in existing_docs
            }

            # Filter out already inserted docs
            new_measurements = [
                doc for doc in measurement_data
                if (doc['metadata']['serial'], doc['timestamp'].isoformat()) not in existing_keys
            ]

            if not new_measurements:
                self.logger.info("No new measurements to insert.")
                return

            self.logger.info(f"Inserting {len(new_measurements)} new measurements...")

            # Safe batching: 500 docs per batch (adjust based on avg doc size)
            BATCH_SIZE = 500  # Safe for 16MB limit (each doc ~20-30KB → 500*30KB = ~15MB)
            total_inserted = 0

            for i in range(0, len(new_measurements), BATCH_SIZE):
                batch = new_measurements[i:i + BATCH_SIZE]
                try:
                    self.db.insert_many('measurements', batch)
                    total_inserted += len(batch)
                except OperationFailure as e:
                    self.logger.error(f"Failed to insert batch {i//BATCH_SIZE + 1}: {e}")
                    # Optionally: reduce batch size and retry
                    raise

            self.logger.info(f"Successfully inserted {total_inserted} measurements")

        except Exception as e:
            self.logger.error(f"Failed to insert measurements: {e}")
            raise

    def download_file(self, s3_key):
        try:
            return self.s3_client.download_file(s3_key, self.temp_dir)
        except Exception as e:
            self.logger.error(f"Failed to download file {s3_key}: {e}")
            raise

    def process_file(self, s3_key):
        local_path = None
        try:
            if self.is_file_processed(s3_key):
                self.logger.info(f"Skipping already processed file: {s3_key}")
                return
            local_path = self.download_file(s3_key)
            df = self.read_data(local_path)
            self.insert_customers(df)
            self.insert_meters(df)
            self.insert_measurements(df)
            self.mark_file_processed(s3_key)
            self.logger.info(f"Successfully processed file: {s3_key}")
        except Exception as e:
            self.logger.error(f"Failed to process file {s3_key}: {e}")
            raise
        finally:
            if local_path and os.path.exists(local_path):
                os.remove(local_path)
                self.logger.info(f"Removed temporary file: {local_path}")