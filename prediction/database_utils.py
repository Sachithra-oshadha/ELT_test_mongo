from imports import *

class DatabaseManager:
    def __init__(self, db_config, logger: logging.Logger):
        self.db_config = db_config
        self.client = None
        self.db = None
        self.logger = logger

    def connect(self):
        try:
            if self.db_config.get('username') and self.db_config.get('password'):
                connection_string = f"mongodb://{self.db_config['username']}:{self.db_config['password']}@{self.db_config['host']}:{self.db_config['port']}/{self.db_config['database']}"
            else:
                connection_string = f"mongodb://{self.db_config['host']}:{self.db_config['port']}/{self.db_config['database']}"
            self.client = pymongo.MongoClient(connection_string)
            self.db = self.client[self.db_config['database']]
            self.client.admin.command('ping')
            self.logger.info("Connected to MongoDB")
        except pymongo.errors.PyMongoError as e:
            self.logger.error(f"Failed to connect to MongoDB: {e}")
            raise

    def close(self):
        if self.client:
            self.client.close()
            self.logger.info("MongoDB connection closed")

    def fetch_customer_refs(self):
        try:
            customer_refs = self.db.meters.distinct("customerRef")
            self.logger.info(f"Fetched {len(customer_refs)} customer references")
            return [int(ref) for ref in customer_refs]
        except pymongo.errors.PyMongoError as e:
            self.logger.error(f"Error fetching customer references: {e}")
            raise

    def fetch_data(self, customer_ref):
        try:
            meter_docs = self.db.meters.find({"customerRef": customer_ref}, {"_id": 1})
            serials = [doc["_id"] for doc in meter_docs]
            if not serials:
                self.logger.warning(f"No meters found for customer {customer_ref}")
                return pd.DataFrame()

            pipeline = [
                {"$match": {
                    "metadata.serial": {"$in": serials},
                    "timestamp": {"$ne": None}
                }},
                {"$sort": {"timestamp": 1}},
                {"$project": {
                    "timestamp": 1,
                    "avg_import_kw": 1,
                    "import_kwh": 1,
                    "power_factor": 1,
                    "phases.A.instCurrent": 1,
                    "phases.A.instVoltage": 1,
                    "phases.B.instCurrent": 1,
                    "phases.B.instVoltage": 1,
                    "phases.C.instCurrent": 1,
                    "phases.C.instVoltage": 1
                }}
            ]

            cursor = self.db.measurements.aggregate(pipeline, allowDiskUse=True)
            df = pd.DataFrame(list(cursor))

            if df.empty:
                self.logger.warning(f"No measurements found for customer {customer_ref}")
                return df

            df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')
            df = df.dropna(subset=['timestamp'])

            if df['timestamp'].dt.tz is not None:
                df['timestamp'] = df['timestamp'].dt.tz_localize(None)

            df['timestamp'] = df['timestamp'].dt.round('15min')

            valid_range = (df['timestamp'] >= '2000-01-01') & (df['timestamp'] <= '2030-12-31')
            if not valid_range.all():
                dropped = len(df[~valid_range])
                self.logger.warning(f"Dropped {dropped} rows with invalid timestamps for customer {customer_ref}")
                df = df[valid_range]

            rename_map = {
                'phases.A.instCurrent': 'phase_a_current',
                'phases.A.instVoltage': 'phase_a_voltage',
                'phases.B.instCurrent': 'phase_b_current',
                'phases.B.instVoltage': 'phase_b_voltage',
                'phases.C.instCurrent': 'phase_c_current',
                'phases.C.instVoltage': 'phase_c_voltage'
            }
            df = df.rename(columns=rename_map)

            expected_columns = [
                'timestamp', 'import_kwh', 'avg_import_kw', 'power_factor',
                'phase_a_current', 'phase_a_voltage',
                'phase_b_current', 'phase_b_voltage',
                'phase_c_current', 'phase_c_voltage'
            ]
            for col in expected_columns:
                if col not in df.columns:
                    df[col] = np.nan

            df = df.sort_values('timestamp').reset_index(drop=True)
            self.logger.info(f"Fetched {len(df)} records for customer {customer_ref}")
            return df
        except Exception as e:
            self.logger.error(f"Error fetching data for customer {customer_ref}: {e}")
            raise