from imports import *

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from config import DB_CONFIG, OUTPUT_BASE_DIR
from database_utils import DatabaseManager
from data_processing import ElectricityDataset, preprocess_data
from model_definition import BiLSTM
from model_training import train_model
from prediction_utils import predict_next_timestep, create_prediction_plot, save_prediction_to_db, save_model_to_db
from logger import setup_logger

logger = setup_logger()

class CustomerBehaviorPipeline:
    def __init__(self, logger: logging.Logger, output_base_dir: str = f"{OUTPUT_BASE_DIR}_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}"):
        self.db_manager = DatabaseManager(db_config=DB_CONFIG, logger=logger)
        self.output_base_dir = output_base_dir
        self.logger = logger
        if not os.path.exists(self.output_base_dir):
            os.makedirs(self.output_base_dir)
            self.logger.info(f"Created output directory: {self.output_base_dir}")

    def connect_db(self):
        self.db_manager.connect()

    def close_db(self):
        self.db_manager.close()

    def fetch_customer_refs(self) -> List[int]:
        return self.db_manager.fetch_customer_refs()

    def fetch_data(self, customer_ref: int) -> "pd.DataFrame":
        return self.db_manager.fetch_data(customer_ref)

    def load_existing_model(self, customer_ref: int) -> tuple["BiLSTM", float, float, datetime]:
        try:
            result = self.db_manager.db.customer_model.find_one({"customer_ref": customer_ref})
            if result:
                model_data = result['model_data']
                mse = result.get('mse')
                r2_score = result.get('r2_score')
                last_trained_time = result.get('last_trained_data_timestamp')
                model = BiLSTM(input_size=9)
                buffer = io.BytesIO(model_data)
                model = torch.jit.load(buffer)
                self.logger.info(f"Loaded existing model for customer {customer_ref}")
                return model, mse, r2_score, last_trained_time
            self.logger.info(f"No existing model for customer {customer_ref}")
            return None, None, None, None
        except Exception as e:
            self.logger.error(f"Error loading model for customer {customer_ref}: {e}")
            raise

    def process_customer(self, customer_ref: int, sequence_length: int = 192, batch_size: int = 32):
        try:
            self.logger.info(f"Processing customer {customer_ref}")
            df = self.fetch_data(customer_ref)
            if len(df) < sequence_length + 96:
                self.logger.warning(f"Insufficient data for customer {customer_ref}")
                return None

            current_max_timestamp = df['timestamp'].max()
            model, prev_mse, prev_r2, last_trained_time = self.load_existing_model(customer_ref)

            if last_trained_time and current_max_timestamp <= last_trained_time:
                self.logger.info(f"Skipping training for {customer_ref} — no new data")
                last_kwh = df['import_kwh'].iloc[-1]
                scaled_data, scaler, orig_kwh = preprocess_data(df, self.logger)
                last_seq = scaled_data[-sequence_length:]
                pred_abs, pred_delta = predict_next_timestep(model, last_seq, scaler, last_kwh, self.logger)
                next_time = df['timestamp'].iloc[-1] + timedelta(minutes=15)
                df_plot = df.copy()
                df_plot['import_kwh'] = orig_kwh
                plot_path = create_prediction_plot(df_plot, pred_abs, customer_ref, sequence_length, self.output_base_dir, self.logger)
                save_prediction_to_db(self.db_manager.db, customer_ref, pred_abs, pred_delta, next_time, self.logger)
                return {
                    'customer_ref': customer_ref,
                    'predictions': pred_abs,
                    'plot_path': plot_path,
                    'skipped_training': True
                }

            last_kwh = df['import_kwh'].iloc[-1]
            scaled_data, scaler, orig_kwh = preprocess_data(df, self.logger)
            dataset = ElectricityDataset(scaled_data, sequence_length)
            if len(dataset) < 2:
                self.logger.warning(f"Not enough sequences for training customer {customer_ref}")
                return None

            train_size = int(0.8 * len(dataset))
            val_size = len(dataset) - train_size
            train_dataset, val_dataset = torch.utils.data.random_split(dataset, [train_size, val_size])
            train_loader = DataLoader(train_dataset, batch_size=batch_size, shuffle=True)
            val_loader = DataLoader(val_dataset, batch_size=batch_size)

            if model is None:
                model = BiLSTM(input_size=9)
                self.logger.info(f"Created new model for customer {customer_ref}")

            model, mse, r2 = train_model(model, train_loader, val_loader, logger=self.logger)
            last_seq = scaled_data[-sequence_length:]
            pred_abs, pred_delta = predict_next_timestep(model, last_seq, scaler, last_kwh, self.logger)
            df_plot = df.copy()
            df_plot['import_kwh'] = orig_kwh
            plot_path = create_prediction_plot(df_plot, pred_abs, customer_ref, sequence_length, self.output_base_dir, self.logger)
            next_time = df['timestamp'].iloc[-1] + timedelta(minutes=15)
            save_prediction_to_db(self.db_manager.db, customer_ref, pred_abs, pred_delta, next_time, self.logger)
            save_model_to_db(self.db_manager.db, model, customer_ref, mse, r2, current_max_timestamp, self.logger)

            return {
                'customer_ref': customer_ref,
                'predictions': pred_abs,
                'plot_path': plot_path,
                'mse': mse,
                'r2_score': r2
            }
        except Exception as e:
            self.logger.error(f"Failed to process customer {customer_ref}: {e}")
            return None

    def run(self, sequence_length: int = 192, batch_size: int = 32) -> List[Dict]:
        try:
            self.connect_db()
            customer_refs = self.fetch_customer_refs()
            results = []
            for ref in customer_refs:
                result = self.process_customer(ref, sequence_length, batch_size)
                if result:
                    results.append(result)
            return results
        except Exception as e:
            self.logger.error(f"Pipeline failed: {e}")
            raise
        finally:
            self.close_db()

if __name__ == "__main__":
    pipeline = CustomerBehaviorPipeline(logger=logger)
    results = pipeline.run()
    for res in results:
        logger.info(f"Customer {res['customer_ref']}: R²={res.get('r2_score', 'N/A'):.4f}, "
                    f"Plot: {res['plot_path']}")