from imports import *

def predict_next_timestep(model: "nn.Module", last_sequence: np.ndarray,
                          scaler: "StandardScaler", last_kwh: float, logger: logging.Logger) -> tuple[np.ndarray, np.ndarray]:
    try:
        model.eval()
        device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')
        x = torch.FloatTensor(last_sequence).unsqueeze(0).to(device)
        with torch.no_grad():
            pred = model(x).cpu().numpy().squeeze()
        dummy = np.zeros((96, len(scaler.mean_)))
        dummy[:, 0] = pred
        pred_kwh_delta = scaler.inverse_transform(dummy)[:, 0]
        pred_kwh_delta = np.maximum(pred_kwh_delta, 0)
        pred_kwh = [last_kwh + pred_kwh_delta[0]]
        for i in range(1, 96):
            pred_kwh.append(pred_kwh[-1] + pred_kwh_delta[i])
        return np.array(pred_kwh), pred_kwh_delta
    except Exception as e:
        logger.error(f"Prediction failed: {e}")
        raise

def create_prediction_plot(df: pd.DataFrame, predictions: np.ndarray, customer_ref: int, sequence_length: int,
                           output_base_dir: str, logger: logging.Logger) -> str:
    try:
        output_dir = os.path.join(output_base_dir, f"customer_{customer_ref}")
        os.makedirs(output_dir, exist_ok=True)
        plt.figure(figsize=(16, 6))
        last_data = df.tail(sequence_length)
        plt.plot(last_data['timestamp'], last_data['import_kwh'], label='Historical', color='blue')
        last_time = last_data['timestamp'].iloc[-1]
        pred_times = [last_time + timedelta(minutes=15 * (i + 1)) for i in range(96)]
        plt.plot(pred_times, predictions, '--', marker='o', color='green', label='Prediction (Next 24h)')
        plt.axvline(last_time, color='red', linestyle='--', label='Prediction Start')
        plt.title(f"Customer {customer_ref} - 24-Hour Energy Prediction")
        plt.xlabel("Time")
        plt.ylabel("Cumulative kWh")
        plt.legend()
        plt.grid(True)
        plt.xticks(rotation=45)
        plt.tight_layout()
        plot_path = os.path.join(output_dir, f"pred_{customer_ref}_{int(datetime.now().timestamp())}.png")
        plt.savefig(plot_path)
        plt.close()
        logger.info(f"Saved plot: {plot_path}")
        return plot_path
    except Exception as e:
        logger.error(f"Plotting failed for customer {customer_ref}: {e}")
        raise

def save_prediction_to_db(db, customer_ref: int, pred_abs: np.ndarray,
                          pred_delta: np.ndarray, start_time: datetime, logger: logging.Logger):
    try:
        entries = [
            {
                "customer_ref": customer_ref,
                "prediction_timestamp": start_time + timedelta(minutes=15 * i),
                "predicted_usage": float(pred_delta[i]),
                "predicted_import_kwh": float(pred_abs[i]),
                "generated_at": datetime.now()
            } for i in range(96)
        ]
        if entries:
            db.customer_prediction.delete_many({"customer_ref": customer_ref})
            db.customer_prediction.insert_many(entries)
            logger.info(f"Saved {len(entries)} predictions for customer {customer_ref}")
    except Exception as e:
        logger.error(f"Failed to save predictions for customer {customer_ref}: {e}")
        raise

def save_model_to_db(db, model: "nn.Module", customer_ref: int,
                     mse: float, r2_score: float, trained_data_timestamp: datetime, logger: logging.Logger):
    try:
        buffer = io.BytesIO()
        torch.jit.save(torch.jit.script(model), buffer)
        model_data = buffer.getvalue()
        db.customer_model.update_one(
            {"customer_ref": customer_ref},
            {"$set": {
                "model_data": model_data,
                "mse": float(mse),
                "r2_score": float(r2_score),
                "last_trained_data_timestamp": trained_data_timestamp,
                "trained_at": datetime.now()
            }},
            upsert=True
        )
        logger.info(f"Saved model for customer {customer_ref}")
    except Exception as e:
        logger.error(f"Error saving model for customer {customer_ref}: {e}")
        raise