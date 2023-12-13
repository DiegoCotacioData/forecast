
from datetime import datetime
import numpy as np
import pandas as pd
from pytorch_forecasting.metrics import MAE, SMAPE, MQF2DistributionLoss, QuantileLoss, NormalDistributionLoss
from utils.logger_config import get_logger
logger = get_logger(__name__)





class ModelsEvaluationStep:

    def __init__(self, data, training_artifacts, training_cutoff):
        
        self.data = data
        self.training_artifacts = training_artifacts
        self.training_cutoff = training_cutoff


    def evaluate_models(self):

        """ Main method that evaluates all the models during the cross validation training
        
        returns:
           training_metrics (pd.DataFrame): the SMAPE for all models in models_config and rolling window
        """
        
        try:
            rolling_window = self.create_sipsa_rolling()

            dict_training_results = self.model_evaluation()

            training_metrics = self.merge_metrics(dict_training_results, rolling_window)

        except Exception as e:
          logger.error(f"An error occurred when running the model evaluation step: {str(e)}")

        return training_metrics



    def model_evaluation(self) -> dict:

        """
        Evaluate each model on the validation dataset (including rolling window) and save the results and metrics.

        Args:
            rolling method (Method): create and instanciate the rolling window dataframe.
            training_artifacts (Dict): contains the models, and validation dataloader.
            training_cutoff (Int): the point in the time to define the validation set.

        Returns:
            rolling_window (Dataframe): A dataframe containing the predict values according to the rolling strategy.
            models_results (Dict): A dict that contains the dataframes predictions and metrics for each model

        """

        dict_training_results = {}

        try:

            for model_name, artifacts in self.training_artifacts.items():

                best_model = artifacts["best_model"] 
                val_dataloader = artifacts["val_dataloader"] 
                self.training_dataset = artifacts["training_dataset"]  

                # Predict method
                predictions = best_model.predict(val_dataloader, mode="raw", return_x=True, return_index=True)
                
                # extract and instanciate Q50 predictions from the tensor object
                pred = predictions.output.prediction.cpu().numpy()[:, :, 4]

                # Create a metrics dataframe to store results and SMAPE
                model_metrics = self.data[lambda x: x.time_idx > self.training_cutoff ]
                model_metrics = model_metrics[["created_at", "product_name", "avg_price"]]
                model_metrics["pred"] = np.nan

                for i, row in predictions.index.iterrows():
                 model_metrics.loc[
                    (model_metrics["product_name"] == row["product_name"]),
                    "pred"] = pred[i]

                float_columns = model_metrics.select_dtypes(include=['float64']).columns
                model_metrics[float_columns] = model_metrics[float_columns].round(0)
                
                model_metrics['smape'] = 2 * np.abs(model_metrics['avg_price'] - model_metrics['pred']) / (np.abs(model_metrics['avg_price']) + np.abs(model_metrics['pred']))
                model_metrics['smape'] = model_metrics['smape'].round(2)
                model_metrics['created_at'] = pd.to_datetime(model_metrics['created_at'])
                
                dict_training_results[model_name] = model_metrics
        
        except Exception as e:
          logger.error(f"An error occurred during trained models evaluation in model evaluation step: {str(e)}")

        return dict_training_results
    

    
    def create_sipsa_rolling(self) -> pd.DataFrame:

        """Create a dataframe that estimate the rolling windows data to be include in the model evaluation as a "model"."""

        real_sipsa = self.data.copy()
        try:
            real_sipsa['created_at'] = pd.to_datetime(real_sipsa['created_at'])
            real_sipsa['rolling_price'] = real_sipsa.groupby('product_name')['avg_price'].shift(5)
            sipsa_rolling = real_sipsa[["created_at","product_name","avg_price","rolling_price"]]
            sipsa_rolling['rolling_smape'] = 2 * abs(sipsa_rolling['rolling_price'] - sipsa_rolling['avg_price']) / (abs(sipsa_rolling['rolling_price']) + abs(sipsa_rolling['avg_price']))
        
        except Exception as e:
          logger.error(f"An error occurred during rolling window generation in model evaluation step: {str(e)}")
        
        return  sipsa_rolling
    


    def merge_metrics(self, dict_training_results, rolling_window):

        """Merge the rolling window and trained models metrics in cross validation (SMAPE)"."""

        rolling_metrics = rolling_window.copy()

        all_models_metrics = []
        
        try:

            for model_name, model_metrics in dict_training_results.items():
                models_metrics = model_metrics.rename(columns={"avg_price":f"{model_name}_avg_price",
                                                                "pred": f"{model_name}_pred",
                                                                "smape": f"{model_name}_smape"})
                all_models_metrics.append(models_metrics)


            training_metrics = rolling_metrics
            training_metrics['created_at'] = pd.to_datetime(training_metrics['created_at'])

            for model_metrics in all_models_metrics:
                model_metrics['created_at'] = pd.to_datetime(model_metrics['created_at'])
                training_metrics = pd.merge(model_metrics, training_metrics, on=["created_at", "product_name"], how="left")

            

            def add_weekdays(date, days):
                current_date = date
                for _ in range(days):
                    current_date += pd.DateOffset(days=1)
                    while current_date.weekday() == 6:
                        current_date += pd.DateOffset(days=1)
                return current_date

            training_metrics['forecast_date'] = training_metrics['created_at'].apply(lambda x: add_weekdays(x, 5))

            today = datetime.now().date()
            training_metrics["update_date"] = today

            unique_dates = sorted(training_metrics['forecast_date'].dt.date.unique())
            date_idx_dict = {forecast_date: idx + 1 for idx, forecast_date in enumerate(unique_dates)}
            training_metrics['forecast_idx'] = training_metrics['forecast_date'].dt.date.map(date_idx_dict)
        
        except Exception as e:
          logger.error(f"An error occurred when merging rolling and models metrics in model evaluation step: {str(e)}")

        return training_metrics



    