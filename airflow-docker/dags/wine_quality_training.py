import datetime
import pendulum
#import pandas as pd
#import numpy as np
#from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score
#from sklearn.model_selection import train_test_split
#from sklearn.linear_model import ElasticNet
from airflow.decorators import dag, task
import pickle
import argparse
import logging


@dag(
    dag_id="wine-quality-training",
    schedule_interval="0 0 * * *",
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=60),
)
def WineQualityTraining():

    def eval_metrics(actual, pred):
        from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score
        import numpy as np
        rmse = np.sqrt(mean_squared_error(actual, pred))
        mae = mean_absolute_error(actual, pred)
        r2 = r2_score(actual, pred)
        return rmse, mae, r2

    @task.bash
    def install_requirements():
        return "pip install -r /opt/airflow/dags/requirements.txt"

    @task
    def read_data():
        import pandas as pd
        FILE_PATH = "/opt/airflow/dags/data/wine-quality.csv"
        df = pd.read_csv(FILE_PATH)
        return df

    @task(multiple_outputs=True)
    def preprocess_data(data):
        from sklearn.model_selection import train_test_split
        train, test = train_test_split(data)
        # The predicted column is "quality" which is a scalar from [3, 9]
        train_x = train.drop(["quality"], axis=1)
        test_x = test.drop(["quality"], axis=1)
        train_y = train[["quality"]]
        test_y = test[["quality"]]
        return_dict = {"train_x": train_x, "test_x": test_x, "train_y": train_y, "test_y": test_y}
        return return_dict

    @task
    def train_model(returned_preprocessed_data: dict):
        train_x, train_y = returned_preprocessed_data["train_x"], returned_preprocessed_data["train_y"]
        from sklearn.linear_model import ElasticNet
        alpha = 0.1
        l1_ratio = 0.2
        num_iterations = 1000
        lr = ElasticNet(alpha=alpha, l1_ratio=l1_ratio, random_state=42, max_iter=num_iterations)
        logging.info(f"Training model with alpha={alpha}, l1_ratio={l1_ratio}, num_iterations={num_iterations}")
        logging.info(f"Training data shape: {train_x.shape}")
        logging.info(f"Training values: {train_x}")
        lr.fit(train_x.values, train_y.values)
        model_path = "/opt/airflow/dags/tmp/model.pkl"
        with open("/opt/airflow/dags/tmp/model.pkl", "wb") as file:
            pickle.dump(lr, file)
        return model_path

    @task(multiple_outputs=True)
    def evaluate_model(returned_preprocessed_data, model_path):
        from sklearn.linear_model import ElasticNet
        test_x, test_y = returned_preprocessed_data["test_x"], returned_preprocessed_data["test_y"]
        lr = pickle.load(open(model_path, 'rb'))
        predicted_qualities = lr.predict(test_x)
        (rmse, mae, r2) = eval_metrics(test_y, predicted_qualities)
        return_dict = {"rmse": rmse, "mae": mae, "r2": r2}
        return return_dict

    @task
    def save_model(returned_evaluation_metrics, model_path):
        rmse, mae, r2 = returned_evaluation_metrics["rmse"], returned_evaluation_metrics["mae"], returned_evaluation_metrics["r2"]
        lr = pickle.load(open(model_path, 'rb'))
        with open("/opt/airflow/dags/files/eval.txt", "w") as file:
            file.write(f"rmse: {rmse}, mae: {mae}, r2: {r2}")
        with open("/opt/airflow/dags/files/model.pkl", "wb") as file:
            pickle.dump(lr, file)

    #install_requirements()
    df = read_data()
    df.set_upstream(install_requirements())
    returned_preprocessed_data = preprocess_data(df)
    model_path = train_model(returned_preprocessed_data)
    returned_evaluation_metrics = evaluate_model(returned_preprocessed_data, model_path)
    save_model(returned_evaluation_metrics, model_path)


WineQualityTraining()
