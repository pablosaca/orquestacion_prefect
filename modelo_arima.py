import os
import pandas as pd
from statsmodels.tsa.statespace.sarimax import SARIMAX
from statsmodels.tsa.statespace.sarimax import SARIMAXResultsWrapper
from prefect import flow, task, get_run_logger
from prefect.artifacts import create_table_artifact
from src.utils import load_json_model, hist_pred_convert_df


@task(retries=3, description="cargar datos escritorio")
def get_input():
    """
    Leer los datos históricos del escritorio
    """

    parent_path = os.getcwd()
    parent_path = parent_path.replace("orquestacion_prefect", "")

    name_data = "data_ine.csv"
    data = pd.read_csv(f"{parent_path}/ine/{name_data}")
    data.set_index('date', inplace=True)
    return data


@task(description="modelizar serie de tiempo", tags=["modelo_arima"])
def arima_fit(path_json: str, data: pd.DataFrame):
    """
    Entrenamiento de la serie de tiempo.
    Los parámetros del modelo son obtenidos del fichero de configuración
    """

    # cargar los términos del modelo
    regular_values, seasonal_values, horizon = load_json_model(path_json=path_json)

    modelo_arima = SARIMAX(data['value'],
                           order=tuple(regular_values),
                           seasonal_order=tuple(seasonal_values))

    # ajsutar el mdoelo
    modelo_arima_fit = modelo_arima.fit(disp=0)
    return modelo_arima_fit, horizon


@task(description="predicción serie de tiempo", tags=["modelo_arima"])
def arima_predict(model: SARIMAXResultsWrapper, horizon: int):
    """
    Predicción del modelo para n horizontes de tiempo
    """
    pred = model.forecast(steps=horizon)
    return pred


@task(description="escribir datos ine", tags=["save_data"])
def write_csv(data):
    """
    Escribir los datos de las predicciones (incluido datos históricos)
    """
    parent_path = os.getcwd()
    parent_path = parent_path.replace("orquestacion_prefect", "")

    name = "data_real_pred_ine.csv"
    parent_path = f"{parent_path}/ine"
    return data.to_csv(os.path.join(parent_path, name), index=False)


@task(description="visualizar datos", tags=["report_predict_data"])
def report_predictions(data):
    """
    Presentar los datos como artefacto - formato tabla
    """
    create_table_artifact(
        key="muestra-tabla",
        table=data.to_dict("records"),
        description="Predicciones del IPC"
    )


@flow(name="Train_Predict")
def train_predict_ine_ts(path: str = "config/config.json"):
    """
    Predicción futuros valores del IPC a partir de los datos del INE
    """

    logger = get_run_logger()

    logger.info("Obtener los datos del INE guardados previamente")
    historical_data = get_input()

    logger.info("Entrenar el modelo")
    model_fit, horizon = arima_fit(path_json=path, data=historical_data)

    logger.info(f"Predecir los datos para {horizon} periodos")
    pred_values = arima_predict(model=model_fit, horizon=horizon)

    final_df, pred_df = hist_pred_convert_df(historical_data, pred_values)

    logger.info("")
    write_csv(final_df)
    report_predictions(pred_df)


if __name__ == "__main__":
    train_predict_ine_ts.serve(name="prediccion_ipc")
