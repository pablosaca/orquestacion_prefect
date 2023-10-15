import os
import json
import requests
import pandas as pd
from datetime import datetime


def load_json(path_json: str):
    """
    Leer json para descargar los ficheros desde la API
    """
    with open(path_json) as user_file:
        info_json = json.load(user_file)["DATA"]

    path_name = info_json["path"]
    code = info_json["code"]
    return path_name, code


def load_json_model(path_json: str):
    """
    Leer json para cargar los parámetros del modelo SARIMA (fase regular y estacional)
    """
    with open(path_json) as user_file:
        info_json = json.load(user_file)["MODEL"]

    regular_values = info_json["regular"]
    seasonal_values = info_json["seasonal"]
    horizon_value = info_json["horizon"]
    return regular_values, seasonal_values, horizon_value


def get_data(path_json: str):
    """
    Cargar datos del INE. Hacer petición get a la API
    """
    path_name, code = load_json(path_json)

    url = path_name + '/' + code + '?nult=999'
    request = requests.get(url)

    # si código no es 200 hay un error.
    if request.status_code == 200:
        data_dict = request.json()
        data = get_serie_df(data_dict)
        return data
    else:
        raise ValueError(f"Incorrect request: {request.status_code}")


def get_table_previous_year_today(table: pd.DataFrame):
    """
    Obtener año previo a la fecha actual
    """
    today = datetime.now()
    pyear = today.year - 1
    table = table[table["year"] >= pyear]
    return table


def get_table_format_date(table: pd.DataFrame):
    """
    Obtener tabla con formato de fecha
    """
    table["date"] = pd.to_datetime({"year": table["year"], "month": table["month"], "day": 1})
    table = table.drop(columns=["year", "month"])
    table = table.set_index("date")
    return table


def get_serie_df(json_file: dict):
    """
    Crear dataframe con la información 
    """

    # creación de listas para mes,
    # año y valor del índice
    month_list = []
    year_list = []
    value_list = []
    # se itera sobre la lista
    for data_dict in json_file["Data"]:
        for key, value in data_dict.items():
            if key == "Anyo":
                year_list.append(value)
            if key == "FK_Periodo":
                month_list.append(value)
            if key == "Valor":
                value_list.append(value)

    output_df = pd.DataFrame({"year": year_list,
                              "month": month_list,
                              "value": value_list})
    return output_df


def create_directory(parent_path):
    """
    Creación directorio para guardar los ficheros (si aplica)
    """
    os.chdir(parent_path)
    carpeta = "ine"
    # Verificar si la carpeta ya existe
    if not os.path.exists(carpeta):
        # Si no existe, crear la carpeta
        os.mkdir(carpeta)
    return carpeta


def hist_pred_convert_df(real_data: pd.DataFrame, pred_data: pd.Series):
    """
    Conversión al formato adecuado de las predicciones del modelo y
    creación de un dataframe que contenga los datos históricos + los datos predichos
    """

    pred_data_df = pred_data.to_frame(name="value").reset_index()
    pred_data_df = pred_data_df.rename(columns={"index": "date"})

    # convertir de datetime a texto
    pred_data_df["date"] = pred_data_df["date"].dt.strftime('%Y-%m-%d')  # se excluye hora, minutos y segundos

    data_final_df = pd.concat([real_data.reset_index(), pred_data_df.reset_index(drop=True)], axis=0)
    return data_final_df, pred_data_df
