import os
from prefect import flow, task, get_run_logger
from prefect.artifacts import create_table_artifact
from src.utils import (
    get_data,
    get_table_previous_year_today,
    get_table_format_date,
    create_directory
)


@task(retries=3, description="cargar datos")
def get_data_from_ine_api(path_json: str):
    """
    Leer datos y convertirlos en tabla de pandas
    """
    data = get_data(path_json=path_json)
    data_show = get_table_previous_year_today(table=data)
    data = get_table_format_date(data)
    return data, data_show


@task(description="visualizar datos", tags=["report_data"])
def report_prev_year_table(data):
    """
    Presentar los datos como artefacto - formato tabla
    """
    create_table_artifact(
        key="muestra-tabla",
        table=data.to_dict("records"),
        description="Datos un año anterior (desde el primer mes hasta hoy)"
    )


@task(description="escribir datos ine", tags=["save_data"])
def write_csv(data):
    """
    Escribir los datos
    """
    parent_path = os.getcwd()
    parent_path = parent_path.replace("orquestacion_prefect", "")

    dir_name = create_directory(parent_path)
    name = "data_ine.csv"
    parent_path = f"{parent_path}/{dir_name}"
    return data.to_csv(os.path.join(parent_path, name))


@flow(name="INE Datos", description="Descarga de valores de IPC del INE")
def data_ine(path: str = "config/config.json"):
    """
    Descarga de datos del INE - IPC
    """
    logger = get_run_logger()

    logger.info("Cargar datos IPC del INE")
    data, data_show = get_data_from_ine_api(path_json=path)

    logger.info("Presentar datos como artefacto")
    report_prev_year_table(data_show)

    logger.info("Guardar datos en escritorio")
    write_csv(data)


if __name__ == "__main__":
    data_ine.serve(name="estudio_ipc")
