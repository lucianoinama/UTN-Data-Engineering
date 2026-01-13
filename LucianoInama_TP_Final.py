import pandas as pd
import requests
from deltalake import write_deltalake, DeltaTable
from datetime import date
import os
import hashlib

### -------------- NOTAS ----------------- ###
# 1. Este archivo solian ser varios archivos distintos con un archivo main que los ejecutaban. Tambien tenia otro archivo con los datos de configuracion pero debido a que se debe entregar un solo archivo lo unifique en este.
# 2. Elegi 1 API con 2 endpoint distintos, una de valores diarios de dolar y la otra sobre feriados nacionales. La primera se deberia ejecutar a diario y la segunda no.
# 3. Elegi usar el metodo DELETE - INSERT para la API dolar ya que la misma se actualiza diariamente. Para la de feriados preferi usar el modo Overwrite ya no esta pensado para ejecutarse de forma diaria.

### // ----------------------- DATOS CONFIG ------------------------- // ###

# Endpoint de datos temporales 
API_DOLAR_URL = 'https://api.argentinadatos.com/v1/cotizaciones/dolares/'
API_DOLAR_PARAMS = 'oficial'


# Endpoint de datos estáticos
API_FERIADOS_URL = 'https://api.argentinadatos.com/v1/feriados'
API_FERIADOS_PARAMS = [2024,2025,2026]

# Configuraciones para guardar los DeltaLake BRONZE

OUTPUT_PATH_FERIADOS_BRONZE = "bronze/static/feriados"
OUTPUT_PATH_DOLAR_BRONZE = "bronze/temp/dolar"
PARTITION_COLS_DOLAR = ["year", "month"] 

# Configuraciones para guardar los DeltaLake Silver

OUTPUT_PATH_FERIADOS_SILVER = "silver/static/feriados"
OUTPUT_PATH_DOLAR_SILVER = "silver/temp/dolar"

# Configuraciones para guardar los Deltalake Gold

OUTPUT_PATH_DOLAR_GOLD = "gold/temp/dolar"





#### // ------------ DEFINICION DE METODOS ------------


### PASO 1: Obtener los datos desde las API

def get_data(base_url, endpoint, data_field=None, params=None, headers=None):
    """
    Realiza una solicitud GET a una API para obtener datos.

    Parámetros:
    base_url (str): La URL base de la API.
    endpoint (str): El endpoint de la API al que se realizará la solicitud, en este caso {Año}

    Retorna:
    dict: Los datos obtenidos de la API en formato JSON.
    """
    if isinstance(endpoint, list): ### Si paso una lista de años, voy al metodo
        return get_data_years(base_url,endpoint,data_field,params,headers)


    try:
        endpoint_url = f"{base_url}/{endpoint}"
        response = requests.get(endpoint_url, params=params, headers=headers) ## Acá se hace la solicitud
        response.raise_for_status()  # Levanta una excepción si hay un error en la respuesta HTTP.

        # Verificar si los datos están en formato JSON.
        try:
            data = response.json()
            if data_field:
              data = data[data_field]
        except:
            print("El formato de respuesta no es el esperado")
            return None
        return data

    except requests.exceptions.RequestException as e:
        # Capturar cualquier error de solicitud, como errores HTTP.
        print(f"La petición ha fallado. Código de error : {e}")
        return None
    
def get_data_years(base_url, years, data_field=None, params=None, headers=None):

    all_dataframes = []

    for year in years:
        
        
        url = f"{API_FERIADOS_URL}/{year}"
        try:
            response = requests.get(url)
            response.raise_for_status()  # Levanta una excepción si hay un error en la respuesta HTTP.

            if data_field:
                data = data[data_field]

            data = pd.DataFrame(response.json())
   
        # Convertir el DataFrame en lista de dicts y agregarlo al total
            all_dataframes.extend(data.to_dict(orient="records"))

        ###df_year["year"] = year   # <- añade el año si la API no lo trae
        ##all_dataframes.append()
        except Exception as e:
            print(f"Error procesando el año {year}: {e}")

    return all_dataframes


### PASO 2: Construir tablas con pandas (DATA FRAME)

def build_table(json_data, record_path=None):

    try:
        df = pd.json_normalize(
            json_data,
            record_path)
        return df
    except:
        print("Los datos no están en el formato esperado")
        return None
    
### PASO 3 TRANSFORMO LOS DATAFRAME

def modify_table_partitions(df): ### Agrego columnas para particionar

        ## Verifico que la columna "Fecha" sea datetime
        df["fecha"] = pd.to_datetime(df["fecha"])
        
        ## Creo las columnas por las cuales voy a particionar
        df["year"] = df["fecha"].dt.year 
        df["month"] = df["fecha"].dt.month


### PASO 4: CREAR LOS DELTALAKES

#### Metodo para la API temporal, ya que los datos se van a traer diariamente
def delete_insert_to_delta(df, path, partition_cols):
    
    today = date.today() ## Obtengo la fecha de hoy
    dt = None

    for col in partition_cols: ## Existen las columnas de particion en el df?
        if col not in df.columns:
            raise ValueError(f"La columna de partición '{col}' no existe en el DataFrame.")
    
    partition_values = {col: df[col].iloc[0] for col in partition_cols} ## Obtengo los valores de las particiones

    if DeltaTable.is_deltatable(path): ## Existe la Delta Table en ese Path ?
        dt = DeltaTable(path)

        # Construimos el filtro dinámico: 
        #condition = f"fecha = '{today}'"
        condition = " AND ".join(
            f"{col} = {val}" for col, val in partition_values.items()
        )
        dt.delete("True") ## Borro los valores que concuerden 

    else:
        print("No existe tabla Delta. Se creará una nueva.")
        
    
    # Insertamos los nuevos registros
    write_deltalake(
        path,
        df,
        mode="append",
        partition_by=partition_cols
    )
    
    print(f"Insertados {len(df)} registros en partición: {partition_values}")
    
    if dt is None:
        dt = DeltaTable(path)
    
    dt.vacuum(retention_hours=0,dry_run=False, enforce_retention_duration=False) ## Borro los parquet viejos para evitar tener duplicados
    ## print("Vacuum")


#### Metodo para los datos de la API static
def save_data_as_delta(df, path, mode="overwrite", partition_cols=None):
    """
    Guarda un dataframe en formato Delta Lake en la ruta especificada.
    
    Args:
      df (pd.DataFrame): El dataframe a guardar.
      path (str): La ruta donde se guardará el dataframe en formato Delta Lake.
      mode (str): El modo de guardado. Usare "overwrite" ya que los datos son estaticos
      No se particionaran columnas
    """
    if "fecha" in df.columns:
        df["fecha"] = pd.to_datetime(df["fecha"]).dt.date.astype(str) ## Convierto la columna fecha a datetime
        df["year"] = df["fecha"].astype(str).str[:4] ## Creo la columna año a partir de la columna fecha
    

    os.makedirs(path, exist_ok=True) ## Crea el directorio en caso de que no exista. Si existe no hace nadaq
    
    write_deltalake(
        path, df, mode=mode, partition_by=["year"]
                                        
    )

#### ---------------------------- HASTA AQUI CUMPLIRIAMOS CON LA PRIMERA PARTE (TP 1) --------------------------------

def read_parquet(path):
    df = pd.read_parquet(path)

    return df

def get_deltalake_to_pandas(path, year=None, month=None):
    try:
        dt = DeltaTable(path)
        df = dt.to_pandas()

        if year is not None and month is not None:
            df = df[(df["year"] == year) & (df["month"] == month)]

        return df

    except Exception as e:
        raise Exception(f"No se pudo procesar la tabla Delta Lake: {e}")



### Modificamos el DataFrame de datos de dolares

def add_calculated_cols_temp(temp_df): ### Agrego columnas calculadas al dataframe de dolar

 meses = {
        1:"Enero",2:"Febrero",3:"Marzo",4:"Abril",5:"Mayo",6:"Junio",
        7:"Julio",8:"Agosto",9:"Septiembre",10:"Octubre",11:"Noviembre",12:"Diciembre"
    }
 
 df = temp_df.groupby(["year","month"]).agg( ##VALORES PROMEDIOS POR MES
    compra_avg = ("compra", "mean"), ## Promedio de valor de compra
    compra_max = ("compra", "max"), ## Maximo de valor de compra
    compra_min = ("compra", "min"), ## Minimo de valor de compra
    venta_avg  = ("venta", "mean"), ## Promedio de valor de venta
    venta_max  = ("venta", "max"), ## Maximo de valor de venta
    venta_min  = ("venta", "min"), ## Minimo de valor de venta
    compra_mayor_1000 = ("compra", lambda x: (x > 1000).any()), ### Valor de compra mayor a 1000? TRUE si se cumple, False si no
    venta_mayor_1000 = ("venta", lambda x: (x > 1000).any()) ### Idem pero con venta
    
    ).reset_index()
 
 df["mes"] = df["month"].map(meses)

 return df

def rename_columns_temp(temp_df):
    temp_df.rename(columns={"compra_mayor_1000":"compra > 1000","venta_mayor_1000": "venta > 1000"})

### Modificamos el dataframe de datos feriados (static)

def add_calculated_cols_static(static_df):

### Agrupo por año y mes y obtengo cantidad de feriados por mes
    
    static_df = static_df.groupby(["year","month"]).size().reset_index(name="cant_feriados")

    static_df["mayor_1"] = static_df["cant_feriados"] > 1
    
    return static_df


### Obtener la ultima fecha de la partición

def get_last_partition_date(df):
    last_date = df["fecha"].max() ### Devuelve la ultima fecha cargada del dataframe
    return last_date.year, last_date.month ## Retornamos ultimo año y ultimo mes


### Aplicamos una enmascaración al mes de la tabla

def get_hash_value(input_str):
    """
    Calcula el valor hash de una cadena de entrada.

    Parameters:
        input_str (str): La cadena de entrada para la cual se calculará el hash.

    Returns:
        str: El valor hash calculado
    """
    cleaned_input = input_str.lower().strip()
    hash_object = hashlib.sha256(cleaned_input.encode())
    hash_value = hash_object.hexdigest()
    return hash_value


def mask_month(temp_df):
    
    try:
        # Crear una columna nueva con el nombre del mes con hashing aplicado
        temp_df.loc[:, "mes_hashed"] = temp_df["mes"].apply(
            lambda row: get_hash_value(row)
            )
        return temp_df
    except KeyError:
        print(f"La columna '{"mes"}' no existe en el DataFrame.")
        return temp_df


### // ------------------------ EJECUCIÓN DEL ARCHIVO ------------------------------ //

### ------------ EJECUCION API FERIADOS ------------

#CAPA BRONZE - Traemos los datos, convertimos a dataframe, agregamos dos columnas y guardamos como deltalake .
api_feriados_data = get_data(API_FERIADOS_URL,API_FERIADOS_PARAMS)
api_feriados_df = build_table(api_feriados_data)
print(api_feriados_df)
modify_table_partitions(api_feriados_df)

##print(api_table)
save_data_as_delta(api_feriados_df, OUTPUT_PATH_FERIADOS_BRONZE)

#CAPA SILVER - Leemos el datalake y extraemos un dataframe. Agregamos columnas calculadas y guardamos como deltalake en capa silver.
dl_feriados = get_deltalake_to_pandas(OUTPUT_PATH_FERIADOS_BRONZE)
dl_feriados = add_calculated_cols_static(dl_feriados)

save_data_as_delta(dl_feriados,OUTPUT_PATH_FERIADOS_SILVER)

### ------------ EJECUCION API DOLARES ------------

#CAPA BRONZE - Traemos los datos, convertimos a dataframe, agregamos dos columnas y guardamos como deltalake con modo DELETE - INSERT.
api_dolar_data = get_data(API_DOLAR_URL, API_DOLAR_PARAMS)
api_dolar_df = build_table(api_dolar_data)
modify_table_partitions(api_dolar_df)
delete_insert_to_delta(api_dolar_df, OUTPUT_PATH_DOLAR_BRONZE,PARTITION_COLS_DOLAR)

#CAPA SILVER - Leemos el datalake y extraemos un dataframe. Agregamos columnas calculadas y guardamos como deltalake en capa silver.
last_year_p, last_month_p = get_last_partition_date(api_dolar_df) ## Obtengo el ultimo valor de año y mes de la partición 

dl_dolar = get_deltalake_to_pandas(OUTPUT_PATH_DOLAR_BRONZE,last_year_p,last_month_p)
dl_dolar_silver = add_calculated_cols_temp(dl_dolar)
delete_insert_to_delta(dl_dolar_silver,OUTPUT_PATH_DOLAR_SILVER, PARTITION_COLS_DOLAR) ## Ahora que procesamos los datos, solo particionamos por año

#CAPA GOLD - Traemos los datos nuevamente, les agregamos una columna y realizamos un proceso de anonimización
###NOTA: Aplico la anonimización solamente para aplicar lo visto en el material de cursada, no representa algo util en este caso.

dl_dolar = get_deltalake_to_pandas(OUTPUT_PATH_DOLAR_SILVER)
dl_dolar_gold = mask_month(dl_dolar)
delete_insert_to_delta(dl_dolar_gold,OUTPUT_PATH_DOLAR_GOLD,PARTITION_COLS_DOLAR)



