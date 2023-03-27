import pandas as pd
import requests
import html


def catalogoEsios(token):
    """
    Descarga todos los identificadores y su descripcion de esios
    
    Parameters
    ----------
    token : str
        El token de esios necesario para realizar las llamadas al API
        
    Returns
    -------
    DataFrame
        Dataframe de pandas con el catalogo de los id de la API
    
    """
    
    
    headers = {'Accept':'application/json; application/vnd.esios-api-v2+json',
           'Content-Type':'application/json',
           'Host':'api.esios.ree.es',
           'Cookie' : '',
           'x-api-key': token,
           'Cache-Control': 'no-cache',
           'Pragma': 'no-cache'
          }
    end_point = 'https://api.esios.ree.es/indicators'
    response = requests.get(end_point, headers=headers).json()
    
    #del resultado en json bruto se convierte en pandas, y se eliminan los tags del campo description

    return (pd
            .json_normalize(data=response['indicators'], errors='ignore')
            .assign(description = lambda df_: df_.apply(lambda df__: html.unescape(df__['description']
                                                            .replace('<p>','')
                                                            .replace('</p>','')
                                                            .replace('<b>','')
                                                            .replace('</b>','')), 
                                                  axis=1)
                   )
           )


def downloadEsios(token,indicadores,fecha_inicio,fecha_fin,time_trunc='hour'):
    """
    Descarga datos esios desde un determinado identidficador y entre dos fechas
    
    Parameters
    ----------
    token : str
        El token de esios necesario para realizar las llamadas al API
    
    indicadores : list
        Lista con los strings de los indicadores de los que queremos bajar datos
        
    fecha_inicio : str
        Fecha con formato %Y-%M-%d, que indica la fecha desde la que se quiere bajar los datos.
        Ejemplo 2022-10-30, 30 Octubre de 2022.
    
    fecha_fin : str
        Fecha con formato %Y-%M-%d, que indica la fecha hasta la que se quiere bajar los datos.
        Ejemplo 2022-10-30, 30 Octubre de 2022.
        
    time_trunc : str, optional
        Campo adicional que nos permite elegir la granularidad de los datos que queremos bajar.
        
    Returns
    -------
    DataFrame
        Dataframe de pandas con los datos solicitados
    
    """
    
    # preparamos la cabecera a insertar en la llamada. Vease la necesidad de disponer el token de esios
    
    headers = {'Accept':'application/json; application/vnd.esios-api-v1+json',
           'Content-Type':'application/json',
           'Host':'api.esios.ree.es',
           'Cookie': '',
           'x-api-key': token,
           'Cache-Control': 'no-cache',
           'Pragma': 'no-cache'
          }
    
    # preparamos la url básica a la que se le añadiran los campos necesarios 
    
    end_point = 'https://api.esios.ree.es/indicators'
    
    # El procedimiento es sencillo: 
    # a) por cada uno de los indicadores configuraremos la url, según las indicaciones de la documentación.
    # b) Hacemos la llamada y recogemos los datos en formato json.
    # c) Añadimos la información a una lista
    
    lista=[]

    for indicador in indicadores:
        url = f'{end_point}/{indicador}?start_date={fecha_inicio}T00:00Z&end_date={fecha_fin}T23:59Z&time_trunc={time_trunc}'
        print (url)
        response = requests.get(url, headers=headers).json()
        lista.append(pd.json_normalize(data=response['indicator'], record_path=['values'], meta=['name','short_name'], errors='ignore'))

    # Devolvemos como salida de la función un df fruto de la concatenación de los elemenos de la lista
    # Este procedimiento, con una sola concatenación al final, es mucho más eficiente que hacer múltiples 
    # concatenaciones.
    
    return pd.concat(lista, ignore_index=True )


def downloadGasRD(year):
    """
    Descarga datos de precio de gas desde MIBGAS para compensación segñun RD10/2022
    
    Parameters
    ----------
    YEAR : str
        Indicamos el año del que nos queremos bajar los datos de precio de gas de RD10/22
        
    Returns
    -------
    DataFrame
        Dataframe de pandas con los datos solicitados, columnas Fecha , Producto y Precio
    
    """
    
    path = f'https://www.mibgas.es/en/file-access/MIBGAS_Data_{year}.xlsx?path=AGNO_{year}/XLS'
    return (pd.read_excel(path,sheet_name='PGN_RD_10_2022',
                        usecols=['Date','PGN Price\n[EUR/MWh]']).
                        rename(columns={'PGN Price\n[EUR/MWh]':'Price(EUR/MWh)'}).
                        sort_values('Date',ascending=True).
                        reset_index(drop=True))


def downloadGas(year):
    """
    Descarga datos de precio de gas desde MIBGAS para GDAES
    
    Parameters
    ----------
    year : str
        Indicamos el año del que nos queremos bajar los datos de precio de gas PVB
        
    Returns
    -------
    DataFrame
        Dataframe de pandas con los datos solicitados, columnas Fecha , Producto y Precio
    
    """
    
    path = f'https://www.mibgas.es/en/file-access/MIBGAS_Data_{year}.xlsx?path=AGNO_{year}/XLS'
    return (pd.read_excel(path,sheet_name='Trading Data PVB&VTP',
        usecols=['Trading day','Product','Daily Reference Price\n[EUR/MWh]']).
        query("Product=='GDAES_D+1'").
        rename(columns={'Trading day':'fecha','Product':'Producto','Daily Reference Price\n[EUR/MWh]':'precio'}).
        sort_values('fecha',ascending=True).
        reset_index(drop=True))

