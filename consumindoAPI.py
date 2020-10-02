# coding: utf-8
# !/usr/bin/env python
### dados covid19
##### diretorio e time 01/04/2020
import shutil
import os.path
####import threading
import time

import traceback
###


import json  # para web API
import requests  # para web API
import pandas as pd

import logging  ##para LOG 30/03/2020

from datetime import datetime  ## teste 01/04/2020 elias

# import logging.handlers  ## ultimo teste 31/03/2020 elias

agora = datetime.now()  ### teste data
agora2 = time.localtime()

# busca diretorio raiz do projeto
os.getcwd()

os.path

logging.basicConfig(filename='consumindoAPI.log', filemode='a',
                    format='%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s',
                    datefmt='%H:%M:%S',
                    level=logging.DEBUG)  # , filemode='w', datefmt='%d-%b-%y %H:%M:%S' ainda não pegou data
if os.path.exists('LOG'):
    print('o diretorio LOG já existe !')
else:
    print('O diretorio LOG não existe e será criado !!!')
    os.mkdir('LOG')
    if os.path.exists('LOG'):
        print('diretorio LOG foi criado com exito!!!!')

logging.debug(agora)  # ('this will get logged')
logging.info('admin logged in')
logging.warning('Warning:config file %s not found', 'server.conf')  ### generico devo autera futuramenet 01/04/2020

try:
    dadoscovid19 = requests.get('https://brasil.io/api/dataset/covid19/caso/data')
    # time.sleep(0.5)
except Exception as e:
    # funciona

    # teste diretorio   , esta pegando diretorio errado !! 10/04/2020
    #   os.getcwd()
    #   os.chdir("LOG")
    # fim

    logging.debug(agora)
    logging.error(e)
    logging.error('(asctime)%s', 'Message with %s', 'args', exc_info=True)
    logging.error('Message with %s', 'args', e)
    logging.exception(traceback.format_exc(), e)  # logging.exception(e, agora)
    logging.info(e)
    logging.critical(e)  ## trazendo as mesma informações terei de formatar
    # break
    pass
""""**Convertendo dados da API para m dicionario**"""""

try:
    """"" Os dados capturados estão no formato JSON e para que possamos manipulá los em python precisamos converter o mesmo em um dicionário e para isso utilizamos a função loads da biblioteca JSON. """
    dicCovid = json.loads(dadoscovid19.content)  # sem internet gera erro nessa linha 31/03/2020 elias
except Exception as e:

    # teste diretorio
    #   os.getcwd()# pega diretorio atual
    #   os.chdir("LOG")# muda para novo diretorio 10/04/2020 elias
    # fim

    logging.debug(agora)
    logging.error(e)
    logging.exception(e)
    logging.critical(e)

    pass
""""" **Exibir as chaves do Dicionário** """

dicCovid.keys()  ## duvida se é k maiusculo ou minusculo 29/03/2020  é minusculo!!!! 15:30

dicCovid['results'][0].keys()  ### duvida se é k maiusculo ou minusculo 29/03/2020  é minusculo!!!! 15:30

""""" **Converte dicionário em Data Frame** """

dfCodiv19 = pd.DataFrame(dicCovid['results'])

""""" **Descrevendo e limpando o dataset**"""
dfCodiv19.count()

####print(dfCodiv19)  elias teste

dfCodiv19.pop('city_ibge_code')
dfCodiv19.pop('confirmed_per_100k_inhabitants')
dfCodiv19.pop('death_rate')
dfCodiv19.pop('estimated_population_2019')
dfCodiv19.pop('is_last')

"""vamos agora analisar a coluna city que identificamos que, possui dados missing(sujeira ou dados faltantes) e vamos iniciar vendo os dados que estão nulos no dataset"""

dfCodiv19[dfCodiv19['city'].isnull()]

"""Com a análise acima podemos ver que os dados que estão com a coluna city nulos são os que mostram as valores de casos confirmados e mortes sumarizados por estado e como não vamos precisar destes dados,pois podemos calcular os mesmo quando precisarmos.Então vamos remover os  dados desnecessários."""
dfCodiv19 = dfCodiv19[dfCodiv19['city'].notnull()]

"""Agora que removemos os dados missing da coluna city, precisamos dar uma analisada no conteúdo da coluna para que possamos ter a certeza que os dados são validos."""
pd.unique(dfCodiv19[['city']].values.ravel('k'))

"""Com isso podemos concluir que a coluna city do nosso dataset está OK Então vamos agora analisar a coluna deaths"""
dfCodiv19[dfCodiv19['deaths'].isnull()]

"""Com esta analise podemos ver que existem cidades que não tem mortes e possuem o valor da mesma ausente, então precisamos preencher as mesmas com 0"""
dfCodiv19 = dfCodiv19.fillna(0)

"""Vamos agora verificar se temos mais algum valor nulos e se corrigimos a coluna deaths"""
dfCodiv19.isna().sum()

"""Vamos verificar os valores da coluna place_type, pois precisamos ter apenas as cidades em nosso dataset."""
pd.unique(dfCodiv19[['place_type']].values.ravel('k'))

"""Com isso confirmamos que só possuímos cidades e não temos a necessidade de termos mais esta coluna em nosso dataset, então vamos eliminá-las"""
dfCodiv19.pop('place_type')

""""" **Traduzir nome das Colunas** """
dfCodiv19 = dfCodiv19.rename(
    columns={'city': 'CIDADE', 'confirmed': 'CASOS CONFIRMADOS', 'date': 'DATA ATUALIZAÇÃO', 'deaths': 'MORTOS',
             'state': 'ESTADO'})

###dfCodiv19 = dfCodiv19.rename(
###    columns={'city': 'cidade', 'confirmed': 'casosConfirmados', 'date': 'dataAtualizacao', 'deaths': 'mortos',
###             'state': 'uf'})

"""**Exibir dataset Final**"""
dfCodiv19.head()

try:
    """**Gera CSV com os dados para Utilizarmos em nosso próximo Artigo**"""
    dfCodiv19.to_csv('datasetCodiv19.csv', index=False, sep=';',
                     encoding='utf-8-sig')  ## implementei utf8 elias 29/03/202  ok
except Exception as e:
    logging.debug(agora)
    logging.error(e)
    logging.critical(e)
    pass

    ### deu certo porem esta gerando no mesmo tempo ae da um erro 01/04/2020

    #####t1 = threading.Thread(time.sleep(5))

    ##time.sleep(5)
    ###os.path.join('usr', 'bin', 'spam')
###    os.chdir("LOG")
# shutil.move('consumindoAPI.log', 'C:/Users/Elias Martins/Desktop/API_REST_PYTHON/LOG') # funciona  shutil.move('consumindoAPI.log', 'C:\\Users\\Elias Martins\\Desktop\\API_REST_PYTHON\\LOG') # funciona
# gerando erro devo tratar 08/04/2020
# print(os.getcwd())

###print("Tamanho : %d" % os.path.getsize('consumindoAPI.log'))  # pegar tamanho do arquivo 09/04/2020 elias

###print("Acessado: %s" % time.ctime(os.path.getmtime('consumindoAPI.log')))  # pegar data acesso do arquivo 09/04/2020 elias
# print("Acessado: %s" % time.ctime(os.path.locatime('consumindoAPI.log')))
