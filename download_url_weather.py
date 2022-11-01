import time
import xmltodict
import requests
import json
import os


file = 'tempo.xml'
i= 0 ## Variavel Contadora
horas = 24 ## Número de horas que o programa irá rodas
url = 'https://w1.weather.gov/xml/current_obs/PADQ.xml' ## URL para Download do arquivo XML


while i < horas:
    r = requests.get(url, allow_redirects=True) ## Primeiro faz o download do arquivo XML pela sua URL
    o = open('tempo.xml', 'wb').write(r.content) ## O abre e salva seu conteúdo
    ## Lê o arquivo e o transforma em um formato json
    with open('tempo.xml') as xml_file:
        data_dict = xmltodict.parse(xml_file.read()) ## Lê o arquivo e o transforma em um formato json
        json_data = json.dumps(data_dict) 
        ## Cria Diretória se não existe
        if not os.path.exists('/home/marcelo/BULK/desafio_streamSpark/files'):
            os.makedirs("/home/marcelo/BULK/desafio_streamSpark/files")
        # Após sua transformação em json, o arquivo é salvo em seu local de destino
        with open("/home/marcelo/BULK/desafio_streamSpark/files/data" + str(i + 1) + ".json", 'w') as json_file:
            json_file.write(json_data + '\n')
        os.remove(file) ## O arquivo XML é deletado, para evitar erros
    i+=1
    time.sleep(3600) ## Intervalo entre um Download e outro
