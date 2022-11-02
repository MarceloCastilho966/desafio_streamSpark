# Desafio BULK Stream Spark

#### Desafio consiste em: 

#### 1. Obter informações do site www.weather.gov, para obter informações de metereologia no formato XML.

#### 2. Transformar o arquivo XML em json

#### 3. Criar uma Stream em spark dos arquivos baixados

#### 4. Fazer o tratamento nos dados

#### 5. Salvar os dados no MongoDB
--------------------------------

## 1. Os Arquivos foram baixados da URL :https://w1.weather.gov/xml/current_obs/PADQ.xml
```
while i < horas:
    r = requests.get(url, allow_redirects=True)
    o = open('tempo.xml', 'wb').write(r.content)
```

## 2. Arquivos transformados e salvos em Json(em outra pasta):
```
    with open('tempo.xml') as xml_file:
        data_dict = xmltodict.parse(xml_file.read())
        json_data = json.dumps(data_dict) 
        if not os.path.exists('/home/marcelo/BULK/desafio_streamSpark/files'):
            os.makedirs("/home/marcelo/BULK/desafio_streamSpark/files")
        with open("/home/marcelo/BULK/desafio_streamSpark/files/data" + str(i + 1) + ".json", 'w') as json_file:
            json_file.write(json_data + '\n')
        os.remove(file) 
    i+=1
    time.sleep(60)
```

## 3. Criar uma Stream em spark dos arquivos baixados:
```
df_json = spark.readStream.format("json")\
.schema(schema)\
.load('/home/marcelo/BULK/desafio_streamSpark/files/*json')
```

## 4. Fazer o tratamento nos dados:
```
select = df_json.selectExpr("CAST (current_observation.location AS STRING) AS Localizacao",
                        "CAST (current_observation.observation_time_rfc822 AS STRING) AS Data_Hora",
                        "CAST (current_observation.temp_c AS DOUBLE) AS Temperatura",
                        "CAST (current_observation.pressure_in AS DOUBLE) AS Pressao",
                        "CAST (current_observation.weather AS STRING) AS Clima")
```

## 5. Salvar os dados no MongoDB
```
select.writeStream\
    .format('mongodb')\
    .option('spark.mongodb.connection.uri', 'mongodb://127.0.0.1:27017/')\
    .option('spark.mongodb.database', 'marcelo')\
    .option('spark.mongodb.collection', 'weather')\
    .option("checkpointLocation", checkpoint)\
    .outputMode("append").start().awaitTermination()
```

![Captura de tela de 2022-11-02 11-54-05](https://user-images.githubusercontent.com/97556793/199522902-b40a8c36-d77e-4ddf-a737-f21267334aa1.png)



