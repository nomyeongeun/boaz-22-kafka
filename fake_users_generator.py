import os
import random
import time
from time import sleep
import json
from faker import Faker
from kafka import KafkaProducer
from datetime import datetime
topicName = "fake-users"
producer = KafkaProducer(bootstrap_servers=["Kafka00Service:9092"],
                         api_version= (0,11,5),
                         value_serializer=lambda x:
                         json.dumps(x).encode("utf-8"))
faker = Faker(["pt_BR"])
quantidade = 0
while quantidade < 30 :
    data = json.loads("{}")
    data["id"]         = random.getrandbits(32)
    data["nome"]       = faker.name()
    data["sexo"]       = random.choice("MF")
    data["endereco"]   = str((faker.address()).replace("\\n", " ").replace("\\r", "").strip())
    data["cidade"]     = str(faker.city())
    data["cep"]        = str(faker.postcode())
    data["uf"]         = str(faker.estado_sigla())
    data["pais"]       = str(faker.current_country())
    data["telefone"]   = faker.phone_number()
    data["email"]      = faker.safe_email()
    data["foto"]       = faker.image_url()
    data["nascimento"] = str(faker.date_of_birth())
    data["profissao"]  = faker.job()
    data["created_at"] = str(datetime.now())
    data["updated_at"] = None
    data["sourceTime"] = round(time.time() * 1000)
    quantidade = quantidade + 1
    print("[Registro: "+str(quantidade)+"]")
    try:
        print("---" * 20)
        print("ID          :  " + str(data["id"]))
        print("Nome        :  " + data["nome"])
        print("Genero      :  " + data["sexo"])
        print("Endereco    :  " + data["endereco"])
        print("Cidade      :  " + data["cidade"])
        print("Cep         :  " + data["cep"])
        print("UF          :  " + data["uf"])
        print("País        :  " + data["pais"])
        print("Telefone    :  " + data["telefone"])
        print("Email       :  " + data["email"])
        print("Foto        :  " + data["foto"])
        print("Nascimento  :  " + data["nascimento"])
        print("Profissao   :  " + data["profissao"])
        print("Criado em   :  " + data["created_at"])
        print("Atualiz. Em :  " + str(None))
        print("sourceTime  :  " + str(data["sourceTime"]))
        print("---" * 20, "\\n")
        producer.send(topicName, value=data)
        sleep(3)
    except (Exception) as error:
        print(error.message)