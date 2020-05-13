import os
import json
from kafka import KafkaProducer
import avro.datafile
import avro.io
import avro.schema
import io
from kafka.errors import KafkaError


avro_schemaPath = "/root/PycharmProjects/KafkaPycharm/Kafka/user.avsc"
folderPath = '/root/PycharmProjects/KafkaPycharm/Kafka/Json_folder_file_dir/'
fileName = 'example_1.json'
bootstrap_servers_url = 'localhost:9092'
Topic_Name = "payments"

class ProducerClassWithoutAvro:

    def __init__(self):
        global avro_schemaPath, folderPath, fileName, bootstrap_servers_url, Topic_Name
        pass

    def main(self, dictObject):
        try:
            # main_object.write(readJson, encoder)
            # raw_bytes = bytes_reader.getvalue()
            kd = KafkaProducer(bootstrap_servers=bootstrap_servers_url
                               # )
                               , value_serializer=lambda x: json.dumps(x).encode('utf-8'))
            kd.send(Topic_Name,
                    # b'My name is Darpan').add_callback(self.on_send_success)
                    dictObject).add_callback(self.on_send_success)
            kd.flush()
            kd.close()

        except KafkaError:
            print("Input File has invalid Schema")

    def on_send_success(self, raw_metadata):
        print("Message is successfully delivered to the Topic {} and ".format(Topic_Name))
        print("the offset value of msg is " + str(raw_metadata.offset))
        pass

    def avroInitializer(self, avro_schemaPath, readJson):
        #schema = avro.schema.Parse(open(avro_schemaPath).read())
        #main_object = avro.io.DatumWriter(schema)
        #bytes_reader = io.BytesIO()
        #encoder = avro.io.BinaryEncoder(bytes_reader)
        try:
            #main_object.write(readJson, encoder)
            #raw_bytes = bytes_reader.getvalue()
            kd = KafkaProducer(bootstrap_servers=bootstrap_servers_url
                               #)
                               ,value_serializer=lambda x: json.dumps(x).encode('utf-8'))
            kd.send(Topic_Name,
                    #b'My name is Darpan').add_callback(self.on_send_success)
                    readJson).add_callback(self.on_send_success)
            kd.flush()
            kd.close()

        except KafkaError:
            print("Input File has invalid Schema")


    def jsonreader(self):
        with open(folderPath + fileName, 'r', encoding='utf-8-sig') as f:
            readJson = json.load(f)
            print(readJson)
            return readJson

# checkForDir
    def dirCheck(self):
        ab = os.path.dirname(folderPath)
        if (os.path.isdir(folderPath)):
            if (os.path.exists(folderPath + fileName)):
                print('{} file exists in {}'.format(fileName, ab))
                readJson = self.jsonreader()
                self.avroInitializer(avro_schemaPath, readJson)
                pass
            pass
test = ProducerClassWithoutAvro()
test.dirCheck()