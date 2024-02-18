import json
import time
import requests
from kafka import KafkaProducer

# To do: Créer via l'invite de command un topic velib-projet
# To do: Envoyer les données vers un topic velib-projet
# To do: Il faut filtrer les données collectées pour ne prendre que les données des deux stations suivants: 16107, 32017


def get_velib_data():
    """
    Get velib data from api
    :return: list of stations information
    """
    response = requests.get('https://velib-metropole-opendata.smovengo.cloud/opendata/Velib_Metropole/station_status'
                            '.json')
    data = json.loads(response.text)

    stations = data["data"]["stations"]
    stations_specific = [station for station in stations if station["stationCode"] in ["16107", "32017"]]
    return stations_specific
def velib_producer():
    """
    Create a producer to write velib data in kafka
    :return:
    """
    producer = KafkaProducer(bootstrap_servers="localhost:9092",
                             value_serializer=lambda x: json.dumps(x).encode('utf-8')
                             )

    while True:
        data = get_velib_data()
        for message in data:
            producer.send("velib_projet", message)
            print("added:", message)
        time.sleep(10)


if __name__ == '__main__':
    velib_producer()
