from aiokafka import AIOKafkaConsumer
from aiokafka import AIOKafkaProducer
import asyncio
import pickle
from PIL import Image
import io
from tensorflow import keras
import scipy
import numpy as np


class Singleton(type):
    _instances = {}
    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(Singleton, cls).__call__(*args, **kwargs)
        return cls._instances[cls]

class Neur_net(metaclass=Singleton):
    def __init__(self):
        self.model = keras.models.load_model("neur_oxford")
        with open("keys.pk", "rb") as f:
            self.keys = pickle.load(f)

    def get_predict(self, image:bytes)->str:
        data_image = io.BytesIO(image)
        image = Image.open(data_image)
        image_array = np.asarray(image.resize((224, 224)))

        predict = self.model.predict(np.array([image_array]), verbose=0)
        return self.keys[predict.argmax()]

async def send_answer(answer):
    producer = AIOKafkaProducer(bootstrap_servers="localhost:9092")
    await producer.start()
    try:
        await producer.send_and_wait("answer", pickle.dumps(answer))
    finally:
        await producer.stop()


async def consume():
    consumer = AIOKafkaConsumer(
        "to_neur",
        bootstrap_servers="localhost:9092",
    )
    await consumer.start()
    try:
        async for msg in consumer:
            neur = Neur_net()
            data = pickle.loads(msg.value)
            data["answer"] = neur.get_predict(data["raw_image"])
            await send_answer(data)
    finally:
        await consumer.stop()

if __name__ == '__main__':
    asyncio.run(consume())
