from aiokafka import AIOKafkaProducer
from aiokafka import AIOKafkaConsumer
import asyncio
from dotenv import load_dotenv
import os
from aiogram import Bot, Dispatcher, executor, types
import io
import pickle


async def send_image(image):
    producer = AIOKafkaProducer(bootstrap_servers="localhost:9092")
    await producer.start()
    try:
        await producer.send_and_wait("to_neur", pickle.dumps(image))
    finally:
        await producer.stop()


load_dotenv()
API_TOKEN = os.getenv("API_TOKEN")
if API_TOKEN is None:
    print("Please create api token")
    exit()
bot = Bot(token=API_TOKEN)
dp = Dispatcher(bot)


@dp.message_handler(content_types=["photo"])
async def send_welcome(message: types.Message):
    id_ = message.from_id
    data_image = await bot.get_file(message.photo[-1].file_id)
    downloaded_file = await bot.download_file_by_id(data_image.file_id)
    image = {
        "id": id_,
        "image_type": data_image.file_path.split(".")[-1],
        "raw_image": downloaded_file.read(),
    }

    await send_image(image)


@dp.message_handler()
async def send_welcome(message: types.Message):
    id_ = message.from_id
    await bot.send_message(id_, "Hi!\nI'm CatDogBot!\n Please send image! ")


async def get_image():
    consumer = AIOKafkaConsumer(
        "answer",
        bootstrap_servers="localhost:9092",
    )

    await consumer.start()
    try:
        async for msg in consumer:
            data = pickle.loads(msg.value)
            await bot.send_photo(data["id"], data["raw_image"], caption=data["answer"])
    finally:
        await consumer.stop()


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.create_task(get_image())
    executor.start_polling(dp, skip_updates=False, loop=loop)

