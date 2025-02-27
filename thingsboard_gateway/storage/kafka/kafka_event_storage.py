# ------------------------------------------------------------------------------
#      Copyright 2025. ThingsBoard
#  #
#      Licensed under the Apache License, Version 2.0 (the "License");
#      you may not use this file except in compliance with the License.
#      You may obtain a copy of the License at
#  #
#          http://www.apache.org/licenses/LICENSE-2.0
#  #
#      Unless required by applicable law or agreed to in writing, software
#      distributed under the License is distributed on an "AS IS" BASIS,
#      WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#      See the License for the specific language governing permissions and
#      limitations under the License.
#
# ------------------------------------------------------------------------------

from threading import Thread, Event
from logging import getLogger
from orjson import dumps

from thingsboard_gateway.storage.event_storage import EventStorage
from thingsboard_gateway.storage.kafka.storage_settings import StorageSettings
from thingsboard_gateway.tb_utility.tb_logger import TbLogger
from thingsboard_gateway.tb_utility.tb_utility import TBUtility

try:
    import asyncio
except ImportError:
    TBUtility.install_package("asyncio")
    import asyncio

try:
    from aiokafka import AIOKafkaProducer, AIO
except ImportError:
    TBUtility.install_package("aiokafka")
    from aiokafka import AIOKafkaProducer

from aiokafka.admin import AIOKafkaAdminClient, NewTopic
from aiokafka.errors import TopicAlreadyExistsError, ProducerClosed


class KafkaEventStorage(EventStorage):
    def __init__(self, config, logger: TbLogger, main_stop_event):
        super().__init__(config, logger, main_stop_event)
        self.__log = logger
        self.__stop_event = Event()
        self.__settings = StorageSettings(config)
        self.__producer = None
        self.__loop = asyncio.new_event_loop()
        self.__loop_thread = Thread(target=self.__run_event_loop, daemon=True)
        self.__loop_thread.start()

        asyncio.run_coroutine_threadsafe(self.__init_kafka(), self.__loop)

    async def __init_kafka(self):
        await self.__ensure_topic_exists()
        self.__producer = AIOKafkaProducer(
            bootstrap_servers=self.__settings.bootstrap_servers,
            value_serializer=lambda v: dumps(v)
        )
        await self.__producer.start()
        self.__log.info("Kafka producer initialized successfully.")

    def put(self, event):
        if not self.__producer:
            self.__log.error("Kafka producer is not initialized. Cannot send message.")
            return False

        future = asyncio.run_coroutine_threadsafe(
            self.__producer.send_and_wait(self.__settings.topic, event), self.__loop
        )
        try:
            future.result(timeout=1)
            return True
        except ProducerClosed:
            pass
        except Exception as e:
            self.__log.error("Failed to send event to Kafka: %r", e, exc_info=e)
        return False

    def stop(self):
        self.__log.info("Shutting down Kafka storage...")

        if self.__producer:
            asyncio.run_coroutine_threadsafe(self.__producer.stop(), self.__loop).result()
            self.__producer = None

        if not self.__loop.is_closed():
            pending = asyncio.all_tasks(self.__loop)
            for task in pending:
                task.cancel()

            self.__loop.call_soon_threadsafe(self.__loop.stop)

        self.__loop_thread.join()

        self.__log.info("Kafka storage stopped successfully.")

    async def __ensure_topic_exists(self):
        admin_client = AIOKafkaAdminClient(bootstrap_servers=self.__settings.bootstrap_servers)
        await admin_client.start()
        existing_topics = await admin_client.list_topics()

        if self.__settings.topic not in existing_topics:
            self.__log.info(f"Creating topic: {self.__settings.topic}...")
            try:
                topic_config = NewTopic(
                    name=self.__settings.topic,
                    num_partitions=self.__settings.partitions,
                    replication_factor=self.__settings.replication_factor
                )
                await admin_client.create_topics([topic_config])
                self.__log.info("Topic created.")
            except TopicAlreadyExistsError:
                pass
        else:
            self.__log.info(f"Topic {self.__settings.topic} found on Kafka server")

        await admin_client.close()

    def __run_event_loop(self):
        try:
            self.__loop.run_forever()
        finally:
            self.__loop.close()

    def event_pack_processing_done(self):
        pass

    def get_event_pack(self):
        return []

    def len(self):
        return 0

    def update_logger(self):
        self.__log = getLogger("storage")
