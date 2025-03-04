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

from threading import Thread, Event, Lock
from logging import getLogger
from time import time

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
        self.__queue = asyncio.Queue(maxsize=100_000)
        self.__loop_thread = Thread(target=self.__run_event_loop, daemon=True, name="KafkaStorageLoop")
        self.__loop_thread.start()

        self.__put_counter = 0
        self.__sent_counter = 0
        self.__counter_lock = Lock()

        future = asyncio.run_coroutine_threadsafe(self.__init_kafka(), self.__loop)
        result = future.result()
        if not result:
            self.stop()
        asyncio.run_coroutine_threadsafe(self.__message_worker(), self.__loop)
        asyncio.run_coroutine_threadsafe(self.__log_stats_worker(), self.__loop)

    async def __init_kafka(self):
        connection_try = 0
        delay = 5
        while (not self.__stop_event.is_set()
               and not self._main_stop_event.is_set()
               and connection_try < 3):
            try:
                connection_try += 1
                await self.__ensure_topic_exists()
                self.__producer = AIOKafkaProducer(
                    bootstrap_servers=self.__settings.bootstrap_servers,
                    value_serializer=lambda v: dumps(v),
                    linger_ms=self.__settings.linger_ms,
                    max_batch_size=self.__settings.max_batch_size
                )
                self.__producer.log = self.__log
                await self.__producer.start()
                self.__log.info("Kafka producer initialized successfully for servers: %s to topic %s",
                                self.__settings.bootstrap_servers,
                                self.__settings.topic)
                return True
            except Exception as e:
                self.__log.error("Failed to initialize Kafka producer: %r. Retrying...", e, exc_info=e)
                await asyncio.sleep(delay)
                delay *= 2
        return False

    def put(self, event):
        try:
            self.__loop.call_soon_threadsafe(self.__queue.put_nowait, event)
            with self.__counter_lock:
                self.__put_counter += 1
            return True
        except asyncio.QueueFull:
            self.__log.warning("Kafka event queue is full. Dropping event.")
            return False

    # async def __message_worker(self):
    #     """ Message worker with manual batching """
    #     while not self.__stop_event.is_set():
    #         try:
    #             try:
    #                 first_event = await asyncio.wait_for(self.__queue.get(), timeout=0.1)
    #             except asyncio.TimeoutError:
    #                 continue
    #
    #             batch = self.__producer.create_batch()
    #             current_ts = int(time() * 1000)
    #             if not batch.append(key=None,
    #                                 value=first_event,
    #                                 timestamp=current_ts):
    #                 self.__log.error("Event too large to append to an empty batch: %s", first_event)
    #             self.__queue.task_done()
    #             with self.__counter_lock:
    #                 self.__sent_counter += 1
    #
    #             start_time = self.__loop.time()
    #             while batch.record_count() < self.__settings.batch_size and (self.__loop.time() - start_time) < 1:
    #                 try:
    #                     event = await asyncio.wait_for(self.__queue.get(), timeout=0.005)
    #                 except asyncio.TimeoutError:
    #                     break
    #                 if not batch.append(key=None,
    #                                     value=event,
    #                                     timestamp=current_ts):
    #                     await self.__producer.send_batch(batch, self.__settings.topic, partition=0)
    #                     batch = self.__producer.create_batch()
    #                     if not batch.append(key=None,
    #                                         value=event,
    #                                         timestamp=current_ts):
    #                         self.__log.error("Event too large for an empty batch: %s", event)
    #                 self.__queue.task_done()
    #                 with self.__counter_lock:
    #                     self.__sent_counter += 1
    #
    #             if batch.record_count() > 0:
    #                 await self.__producer.send_batch(batch, self.__settings.topic, partition=)
    #         except ProducerClosed:
    #             break
    #         except Exception as e:
    #             self.__log.error("Error in Kafka producer worker: %r", e, exc_info=e)

    async def __message_worker(self):
        while not self.__stop_event.is_set():
            try:
                try:
                    event = await asyncio.wait_for(self.__queue.get(), timeout=0.1)
                except asyncio.TimeoutError:
                    continue

                current_ts = int(time() * 1000)
                await self.__producer.send(self.__settings.topic, event, timestamp_ms=current_ts)
                self.__queue.task_done()
                with self.__counter_lock:
                    self.__sent_counter += 1

            except ProducerClosed:
                self.__stop_event.wait(1)
                continue
            except Exception as e:
                self.__log.error("Error in Kafka producer worker: %r", e, exc_info=e)

    async def __log_stats_worker(self):
        while not self.__stop_event.is_set():
            await asyncio.sleep(1)
            with self.__counter_lock:
                put_count = self.__put_counter
                sent_count = self.__sent_counter
                self.__put_counter = 0
                self.__sent_counter = 0
            if put_count > 0 or sent_count > 0:
                self.__log.info("Events queued per second: %d, events sent per second: %d, queue size: %d, approx sent size: %.2f MB",
                                put_count, sent_count, self.__queue.qsize(), (sent_count * 2.38)/1024.0)

    def stop(self):
        self.__log.info("Shutting down Kafka storage...")

        if self.__producer:
            asyncio.run_coroutine_threadsafe(self.__producer.stop(), self.__loop).result()
            self.__producer = None

        self.__stop_event.set()

        async def get_pending_tasks():
            return asyncio.all_tasks(self.__loop)

        if not self.__loop.is_closed():
            pending = asyncio.run_coroutine_threadsafe(get_pending_tasks(), self.__loop).result()
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
                    replication_factor=self.__settings.replication_factor,
                    replica_assignments=self.__settings.replica_assignments,
                    topic_configs=self.__settings.topic_configs
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
