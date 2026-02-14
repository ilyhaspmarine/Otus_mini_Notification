import os
from confluent_kafka import Consumer, KafkaError
import json
from typing import Callable, Any
import asyncio
from notif_models import OrderUpdateMessage
from threading import Thread, Event
from pydantic import ValidationError

class KafkaCons:
    def __init__(self):
        self.__bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
        self.__topic = os.getenv("KAFKA_ORDERS_TOPIC", "orders")
        self.__password = os.getenv("KAFKA_PASSWORD")
        self.__user = os.getenv("KAFKA_USER")
        self.__consumer = None
        self.__thread = None
        self.__stop_event = Event()
        self.__message_handler = None
        self.__loop = None

    
    async def init_connection(
        self,
        message_handler: Callable[[OrderUpdateMessage], Any]
    ):
        
        self.__message_handler = message_handler
        self.__loop = asyncio.get_running_loop()

        # Создание консьюмера (синхронная операция)
        self.__consumer = Consumer({
            'bootstrap.servers': self.__bootstrap_servers,
            'security.protocol': 'SASL_PLAINTEXT',
            'sasl.mechanism': 'PLAIN',
            'sasl.username': self.__user,
            'sasl.password': self.__password,
            'group.id': "notification-service",
            'auto.offset.reset': 'earliest',  # читать с начала, если нет коммита
            'enable.auto.commit': True,       # автоматический коммит оффсетов
            'auto.commit.interval.ms': 1000,  # коммит каждую секунду
            'session.timeout.ms': 30000,
            'max.poll.interval.ms': 300000    # 5 минут на обработку сообщения
        })

        # Подписка на топик
        self.__consumer.subscribe([self.__topic])
        print(f"Subscribed to topic: {self.__topic}")

        # Тестовое подключение (проверка метаданных)
        await self.__test_connection()

        # Запуск фонового потока для чтения сообщений
        self.__stop_event.clear()
        self.__thread = Thread(target=self.__consume_messages, daemon=True)
        self.__thread.start()
        
        print("✅ Kafka consumer initialized and started")


    async def __test_connection(self):
        """Тестовое подключение к брокеру"""
        try:
            # Получаем метаданные кластера (проверка подключения)
            metadata = self.__consumer.list_topics(timeout=10)
            
            if self.__topic not in metadata.topics:
                print(f"Topic '{self.__topic}' not found. It will be created on first message.")
            else:
                print(f"Topic '{self.__topic}' exists with {len(metadata.topics[self.__topic].partitions)} partitions")
            
        except Exception as e:
            raise Exception(f"Kafka connection failed: {e}")        


    def __consume_messages(self):
        """Фоновый поток для потребления сообщений"""
        print("Kafka consumer thread started")
        
        while not self.__stop_event.is_set():
            try:
                # poll() блокируется на указанное время (мс)
                msg = self.__consumer.poll(1.0)  # таймаут 1 сек
                
                if msg is None:
                    continue
                
                if msg.error():
                    # EOF - конец партиции (нормально при чтении с начала)
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    else:
                        print(f"Consumer error: {msg.error()}")
                        continue
                
                # Обработка сообщения
                self.__handle_message(msg)
                
            except Exception as e:
                print(f"Error in consumer thread: {e}")
        
        print("Kafka consumer thread stopped")        

    
    async def close(self):
        """Корректное закрытие консьюмера"""
        print("Stopping Kafka consumer...")
        
        # Сигнализируем потоку остановиться
        self.__stop_event.set()
        
        # Ждём завершения потока (максимум 5 сек)
        if self.__thread and self.__thread.is_alive():
            self.__thread.join(timeout=5.0)
        
        # Закрываем консьюмер
        if self.__consumer:
            try:
                # Коммитим оффсеты перед закрытием
                self.__consumer.commit(asynchronous=False)
            except Exception as e:
                print(f"Failed to commit offsets: {e}")
            finally:
                self.__consumer.close()
                self.__consumer = None
        
        print("✅ Kafka consumer closed")


    def __handle_message(self, msg):
        """Обработка одного сообщения"""
        try:
            # Декодируем значение сообщения
            message_bytes = msg.value()
            if message_bytes is None:
                print("Received message with empty value")
                return
            
            message_str = message_bytes.decode('utf-8')
            message_dict = json.loads(message_str)
            
            # Валидация через Pydantic модель
            order_message = OrderUpdateMessage(**message_dict)
            
            print(f"📩 Received order update: {order_message.order_id}, event: {order_message.event}")
            
            # Вызываем обработчик сообщения (асинхронный)
            if self.__message_handler and self.__loop:
                asyncio.run_coroutine_threadsafe(
                    self.__message_handler(order_message),
                    self.__loop
                )
            
        except json.JSONDecodeError as e:
            print(f"Failed to decode JSON message: {e}")
        except ValidationError as e:
            print(f"Message validation failed: {e}")
        except Exception as e:
            print(f"Error handling message: {e}")