import { Kafka, Producer, CompressionTypes } from 'kafkajs';
import { Event } from './types';

const kafka = new Kafka({
  clientId: 'event-producer',
  brokers: [process.env.KAFKA_BROKER || 'localhost:9092'],
});

let producer: Producer;

export async function initProducer(): Promise<void> {
  producer = kafka.producer();
  await producer.connect();
  console.log('Producer connected');
}

/**
 * Отправляет событие в Kafka
 */
export async function sendEvent(event: Event): Promise<void> {
  if (!producer) {
    throw new Error('Producer not initialized');
  }

  await producer.send({
    topic: 'events',
    messages: [
      {
        key: event.userId, // Партиционирование по userId
        value: JSON.stringify(event),
        headers: {
          'event-type': event.type,
          'event-id': event.id,
        },
      },
    ],
    acks: 1,
    compression: CompressionTypes.None,
  });
}

/**
 * Отправляет batch событий
 */
export async function sendEventsBatch(events: Event[]): Promise<void> {
  if (!producer) {
    throw new Error('Producer not initialized');
  }

  const messages = events.map((event) => ({
    key: event.userId,
    value: JSON.stringify(event),
    headers: {
      'event-type': event.type,
      'event-id': event.id,
    },
  }));

  await producer.send({
    topic: 'events',
    messages,
    acks: 1,
    compression: CompressionTypes.None,
  });
}

export async function disconnectProducer(): Promise<void> {
  if (producer) {
    await producer.disconnect();
    console.log('Producer disconnected');
  }
}
// v2.3
