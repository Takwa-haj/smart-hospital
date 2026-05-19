'use strict';

const path   = require('path');
const grpc   = require('@grpc/grpc-js');
const loader = require('@grpc/proto-loader');
const { v4: uuidv4 } = require('uuid');
const { Kafka } = require('kafkajs');

// ── RxDB ──────────────────────────────────────────────────────────────────────
const { createRxDatabase, addRxPlugin } = require('rxdb');
const { getRxStorageMemory }            = require('rxdb/plugins/storage-memory');
const { RxDBQueryBuilderPlugin }        = require('rxdb/plugins/query-builder');

addRxPlugin(RxDBQueryBuilderPlugin);

const notificationSchema = {
  version: 0,
  primaryKey: 'id',
  type: 'object',
  properties: {
    id:        { type: 'string', maxLength: 36 },
    patientId: { type: 'string', maxLength: 36 },
    type:      { type: 'string', maxLength: 100 },
    message:   { type: 'string', maxLength: 1000 },
    status:    { type: 'string', maxLength: 50 },
    createdAt: { type: 'string', maxLength: 50 },
  },
  required: ['id', 'patientId', 'type', 'message', 'status', 'createdAt'],
};

let notificationsCollection;

async function initRxDB() {
  const db = await createRxDatabase({ name: 'notificationsdb', storage: getRxStorageMemory() });
  await db.addCollections({ notifications: { schema: notificationSchema } });
  notificationsCollection = db.notifications;
  console.log('[NotificationService] RxDB initialised');
}

// ── Proto ─────────────────────────────────────────────────────────────────────
const PROTO_PATH = path.join(__dirname, '../../proto/notification.proto');
const pkgDef = loader.loadSync(PROTO_PATH, {
  keepCase: false, longs: String, enums: String, defaults: true, oneofs: true,
});
const { hospital } = grpc.loadPackageDefinition(pkgDef);

// ── Kafka Consumer ─────────────────────────────────────────────────────────────
async function initKafkaConsumer() {
  try {
    const kafka = new Kafka({ clientId: 'notification-service', brokers: ['localhost:9092'] });
    const consumer = kafka.consumer({ groupId: 'notification-group' });
    await consumer.connect();
    await consumer.subscribe({ topics: ['appointment.created', 'appointment.cancelled', 'patient.created'], fromBeginning: false });
    await consumer.run({
      eachMessage: async ({ topic, message }) => {
        const event = JSON.parse(message.value.toString());
        let notifMsg = '';
        let type = '';
        if (topic === 'appointment.created') {
          type = 'APPOINTMENT_REMINDER';
          notifMsg = `Votre rendez-vous avec ${event.doctorName} (${event.department}) est confirmé pour le ${event.dateTime}.`;
        } else if (topic === 'appointment.cancelled') {
          type = 'APPOINTMENT_CANCELLED';
          notifMsg = `Votre rendez-vous a été annulé.`;
        } else if (topic === 'patient.created') {
          type = 'WELCOME';
          notifMsg = `Bienvenue ${event.firstName} ${event.lastName} à l'Hôpital Smart !`;
        }
        if (type && event.patientId) {
          await saveNotification(event.patientId, type, notifMsg);
        }
      },
    });
    console.log('[NotificationService] Kafka consumer running');
  } catch (e) {
    console.warn('[NotificationService] Kafka not available:', e.message);
  }
}

async function saveNotification(patientId, type, message) {
  const doc = {
    id: uuidv4(),
    patientId,
    type,
    message,
    status: 'sent',
    createdAt: new Date().toISOString(),
  };
  await notificationsCollection.insert(doc);
  console.log(`[NotificationService] Notification sent to patient ${patientId}: ${type}`);
  return doc;
}

// ── gRPC Handlers ─────────────────────────────────────────────────────────────
async function sendNotification(call, callback) {
  const { patientId, type, message } = call.request;
  try {
    const doc = await saveNotification(patientId, type, message);
    callback(null, { notification: doc });
  } catch (err) {
    callback({ code: grpc.status.INTERNAL, message: err.message });
  }
}

async function listNotifications(call, callback) {
  const { patientId } = call.request;
  try {
    let query;
    if (patientId) {
      query = notificationsCollection.find().where('patientId').equals(patientId);
    } else {
      query = notificationsCollection.find();
    }
    const docs = await query.exec();
    const notifications = docs.map(d => d.toJSON());
    callback(null, { notifications });
  } catch (err) {
    callback({ code: grpc.status.INTERNAL, message: err.message });
  }
}

// ── Server Start ──────────────────────────────────────────────────────────────
async function main() {
  await initRxDB();
  await initKafkaConsumer();

  const server = new grpc.Server();
  server.addService(hospital.NotificationService.service, {
    sendNotification, listNotifications,
  });

  server.bindAsync('0.0.0.0:50053', grpc.ServerCredentials.createInsecure(), (err, port) => {
    if (err) { console.error(err); process.exit(1); }
    console.log(`[NotificationService] gRPC server running on port ${port}`);
  });
}

main().catch(console.error);
