'use strict';

const path       = require('path');
const express    = require('express');
const cors       = require('cors');
const rateLimit  = require('express-rate-limit');
const grpc       = require('@grpc/grpc-js');
const loader     = require('@grpc/proto-loader');
const { createHandler } = require('graphql-http/lib/use/express');
const { buildSchema }   = require('graphql');

// ── Load proto definitions ────────────────────────────────────────────────────
function loadProto(file) {
  return loader.loadSync(path.join(__dirname, '../proto', file), {
    keepCase: false, longs: String, enums: String, defaults: true, oneofs: true,
  });
}

const { hospital: patientPkg }      = grpc.loadPackageDefinition(loadProto('patient.proto'));
const { hospital: appointmentPkg }  = grpc.loadPackageDefinition(loadProto('appointment.proto'));
const { hospital: notificationPkg } = grpc.loadPackageDefinition(loadProto('notification.proto'));

// ── gRPC Clients ──────────────────────────────────────────────────────────────
const ins = grpc.credentials.createInsecure();
const patientClient      = new patientPkg.PatientService('localhost:50051', ins);
const appointmentClient  = new appointmentPkg.AppointmentService('localhost:50052', ins);
const notificationClient = new notificationPkg.NotificationService('localhost:50053', ins);

// ── Promisify gRPC calls ───────────────────────────────────────────────────────
function call(client, method, req) {
  return new Promise((resolve, reject) => {
    client[method](req, (err, res) => err ? reject(err) : resolve(res));
  });
}

// ── Express Setup ─────────────────────────────────────────────────────────────
const app  = express();
const PORT = 3000;

app.use(express.json());
app.use(cors());
app.use(rateLimit({ windowMs: 15 * 60 * 1000, max: 200,
  message: { error: 'Trop de requêtes — réessayez dans 15 minutes.' } }));

// ══════════════════════════════════════════════════════════════════════════════
// REST — Patients
// ══════════════════════════════════════════════════════════════════════════════
app.get('/patients', async (req, res) => {
  try {
    const { patients } = await call(patientClient, 'listPatients', {});
    res.json(patients);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.get('/patients/:id', async (req, res) => {
  try {
    const { patient } = await call(patientClient, 'getPatient', { id: req.params.id });
    res.json(patient);
  } catch (e) { res.status(404).json({ error: 'Patient not found' }); }
});

app.post('/patients', async (req, res) => {
  try {
    const { patient } = await call(patientClient, 'createPatient', req.body);
    res.status(201).json(patient);
  } catch (e) { res.status(400).json({ error: e.message }); }
});

app.put('/patients/:id', async (req, res) => {
  try {
    const { patient } = await call(patientClient, 'updatePatient', { id: req.params.id, ...req.body });
    res.json(patient);
  } catch (e) { res.status(400).json({ error: e.message }); }
});

app.delete('/patients/:id', async (req, res) => {
  try {
    await call(patientClient, 'deletePatient', { id: req.params.id });
    res.status(204).send();
  } catch (e) { res.status(400).json({ error: e.message }); }
});

// ══════════════════════════════════════════════════════════════════════════════
// REST — Appointments
// ══════════════════════════════════════════════════════════════════════════════
app.get('/appointments', async (req, res) => {
  try {
    const { appointments } = await call(appointmentClient, 'listAppointments', { patientId: req.query.patientId || '' });
    res.json(appointments);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.get('/appointments/:id', async (req, res) => {
  try {
    const { appointment } = await call(appointmentClient, 'getAppointment', { id: req.params.id });
    res.json(appointment);
  } catch (e) { res.status(404).json({ error: 'Appointment not found' }); }
});

app.post('/appointments', async (req, res) => {
  try {
    const { appointment } = await call(appointmentClient, 'createAppointment', req.body);
    res.status(201).json(appointment);
  } catch (e) { res.status(400).json({ error: e.message }); }
});

app.put('/appointments/:id', async (req, res) => {
  try {
    const { appointment } = await call(appointmentClient, 'updateAppointment', { id: req.params.id, ...req.body });
    res.json(appointment);
  } catch (e) { res.status(400).json({ error: e.message }); }
});

app.delete('/appointments/:id', async (req, res) => {
  try {
    await call(appointmentClient, 'cancelAppointment', { id: req.params.id });
    res.status(204).send();
  } catch (e) { res.status(400).json({ error: e.message }); }
});

// ══════════════════════════════════════════════════════════════════════════════
// REST — Notifications
// ══════════════════════════════════════════════════════════════════════════════
app.get('/notifications', async (req, res) => {
  try {
    const { notifications } = await call(notificationClient, 'listNotifications', { patientId: req.query.patientId || '' });
    res.json(notifications);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.post('/notifications', async (req, res) => {
  try {
    const { notification } = await call(notificationClient, 'sendNotification', req.body);
    res.status(201).json(notification);
  } catch (e) { res.status(400).json({ error: e.message }); }
});

// ══════════════════════════════════════════════════════════════════════════════
// GraphQL Schema
// ══════════════════════════════════════════════════════════════════════════════
const schema = buildSchema(`
  type Patient {
    id: String!
    firstName: String!
    lastName: String!
    dateOfBirth: String
    bloodType: String
    email: String
    phone: String
    condition: String
  }

  type Appointment {
    id: String!
    patientId: String!
    doctorName: String!
    department: String!
    dateTime: String!
    status: String!
    notes: String
  }

  type Notification {
    id: String!
    patientId: String!
    type: String!
    message: String!
    status: String!
    createdAt: String!
  }

  type Query {
    patients: [Patient!]!
    patient(id: String!): Patient
    appointments(patientId: String): [Appointment!]!
    appointment(id: String!): Appointment
    notifications(patientId: String): [Notification!]!
  }

  type Mutation {
    createPatient(
      firstName: String!, lastName: String!, dateOfBirth: String,
      bloodType: String, email: String!, phone: String, condition: String
    ): Patient

    updatePatient(
      id: String!, firstName: String, lastName: String, dateOfBirth: String,
      bloodType: String, email: String, phone: String, condition: String
    ): Patient

    deletePatient(id: String!): Boolean

    createAppointment(
      patientId: String!, doctorName: String!, department: String!,
      dateTime: String!, notes: String
    ): Appointment

    cancelAppointment(id: String!): Boolean

    sendNotification(patientId: String!, type: String!, message: String!): Notification
  }
`);

// ══════════════════════════════════════════════════════════════════════════════
// GraphQL Resolvers (root value)
// ══════════════════════════════════════════════════════════════════════════════
const rootValue = {
  // Queries
  patients:     async ()          => (await call(patientClient, 'listPatients', {})).patients,
  patient:      async ({ id })    => (await call(patientClient, 'getPatient', { id })).patient,
  appointments: async ({ patientId }) => (await call(appointmentClient, 'listAppointments', { patientId: patientId || '' })).appointments,
  appointment:  async ({ id })    => (await call(appointmentClient, 'getAppointment', { id })).appointment,
  notifications:async ({ patientId }) => (await call(notificationClient, 'listNotifications', { patientId: patientId || '' })).notifications,

  // Mutations
  createPatient: async (args) => (await call(patientClient, 'createPatient', args)).patient,
  updatePatient: async (args) => (await call(patientClient, 'updatePatient', args)).patient,
  deletePatient: async ({ id }) => { await call(patientClient, 'deletePatient', { id }); return true; },

  createAppointment: async (args) => (await call(appointmentClient, 'createAppointment', args)).appointment,
  cancelAppointment: async ({ id }) => { await call(appointmentClient, 'cancelAppointment', { id }); return true; },

  sendNotification: async (args) => (await call(notificationClient, 'sendNotification', args)).notification,
};

// Mount GraphQL endpoint
app.use('/graphql', createHandler({ schema, rootValue }));

// ── Health check ───────────────────────────────────────────────────────────────
app.get('/health', (req, res) => res.json({
  status: 'ok',
  service: 'smart-hospital-api-gateway',
  version: '1.0.0',
  timestamp: new Date().toISOString(),
}));

// ── 404 handler ───────────────────────────────────────────────────────────────
app.use((req, res) => res.status(404).json({ error: `Route ${req.method} ${req.path} not found` }));

// ── Start ──────────────────────────────────────────────────────────────────────
app.listen(PORT, () => {
  console.log(`
╔══════════════════════════════════════════════════════╗
║   Smart Hospital Management System — API Gateway     ║
╠══════════════════════════════════════════════════════╣
║  REST    →  http://localhost:${PORT}                    ║
║  GraphQL →  http://localhost:${PORT}/graphql            ║
║  Health  →  http://localhost:${PORT}/health             ║
╚══════════════════════════════════════════════════════╝
  `);
});
