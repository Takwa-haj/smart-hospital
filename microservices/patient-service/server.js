'use strict';

const path   = require('path');
const grpc   = require('@grpc/grpc-js');
const loader = require('@grpc/proto-loader');
const { v4: uuidv4 } = require('uuid');
const initSqlJs = require('sql.js');
const { Kafka } = require('kafkajs');

// ── Proto ────────────────────────────────────────────────────────────────────
const PROTO_PATH = path.join(__dirname, '../../proto/patient.proto');
const pkgDef = loader.loadSync(PROTO_PATH, {
  keepCase: false, longs: String, enums: String, defaults: true, oneofs: true,
});
const { hospital } = grpc.loadPackageDefinition(pkgDef);

// ── SQLite (sql.js — pure JS, no native build needed) ───────────────────────
let db;
async function initDB() {
  const SQL = await initSqlJs();
  db = new SQL.Database();
  db.run(`
    CREATE TABLE IF NOT EXISTS patients (
      id          TEXT PRIMARY KEY,
      firstName   TEXT NOT NULL,
      lastName    TEXT NOT NULL,
      dateOfBirth TEXT,
      bloodType   TEXT,
      email       TEXT UNIQUE,
      phone       TEXT,
      condition   TEXT
    )
  `);
  console.log('[PatientService] SQLite initialised (in-memory)');
  // seed sample data
  seedPatients();
}

function seedPatients() {
  const samples = [
    { id: uuidv4(), firstName: 'Ali', lastName: 'Ben Salah', dateOfBirth: '1985-03-12', bloodType: 'A+', email: 'ali.bensalah@hospital.tn', phone: '+21622001001', condition: 'Hypertension' },
    { id: uuidv4(), firstName: 'Amira', lastName: 'Trabelsi', dateOfBirth: '1990-07-24', bloodType: 'O-', email: 'amira.trabelsi@hospital.tn', phone: '+21622001002', condition: 'Diabète Type 2' },
    { id: uuidv4(), firstName: 'Khaled', lastName: 'Mansour', dateOfBirth: '1978-11-05', bloodType: 'B+', email: 'khaled.mansour@hospital.tn', phone: '+21622001003', condition: 'Asthme' },
  ];
  for (const p of samples) {
    db.run('INSERT OR IGNORE INTO patients VALUES (?,?,?,?,?,?,?,?)',
      [p.id, p.firstName, p.lastName, p.dateOfBirth, p.bloodType, p.email, p.phone, p.condition]);
  }
  console.log('[PatientService] Seed data inserted');
}

function rowToPatient(row) {
  return {
    id: row[0], firstName: row[1], lastName: row[2], dateOfBirth: row[3],
    bloodType: row[4], email: row[5], phone: row[6], condition: row[7],
  };
}

// ── Kafka Producer ────────────────────────────────────────────────────────────
let producer;
async function initKafka() {
  try {
    const kafka = new Kafka({ clientId: 'patient-service', brokers: ['localhost:9092'] });
    producer = kafka.producer();
    await producer.connect();
    console.log('[PatientService] Kafka producer connected');
  } catch (e) {
    console.warn('[PatientService] Kafka not available – events will be skipped:', e.message);
    producer = null;
  }
}

async function emitEvent(topic, event) {
  if (!producer) return;
  try {
    await producer.send({ topic, messages: [{ key: event.id, value: JSON.stringify(event) }] });
  } catch (e) {
    console.warn('[PatientService] Kafka emit failed:', e.message);
  }
}

// ── gRPC Handlers ─────────────────────────────────────────────────────────────
function createPatient(call, callback) {
  const { firstName, lastName, dateOfBirth, bloodType, email, phone, condition } = call.request;
  const id = uuidv4();
  try {
    db.run('INSERT INTO patients VALUES (?,?,?,?,?,?,?,?)',
      [id, firstName, lastName, dateOfBirth, bloodType, email, phone, condition]);
    const patient = { id, firstName, lastName, dateOfBirth, bloodType, email, phone, condition };
    emitEvent('patient.created', patient);
    callback(null, { patient });
  } catch (err) {
    callback({ code: grpc.status.INTERNAL, message: err.message });
  }
}

function getPatient(call, callback) {
  const stmt = db.prepare('SELECT * FROM patients WHERE id = ?');
  const rows = stmt.getAsObject([call.request.id]);
  if (!rows.id) return callback({ code: grpc.status.NOT_FOUND, message: 'Patient not found' });
  callback(null, {
    patient: {
      id: rows.id, firstName: rows.firstName, lastName: rows.lastName,
      dateOfBirth: rows.dateOfBirth, bloodType: rows.bloodType,
      email: rows.email, phone: rows.phone, condition: rows.condition,
    },
  });
}

function listPatients(call, callback) {
  const res = db.exec('SELECT * FROM patients');
  const patients = res.length ? res[0].values.map(rowToPatient) : [];
  callback(null, { patients });
}

function updatePatient(call, callback) {
  const { id, firstName, lastName, dateOfBirth, bloodType, email, phone, condition } = call.request;
  db.run('UPDATE patients SET firstName=?,lastName=?,dateOfBirth=?,bloodType=?,email=?,phone=?,condition=? WHERE id=?',
    [firstName, lastName, dateOfBirth, bloodType, email, phone, condition, id]);
  const patient = { id, firstName, lastName, dateOfBirth, bloodType, email, phone, condition };
  emitEvent('patient.updated', patient);
  callback(null, { patient });
}

function deletePatient(call, callback) {
  db.run('DELETE FROM patients WHERE id = ?', [call.request.id]);
  emitEvent('patient.deleted', { id: call.request.id });
  callback(null, { success: true });
}

// ── Server Start ──────────────────────────────────────────────────────────────
async function main() {
  await initDB();
  await initKafka();

  const server = new grpc.Server();
  server.addService(hospital.PatientService.service, {
    createPatient, getPatient, listPatients, updatePatient, deletePatient,
  });

  server.bindAsync('0.0.0.0:50051', grpc.ServerCredentials.createInsecure(), (err, port) => {
    if (err) { console.error(err); process.exit(1); }
    console.log(`[PatientService] gRPC server running on port ${port}`);
  });
}

main().catch(console.error);
