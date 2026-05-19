'use strict';

const path   = require('path');
const grpc   = require('@grpc/grpc-js');
const loader = require('@grpc/proto-loader');
const { v4: uuidv4 } = require('uuid');
const initSqlJs = require('sql.js');
const { Kafka } = require('kafkajs');

// ── Proto ─────────────────────────────────────────────────────────────────────
const PROTO_PATH = path.join(__dirname, '../../proto/appointment.proto');
const pkgDef = loader.loadSync(PROTO_PATH, {
  keepCase: false, longs: String, enums: String, defaults: true, oneofs: true,
});
const { hospital } = grpc.loadPackageDefinition(pkgDef);

// ── SQLite ────────────────────────────────────────────────────────────────────
let db;
async function initDB() {
  const SQL = await initSqlJs();
  db = new SQL.Database();
  db.run(`
    CREATE TABLE IF NOT EXISTS appointments (
      id          TEXT PRIMARY KEY,
      patientId   TEXT NOT NULL,
      doctorName  TEXT NOT NULL,
      department  TEXT NOT NULL,
      dateTime    TEXT NOT NULL,
      status      TEXT DEFAULT 'scheduled',
      notes       TEXT
    )
  `);
  console.log('[AppointmentService] SQLite initialised');
}

function rowToAppt(row) {
  return { id: row[0], patientId: row[1], doctorName: row[2], department: row[3], dateTime: row[4], status: row[5], notes: row[6] || '' };
}

// ── Kafka ─────────────────────────────────────────────────────────────────────
let producer;
async function initKafka() {
  try {
    const kafka = new Kafka({ clientId: 'appointment-service', brokers: ['localhost:9092'] });
    producer = kafka.producer();
    await producer.connect();
    console.log('[AppointmentService] Kafka producer connected');
  } catch (e) {
    console.warn('[AppointmentService] Kafka not available:', e.message);
    producer = null;
  }
}

async function emitEvent(topic, event) {
  if (!producer) return;
  try {
    await producer.send({ topic, messages: [{ key: event.id, value: JSON.stringify(event) }] });
  } catch (e) {
    console.warn('[AppointmentService] Kafka emit failed:', e.message);
  }
}

// ── gRPC Handlers ─────────────────────────────────────────────────────────────
function createAppointment(call, callback) {
  const { patientId, doctorName, department, dateTime, notes } = call.request;
  const id = uuidv4();
  const status = 'scheduled';
  try {
    db.run('INSERT INTO appointments VALUES (?,?,?,?,?,?,?)',
      [id, patientId, doctorName, department, dateTime, status, notes || '']);
    const appointment = { id, patientId, doctorName, department, dateTime, status, notes: notes || '' };
    emitEvent('appointment.created', { ...appointment, type: 'APPOINTMENT_CREATED' });
    callback(null, { appointment });
  } catch (err) {
    callback({ code: grpc.status.INTERNAL, message: err.message });
  }
}

function getAppointment(call, callback) {
  const stmt = db.prepare('SELECT * FROM appointments WHERE id = ?');
  const row = stmt.getAsObject([call.request.id]);
  if (!row.id) return callback({ code: grpc.status.NOT_FOUND, message: 'Appointment not found' });
  callback(null, { appointment: { id: row.id, patientId: row.patientId, doctorName: row.doctorName, department: row.department, dateTime: row.dateTime, status: row.status, notes: row.notes || '' } });
}

function listAppointments(call, callback) {
  const { patientId } = call.request;
  let res;
  if (patientId) {
    res = db.exec('SELECT * FROM appointments WHERE patientId = ?', [patientId]);
  } else {
    res = db.exec('SELECT * FROM appointments');
  }
  const appointments = res.length ? res[0].values.map(rowToAppt) : [];
  callback(null, { appointments });
}

function updateAppointment(call, callback) {
  const { id, doctorName, department, dateTime, status, notes } = call.request;
  db.run('UPDATE appointments SET doctorName=?,department=?,dateTime=?,status=?,notes=? WHERE id=?',
    [doctorName, department, dateTime, status, notes || '', id]);
  const stmt = db.prepare('SELECT * FROM appointments WHERE id = ?');
  const row = stmt.getAsObject([id]);
  const appointment = { id: row.id, patientId: row.patientId, doctorName: row.doctorName, department: row.department, dateTime: row.dateTime, status: row.status, notes: row.notes || '' };
  if (status === 'cancelled') {
    emitEvent('appointment.cancelled', { ...appointment, type: 'APPOINTMENT_CANCELLED' });
  }
  callback(null, { appointment });
}

function cancelAppointment(call, callback) {
  const { id } = call.request;
  const stmt = db.prepare('SELECT * FROM appointments WHERE id = ?');
  const row = stmt.getAsObject([id]);
  db.run("UPDATE appointments SET status='cancelled' WHERE id=?", [id]);
  emitEvent('appointment.cancelled', { id, patientId: row.patientId, type: 'APPOINTMENT_CANCELLED' });
  callback(null, { success: true });
}

// ── Server Start ──────────────────────────────────────────────────────────────
async function main() {
  await initDB();
  await initKafka();

  const server = new grpc.Server();
  server.addService(hospital.AppointmentService.service, {
    createAppointment, getAppointment, listAppointments, updateAppointment, cancelAppointment,
  });

  server.bindAsync('0.0.0.0:50052', grpc.ServerCredentials.createInsecure(), (err, port) => {
    if (err) { console.error(err); process.exit(1); }
    console.log(`[AppointmentService] gRPC server running on port ${port}`);
  });
}

main().catch(console.error);
