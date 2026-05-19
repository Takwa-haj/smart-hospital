# 🏥 Smart Hospital Management System
**Matière :** SoA et Microservices | **Classe :** 4Info | **A.U. :** 2025/2026  
**Auteur :** Salah Gontara

---

## 📐 Architecture

```
Client (REST / GraphQL)
        │
        ▼
 ┌─────────────┐
 │ API Gateway │  ← Express + REST + GraphQL  (port 3000)
 └──────┬──────┘
        │  gRPC (HTTP/2 + Protobuf)
   ┌────┴─────────────────────┐
   │          │               │
   ▼          ▼               ▼
Patient   Appointment   Notification
Service    Service        Service
:50051     :50052         :50053
SQLite3    SQLite3        RxDB (NoSQL)
   │          │               │
   └────┬─────┘               │
        ▼                     ▼
   Kafka Broker  ──────► Consumer
   (Topics)               (Kafka)
```

---

## 🚀 Démarrage Rapide

### Sans Docker (développement local)

**Prérequis :** Node.js 20+, Kafka 4.x en mode KRaft

```bash
# Terminal 1 — Patient Service
cd microservices/patient-service && node server.js

# Terminal 2 — Appointment Service
cd microservices/appointment-service && node server.js

# Terminal 3 — Notification Service
cd microservices/notification-service && node server.js

# Terminal 4 — API Gateway
cd api-gateway && node server.js
```

### Avec Docker Compose (recommandé)

```bash
docker-compose up --build
```

---

## 📡 Endpoints REST

### Patients

| Méthode | Route           | Description                       |
|---------|-----------------|-----------------------------------|
| GET     | /patients       | Lister tous les patients          |
| GET     | /patients/:id   | Récupérer un patient par ID       |
| POST    | /patients       | Créer un nouveau patient          |
| PUT     | /patients/:id   | Mettre à jour un patient          |
| DELETE  | /patients/:id   | Supprimer un patient              |

### Appointments

| Méthode | Route              | Description                        |
|---------|--------------------|------------------------------------|
| GET     | /appointments      | Lister tous les rendez-vous        |
| GET     | /appointments/:id  | Récupérer un rendez-vous           |
| POST    | /appointments      | Créer un rendez-vous               |
| PUT     | /appointments/:id  | Mettre à jour un rendez-vous       |
| DELETE  | /appointments/:id  | Annuler un rendez-vous             |

### Notifications

| Méthode | Route              | Description                        |
|---------|--------------------|------------------------------------|
| GET     | /notifications     | Lister les notifications           |
| POST    | /notifications     | Envoyer une notification manuelle  |

---

## 🔮 GraphQL

**Endpoint :** `POST http://localhost:3000/graphql`

### Queries

```graphql
# Tous les patients
{ patients { id firstName lastName condition } }

# Un patient spécifique
{ patient(id: "uuid") { id firstName lastName bloodType email } }

# Rendez-vous d'un patient
{ appointments(patientId: "uuid") { id doctorName department dateTime status } }

# Toutes les notifications
{ notifications(patientId: "uuid") { id type message createdAt } }
```

### Mutations

```graphql
# Créer un patient
mutation {
  createPatient(
    firstName: "Sami", lastName: "Ben Ali",
    dateOfBirth: "1992-05-20", bloodType: "AB+",
    email: "sami@hospital.tn", phone: "+21622333444",
    condition: "Fracture bras gauche"
  ) { id firstName lastName }
}

# Créer un rendez-vous
mutation {
  createAppointment(
    patientId: "uuid",
    doctorName: "Dr. Karim Jendoubi",
    department: "Orthopédie",
    dateTime: "2026-05-20T10:00:00",
    notes: "Contrôle post-opératoire"
  ) { id status }
}
```

---

## 🔌 gRPC — Services & Méthodes

### PatientService (port 50051)
| Méthode        | Request                  | Response               |
|----------------|--------------------------|------------------------|
| CreatePatient  | CreatePatientRequest     | PatientResponse        |
| GetPatient     | GetPatientRequest        | PatientResponse        |
| ListPatients   | Empty                    | PatientListResponse    |
| UpdatePatient  | UpdatePatientRequest     | PatientResponse        |
| DeletePatient  | DeletePatientRequest     | DeleteResponse         |

### AppointmentService (port 50052)
| Méthode             | Request                       | Response                  |
|---------------------|-------------------------------|---------------------------|
| CreateAppointment   | CreateAppointmentRequest      | AppointmentResponse       |
| GetAppointment      | GetAppointmentRequest         | AppointmentResponse       |
| ListAppointments    | ListAppointmentsRequest       | AppointmentListResponse   |
| UpdateAppointment   | UpdateAppointmentRequest      | AppointmentResponse       |
| CancelAppointment   | CancelAppointmentRequest      | DeleteResponse            |

### NotificationService (port 50053)
| Méthode              | Request                        | Response                   |
|----------------------|--------------------------------|----------------------------|
| SendNotification     | SendNotificationRequest        | NotificationResponse       |
| ListNotifications    | ListNotificationsRequest       | NotificationListResponse   |

---

## 📨 Kafka — Topics

| Topic                    | Producteur           | Consommateur         | Événement                          |
|--------------------------|----------------------|----------------------|------------------------------------|
| `patient.created`        | PatientService       | NotificationService  | Nouveau patient enregistré         |
| `patient.updated`        | PatientService       | (extensible)         | Données patient modifiées          |
| `patient.deleted`        | PatientService       | AppointmentService   | Patient supprimé → nettoyage RDV   |
| `appointment.created`    | AppointmentService   | NotificationService  | RDV confirmé → notification envoyée|
| `appointment.cancelled`  | AppointmentService   | NotificationService  | RDV annulé → alerte patient        |

---

## 🗄️ Bases de données

| Microservice        | Technologie | Type   | Collections / Tables |
|---------------------|-------------|--------|----------------------|
| patient-service     | SQLite3      | SQL    | `patients`           |
| appointment-service | SQLite3      | SQL    | `appointments`       |
| notification-service| RxDB        | NoSQL  | `notifications`      |

---

## 🧪 Exemples de Tests (Postman / curl)

```bash
# Créer un patient
curl -X POST http://localhost:3000/patients \
  -H "Content-Type: application/json" \
  -d '{"firstName":"Sami","lastName":"Jebali","email":"sami@h.tn","bloodType":"O+","condition":"Diabète"}'

# Lister les patients
curl http://localhost:3000/patients

# Créer un rendez-vous
curl -X POST http://localhost:3000/appointments \
  -H "Content-Type: application/json" \
  -d '{"patientId":"<id>","doctorName":"Dr. Hedi","department":"Cardiologie","dateTime":"2026-06-01T09:00:00","notes":"Bilan annuel"}'

# Vérifier les notifications
curl http://localhost:3000/notifications?patientId=<id>

# Health check
curl http://localhost:3000/health
```
