require('dotenv').config();
const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const mysql = require('mysql2/promise');
const { S3Client, ListObjectsV2Command } = require('@aws-sdk/client-s3');
const fs = require('fs');
const path = require('path');

const app = express();
const server = http.createServer(app);
const io = new Server(server);

app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));

const PORT = process.env.PORT || 3000;
const AWS_REGION = process.env.AWS_REGION || 'us-east-1';
const S3_BUCKET = process.env.S3_BUCKET || 'vaah-terminal-charts';
const EBS_LOG_PATH = process.env.EBS_LOG_PATH || '/mnt/atc-logs/clearance_audit.log';

// AWS S3 Client (Automatically uses LabInstanceProfile / LabRole on EC2)
const s3 = new S3Client({ region: AWS_REGION });

// System & Cloud Logs memory buffer for the UI
const cloudLogs = [];
function addCloudLog(service, message) {
  const entry = {
    timestamp: new Date().toISOString(),
    service,
    message
  };
  cloudLogs.unshift(entry);
  if (cloudLogs.length > 100) cloudLogs.pop();
  io.emit('cloud_log', entry);

  // Write to attached EBS Volume (Practical 4) if mounted
  try {
    const dir = path.dirname(EBS_LOG_PATH);
    if (fs.existsSync(dir)) {
      fs.appendFileSync(EBS_LOG_PATH, `[${entry.timestamp}] [${service}] ${message}\n`);
    }
  } catch (err) {
    // Ignore if local dev without /mnt/atc-logs
  }
}

// Initial seed flights matching the VAAH UI reference
let memoryFlights = [
  {
    id: 1,
    callsign: 'AIC012',
    aircraft: 'A320',
    origin: 'VAAH',
    destination: 'VABB',
    dest_name: 'VABB - Mumbai',
    squawk: '3412',
    sid: 'BODAR 1A',
    departure_trans: 'DEPA 1A',
    altitude: 'FL120',
    status: 'REQUESTED',
    pushback: 'PENDING',
    updated_at: new Date().toISOString()
  },
  {
    id: 2,
    callsign: '6E451',
    aircraft: 'A321',
    origin: 'VAAH',
    destination: 'VIDP',
    dest_name: 'VIDP - New Delhi',
    squawk: '2145',
    sid: 'LIGPI 1A',
    departure_trans: 'SASRO 1A',
    altitude: 'FL140',
    status: 'CLEARED',
    pushback: 'APPROVED',
    updated_at: new Date().toISOString()
  },
  {
    id: 3,
    callsign: 'SGE892',
    aircraft: 'B738',
    origin: 'VAAH',
    destination: 'VOMM',
    dest_name: 'VOMM - Chennai',
    squawk: '4621',
    sid: 'BODAR 1B',
    departure_trans: 'QQZ 1A',
    altitude: 'FL110',
    status: 'PENDING SID',
    pushback: 'PENDING',
    updated_at: new Date().toISOString()
  }
];

let dbPool = null;
let rdsConnected = false;

async function initRDS() {
  if (!process.env.RDS_HOST) {
    addCloudLog('RDS-FALLBACK', 'No RDS_HOST configured. Running with in-memory flight strip store.');
    return;
  }
  try {
    dbPool = mysql.createPool({
      host: process.env.RDS_HOST,
      user: process.env.RDS_USER || 'admin',
      password: process.env.RDS_PASSWORD,
      database: process.env.RDS_DB || 'vaah_atc',
      port: 3306,
      waitForConnections: true,
      connectionLimit: 5
    });

    await dbPool.query(`
      CREATE TABLE IF NOT EXISTS flights (
        id INT AUTO_INCREMENT PRIMARY KEY,
        callsign VARCHAR(20) NOT NULL,
        aircraft VARCHAR(20) DEFAULT 'A320',
        origin VARCHAR(10) DEFAULT 'VAAH',
        destination VARCHAR(20) NOT NULL,
        dest_name VARCHAR(60),
        squawk VARCHAR(10) DEFAULT '0000',
        sid VARCHAR(30) DEFAULT 'UNASSIGNED',
        departure_trans VARCHAR(30) DEFAULT 'STANDARD',
        altitude VARCHAR(20) DEFAULT 'FL100',
        status VARCHAR(30) DEFAULT 'REQUESTED',
        pushback VARCHAR(30) DEFAULT 'PENDING',
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
      )
    `);

    const [rows] = await dbPool.query('SELECT COUNT(*) AS cnt FROM flights');
    if (rows[0].cnt === 0) {
      for (const f of memoryFlights) {
        await dbPool.query(
          `INSERT INTO flights (callsign, aircraft, origin, destination, dest_name, squawk, sid, departure_trans, altitude, status, pushback)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
          [f.callsign, f.aircraft, f.origin, f.destination, f.dest_name, f.squawk, f.sid, f.departure_trans, f.altitude, f.status, f.pushback]
        );
      }
    }
    rdsConnected = true;
    addCloudLog('RDS-MYSQL', `Connected to KMS-encrypted RDS instance at ${process.env.RDS_HOST}`);
  } catch (err) {
    rdsConnected = false;
    addCloudLog('RDS-WARN', `RDS connection failed (${err.message}). Using resilient memory store.`);
  }
}

async function getAllFlights() {
  if (rdsConnected && dbPool) {
    const [rows] = await dbPool.query('SELECT * FROM flights ORDER BY id DESC');
    return rows;
  }
  return memoryFlights;
}

// REST API: Fetch S3 Charts metadata
app.get('/api/charts', async (req, res) => {
  const defaultCharts = [
    { key: 'VAAH-BODAR-1A-SID.svg', title: 'BODAR 1A RNAV DEP (RWY 23)', rwy: '05/23', procedure: 'BODAR 1A' },
    { key: 'VAAH-LIGPI-1A-SID.svg', title: 'LIGPI 1A RNAV DEP (RWY 23)', rwy: '05/23', procedure: 'LIGPI 1A' },
    { key: 'VAAH-ILS-RWY23.svg', title: 'ILS OR LOC RWY 23 APPROACH', rwy: '23', procedure: 'ILS RWY 23' },
    { key: 'VAAH-GND-TAXI.svg', title: 'AERODROME GROUND & PARKING', rwy: '05/23', procedure: 'APRON 1/2' }
  ];

  try {
    const cmd = new ListObjectsV2Command({ Bucket: S3_BUCKET, MaxKeys: 20 });
    const data = await s3.send(cmd);
    addCloudLog('S3-BUCKET', `Listed ${(data.Contents || []).length} objects from s3://${S3_BUCKET}`);
    const s3Items = (data.Contents || []).map(obj => ({
      key: obj.Key,
      title: obj.Key.replace(/\.[^/.]+$/, '').replace(/[-_]/g, ' ').toUpperCase(),
      rwy: '05/23',
      procedure: obj.Key.split('.')[0].toUpperCase(),
      url: `https://${S3_BUCKET}.s3.${AWS_REGION}.amazonaws.com/${encodeURIComponent(obj.Key)}`
    }));
    res.json({ bucket: S3_BUCKET, connected: true, charts: s3Items.length ? s3Items : defaultCharts });
  } catch (err) {
    addCloudLog('S3-SIM', `Serving built-in VAAH eAIP charts (S3 Bucket: ${S3_BUCKET})`);
    res.json({ bucket: S3_BUCKET, connected: false, charts: defaultCharts });
  }
});

// Health Check Endpoint for AWS Application Load Balancer (Practical 5)
app.get('/health', (req, res) => {
  res.status(200).json({
    status: 'HEALTHY',
    vpc: 'AWS Active - VPC',
    rds: rdsConnected ? 'CONNECTED (KMS Encrypted)' : 'IN-MEMORY FALLBACK',
    s3Bucket: S3_BUCKET,
    version: 'v1.4.2'
  });
});

// Socket.io Real-Time Multi-Pilot & Controller Engine
io.on('connection', async (socket) => {
  addCloudLog('WEBSOCKET', `Client connected [Socket ID: ${socket.id.slice(0, 6)}]`);
  socket.emit('flights_init', await getAllFlights());
  socket.emit('cloud_logs_init', cloudLogs);

  // Pilot submits a new Flight Plan / Clearance Request
  socket.on('pilot_request_clearance', async (payload) => {
    const callsign = (payload.callsign || 'AIC999').toUpperCase().trim();
    const aircraft = (payload.aircraft || 'A320').toUpperCase().trim();
    const origin = (payload.origin || 'VAAH').toUpperCase().trim();
    const destination = (payload.destination || 'VABB').toUpperCase().trim();
    const dest_name = payload.dest_name || `${destination} - Dest`;
    const altitude = (payload.altitude || 'FL120').toUpperCase().trim();
    const squawk = String(Math.floor(1000 + Math.random() * 6000));

    if (rdsConnected && dbPool) {
      await dbPool.query(
        `INSERT INTO flights (callsign, aircraft, origin, destination, dest_name, squawk, sid, departure_trans, altitude, status, pushback)
         VALUES (?, ?, ?, ?, ?, ?, 'PENDING', 'DEPA 1A', ?, 'REQUESTED', 'PENDING')`,
        [callsign, aircraft, origin, destination, dest_name, squawk, altitude]
      );
    } else {
      memoryFlights.unshift({
        id: Date.now(),
        callsign,
        aircraft,
        origin,
        destination,
        dest_name,
        squawk,
        sid: 'PENDING',
        departure_trans: 'DEPA 1A',
        altitude,
        status: 'REQUESTED',
        pushback: 'PENDING',
        updated_at: new Date().toISOString()
      });
    }

    addCloudLog('DCL-UPLINK', `Pilot ${callsign} (${aircraft}) filed FPL ${origin}->${destination} req ${altitude}`);
    io.emit('flights_update', await getAllFlights());
  });

  // Controller transmits or approves clearance from Clearance Editor
  socket.on('controller_transmit_clearance', async (data) => {
    const { callsign, sid, altitude, squawk, departure_trans, status } = data;
    const newStatus = status || 'CLEARED';

    if (rdsConnected && dbPool) {
      await dbPool.query(
        `UPDATE flights SET sid = ?, altitude = ?, squawk = ?, departure_trans = ?, status = ? WHERE callsign = ?`,
        [sid, altitude, squawk, departure_trans, newStatus, callsign]
      );
    } else {
      memoryFlights = memoryFlights.map(f =>
        f.callsign === callsign
          ? { ...f, sid, altitude, squawk, departure_trans, status: newStatus, updated_at: new Date().toISOString() }
          : f
      );
    }

    addCloudLog('ATC-CLEARANCE', `Controller issued ${newStatus} to ${callsign}: SID ${sid}, ${altitude}, SQK ${squawk}`);
    io.emit('flights_update', await getAllFlights());
    io.emit('clearance_transmitted', { callsign, sid, altitude, squawk, departure_trans, status: newStatus });
  });

  // Pilot acknowledges clearance (WILCO) or requests pushback
  socket.on('pilot_action', async ({ callsign, action }) => {
    let statusUpdate = null;
    let pushbackUpdate = null;

    if (action === 'WILCO') {
      statusUpdate = 'CLEARED (WILCO)';
      addCloudLog('PILOT-READBACK', `${callsign} readback correct (WILCO) on 121.90 MHz`);
    } else if (action === 'REQ_PUSHBACK') {
      pushbackUpdate = 'REQ_PUSHBACK';
      addCloudLog('GND-CONTROL', `${callsign} requesting startup & pushback at VAAH`);
    }

    if (rdsConnected && dbPool) {
      if (statusUpdate) await dbPool.query(`UPDATE flights SET status = ? WHERE callsign = ?`, [statusUpdate, callsign]);
      if (pushbackUpdate) await dbPool.query(`UPDATE flights SET pushback = ? WHERE callsign = ?`, [pushbackUpdate, callsign]);
    } else {
      memoryFlights = memoryFlights.map(f => {
        if (f.callsign !== callsign) return f;
        return {
          ...f,
          status: statusUpdate || f.status,
          pushback: pushbackUpdate || f.pushback
        };
      });
    }
    io.emit('flights_update', await getAllFlights());
  });

  // Controller approves pushback
  socket.on('controller_approve_pushback', async ({ callsign }) => {
    if (rdsConnected && dbPool) {
      await dbPool.query(`UPDATE flights SET pushback = 'APPROVED', status = 'CLEARED' WHERE callsign = ?`, [callsign]);
    } else {
      memoryFlights = memoryFlights.map(f =>
        f.callsign === callsign ? { ...f, pushback: 'APPROVED', status: 'CLEARED' } : f
      );
    }
    addCloudLog('ATC-GND', `Pushback & engine start APPROVED for ${callsign} RWY 23`);
    io.emit('flights_update', await getAllFlights());
  });
});

initRDS().then(() => {
  server.listen(PORT, () => {
    addCloudLog('EC2-BOOT', `VAAH ATC Server listening on port ${PORT} in ${AWS_REGION}`);
    console.log(`VAAH Air Traffic Clearance System running on http://localhost:${PORT}`);
  });
});