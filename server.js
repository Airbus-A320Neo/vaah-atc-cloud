require('dotenv').config();
const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const mysql = require('mysql2/promise');
const { S3Client, ListObjectsV2Command, GetObjectCommand } = require('@aws-sdk/client-s3');
const { Readable } = require('stream');
const fs = require('fs');
const path = require('path');

const app = express();
const server = http.createServer(app);
const io = new Server(server);

app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));

const PORT = process.env.PORT || 3000;
const AWS_REGION = process.env.AWS_REGION || 'us-east-1';
const S3_BUCKET = process.env.S3_BUCKET || 'vaah-terminal-charts-012';
const EBS_LOG_PATH = process.env.EBS_LOG_PATH || '/mnt/atc-logs/clearance_audit.log';

const s3 = new S3Client({ region: AWS_REGION });

const cloudLogs = [];
function addCloudLog(service, message) {
  const entry = {
    timestamp: new Date().toISOString(),
    service,
    message
  };
  cloudLogs.unshift(entry);
  if (cloudLogs.length > 120) cloudLogs.pop();
  io.emit('cloud_log', entry);

  try {
    const dir = path.dirname(EBS_LOG_PATH);
    if (fs.existsSync(dir)) {
      fs.appendFileSync(EBS_LOG_PATH, `[${entry.timestamp}] [${service}] ${message}\n`);
    }
  } catch (err) {
    // Skip if running locally without /mnt/atc-logs
  }
}

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
    departure_trans: 'AH382',
    altitude: 'FL120',
    status: 'IFR REQUESTED',
    pushback: 'PENDING',
    taxi_route: 'UNASSIGNED',
    holding_point: 'R4 RWY 23',
    tower_freq: '119.600 MHz',
    handed_off: 0,
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
    sid: 'UUD 1A',
    departure_trans: 'AH370',
    altitude: 'FL140',
    status: 'PUSHBACK APPROVED',
    pushback: 'APPROVED',
    taxi_route: 'UNASSIGNED',
    holding_point: 'R4 RWY 23',
    tower_freq: '119.600 MHz',
    handed_off: 0,
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
    sid: 'QQZ 1A',
    departure_trans: 'AH366',
    altitude: 'FL110',
    status: 'IFR CLEARED',
    pushback: 'REQUESTED',
    taxi_route: 'UNASSIGNED',
    holding_point: 'C RWY 23',
    tower_freq: '119.600 MHz',
    handed_off: 0,
    updated_at: new Date().toISOString()
  }
];

let dbPool = null;
let rdsConnected = false;

async function safeAlter(sql) {
  try {
    await dbPool.query(sql);
  } catch (err) {
    // Ignore ER_DUP_FIELDNAME (1060) if column already exists from previous deployment
    if (err.errno !== 1060) {
      addCloudLog('RDS-MIGRATE', `Schema note: ${err.message}`);
    }
  }
}

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
        altitude VARCHAR(20) DEFAULT 'FL120',
        status VARCHAR(40) DEFAULT 'IFR REQUESTED',
        pushback VARCHAR(30) DEFAULT 'PENDING',
        taxi_route VARCHAR(120) DEFAULT 'UNASSIGNED',
        holding_point VARCHAR(40) DEFAULT 'R4 RWY 23',
        tower_freq VARCHAR(30) DEFAULT '119.600 MHz',
        handed_off TINYINT(1) DEFAULT 0,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
      )
    `);

    // Upgrade existing RDS table seamlessly if created in earlier version
    await safeAlter(`ALTER TABLE flights ADD COLUMN taxi_route VARCHAR(120) DEFAULT 'UNASSIGNED'`);
    await safeAlter(`ALTER TABLE flights ADD COLUMN holding_point VARCHAR(40) DEFAULT 'R4 RWY 23'`);
    await safeAlter(`ALTER TABLE flights ADD COLUMN tower_freq VARCHAR(30) DEFAULT '119.600 MHz'`);
    await safeAlter(`ALTER TABLE flights ADD COLUMN handed_off TINYINT(1) DEFAULT 0`);
    await safeAlter(`ALTER TABLE flights MODIFY COLUMN status VARCHAR(40) DEFAULT 'IFR REQUESTED'`);

    const [rows] = await dbPool.query('SELECT COUNT(*) AS cnt FROM flights');
    if (rows[0].cnt === 0) {
      for (const f of memoryFlights) {
        await dbPool.query(
          `INSERT INTO flights (callsign, aircraft, origin, destination, dest_name, squawk, sid, departure_trans, altitude, status, pushback, taxi_route, holding_point, tower_freq, handed_off)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0)`,
          [f.callsign, f.aircraft, f.origin, f.destination, f.dest_name, f.squawk, f.sid, f.departure_trans, f.altitude, f.status, f.pushback, f.taxi_route, f.holding_point, f.tower_freq]
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

async function getActiveFlights() {
  if (rdsConnected && dbPool) {
    const [rows] = await dbPool.query('SELECT * FROM flights WHERE COALESCE(handed_off, 0) = 0 ORDER BY id DESC');
    return rows;
  }
  return memoryFlights.filter(f => !f.handed_off);
}

// Helper to clean destination input (e.g., "VAAH-VIDP" or "VABB - Mumbai" -> "VIDP" / "VABB")
function parseDestination(raw) {
  const cleaned = (raw || 'VABB').toUpperCase().trim();
  const hyphenMatch = cleaned.match(/VAAH\s*[-–>to]+\s*([A-Z]{4})/i);
  if (hyphenMatch) return hyphenMatch[1].toUpperCase();
  const firstToken = cleaned.split(/[\s,-]+/)[0];
  return firstToken || 'VABB';
}

// REST API: List S3 Charts
app.get('/api/charts', async (req, res) => {
  try {
    const cmd = new ListObjectsV2Command({ Bucket: S3_BUCKET, MaxKeys: 30 });
    const data = await s3.send(cmd);
    const files = (data.Contents || []).filter(obj => obj.Size > 0 && !obj.Key.endsWith('/'));
    addCloudLog('S3-BUCKET', `Listed ${files.length} chart object(s) from s3://${S3_BUCKET}`);

    const s3Items = files.map(obj => ({
      key: obj.Key,
      title: obj.Key.replace(/\.[^/.]+$/, '').replace(/[-_]/g, ' ').toUpperCase(),
      sizeKB: Math.max(1, Math.round(obj.Size / 1024)),
      isPdf: obj.Key.toLowerCase().endsWith('.pdf'),
      streamUrl: `/api/charts/file?key=${encodeURIComponent(obj.Key)}`
    }));

    res.json({ bucket: S3_BUCKET, connected: true, charts: s3Items });
  } catch (err) {
    addCloudLog('S3-ERROR', `Unable to list s3://${S3_BUCKET}: ${err.message}`);
    res.json({ bucket: S3_BUCKET, connected: false, error: err.message, charts: [] });
  }
});

// REST API: Securely stream private S3 PDF/Image via EC2 LabRole (keeps S3 bucket private!)
app.get('/api/charts/file', async (req, res) => {
  const key = req.query.key;
  if (!key) return res.status(400).send('Missing S3 object key');

  try {
    const cmd = new GetObjectCommand({ Bucket: S3_BUCKET, Key: key });
    const s3Response = await s3.send(cmd);

    let contentType = s3Response.ContentType;
    if (!contentType || contentType === 'application/octet-stream') {
      if (key.toLowerCase().endsWith('.pdf')) contentType = 'application/pdf';
      else if (key.toLowerCase().endsWith('.png')) contentType = 'image/png';
      else if (key.toLowerCase().endsWith('.jpg') || key.toLowerCase().endsWith('.jpeg')) contentType = 'image/jpeg';
    }

    res.setHeader('Content-Type', contentType || 'application/pdf');
    res.setHeader('Content-Disposition', `inline; filename="${path.basename(key)}"`);
    res.setHeader('Cache-Control', 'private, max-age=300');

    addCloudLog('S3-STREAM', `Streamed s3://${S3_BUCKET}/${key} (${contentType}) to Chart Viewer`);

    if (typeof s3Response.Body.pipe === 'function') {
      s3Response.Body.pipe(res);
    } else {
      Readable.fromWeb(s3Response.Body).pipe(res);
    }
  } catch (err) {
    addCloudLog('S3-ERR', `Failed to stream ${key} from s3://${S3_BUCKET}: ${err.message}`);
    res.status(404).send(`Could not load S3 object "${key}": ${err.message}`);
  }
});

// Health Check Endpoint for AWS Application Load Balancer
app.get('/health', (req, res) => {
  res.status(200).json({
    status: 'HEALTHY',
    vpc: 'AWS Active - VPC',
    rds: rdsConnected ? 'CONNECTED (KMS Encrypted)' : 'IN-MEMORY FALLBACK',
    s3Bucket: S3_BUCKET,
    version: 'v1.5.0'
  });
});

// Socket.io Real-Time Multi-Pilot & Controller Engine
io.on('connection', async (socket) => {
  addCloudLog('WEBSOCKET', `Client connected [Socket ID: ${socket.id.slice(0, 6)}]`);
  socket.emit('flights_init', await getActiveFlights());
  socket.emit('cloud_logs_init', cloudLogs);

  // STEP 1: Pilot requests IFR Clearance
  socket.on('pilot_request_clearance', async (payload) => {
    const callsign = (payload.callsign || 'AIC999').toUpperCase().trim();
    const aircraft = (payload.aircraft || 'A320').toUpperCase().trim();
    const origin = 'VAAH';
    const destination = parseDestination(payload.destination || payload.dest_name);
    const dest_name = (payload.dest_name || destination).trim();
    const altitude = (payload.altitude || 'FL120').toUpperCase().trim();
    const squawk = String(Math.floor(1000 + Math.random() * 6000));

    if (rdsConnected && dbPool) {
      await dbPool.query(
        `INSERT INTO flights (callsign, aircraft, origin, destination, dest_name, squawk, sid, departure_trans, altitude, status, pushback, taxi_route, holding_point, tower_freq, handed_off)
         VALUES (?, ?, ?, ?, ?, ?, 'PENDING', 'AH382', ?, 'IFR REQUESTED', 'PENDING', 'UNASSIGNED', 'R4 RWY 23', '119.600 MHz', 0)`,
        [callsign, aircraft, origin, destination, dest_name, squawk, altitude]
      );
    } else {
      memoryFlights.unshift({
        id: Date.now(),
        callsign, aircraft, origin, destination, dest_name, squawk,
        sid: 'PENDING', departure_trans: 'AH382', altitude,
        status: 'IFR REQUESTED', pushback: 'PENDING',
        taxi_route: 'UNASSIGNED', holding_point: 'R4 RWY 23',
        tower_freq: '119.600 MHz', handed_off: 0,
        updated_at: new Date().toISOString()
      });
    }

    addCloudLog('DCL-UPLINK', `Pilot ${callsign} (${aircraft}) requested IFR clearance ${origin}->${destination} ${altitude}`);
    io.emit('flights_update', await getActiveFlights());
  });

  // STEP 2: Controller transmits IFR Clearance (SID, Alt, Squawk)
  socket.on('controller_transmit_clearance', async (data) => {
    const { callsign, sid, altitude, squawk, departure_trans } = data;
    const newStatus = 'IFR CLEARED';

    if (rdsConnected && dbPool) {
      await dbPool.query(
        `UPDATE flights SET sid = ?, altitude = ?, squawk = ?, departure_trans = ?, status = ? WHERE callsign = ? AND COALESCE(handed_off, 0) = 0`,
        [sid, altitude, squawk, departure_trans, newStatus, callsign]
      );
    } else {
      memoryFlights = memoryFlights.map(f =>
        (f.callsign === callsign && !f.handed_off)
          ? { ...f, sid, altitude, squawk, departure_trans, status: newStatus, updated_at: new Date().toISOString() }
          : f
      );
    }

    addCloudLog('ATC-DELIVERY', `IFR Clearance issued to ${callsign}: SID ${sid}, Climb ${altitude}, SQK ${squawk}`);
    io.emit('flights_update', await getActiveFlights());
  });

  // STEP 3: Controller approves Pushback & Startup
  socket.on('controller_approve_pushback', async ({ callsign }) => {
    if (rdsConnected && dbPool) {
      await dbPool.query(
        `UPDATE flights SET pushback = 'APPROVED', status = 'PUSHBACK APPROVED' WHERE callsign = ? AND COALESCE(handed_off, 0) = 0`,
        [callsign]
      );
    } else {
      memoryFlights = memoryFlights.map(f =>
        (f.callsign === callsign && !f.handed_off)
          ? { ...f, pushback: 'APPROVED', status: 'PUSHBACK APPROVED' }
          : f
      );
    }
    addCloudLog('ATC-GROUND', `Pushback & startup APPROVED for ${callsign} (Release Point Y1/Y2) on 121.675 MHz`);
    io.emit('flights_update', await getActiveFlights());
  });

  // STEP 4: Controller issues Taxi Clearance
  socket.on('controller_issue_taxi', async ({ callsign, taxi_route, holding_point }) => {
    const route = (taxi_route || 'P, P1, D').trim();
    const holdPt = (holding_point || 'R4 RWY 23').trim();

    if (rdsConnected && dbPool) {
      await dbPool.query(
        `UPDATE flights SET taxi_route = ?, holding_point = ?, pushback = 'COMPLETE', status = 'TAXI CLEARED' WHERE callsign = ? AND COALESCE(handed_off, 0) = 0`,
        [route, holdPt, callsign]
      );
    } else {
      memoryFlights = memoryFlights.map(f =>
        (f.callsign === callsign && !f.handed_off)
          ? { ...f, taxi_route: route, holding_point: holdPt, pushback: 'COMPLETE', status: 'TAXI CLEARED' }
          : f
      );
    }

    addCloudLog('ATC-GROUND', `Taxi clearance issued to ${callsign}: VIA ${route}, HOLD SHORT ${holdPt}`);
    io.emit('flights_update', await getActiveFlights());
  });

  // STEP 5: Controller hands off to VAAH Tower (119.600 MHz) and removes strip from dashboard
  socket.on('controller_handoff_tower', async ({ callsign, tower_freq }) => {
    const freq = (tower_freq || '119.600 MHz').trim();

    if (rdsConnected && dbPool) {
      await dbPool.query(
        `UPDATE flights SET tower_freq = ?, status = 'HANDED OFF TO TWR', handed_off = 1 WHERE callsign = ? AND COALESCE(handed_off, 0) = 0`,
        [freq, callsign]
      );
    } else {
      memoryFlights = memoryFlights.map(f =>
        (f.callsign === callsign && !f.handed_off)
          ? { ...f, tower_freq: freq, status: 'HANDED OFF TO TWR', handed_off: 1 }
          : f
      );
    }

    addCloudLog('ATC-HANDOFF', `${callsign} handed off to Ahmedabad Tower on ${freq} (Sec 118.100 MHz). Strip archived.`);
    io.emit('tower_handoff', {
      callsign,
      tower_freq: freq,
      secondary_freq: '118.100 MHz',
      timestamp: new Date().toISOString()
    });
    io.emit('flights_update', await getActiveFlights());
  });

  // Pilot Interactive Actions (WILCO, Pushback Req, Taxi Req, Holding Short)
  socket.on('pilot_action', async ({ callsign, action }) => {
    let statusUpdate = null;
    let pushbackUpdate = null;

    if (action === 'WILCO_IFR') {
      statusUpdate = 'IFR CLEARED (WILCO)';
      addCloudLog('PILOT-READBACK', `${callsign} IFR clearance readback correct (WILCO)`);
    } else if (action === 'REQ_PUSHBACK') {
      statusUpdate = 'PUSHBACK REQUESTED';
      pushbackUpdate = 'REQUESTED';
      addCloudLog('PILOT-GND', `${callsign} requesting startup & pushback on GND 121.675 MHz`);
    } else if (action === 'REQ_TAXI') {
      statusUpdate = 'TAXI REQUESTED';
      pushbackUpdate = 'COMPLETE';
      addCloudLog('PILOT-GND', `${callsign} pushback complete, requesting taxi to RWY 23`);
    } else if (action === 'HOLDING_SHORT') {
      statusUpdate = 'READY FOR TOWER';
      addCloudLog('PILOT-GND', `${callsign} holding short RWY 23, ready for Tower handoff`);
    }

    if (rdsConnected && dbPool) {
      if (statusUpdate && pushbackUpdate) {
        await dbPool.query(`UPDATE flights SET status = ?, pushback = ? WHERE callsign = ? AND COALESCE(handed_off, 0) = 0`, [statusUpdate, pushbackUpdate, callsign]);
      } else if (statusUpdate) {
        await dbPool.query(`UPDATE flights SET status = ? WHERE callsign = ? AND COALESCE(handed_off, 0) = 0`, [statusUpdate, callsign]);
      }
    } else {
      memoryFlights = memoryFlights.map(f => {
        if (f.callsign !== callsign || f.handed_off) return f;
        return {
          ...f,
          status: statusUpdate || f.status,
          pushback: pushbackUpdate || f.pushback
        };
      });
    }
    io.emit('flights_update', await getActiveFlights());
  });
});

initRDS().then(() => {
  server.listen(PORT, () => {
    addCloudLog('EC2-BOOT', `VAAH ATC Server v1.5.0 listening on port ${PORT} in ${AWS_REGION}`);
    console.log(`VAAH Air Traffic Clearance System running on http://localhost:${PORT}`);
  });
});