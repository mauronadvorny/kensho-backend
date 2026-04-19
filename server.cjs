const express = require('express');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const nodemailer = require('nodemailer');
const cors = require('cors');
require('dotenv').config();

const app = express();
app.use(express.json());
app.use(cors());

const transporter = nodemailer.createTransport({
  service: 'gmail',
  auth: {
    user: process.env.GMAIL_USER || '',
    pass: process.env.GMAIL_APP_PASSWORD || '',
  },
});

const FILE_PATH = path.join(__dirname, 'sent_history.csv');
const OPEN_EVENTS_FILE_PATH = path.join(__dirname, 'open_events.csv');
const TRACKING_BASE_URL = (process.env.TRACKING_BASE_URL || process.env.APP_URL || '').trim().replace(/\/+$/, '');
const TRACKING_ENABLED = Boolean(
  TRACKING_BASE_URL &&
  !/localhost|127\.0\.0\.1/i.test(TRACKING_BASE_URL)
);
const TRANSPARENT_GIF = Buffer.from(
  'R0lGODlhAQABAIABAP///wAAACwAAAAAAQABAAACAkQBADs=',
  'base64'
);

if (!fs.existsSync(FILE_PATH)) {
  fs.writeFileSync(
    FILE_PATH,
    'name,outlet,email,country,topic,timestamp,status,trackingId\n',
    'utf-8'
  );
}

if (!fs.existsSync(OPEN_EVENTS_FILE_PATH)) {
  fs.writeFileSync(
    OPEN_EVENTS_FILE_PATH,
    'trackingId,timestamp,userAgent,ip\n',
    'utf-8'
  );
}

const csvEscape = (value) => {
  const str = String(value ?? '');
  return `"${str.replace(/"/g, '""')}"`;
};

const isValidEmail = (email) => /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(email);

const normalizeEmails = (emails = []) => Array.from(
  new Set(
    emails
      .filter(Boolean)
      .map((email) => String(email).trim().toLowerCase())
      .filter(isValidEmail)
  )
);

const readOpenEventRecords = () => {
  if (!fs.existsSync(OPEN_EVENTS_FILE_PATH)) return [];

  const content = fs.readFileSync(OPEN_EVENTS_FILE_PATH, 'utf-8');
  const lines = content.split('\n').slice(1);

  return lines
    .filter((line) => line.trim() !== '')
    .map((line) => {
      const parts = line.match(/"([^"]*)"/g)?.map((p) => p.replace(/"/g, '')) || [];

      return {
        trackingId: parts[0],
        timestamp: parts[1],
        userAgent: parts[2],
        ip: parts[3],
      };
    });
};

const buildTrackedHtml = (html, trackingId) => {
  if (!TRACKING_ENABLED || !trackingId) return html;

  const trackingPixel = `<img src="${TRACKING_BASE_URL}/track/open/${trackingId}" alt="" width="1" height="1" style="display:block;width:1px;height:1px;border:0;opacity:0;" />`;

  if (typeof html === 'string' && html.includes('</body>')) {
    return html.replace('</body>', `${trackingPixel}</body>`);
  }

  return `${html || ''}${trackingPixel}`;
};

const readSentHistoryRecords = () => {
  if (!fs.existsSync(FILE_PATH)) return [];

  const content = fs.readFileSync(FILE_PATH, 'utf-8');
  const lines = content.split('\n').slice(1);

  return lines
    .filter((line) => line.trim() !== '')
    .map((line) => {
      const parts = line.match(/"([^"]*)"/g)?.map((p) => p.replace(/"/g, '')) || [];

      return {
        name: parts[0],
        outlet: parts[1],
        email: parts[2],
        country: parts[3],
        topic: parts[4],
        timestamp: parts[5],
        status: parts[6],
        trackingId: parts[7] || '',
      };
    });
};

if (!TRACKING_ENABLED) {
  console.log('Tracking de abertura desativado: defina TRACKING_BASE_URL publico para registrar opens.');
}

app.post('/send-bulk', async (req, res) => {
  try {
    const { journalists } = req.body;
    const results = [];
    const sentHistory = readSentHistoryRecords();
    const sentEmailSet = new Set(
      sentHistory
        .filter((record) =>
          record.status === 'sent' &&
          record.country === (req.body.country || '') &&
          record.topic === (req.body.topic || '')
        )
        .map((record) => String(record.email || '').trim().toLowerCase())
        .filter(Boolean)
    );

    for (const j of journalists) {
      const verifiedEmails = normalizeEmails(j.verifiedEmails || []);
      const possibleEmails = normalizeEmails(j.possibleEmails || []);
      const candidateEmails = verifiedEmails.length > 0 ? verifiedEmails : possibleEmails;
      const targetEmails = candidateEmails.filter((email) => !sentEmailSet.has(email));
      const emailType = verifiedEmails.length > 0 ? 'verified' : (possibleEmails.length > 0 ? 'possible' : 'missing');

      if (candidateEmails.length > 0 && targetEmails.length === 0) {
        console.log(`EMAILS JA UTILIZADOS PARA: ${j.name}`);
        results.push({ name: j.name, email: '', status: 'skipped_duplicate_email', emailType });
        continue;
      }

      if (targetEmails.length === 0) {
        console.log(`SEM EMAIL UTILIZAVEL PARA: ${j.name}`);
        results.push({ name: j.name, email: '', status: 'skipped_no_email', emailType });
        continue;
      }

      for (const email of targetEmails) {
        try {
          const trackingId = TRACKING_ENABLED ? crypto.randomUUID() : '';
          console.log(`ENVIANDO EMAIL INDIVIDUAL PARA: ${email} (${emailType})`);

          await transporter.sendMail({
            from: process.env.GMAIL_USER,
            to: email,
            subject: j.pitchSubject,
            html: buildTrackedHtml(j.pitchBody, trackingId),
          });

          results.push({ name: j.name, email, status: 'sent', emailType, trackingEnabled: TRACKING_ENABLED, trackingId });

          const line = [
            csvEscape(j.name),
            csvEscape(j.outlet || ''),
            csvEscape(email),
            csvEscape(req.body.country || ''),
            csvEscape(req.body.topic || ''),
            csvEscape(new Date().toISOString()),
            csvEscape('sent'),
            csvEscape(trackingId),
          ].join(',');

          fs.appendFileSync(FILE_PATH, line + '\n', 'utf-8');
          sentEmailSet.add(email);
          console.log(`OK INDIVIDUAL: ${email}`);
        } catch (err) {
          console.error(`ERRO no email ${email}:`, err.message);
          results.push({ name: j.name, email, status: 'error', emailType });
        }
      }
    }

    console.log('Enviando resposta ao frontend com', results.length, 'itens');
    res.status(200).json(results);
  } catch (error) {
    console.error('ERRO CRITICO NO BACKEND:', error);
    res.status(500).json([]);
  }
});

app.get('/track/open/:trackingId', (req, res) => {
  const trackingId = String(req.params.trackingId || '').trim();

  if (trackingId) {
    const line = [
      csvEscape(trackingId),
      csvEscape(new Date().toISOString()),
      csvEscape(req.get('user-agent') || ''),
      csvEscape(req.ip || req.socket?.remoteAddress || ''),
    ].join(',');

    fs.appendFileSync(OPEN_EVENTS_FILE_PATH, line + '\n', 'utf-8');
  }

  res.setHeader('Content-Type', 'image/gif');
  res.setHeader('Cache-Control', 'no-store, no-cache, must-revalidate, proxy-revalidate');
  res.setHeader('Pragma', 'no-cache');
  res.setHeader('Expires', '0');
  res.end(TRANSPARENT_GIF);
});

app.get('/read-sent/:country', (req, res) => {
  if (!fs.existsSync(FILE_PATH)) return res.json([]);
  const content = fs.readFileSync(FILE_PATH, 'utf-8');
  res.json(content.split('\n').filter((l) => l.includes(`[Pais: ${req.params.country}]`)).map((l) => l.split(' - ')[0].trim()));
});

const PORT = process.env.PORT || 10000;

app.get('/', (req, res) => {
  res.send('OK');
});

app.listen(PORT, "0.0.0.0", () => {
  console.log(`Servidor atualizado e pronto na porta ${PORT}`);
});

app.get('/sent-history', (req, res) => {
  const openEvents = readOpenEventRecords();
  const eventsByTrackingId = openEvents.reduce((acc, event) => {
    if (!event.trackingId) return acc;
    if (!acc[event.trackingId]) acc[event.trackingId] = [];
    acc[event.trackingId].push(event);
    return acc;
  }, {});

  const records = readSentHistoryRecords().map((record) => {
    const openEventsForRecord = record.trackingId ? (eventsByTrackingId[record.trackingId] || []) : [];
    const openTimestamps = openEventsForRecord
      .map((event) => event.timestamp)
      .filter(Boolean)
      .sort();

    return {
      ...record,
      trackingEnabled: Boolean(record.trackingId),
      openCount: openEventsForRecord.length,
      firstOpenedAt: openTimestamps[0] || '',
      lastOpenedAt: openTimestamps[openTimestamps.length - 1] || '',
      opened: openEventsForRecord.length > 0,
    };
  });

  res.json(records);
});
