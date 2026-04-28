const express = require('express');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const nodemailer = require('nodemailer');
const cors = require('cors');
require('dotenv').config({
  path: process.env.ENV_FILE_PATH || path.join(__dirname, '..', '.env'),
});

const app = express();
app.use(express.json());
app.use(cors());

const DATA_DIR = process.env.DATA_DIR || __dirname;
fs.mkdirSync(DATA_DIR, { recursive: true });

const transporter = nodemailer.createTransport({
  service: 'gmail',
  auth: {
    user: process.env.GMAIL_USER || '',
    pass: String(process.env.GMAIL_APP_PASSWORD || '').replace(/\s+/g, ''),
  },
});

const FILE_PATH = path.join(DATA_DIR, 'sent_history.csv');
const OPEN_EVENTS_FILE_PATH = path.join(DATA_DIR, 'open_events.csv');
const CLICK_EVENTS_FILE_PATH = path.join(DATA_DIR, 'click_events.csv');
const SITE_EVENTS_FILE_PATH = path.join(DATA_DIR, 'site_events.csv');
const DELIVERY_REPORT_FILE_PATH = path.join(DATA_DIR, 'delivery_report.csv');
const TRACKING_BASE_URL = (process.env.TRACKING_BASE_URL || process.env.APP_URL || '').trim().replace(/\/+$/, '');
const TRACKING_EVENTS_URL = (process.env.TRACKING_EVENTS_URL || (TRACKING_BASE_URL ? `${TRACKING_BASE_URL}/open-events` : '')).trim();
const CLICK_EVENTS_URL = (process.env.CLICK_EVENTS_URL || (TRACKING_BASE_URL ? `${TRACKING_BASE_URL}/click-events` : '')).trim();
const SITE_EVENTS_URL = (process.env.SITE_EVENTS_URL || (TRACKING_BASE_URL ? `${TRACKING_BASE_URL}/site-events` : '')).trim();
const DEFAULT_CLICK_TARGET_URL = 'https://getkensho.app';
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

if (!fs.existsSync(CLICK_EVENTS_FILE_PATH)) {
  fs.writeFileSync(
    CLICK_EVENTS_FILE_PATH,
    'trackingId,timestamp,target,userAgent,ip\n',
    'utf-8'
  );
}

if (!fs.existsSync(SITE_EVENTS_FILE_PATH)) {
  fs.writeFileSync(
    SITE_EVENTS_FILE_PATH,
    'trackingId,sessionId,eventType,timestamp,path,label,value,durationMs,userAgent,ip,referrer\n',
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

const readClickEventRecords = () => {
  if (!fs.existsSync(CLICK_EVENTS_FILE_PATH)) return [];

  const content = fs.readFileSync(CLICK_EVENTS_FILE_PATH, 'utf-8');
  const lines = content.split('\n').slice(1);

  return lines
    .filter((line) => line.trim() !== '')
    .map((line) => {
      const parts = line.match(/"([^"]*)"/g)?.map((p) => p.replace(/"/g, '')) || [];

      return {
        trackingId: parts[0],
        timestamp: parts[1],
        target: parts[2],
        userAgent: parts[3],
        ip: parts[4],
      };
    });
};

const readSiteEventRecords = () => {
  if (!fs.existsSync(SITE_EVENTS_FILE_PATH)) return [];

  const content = fs.readFileSync(SITE_EVENTS_FILE_PATH, 'utf-8');
  const lines = content.split('\n').slice(1);

  return lines
    .filter((line) => line.trim() !== '')
    .map((line) => {
      const parts = line.match(/"([^"]*)"/g)?.map((p) => p.replace(/"/g, '')) || [];

      return {
        trackingId: parts[0],
        sessionId: parts[1],
        eventType: parts[2],
        timestamp: parts[3],
        path: parts[4],
        label: parts[5],
        value: parts[6],
        durationMs: parts[7],
        userAgent: parts[8],
        ip: parts[9],
        referrer: parts[10],
      };
    });
};

const appendUniqueCsvEvents = (filePath, header, events, fields, keyBuilder, readExistingEvents) => {
  if (!fs.existsSync(filePath)) {
    fs.writeFileSync(filePath, header, 'utf-8');
  }

  const existingEvents = readExistingEvents();
  const existingKeys = new Set(existingEvents.map(keyBuilder));

  for (const event of events) {
    const key = keyBuilder(event);
    if (!key || existingKeys.has(key)) continue;

    const line = fields.map((field) => csvEscape(event[field] || '')).join(',');
    fs.appendFileSync(filePath, line + '\n', 'utf-8');
    existingKeys.add(key);
  }
};

const fetchRemoteEvents = async (url) => {
  if (!url || /localhost|127\.0\.0\.1/i.test(url) || typeof fetch !== 'function') return [];

  try {
    const response = await fetch(url, {
      headers: { Accept: 'application/json' },
    });

    if (!response.ok) {
      console.warn(`Nao foi possivel sincronizar tracking remoto: HTTP ${response.status}`);
      return [];
    }

    const data = await response.json();
    return Array.isArray(data) ? data : [];
  } catch (error) {
    console.warn('Nao foi possivel sincronizar tracking remoto:', error.message);
    return [];
  }
};

const syncRemoteTrackingEvents = async () => {
  const remoteOpenEvents = await fetchRemoteEvents(TRACKING_EVENTS_URL);
  appendUniqueCsvEvents(
    OPEN_EVENTS_FILE_PATH,
    'trackingId,timestamp,userAgent,ip\n',
    remoteOpenEvents,
    ['trackingId', 'timestamp', 'userAgent', 'ip'],
    (event) => `${event.trackingId || ''}|${event.timestamp || ''}|${event.userAgent || ''}|${event.ip || ''}`,
    readOpenEventRecords
  );

  const remoteClickEvents = await fetchRemoteEvents(CLICK_EVENTS_URL);
  appendUniqueCsvEvents(
    CLICK_EVENTS_FILE_PATH,
    'trackingId,timestamp,target,userAgent,ip\n',
    remoteClickEvents,
    ['trackingId', 'timestamp', 'target', 'userAgent', 'ip'],
    (event) => `${event.trackingId || ''}|${event.timestamp || ''}|${event.target || ''}|${event.userAgent || ''}|${event.ip || ''}`,
    readClickEventRecords
  );

  const remoteSiteEvents = await fetchRemoteEvents(SITE_EVENTS_URL);
  appendUniqueCsvEvents(
    SITE_EVENTS_FILE_PATH,
    'trackingId,sessionId,eventType,timestamp,path,label,value,durationMs,userAgent,ip,referrer\n',
    remoteSiteEvents,
    ['trackingId', 'sessionId', 'eventType', 'timestamp', 'path', 'label', 'value', 'durationMs', 'userAgent', 'ip', 'referrer'],
    (event) => `${event.trackingId || ''}|${event.sessionId || ''}|${event.eventType || ''}|${event.timestamp || ''}|${event.path || ''}|${event.label || ''}|${event.value || ''}|${event.durationMs || ''}`,
    readSiteEventRecords
  );
};

const buildClickTrackingUrl = (trackingId, targetUrl = DEFAULT_CLICK_TARGET_URL) => {
  if (!TRACKING_ENABLED || !trackingId) return targetUrl;

  const encodedTarget = encodeURIComponent(targetUrl);
  return `${TRACKING_BASE_URL}/track/click/${trackingId}?to=${encodedTarget}`;
};

const rewriteTrackedLinks = (html, trackingId) => {
  if (!TRACKING_ENABLED || !trackingId || typeof html !== 'string') return html;

  const trackedUrl = buildClickTrackingUrl(trackingId, DEFAULT_CLICK_TARGET_URL);
  return html.replace(/href="https:\/\/getkensho\.app\/?"/g, `href="${trackedUrl}"`);
};

const buildTrackedHtml = (html, trackingId) => {
  if (!TRACKING_ENABLED || !trackingId) return html;

  const trackingPixel = `<img src="${TRACKING_BASE_URL}/track/open/${trackingId}" alt="" width="1" height="1" style="display:block;width:1px;height:1px;border:0;opacity:0;" />`;
  const htmlWithTrackedLinks = rewriteTrackedLinks(html, trackingId);

  if (typeof htmlWithTrackedLinks === 'string' && htmlWithTrackedLinks.includes('</body>')) {
    return htmlWithTrackedLinks.replace('</body>', `${trackingPixel}</body>`);
  }

  return `${htmlWithTrackedLinks || ''}${trackingPixel}`;
};

const appendTrackingIdToTargetUrl = (targetUrl, trackingId) => {
  if (!trackingId) return targetUrl;

  try {
    const url = new URL(targetUrl);
    url.searchParams.set('trk', trackingId);
    return url.toString();
  } catch {
    return targetUrl;
  }
};

const escapeHtml = (value) => String(value ?? '')
  .replace(/&/g, '&amp;')
  .replace(/</g, '&lt;')
  .replace(/>/g, '&gt;')
  .replace(/"/g, '&quot;')
  .replace(/'/g, '&#39;');

const linkifyUrls = (value) => value.replace(
  /(https?:\/\/[^\s<]+)/g,
  '<a href="$1" style="color:#2563eb;text-decoration:underline;">$1</a>'
);

const normalizePlainTextEmail = (body) => {
  const text = String(body || '').replace(/\r\n/g, '\n').trim();
  if (!text) return '';
  if (/\n\s*\n/.test(text)) return text;
  if (text.includes('\n')) return text.replace(/\n+/g, '\n\n');

  return text
    .replace(/([.!?。！？।])\s+(?=\S)/g, '$1\n\n')
    .replace(/\s+(?=(Sincerely|Best regards|Kind regards|Regards|Atenciosamente|Cordialmente|Saludos|Cordialement|भवदीय|בברכה|مع تحياتي)\b)/gi, '\n\n')
    .replace(/\s+(?=(PS:|P\.S\.|P\.S:|पुनश्च:|נ\.ב\.|ملاحظة:))/gi, '\n\n')
    .replace(/\n{3,}/g, '\n\n');
};

const buildEmailHtml = (body) => {
  const paragraphs = normalizePlainTextEmail(body)
    .split(/\n\s*\n/)
    .map((paragraph) => paragraph.trim())
    .filter(Boolean);

  const htmlParagraphs = paragraphs.map((paragraph) => {
    const escapedParagraph = linkifyUrls(escapeHtml(paragraph)).replace(/\n/g, '<br>');
    return `<p style="margin:0 0 14px 0;">${escapedParagraph}</p>`;
  });

  return `
    <div style="font-family:Arial,Helvetica,sans-serif;font-size:14px;line-height:1.55;color:#111827;">
      ${htmlParagraphs.join('')}
    </div>
  `;
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

const writeDeliveryReportCsv = (records) => {
  const headers = [
    'name',
    'outlet',
    'email',
    'country',
    'topic',
    'timestamp',
    'status',
    'trackingId',
    'trackingEnabled',
    'opened',
    'openCount',
    'firstOpenedAt',
    'lastOpenedAt',
    'clicked',
    'clickCount',
    'firstClickedAt',
    'lastClickedAt',
    'lastClickedTarget',
    'siteVisited',
    'siteEventCount',
    'firstSiteEventAt',
    'lastSiteEventAt',
    'lastSiteEventType',
    'playStoreClickCount',
    'footerClickCount',
    'privacyClickCount',
    'termsClickCount',
    'contactClickCount',
    'totalEngagementMs',
  ];

  const lines = [
    headers.join(','),
    ...records.map((record) => headers.map((field) => csvEscape(record[field] ?? '')).join(',')),
  ];

  fs.writeFileSync(DELIVERY_REPORT_FILE_PATH, `${lines.join('\n')}\n`, 'utf-8');
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
            text: normalizePlainTextEmail(j.pitchBody),
            html: buildTrackedHtml(buildEmailHtml(j.pitchBody), trackingId),
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

app.post('/send-test', async (req, res) => {
  try {
    const { pitchSubject, pitchBody, to } = req.body || {};
    const testRecipient = String(to || process.env.GMAIL_USER || '').trim();

    if (!testRecipient || !isValidEmail(testRecipient)) {
      return res.status(400).json({ ok: false, error: 'Test recipient email is not configured.' });
    }

    if (!pitchSubject || !pitchBody) {
      return res.status(400).json({ ok: false, error: 'Pitch subject and body are required.' });
    }

    const trackingId = TRACKING_ENABLED ? crypto.randomUUID() : '';

    await transporter.sendMail({
      from: process.env.GMAIL_USER,
      to: testRecipient,
      subject: `[TEST] ${pitchSubject}`,
      text: normalizePlainTextEmail(pitchBody),
      html: buildTrackedHtml(buildEmailHtml(pitchBody), trackingId),
    });

    res.status(200).json({
      ok: true,
      to: testRecipient,
      trackingEnabled: TRACKING_ENABLED,
      trackingId,
    });
  } catch (error) {
    console.error('ERRO NO ENVIO DE TESTE:', error);
    res.status(500).json({ ok: false, error: error.message || 'Failed to send test email.' });
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

app.get('/track/click/:trackingId', (req, res) => {
  const trackingId = String(req.params.trackingId || '').trim();
  const target = String(req.query.to || DEFAULT_CLICK_TARGET_URL).trim();
  const safeTarget = /^https:\/\/(www\.)?getkensho\.app\/?/i.test(target)
    ? target
    : DEFAULT_CLICK_TARGET_URL;
  const targetWithTracking = appendTrackingIdToTargetUrl(safeTarget, trackingId);

  if (trackingId) {
    const line = [
      csvEscape(trackingId),
      csvEscape(new Date().toISOString()),
      csvEscape(safeTarget),
      csvEscape(req.get('user-agent') || ''),
      csvEscape(req.ip || req.socket?.remoteAddress || ''),
    ].join(',');

    fs.appendFileSync(CLICK_EVENTS_FILE_PATH, line + '\n', 'utf-8');
  }

  res.redirect(302, targetWithTracking);
});

app.get('/open-events', (req, res) => {
  res.json(readOpenEventRecords());
});

app.get('/click-events', (req, res) => {
  res.json(readClickEventRecords());
});

app.post('/site-event', (req, res) => {
  const trackingId = String(req.body?.trackingId || '').trim();
  const eventType = String(req.body?.eventType || '').trim();

  if (!trackingId || !eventType) {
    return res.status(400).json({ ok: false, error: 'trackingId and eventType are required.' });
  }

  const sessionId = String(req.body?.sessionId || '').trim();
  const eventTimestamp = String(req.body?.timestamp || '').trim() || new Date().toISOString();
  const pathValue = String(req.body?.path || '').trim();
  const label = String(req.body?.label || '').trim();
  const value = String(req.body?.value || '').trim();
  const referrer = String(req.body?.referrer || '').trim();
  const durationMsRaw = Number(req.body?.durationMs);
  const durationMs = Number.isFinite(durationMsRaw) && durationMsRaw >= 0
    ? String(Math.round(durationMsRaw))
    : '';

  const line = [
    csvEscape(trackingId),
    csvEscape(sessionId),
    csvEscape(eventType),
    csvEscape(eventTimestamp),
    csvEscape(pathValue),
    csvEscape(label),
    csvEscape(value),
    csvEscape(durationMs),
    csvEscape(req.get('user-agent') || ''),
    csvEscape(req.ip || req.socket?.remoteAddress || ''),
    csvEscape(referrer),
  ].join(',');

  fs.appendFileSync(SITE_EVENTS_FILE_PATH, line + '\n', 'utf-8');
  res.status(200).json({ ok: true });
});

app.get('/site-events', (req, res) => {
  res.json(readSiteEventRecords());
});

app.get('/read-sent/:country', (req, res) => {
  if (!fs.existsSync(FILE_PATH)) return res.json([]);
  const content = fs.readFileSync(FILE_PATH, 'utf-8');
  res.json(content.split('\n').filter((l) => l.includes(`[Pais: ${req.params.country}]`)).map((l) => l.split(' - ')[0].trim()));
});

const PORT = process.env.PORT || 3001;

app.get('/', (req, res) => {
  res.send('OK');
});

app.get('/open/:id', (req, res) => {
  const { id } = req.params;
  console.log('Open tracking:', id, new Date().toISOString());

  const pixel = Buffer.from(
    'R0lGODlhAQABAPAAAAAAAAAAACH5BAEAAAAALAAAAAABAAEAAAICRAEAOw==',
    'base64'
  );

  res.set({
    'Content-Type': 'image/gif',
    'Content-Length': pixel.length,
    'Cache-Control': 'no-store, no-cache, must-revalidate, proxy-revalidate',
    'Pragma': 'no-cache',
    'Expires': '0'
  });

  res.end(pixel);
});

app.listen(PORT, "0.0.0.0", () => {
  console.log(`Servidor atualizado e pronto na porta ${PORT}`);
});

app.get('/sent-history', async (req, res) => {
  await syncRemoteTrackingEvents();

  const openEvents = readOpenEventRecords();
  const clickEvents = readClickEventRecords();
  const siteEvents = readSiteEventRecords();
  const eventsByTrackingId = openEvents.reduce((acc, event) => {
    if (!event.trackingId) return acc;
    if (!acc[event.trackingId]) acc[event.trackingId] = [];
    acc[event.trackingId].push(event);
    return acc;
  }, {});
  const clickEventsByTrackingId = clickEvents.reduce((acc, event) => {
    if (!event.trackingId) return acc;
    if (!acc[event.trackingId]) acc[event.trackingId] = [];
    acc[event.trackingId].push(event);
    return acc;
  }, {});
  const siteEventsByTrackingId = siteEvents.reduce((acc, event) => {
    if (!event.trackingId) return acc;
    if (!acc[event.trackingId]) acc[event.trackingId] = [];
    acc[event.trackingId].push(event);
    return acc;
  }, {});

  const records = readSentHistoryRecords().map((record) => {
    const openEventsForRecord = record.trackingId ? (eventsByTrackingId[record.trackingId] || []) : [];
    const clickEventsForRecord = record.trackingId ? (clickEventsByTrackingId[record.trackingId] || []) : [];
    const siteEventsForRecord = record.trackingId ? (siteEventsByTrackingId[record.trackingId] || []) : [];
    const openTimestamps = openEventsForRecord
      .map((event) => event.timestamp)
      .filter(Boolean)
      .sort();
    const clickTimestamps = clickEventsForRecord
      .map((event) => event.timestamp)
      .filter(Boolean)
      .sort();
    const siteTimestamps = siteEventsForRecord
      .map((event) => event.timestamp)
      .filter(Boolean)
      .sort();
    const playStoreClickCount = siteEventsForRecord.filter((event) => event.eventType === 'cta_click' && event.label === 'google_play').length;
    const privacyClickCount = siteEventsForRecord.filter((event) => event.eventType === 'footer_click' && event.label === 'privacy_policy').length;
    const termsClickCount = siteEventsForRecord.filter((event) => event.eventType === 'footer_click' && event.label === 'terms_of_use').length;
    const contactClickCount = siteEventsForRecord.filter((event) => event.eventType === 'footer_click' && event.label === 'contact_email').length;
    const footerClickCount = privacyClickCount + termsClickCount + contactClickCount;
    const totalEngagementMs = siteEventsForRecord.reduce((sum, event) => sum + Number(event.durationMs || 0), 0);
    const lastSiteEvent = siteEventsForRecord
      .slice()
      .sort((a, b) => String(a.timestamp || '').localeCompare(String(b.timestamp || '')))
      .pop();

    return {
      ...record,
      trackingEnabled: Boolean(record.trackingId),
      openCount: openEventsForRecord.length,
      firstOpenedAt: openTimestamps[0] || '',
      lastOpenedAt: openTimestamps[openTimestamps.length - 1] || '',
      opened: openEventsForRecord.length > 0,
      clickCount: clickEventsForRecord.length,
      firstClickedAt: clickTimestamps[0] || '',
      lastClickedAt: clickTimestamps[clickTimestamps.length - 1] || '',
      clicked: clickEventsForRecord.length > 0,
      lastClickedTarget: clickEventsForRecord[clickEventsForRecord.length - 1]?.target || '',
      siteVisited: siteEventsForRecord.length > 0,
      siteEventCount: siteEventsForRecord.length,
      firstSiteEventAt: siteTimestamps[0] || '',
      lastSiteEventAt: siteTimestamps[siteTimestamps.length - 1] || '',
      lastSiteEventType: lastSiteEvent?.eventType || '',
      playStoreClickCount,
      footerClickCount,
      privacyClickCount,
      termsClickCount,
      contactClickCount,
      totalEngagementMs,
    };
  });

  writeDeliveryReportCsv(records);

  res.json(records);
});
