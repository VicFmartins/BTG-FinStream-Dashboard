import { useEffect, useState } from "react";
import {
  Bar,
  BarChart,
  CartesianGrid,
  Cell,
  Line,
  LineChart,
  Pie,
  PieChart,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";

const API_BASE_URL = import.meta.env.VITE_API_BASE_URL || "/api";
const HEALTH_URL = `${API_BASE_URL}/health`;
const EVENTS_URL = `${API_BASE_URL}/events/latest`;
const HISTORY_URL = `${API_BASE_URL}/events/history?limit=72`;
const OPS_HEALTH_URL = `${API_BASE_URL}/ops/health`;
const OPS_METRICS_URL = `${API_BASE_URL}/ops/metrics`;
const OPS_DLQ_URL = `${API_BASE_URL}/ops/dlq`;
const MAX_EVENTS = 10;
const MAX_HISTORY_EVENTS = 72;
const NAV_ITEMS = [
  { key: "overview", title: "Overview" },
  { key: "transactionFeed", title: "Transaction Feed" },
  { key: "analytics", title: "Analytics" },
  { key: "operations", title: "Operations" },
];

const EVENT_TYPE_META = {
  BUY: {
    tone: "buy",
    accent: "Positive flow",
  },
  SELL: {
    tone: "sell",
    accent: "Exit flow",
  },
  DEPOSIT: {
    tone: "deposit",
    accent: "Funding flow",
  },
  WITHDRAWAL: {
    tone: "withdrawal",
    accent: "Risk watch",
  },
};

const EVENT_TYPE_COLORS = {
  BUY: "#45b787",
  SELL: "#c6964a",
  DEPOSIT: "#819edb",
  WITHDRAWAL: "#c96f77",
};

const EMPTY_METRICS = {
  total_processed_events: 0,
  total_transaction_volume: 0,
  events_by_type: {
    BUY: 0,
    SELL: 0,
    DEPOSIT: 0,
    WITHDRAWAL: 0,
  },
  latest_client_id: null,
  latest_asset: null,
  latest_event_timestamp: null,
};

const EMPTY_OPS = {
  total_valid_events: 0,
  total_invalid_events: 0,
  total_persisted_events: 0,
  duplicate_events_skipped: 0,
  last_successful_event_timestamp: null,
  last_invalid_event_timestamp: null,
};

function createWebSocketUrl() {
  if (import.meta.env.VITE_WS_URL) {
    return import.meta.env.VITE_WS_URL;
  }

  const protocol = window.location.protocol === "https:" ? "wss" : "ws";
  return `${protocol}://${window.location.host}/ws/transactions`;
}

function formatCurrency(value) {
  return new Intl.NumberFormat("en-US", {
    style: "currency",
    currency: "USD",
    maximumFractionDigits: 2,
  }).format(value);
}

function formatCompactNumber(value) {
  return new Intl.NumberFormat("en-US", {
    notation: "compact",
    maximumFractionDigits: 1,
  }).format(value);
}

function formatQuantity(value) {
  return new Intl.NumberFormat("en-US", {
    maximumFractionDigits: 0,
  }).format(value);
}

function formatAxisTime(timestamp) {
  return new Date(timestamp).toLocaleTimeString([], {
    hour: "2-digit",
    minute: "2-digit",
  });
}

function parseEventDate(timestamp) {
  const parsedDate = new Date(timestamp);
  return Number.isNaN(parsedDate.getTime()) ? null : parsedDate;
}

function toMinuteBucket(date) {
  const bucketDate = new Date(date);
  bucketDate.setSeconds(0, 0);
  return bucketDate;
}

function formatTimestamp(timestamp) {
  return new Date(timestamp).toLocaleString();
}

function formatStatusLabel(value) {
  if (!value) {
    return "Unknown";
  }

  return value
    .split("-")
    .map((token) => token.charAt(0).toUpperCase() + token.slice(1))
    .join(" ");
}

function upsertEvent(currentEvents, incomingEvent) {
  const nextEvents = [
    incomingEvent,
    ...currentEvents.filter((event) => event.event_id !== incomingEvent.event_id),
  ];

  return nextEvents.slice(0, MAX_EVENTS);
}

function upsertHistoryEvent(currentEvents, incomingEvent) {
  const nextEvents = [
    incomingEvent,
    ...currentEvents.filter((event) => event.event_id !== incomingEvent.event_id),
  ];

  nextEvents.sort((left, right) => new Date(right.timestamp) - new Date(left.timestamp));
  return nextEvents.slice(0, MAX_HISTORY_EVENTS);
}

function percentage(value, total) {
  if (!total) {
    return 0;
  }

  return Math.min((value / total) * 100, 100);
}

function sparklineFromValues(values) {
  const safeValues = values.map((value) => Number(value) || 0);
  const maxValue = Math.max(...safeValues, 1);

  return safeValues.map((value) => Math.max(18, Math.round((value / maxValue) * 100)));
}

function ChartTooltip({ active, payload, label }) {
  if (!active || !payload?.length) {
    return null;
  }

  return (
    <div className="chart-tooltip">
      {label ? <p className="chart-tooltip-label">{label}</p> : null}
      {payload.map((entry) => (
        <div key={`${entry.name}-${entry.dataKey}`} className="chart-tooltip-row">
          <span className="chart-tooltip-key" style={{ color: entry.color }}>
            {entry.name}
          </span>
          <strong>
            {entry.payload?.format === "currency" || entry.dataKey === "notional"
              ? formatCurrency(entry.value)
              : formatCompactNumber(entry.value)}
          </strong>
        </div>
      ))}
    </div>
  );
}

function StatusBadge({ status }) {
  return (
    <div className={`status-badge status-${status}`}>
      <span className="status-badge-dot" />
      <span>{formatStatusLabel(status)}</span>
    </div>
  );
}

function MetricCard({
  label,
  value,
  note,
  trend,
  sparkline = [],
  accent = "blue",
  href,
  hrefLabel,
}) {
  return (
    <article className={`metric-card metric-card-${accent}`}>
      <div className="metric-card-topline">
        <span className="card-label">{label}</span>
        {trend ? <span className={`trend-chip trend-chip-${accent}`}>{trend}</span> : null}
      </div>
      <strong className="metric-value">{value}</strong>
      <div className="metric-card-bottom">
        <span className="card-note">{note}</span>
        {sparkline.length ? (
          <div className="metric-sparkline" aria-hidden="true">
            {sparkline.map((height, index) => (
              <span key={`${label}-${index}`} style={{ height: `${height}%` }} />
            ))}
          </div>
        ) : null}
      </div>
      {href ? (
        <a className="card-link" href={href} target="_blank" rel="noreferrer">
          {hrefLabel}
        </a>
      ) : null}
    </article>
  );
}

function PanelHeader({ eyebrow, title, aside }) {
  return (
    <div className="panel-header">
      <div>
        <p className="panel-eyebrow">{eyebrow}</p>
        <h3>{title}</h3>
      </div>
      {aside ? <div className="panel-aside">{aside}</div> : null}
    </div>
  );
}

function TypeBadge({ type }) {
  const tone = EVENT_TYPE_META[type]?.tone ?? "deposit";

  return <span className={`type-badge type-badge-${tone}`}>{type}</span>;
}

function OperationalBar({ label, value, total, tone, helper }) {
  const width = percentage(value, total);

  return (
    <div className="ops-row">
      <div className="ops-row-copy">
        <div>
          <span className="card-label">{label}</span>
          <strong>{value}</strong>
        </div>
        <span className={`ops-helper ops-helper-${tone}`}>{helper}</span>
      </div>
      <div className="ops-track">
        <div className={`ops-fill ops-fill-${tone}`} style={{ width: `${width}%` }} />
      </div>
    </div>
  );
}

function App() {
  const [connectionStatus, setConnectionStatus] = useState("connecting");
  const [activeSection, setActiveSection] = useState("overview");
  const [events, setEvents] = useState([]);
  const [historyEvents, setHistoryEvents] = useState([]);
  const [metrics, setMetrics] = useState(EMPTY_METRICS);
  const [opsMetrics, setOpsMetrics] = useState(EMPTY_OPS);
  const [pipelineHealth, setPipelineHealth] = useState("checking");
  const [dlqPreview, setDlqPreview] = useState([]);
  const [backendHealth, setBackendHealth] = useState("checking");

  useEffect(() => {
    let isMounted = true;

    async function loadInitialState() {
      try {
        const [
          healthResponse,
          eventsResponse,
          historyResponse,
          opsHealthResponse,
          opsMetricsResponse,
          opsDlqResponse,
        ] = await Promise.all([
          fetch(HEALTH_URL),
          fetch(EVENTS_URL),
          fetch(HISTORY_URL),
          fetch(OPS_HEALTH_URL),
          fetch(OPS_METRICS_URL),
          fetch(OPS_DLQ_URL),
        ]);

        if (!isMounted) {
          return;
        }

        setBackendHealth(healthResponse.ok ? "healthy" : "degraded");

        if (eventsResponse.ok) {
          const payload = await eventsResponse.json();
          setMetrics(payload.metrics ?? EMPTY_METRICS);
          if (payload.latest_event) {
            setEvents([payload.latest_event]);
          }
        }

        if (historyResponse.ok) {
          const payload = await historyResponse.json();
          setHistoryEvents(payload);
        }

        if (opsHealthResponse.ok) {
          const payload = await opsHealthResponse.json();
          setPipelineHealth(payload.status ?? "unknown");
        }

        if (opsMetricsResponse.ok) {
          const payload = await opsMetricsResponse.json();
          setOpsMetrics(payload);
        }

        if (opsDlqResponse.ok) {
          const payload = await opsDlqResponse.json();
          setDlqPreview(payload.slice(0, 4));
        }
      } catch {
        if (isMounted) {
          setBackendHealth("unreachable");
          setPipelineHealth("unreachable");
        }
      }
    }

    loadInitialState();

    return () => {
      isMounted = false;
    };
  }, []);

  useEffect(() => {
    let websocket;
    let reconnectTimer;
    let closedByApp = false;

    function connect() {
      setConnectionStatus("connecting");
      websocket = new WebSocket(createWebSocketUrl());

      websocket.onopen = () => {
        setConnectionStatus("connected");
      };

      websocket.onmessage = (message) => {
        const payload = JSON.parse(message.data);

        if (payload.metrics) {
          setMetrics(payload.metrics);
        }

        if (payload.ops) {
          setOpsMetrics(payload.ops);
          setPipelineHealth(payload.ops.total_invalid_events > 0 ? "degraded" : "healthy");
        }

        if (payload.dlq) {
          setDlqPreview(payload.dlq.slice(0, 4));
        }

        if (payload.type === "snapshot" && payload.event) {
          setEvents((currentEvents) => upsertEvent(currentEvents, payload.event));
          setHistoryEvents((currentEvents) => upsertHistoryEvent(currentEvents, payload.event));
          return;
        }

        if (payload.type === "transaction_event" && payload.event) {
          setEvents((currentEvents) => upsertEvent(currentEvents, payload.event));
          setHistoryEvents((currentEvents) => upsertHistoryEvent(currentEvents, payload.event));
        }
      };

      websocket.onerror = () => {
        setConnectionStatus("degraded");
      };

      websocket.onclose = () => {
        if (closedByApp) {
          return;
        }

        setConnectionStatus("reconnecting");
        reconnectTimer = window.setTimeout(connect, 2000);
      };
    }

    connect();

    return () => {
      closedByApp = true;
      window.clearTimeout(reconnectTimer);
      websocket?.close();
    };
  }, []);

  const latestEvent = events[0] ?? null;
  const liveWindowLabel = metrics.latest_event_timestamp
    ? `Last event ${formatTimestamp(metrics.latest_event_timestamp)}`
    : "No live traffic yet";
  const latestClientLabel = metrics.latest_client_id ?? "Awaiting flow";
  const latestAssetLabel = metrics.latest_asset ?? "Awaiting flow";
  const lastInvalidLabel = opsMetrics.last_invalid_event_timestamp
    ? formatTimestamp(opsMetrics.last_invalid_event_timestamp)
    : "No invalid events";
  const latestSuccessfulLabel = opsMetrics.last_successful_event_timestamp
    ? formatTimestamp(opsMetrics.last_successful_event_timestamp)
    : "No persisted events yet";
  const analyticsSourceEvents = historyEvents.length ? historyEvents : events;

  const eventTypeEntries = Object.entries(metrics.events_by_type ?? EMPTY_METRICS.events_by_type);
  const totalObservedOps = Math.max(
    opsMetrics.total_valid_events + opsMetrics.total_invalid_events,
    1,
  );
  const totalValidEvents = Math.max(opsMetrics.total_valid_events, 1);
  const processedSparkline = sparklineFromValues([
    metrics.events_by_type.BUY,
    metrics.events_by_type.SELL,
    metrics.events_by_type.DEPOSIT,
    metrics.events_by_type.WITHDRAWAL,
    metrics.total_processed_events,
  ]);
  const volumeSparkline = sparklineFromValues([
    metrics.total_transaction_volume * 0.2,
    metrics.total_transaction_volume * 0.35,
    metrics.total_transaction_volume * 0.5,
    metrics.total_transaction_volume * 0.72,
    metrics.total_transaction_volume,
  ]);
  const clientSparkline = sparklineFromValues([
    opsMetrics.total_valid_events,
    opsMetrics.total_persisted_events,
    Math.max(opsMetrics.total_valid_events - opsMetrics.duplicate_events_skipped, 1),
    metrics.total_processed_events,
  ]);
  const assetSparkline = sparklineFromValues([
    metrics.events_by_type.DEPOSIT,
    metrics.events_by_type.BUY,
    metrics.events_by_type.SELL,
    metrics.events_by_type.WITHDRAWAL,
  ]);
  const dominantEvent = eventTypeEntries.reduce(
    (current, [type, count]) => (count > current.count ? { type, count } : current),
    { type: "BUY", count: 0 },
  );
  const invalidRate = percentage(opsMetrics.total_invalid_events, totalObservedOps).toFixed(1);
  const duplicateRate = percentage(opsMetrics.duplicate_events_skipped, totalValidEvents).toFixed(1);
  const persistedRate = percentage(opsMetrics.total_persisted_events, totalValidEvents).toFixed(1);
  const validAnalyticsEvents = analyticsSourceEvents
    .map((event) => {
      const parsedDate = parseEventDate(event.timestamp);
      if (!parsedDate) {
        return null;
      }

      return {
        ...event,
        parsedDate,
      };
    })
    .filter(Boolean);
  const volumeBuckets = validAnalyticsEvents.reduce((accumulator, event) => {
    const bucketDate = toMinuteBucket(event.parsedDate);
    const bucketKey = bucketDate.toISOString();
    const currentBucket = accumulator.get(bucketKey) ?? {
      time: formatAxisTime(bucketKey),
      timestamp: formatTimestamp(bucketKey),
      volume: 0,
      format: "currency",
    };

    currentBucket.volume += Number(event.notional_amount ?? event.amount ?? 0);
    accumulator.set(bucketKey, currentBucket);
    return accumulator;
  }, new Map());
  const volumeSeries = [...volumeBuckets.entries()]
    .sort(([leftKey], [rightKey]) => new Date(leftKey) - new Date(rightKey))
    .slice(-24)
    .map(([, bucket]) => ({
      time: bucket.time,
      volume: Number(bucket.volume.toFixed(2)),
      timestamp: bucket.timestamp,
      format: "currency",
    }));
  const eventDistributionChartData = Object.entries(
    analyticsSourceEvents.reduce(
      (accumulator, event) => ({
        ...accumulator,
        [event.event_type]: (accumulator[event.event_type] ?? 0) + 1,
      }),
      { BUY: 0, SELL: 0, DEPOSIT: 0, WITHDRAWAL: 0 },
    ),
  ).map(([name, value]) => ({
    name,
    value,
    color: EVENT_TYPE_COLORS[name],
    format: "count",
  }));
  const assetActivityData = Object.values(
    analyticsSourceEvents.reduce((accumulator, event) => {
      const entry = accumulator[event.asset] ?? {
        asset: event.asset,
        trades: 0,
        notional: 0,
        format: "currency",
      };

      entry.trades += 1;
      entry.notional += Number(event.notional_amount ?? event.amount ?? 0);
      accumulator[event.asset] = entry;
      return accumulator;
    }, {}),
  )
    .sort((left, right) => right.notional - left.notional)
    .slice(0, 5);
  const dominantAsset = assetActivityData[0];

  if (import.meta.env.DEV) {
    console.debug("Analytics chart pipeline", {
      rawHistoryEventsCount: historyEvents.length,
      liveEventsCount: events.length,
      parsedValidEventsCount: validAnalyticsEvents.length,
      finalAggregatedSeries: volumeSeries,
    });
  }

  const sectionDescriptions = {
    overview:
      "Continuous visibility into transaction ingestion, persistence, and operational integrity across the live event pipeline.",
    transactionFeed:
      "Focused monitoring of the live transaction tape with the latest processed event and the most recent transaction rows.",
    analytics:
      "Derived monitoring of flow composition, event distribution, and trend context from the live in-memory dashboard state.",
    operations:
      "Operational supervision of backend health, consumer integrity, persistence quality, and invalid-event handling.",
  };

  const latestEventPanel = (
    <article className="panel panel-highlight">
      <PanelHeader
        eyebrow="Live Transaction Feed"
        title="Latest processed event"
        aside={<span className="panel-pill">Streaming terminal</span>}
      />

      {latestEvent ? (
        <div className="highlight-grid">
          <div className="highlight-primary">
            <div className="highlight-header">
              <div>
                <span className="card-label">Client</span>
                <strong>{latestEvent.client_id}</strong>
              </div>
              <TypeBadge type={latestEvent.event_type} />
            </div>

            <div className="highlight-amount">
              <span className="card-label">Notional</span>
              <strong>{formatCurrency(latestEvent.notional_amount ?? latestEvent.amount)}</strong>
            </div>

            <p className="card-note">
              Event {latestEvent.event_id} processed through the backend consumer and
              published to the live dashboard stream.
            </p>
          </div>

          <div className="highlight-details">
            <div className="detail-row">
              <span className="card-label">Asset</span>
              <strong>{latestEvent.asset}</strong>
            </div>
            <div className="detail-row">
              <span className="card-label">Unit Price</span>
              <strong>{formatCurrency(latestEvent.unit_price ?? latestEvent.amount)}</strong>
            </div>
            <div className="detail-row">
              <span className="card-label">Quantity</span>
              <strong>{formatQuantity(latestEvent.quantity ?? 1)}</strong>
            </div>
            <div className="detail-row">
              <span className="card-label">Timestamp</span>
              <strong>{formatTimestamp(latestEvent.timestamp)}</strong>
            </div>
            <div className="detail-row">
              <span className="card-label">Pipeline Health</span>
              <strong>{formatStatusLabel(pipelineHealth)}</strong>
            </div>
          </div>
        </div>
      ) : (
        <div className="empty-state">
          Waiting for the first processed transaction event to arrive.
        </div>
      )}
    </article>
  );

  const recentTransactionsPanel = (
    <article className="panel">
      <PanelHeader
        eyebrow="Recent Transactions"
        title="Live transaction tape"
        aside={<span className="panel-pill panel-pill-muted">Latest {MAX_EVENTS} events</span>}
      />

      <div className="table-wrap">
        <table>
          <thead>
            <tr>
              <th>Time</th>
              <th>Client</th>
              <th>Asset</th>
              <th>Type</th>
              <th>Unit Price</th>
              <th>Qty</th>
              <th>Notional</th>
            </tr>
          </thead>
          <tbody>
            {events.length ? (
              events.map((event) => (
                <tr key={event.event_id}>
                  <td>{formatTimestamp(event.timestamp)}</td>
                  <td>{event.client_id}</td>
                  <td className="asset-cell">{event.asset}</td>
                  <td>
                    <TypeBadge type={event.event_type} />
                  </td>
                  <td>{formatCurrency(event.unit_price ?? event.amount)}</td>
                  <td>{formatQuantity(event.quantity ?? 1)}</td>
                  <td>{formatCurrency(event.notional_amount ?? event.amount)}</td>
                </tr>
              ))
            ) : (
              <tr>
                <td colSpan="7" className="empty-table">
                  No events received yet.
                </td>
              </tr>
            )}
          </tbody>
        </table>
      </div>

      <p className="panel-footnote">
        Historical data is available at <strong>/api/events/history</strong>.
      </p>
    </article>
  );

  const eventMixPanel = (
    <article className="panel">
      <PanelHeader eyebrow="Events By Type" title="Flow composition" />

      <div className="type-grid">
        {eventTypeEntries.map(([eventType, count]) => (
          <div key={eventType} className={`type-card type-card-${EVENT_TYPE_META[eventType]?.tone}`}>
            <span className="card-label">{eventType}</span>
            <strong>{count}</strong>
            <span className="card-note">{EVENT_TYPE_META[eventType]?.accent}</span>
          </div>
        ))}
      </div>
    </article>
  );

  const operationsPanel = (
    <article className="panel">
      <PanelHeader
        eyebrow="Operational Metrics"
        title="Pipeline status"
        aside={<span className="panel-pill panel-pill-muted">{formatStatusLabel(pipelineHealth)}</span>}
      />

      <div className="ops-stack">
        <OperationalBar
          label="Valid"
          value={opsMetrics.total_valid_events}
          total={totalObservedOps}
          tone="good"
          helper={`${percentage(opsMetrics.total_valid_events, totalObservedOps).toFixed(1)}%`}
        />
        <OperationalBar
          label="Persisted"
          value={opsMetrics.total_persisted_events}
          total={totalValidEvents}
          tone="blue"
          helper={`${persistedRate}%`}
        />
        <OperationalBar
          label="Duplicates"
          value={opsMetrics.duplicate_events_skipped}
          total={totalValidEvents}
          tone="muted"
          helper={`${duplicateRate}%`}
        />
        <OperationalBar
          label="Invalid"
          value={opsMetrics.total_invalid_events}
          total={totalObservedOps}
          tone="danger"
          helper={`${invalidRate}%`}
        />
      </div>

      <div className="ops-summary-grid">
        <div className="ops-stat">
          <span className="card-label">Persisted</span>
          <strong>{formatCompactNumber(opsMetrics.total_persisted_events)}</strong>
        </div>
        <div className="ops-stat">
          <span className="card-label">Duplicates</span>
          <strong>{formatCompactNumber(opsMetrics.duplicate_events_skipped)}</strong>
        </div>
        <div className="ops-stat">
          <span className="card-label">Last Success</span>
          <strong>{latestSuccessfulLabel}</strong>
        </div>
        <div className="ops-stat">
          <span className="card-label">Last Invalid</span>
          <strong>{lastInvalidLabel}</strong>
        </div>
      </div>
    </article>
  );

  const dlqPanel = (
    <article className="panel panel-dlq">
      <PanelHeader
        eyebrow="DLQ Preview"
        title="Invalid event watch"
        aside={<span className="panel-pill panel-pill-danger">{dlqPreview.length} recent</span>}
      />

      {dlqPreview.length ? (
        <div className="dlq-list">
          {dlqPreview.map((item) => (
            <div key={`${item.received_at}-${item.error}`} className="dlq-item">
              <div className="dlq-item-header">
                <span className="dlq-error">{item.error}</span>
                <strong>{formatTimestamp(item.received_at)}</strong>
              </div>
              <code>{JSON.stringify(item.payload)}</code>
            </div>
          ))}
        </div>
      ) : (
        <div className="empty-state">
          No invalid payloads are currently present in the dead-letter preview.
        </div>
      )}
    </article>
  );

  const analyticsInsightsPanel = (
    <article className="panel">
      <PanelHeader
        eyebrow="Derived Signals"
        title="Live pattern snapshot"
        aside={<span className="panel-pill panel-pill-muted">In-memory view</span>}
      />

      <div className="analytics-insights-grid">
        <div className="ops-stat">
          <span className="card-label">Dominant Event</span>
          <strong>{dominantEvent.type}</strong>
          <span className="card-note">{dominantEvent.count} observed</span>
        </div>
        <div className="ops-stat">
          <span className="card-label">Persisted Rate</span>
          <strong>{persistedRate}%</strong>
          <span className="card-note">valid events written to Postgres</span>
        </div>
        <div className="ops-stat">
          <span className="card-label">Duplicate Rate</span>
          <strong>{duplicateRate}%</strong>
          <span className="card-note">duplicate event_id suppression</span>
        </div>
        <div className="ops-stat">
          <span className="card-label">Invalid Rate</span>
          <strong>{invalidRate}%</strong>
          <span className="card-note">malformed payload visibility</span>
        </div>
      </div>
    </article>
  );

  const analyticsVolumePanel = (
    <article className="panel analytics-chart-panel">
      <PanelHeader
        eyebrow="Volume Over Time"
        title="Intraday transaction volume"
        aside={<span className="panel-pill panel-pill-muted">Last {volumeSeries.length} prints</span>}
      />

      <div className="chart-shell chart-shell-wide">
        {volumeSeries.length ? (
          <ResponsiveContainer width="100%" height={252}>
            <LineChart data={volumeSeries} margin={{ top: 8, right: 8, left: 0, bottom: 0 }}>
              <CartesianGrid stroke="rgba(129, 158, 219, 0.08)" vertical={false} />
              <XAxis
                dataKey="time"
                tickLine={false}
                axisLine={false}
                tick={{ fill: "#7d879d", fontSize: 11 }}
              />
              <YAxis
                tickFormatter={(value) => formatCompactNumber(value)}
                tickLine={false}
                axisLine={false}
                tick={{ fill: "#7d879d", fontSize: 11 }}
                width={44}
              />
              <Tooltip content={<ChartTooltip />} />
              <Line
                type="monotone"
                dataKey="volume"
                name="Volume"
                stroke="#819edb"
                strokeWidth={2}
                dot={false}
                activeDot={{ r: 4, strokeWidth: 0, fill: "#819edb" }}
              />
            </LineChart>
          </ResponsiveContainer>
        ) : (
          <div className="empty-state">
            No valid transaction buckets available yet for volume visualization.
          </div>
        )}
      </div>
    </article>
  );

  const analyticsDistributionPanel = (
    <article className="panel analytics-chart-panel">
      <PanelHeader
        eyebrow="Event Distribution"
        title="Flow mix by event type"
        aside={<span className="panel-pill panel-pill-muted">Rolling window</span>}
      />

      <div className="chart-shell chart-shell-split">
        {eventDistributionChartData.some((entry) => entry.value > 0) ? (
          <>
            <ResponsiveContainer width="100%" height="100%">
              <PieChart>
                <Pie
                  data={eventDistributionChartData}
                  dataKey="value"
                  nameKey="name"
                  innerRadius={62}
                  outerRadius={88}
                  paddingAngle={3}
                >
                  {eventDistributionChartData.map((entry) => (
                    <Cell key={entry.name} fill={entry.color} />
                  ))}
                </Pie>
                <Tooltip content={<ChartTooltip />} />
              </PieChart>
            </ResponsiveContainer>

            <div className="chart-legend">
              {eventDistributionChartData.map((entry) => (
                <div key={entry.name} className="chart-legend-row">
                  <span className="chart-legend-key">
                    <span
                      className="chart-legend-dot"
                      style={{ backgroundColor: entry.color }}
                    />
                    {entry.name}
                  </span>
                  <strong>{entry.value}</strong>
                </div>
              ))}
            </div>
          </>
        ) : (
          <div className="empty-state">Event distribution will appear once live events are processed.</div>
        )}
      </div>
    </article>
  );

  const analyticsAssetPanel = (
    <article className="panel analytics-chart-panel">
      <PanelHeader
        eyebrow="Asset Concentration"
        title="Most active assets"
        aside={
          dominantAsset ? (
            <span className="panel-pill panel-pill-muted">{dominantAsset.asset} leads</span>
          ) : null
        }
      />

      <div className="chart-shell chart-shell-wide">
        {assetActivityData.length ? (
          <ResponsiveContainer width="100%" height="100%">
            <BarChart
              data={assetActivityData}
              layout="vertical"
              margin={{ top: 0, right: 8, left: 8, bottom: 0 }}
            >
              <CartesianGrid stroke="rgba(129, 158, 219, 0.08)" horizontal={false} />
              <XAxis
                type="number"
                tickFormatter={(value) => formatCompactNumber(value)}
                tickLine={false}
                axisLine={false}
                tick={{ fill: "#7d879d", fontSize: 11 }}
              />
              <YAxis
                type="category"
                dataKey="asset"
                tickLine={false}
                axisLine={false}
                tick={{ fill: "#d9e2f5", fontSize: 12 }}
                width={62}
              />
              <Tooltip content={<ChartTooltip />} />
              <Bar
                dataKey="notional"
                name="Notional"
                fill="#5874a7"
                radius={[0, 8, 8, 0]}
                maxBarSize={18}
              />
            </BarChart>
          </ResponsiveContainer>
        ) : (
          <div className="empty-state">Asset concentration becomes available as history builds up.</div>
        )}
      </div>

      <div className="chart-summary-grid">
        {assetActivityData.slice(0, 3).map((entry) => (
          <div key={entry.asset} className="chart-summary-card">
            <span className="card-label">{entry.asset}</span>
            <strong>{formatCurrency(entry.notional)}</strong>
            <span className="card-note">{entry.trades} transactions in window</span>
          </div>
        ))}
      </div>
    </article>
  );

  const operationsSnapshotPanel = (
    <article className="panel">
      <PanelHeader
        eyebrow="System Snapshot"
        title="Operational endpoints"
        aside={<span className="panel-pill panel-pill-muted">Frontend only</span>}
      />

      <div className="analytics-insights-grid">
        <div className="ops-stat">
          <span className="card-label">Backend Health</span>
          <strong>{formatStatusLabel(backendHealth)}</strong>
          <span className="card-note">/api/health available</span>
        </div>
        <div className="ops-stat">
          <span className="card-label">Consumer</span>
          <strong>{formatStatusLabel(connectionStatus)}</strong>
          <span className="card-note">WebSocket stream remains active</span>
        </div>
        <div className="ops-stat">
          <span className="card-label">History Endpoint</span>
          <strong>/events/history</strong>
          <span className="card-note">historical query source ready</span>
        </div>
        <div className="ops-stat">
          <span className="card-label">DLQ Items</span>
          <strong>{dlqPreview.length}</strong>
          <span className="card-note">recent invalid payload preview</span>
        </div>
      </div>
    </article>
  );

  const transactionFeedSidebarPanel = (
    <article className="panel">
      <PanelHeader
        eyebrow="Feed Context"
        title="Live tape monitor"
        aside={<span className="panel-pill panel-pill-muted">{formatStatusLabel(connectionStatus)}</span>}
      />

      <div className="analytics-insights-grid">
        <div className="ops-stat">
          <span className="card-label">Rows Loaded</span>
          <strong>{events.length}</strong>
          <span className="card-note">deduplicated recent events</span>
        </div>
        <div className="ops-stat">
          <span className="card-label">Latest Asset</span>
          <strong>{latestAssetLabel}</strong>
          <span className="card-note">currently leading the live tape</span>
        </div>
      </div>
    </article>
  );

  const sectionContent = {
    overview: (
      <div className="section-stack" key="overview">
        <section className="metrics-grid">
          <MetricCard
            label="Total Processed Events"
            value={formatCompactNumber(metrics.total_processed_events)}
            note="Consumer-accepted event volume"
            trend={`${formatStatusLabel(pipelineHealth)}`}
            sparkline={processedSparkline}
            accent="blue"
          />
          <MetricCard
            label="Total Transaction Volume"
            value={formatCurrency(metrics.total_transaction_volume)}
            note="Live aggregated transaction value"
            trend={`${opsMetrics.total_persisted_events} persisted`}
            sparkline={volumeSparkline}
            accent="blue"
          />
          <MetricCard
            label="Latest Client"
            value={latestClientLabel}
            note={liveWindowLabel}
            trend={`${opsMetrics.total_valid_events} valid`}
            sparkline={clientSparkline}
            accent="neutral"
          />
          <MetricCard
            label="Latest Asset"
            value={latestAssetLabel}
            note={`Backend ${formatStatusLabel(backendHealth)}`}
            trend={`${dlqPreview.length} dlq`}
            sparkline={assetSparkline}
            accent="neutral"
            href={HEALTH_URL}
            hrefLabel="Open health endpoint"
          />
        </section>

        <section className="workspace-grid">
          <div className="workspace-main">
            {latestEventPanel}
            {recentTransactionsPanel}
          </div>

          <aside className="workspace-side">
            {eventMixPanel}
            {operationsPanel}
            {dlqPanel}
          </aside>
        </section>
      </div>
    ),
    transactionFeed: (
      <div className="section-stack" key="transaction-feed">
        <section className="workspace-grid">
          <div className="workspace-main">
            {latestEventPanel}
            {recentTransactionsPanel}
          </div>
          <aside className="workspace-side">
            {transactionFeedSidebarPanel}
            {eventMixPanel}
          </aside>
        </section>
      </div>
    ),
    analytics: (
      <div className="section-stack" key="analytics">
        <section className="metrics-grid">
          <MetricCard
            label="Total Processed Events"
            value={formatCompactNumber(metrics.total_processed_events)}
            note="Consumer-accepted event volume"
            trend={`${formatStatusLabel(pipelineHealth)}`}
            sparkline={processedSparkline}
            accent="blue"
          />
          <MetricCard
            label="Total Transaction Volume"
            value={formatCurrency(metrics.total_transaction_volume)}
            note="Live aggregated transaction value"
            trend={`${persistedRate}% persisted`}
            sparkline={volumeSparkline}
            accent="blue"
          />
          <MetricCard
            label="Latest Client"
            value={latestClientLabel}
            note="most recent observed client"
            trend={`${dominantEvent.type} leading`}
            sparkline={clientSparkline}
            accent="neutral"
          />
          <MetricCard
            label="Latest Asset"
            value={latestAssetLabel}
            note="current front-of-book asset"
            trend={`${invalidRate}% invalid`}
            sparkline={assetSparkline}
            accent="neutral"
          />
        </section>

        <section className="analytics-grid">
          <div className="workspace-main">
            {analyticsVolumePanel}
            <div className="analytics-chart-grid">
              {analyticsDistributionPanel}
              {analyticsAssetPanel}
            </div>
            {analyticsInsightsPanel}
          </div>
          <aside className="workspace-side">
            {operationsPanel}
            {transactionFeedSidebarPanel}
          </aside>
        </section>
      </div>
    ),
    operations: (
      <div className="section-stack" key="operations">
        <section className="metrics-grid">
          <MetricCard
            label="Pipeline Health"
            value={formatStatusLabel(pipelineHealth)}
            note="consumer and DLQ operational state"
            trend={`${invalidRate}% invalid`}
            sparkline={processedSparkline}
            accent="neutral"
          />
          <MetricCard
            label="Persisted Events"
            value={formatCompactNumber(opsMetrics.total_persisted_events)}
            note={latestSuccessfulLabel}
            trend={`${persistedRate}% persisted`}
            sparkline={volumeSparkline}
            accent="blue"
          />
          <MetricCard
            label="Duplicate Events"
            value={formatCompactNumber(opsMetrics.duplicate_events_skipped)}
            note="event_id duplicates skipped"
            trend={`${duplicateRate}% duplicate`}
            sparkline={clientSparkline}
            accent="neutral"
          />
          <MetricCard
            label="Invalid Events"
            value={formatCompactNumber(opsMetrics.total_invalid_events)}
            note={lastInvalidLabel}
            trend={`${dlqPreview.length} in preview`}
            sparkline={assetSparkline}
            accent="neutral"
            href={OPS_HEALTH_URL}
            hrefLabel="Open ops health"
          />
        </section>

        <section className="operations-grid">
          <div className="workspace-main">
            {operationsPanel}
            {operationsSnapshotPanel}
          </div>
          <aside className="workspace-side">
            {dlqPanel}
            {transactionFeedSidebarPanel}
          </aside>
        </section>
      </div>
    ),
  };

  return (
    <main className="terminal-shell">
      <div className="ambient ambient-primary" />
      <div className="ambient ambient-secondary" />
      <div className="ambient ambient-grid" />
      <div className="terminal-layout">
        <aside className="sidebar">
          <div className="sidebar-brand">
            <p className="sidebar-mark">BTG FinStream</p>
            <span>Institutional terminal</span>
          </div>

          <nav className="sidebar-nav" aria-label="Primary">
            {NAV_ITEMS.map((item) => {
              let meta = "Live";
              if (item.key === "transactionFeed") {
                meta = `${events.length} rows`;
              }
              if (item.key === "analytics") {
                meta = dominantEvent.type;
              }
              if (item.key === "operations") {
                meta = formatStatusLabel(pipelineHealth);
              }

              return (
                <button
                  key={item.key}
                  className={`sidebar-item ${
                    activeSection === item.key ? "sidebar-item-active" : ""
                  }`}
                  type="button"
                  onClick={() => setActiveSection(item.key)}
                >
                  <span className="sidebar-item-title">{item.title}</span>
                  <span className="sidebar-item-meta">{meta}</span>
                </button>
              );
            })}
          </nav>

          <div className="sidebar-status">
            <div className="sidebar-status-card">
              <span className="card-label">Realtime Stream</span>
              <strong>{formatStatusLabel(connectionStatus)}</strong>
              <span className="card-note">{liveWindowLabel}</span>
            </div>
            <div className="sidebar-status-card">
              <span className="card-label">DLQ Watch</span>
              <strong>{dlqPreview.length}</strong>
              <span className="card-note">recent invalid payloads</span>
            </div>
          </div>
        </aside>

        <section className="terminal-main">
          <header className="topbar">
            <div className="brand-block">
              <p className="kicker">Institutional Monitoring</p>
              <div className="title-row">
                <h1>BTG FinStream Dashboard</h1>
                <StatusBadge status={connectionStatus} />
              </div>
              <p className="topbar-copy">
                Real-time oversight for transaction flows, client activity, and
                pipeline integrity across advisor and trading desk operations.
              </p>
            </div>

            <div className="topbar-rail">
              <div className="rail-chip">
                <span className="rail-chip-label">Backend</span>
                <strong>{formatStatusLabel(backendHealth)}</strong>
              </div>
              <div className="rail-chip">
                <span className="rail-chip-label">Pipeline</span>
                <strong>{formatStatusLabel(pipelineHealth)}</strong>
              </div>
              <div className="rail-chip">
                <span className="rail-chip-label">Topic</span>
                <strong>transactions.events</strong>
              </div>
            </div>
          </header>

          <section className="hero-panel">
            <div className="hero-copy">
              <p className="kicker">{NAV_ITEMS.find((item) => item.key === activeSection)?.title}</p>
              <h2>
                Real-time transaction <span>monitor</span>
              </h2>
              <p className="hero-subtitle">
                {sectionDescriptions[activeSection]}
              </p>
            </div>

            <div className="hero-summary">
              <div className="hero-summary-card">
                <span className="card-label">Active View</span>
                <strong>{NAV_ITEMS.find((item) => item.key === activeSection)?.title}</strong>
                <span className="card-note">live data remains active in the background</span>
              </div>
              <div className="hero-summary-card">
                <span className="card-label">Persisted State</span>
                <strong>{formatCompactNumber(opsMetrics.total_persisted_events)}</strong>
                <span className="card-note">{latestSuccessfulLabel}</span>
              </div>
            </div>
          </section>

          {sectionContent[activeSection]}
        </section>
      </div>
    </main>
  );
}

export default App;
