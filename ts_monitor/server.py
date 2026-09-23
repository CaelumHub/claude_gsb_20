"""
Time-Series Monitoring Server
- HTTP API for data ingestion, querying, and management
- Real-time data simulation
- Anomaly detection pipeline
"""

import json
import os
import time
import threading
import random
import math
from http.server import HTTPServer, BaseHTTPRequestHandler
from socketserver import ThreadingMixIn
from urllib.parse import urlparse, parse_qs
from datetime import datetime, timezone
from typing import Dict, Any, Optional, List

from storage import TimeSeriesStorage
from anomaly import AnomalyDetector
from downsample import downsample_simple


# The three environments being monitored. Each has its own independent
# data source; a data point belongs to an environment via tags.env.
DEFAULT_ENVS = ["production", "staging", "development"]
ENV_COLORS = {
    "production": "#ef4444",
    "staging": "#f59e0b",
    "development": "#10b981",
}
ENV_LABELS = {
    "production": "生产",
    "staging": "预发",
    "development": "开发",
}


class ThreadedHTTPServer(ThreadingMixIn, HTTPServer):
    """Multi-threaded HTTP server."""
    daemon_threads = True
    allow_reuse_address = True


def _point_env(points_tags: dict) -> str:
    """Extract environment from a point's tags, defaulting to 'default'."""
    return (points_tags or {}).get("env", "default")


def _parse_envs(query: Dict) -> Optional[List[str]]:
    """Parse repeated 'env' query params (?env=production&env=staging).

    Also accepts 'envs' and comma-separated values for convenience.
    """
    envs = query.get("env") or query.get("envs")
    if not envs:
        return None
    # Support both repeated params and a comma-separated value
    result = []
    for raw in envs:
        result.extend(e.strip() for e in raw.split(",") if e.strip())
    return result or None


def _run_rules_on_point(storage: TimeSeriesStorage, detector: AnomalyDetector,
                        metric: str, value: float, timestamp: float,
                        source: str, tags: dict, env: str):
    """Evaluate all applicable rules for one point; persist alerts.

    Rule matching honors an optional rule-level ``env`` scope. A rule with
    no env applies to every environment; a scoped rule only matches its env.
    Detection state is keyed by (metric, env) so environments never pollute
    each other's baselines.
    """
    alerts = []
    state_key = f"{env}::{metric}"
    for rule in storage.get_rules():
        if rule.get("metric") != metric or not rule.get("enabled", True):
            continue
        rule_env = rule.get("env")
        if rule_env and rule_env != env:
            continue
        is_anomaly, result = detector.detect(state_key, float(value), rule)
        if is_anomaly:
            alert = {
                "metric": metric,
                "env": env,
                "value": float(value),
                "rule_id": rule.get("id"),
                "rule_name": rule.get("name", "Unknown"),
                "algorithm": rule.get("algorithm"),
                "severity": rule.get("severity", "warning"),
                "score": result.get("score"),
                "details": result.get("details"),
                "timestamp": float(timestamp),
                "source": source,
                "tags": tags,
            }
            alerts.append(storage.add_alert(alert))
    return alerts


class TimeSeriesHandler(BaseHTTPRequestHandler):
    """HTTP request handler for the time-series API."""

    storage: TimeSeriesStorage = None
    detector: AnomalyDetector = None

    def log_message(self, format, *args):
        """Suppress default logging for cleaner output."""
        pass

    def _send_json(self, data: Any, status: int = 200):
        """Send JSON response."""
        self.send_response(status)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Access-Control-Allow-Methods', 'GET, POST, PUT, DELETE, OPTIONS')
        self.send_header('Access-Control-Allow-Headers', 'Content-Type')
        self.end_headers()
        self.wfile.write(json.dumps(data, default=str).encode())

    def _send_error(self, message: str, status: int = 400):
        """Send error response."""
        self._send_json({"error": message}, status)

    def _read_body(self) -> Dict:
        """Read and parse JSON request body."""
        content_length = int(self.headers.get('Content-Length', 0))
        if content_length == 0:
            return {}
        body = self.rfile.read(content_length)
        try:
            return json.loads(body)
        except json.JSONDecodeError:
            return {}

    def _parse_query(self) -> Dict:
        """Parse URL query parameters."""
        parsed = urlparse(self.path)
        return parse_qs(parsed.query)

    def do_OPTIONS(self):
        """Handle CORS preflight."""
        self.send_response(200)
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Access-Control-Allow-Methods', 'GET, POST, PUT, DELETE, OPTIONS')
        self.send_header('Access-Control-Allow-Headers', 'Content-Type')
        self.end_headers()

    def do_GET(self):
        """Handle GET requests."""
        parsed = urlparse(self.path)
        path = parsed.path
        # Normalize: strip trailing slash but keep root "/"
        if path != '/' and path.endswith('/'):
            path = path.rstrip('/')
        query = self._parse_query()

        try:
            if path == '/api/status':
                self._handle_status()
            elif path == '/api/data/query':
                self._handle_data_query(query)
            elif path == '/api/data/downsample':
                self._handle_downsample(query)
            elif path == '/api/data/metrics':
                self._handle_metrics(query)
            elif path == '/api/data/environments':
                self._handle_environments()
            elif path == '/api/dashboard':
                self._handle_dashboard(query)
            elif path == '/api/alerts':
                self._handle_get_alerts(query)
            elif path == '/api/rules':
                self._handle_get_rules()
            elif path == '/api/sources':
                self._handle_get_sources()
            elif path == '/api/shards':
                self._handle_shard_info()
            elif path == '/api/detector/state':
                self._handle_detector_state()
            elif path == '/':
                self._serve_frontend()
            else:
                self._send_error("Not found", 404)
        except Exception as e:
            self._send_error(f"Internal error: {str(e)}", 500)

    def do_POST(self):
        """Handle POST requests."""
        parsed = urlparse(self.path)
        path = parsed.path.rstrip('/')

        try:
            if path == '/api/data/ingest':
                self._handle_ingest()
            elif path == '/api/data/ingest/batch':
                self._handle_batch_ingest()
            elif path == '/api/alerts/acknowledge':
                self._handle_acknowledge_alert()
            elif path == '/api/alerts/resolve':
                self._handle_resolve_alert()
            elif path == '/api/rules':
                self._handle_add_rule()
            elif path == '/api/sources':
                self._handle_add_source()
            elif path == '/api/simulate':
                self._handle_simulate()
            else:
                self._send_error("Not found", 404)
        except Exception as e:
            self._send_error(f"Internal error: {str(e)}", 500)

    def do_DELETE(self):
        """Handle DELETE requests."""
        parsed = urlparse(self.path)
        path = parsed.path.rstrip('/')

        try:
            if path.startswith('/api/rules/'):
                rule_id = path.split('/')[-1]
                self._handle_delete_rule(rule_id)
            elif path.startswith('/api/sources/'):
                source_id = path.split('/')[-1]
                self._handle_delete_source(source_id)
            else:
                self._send_error("Not found", 404)
        except Exception as e:
            self._send_error(f"Internal error: {str(e)}", 500)

    # ---- API Handlers ----

    def _handle_status(self):
        """Server status endpoint."""
        stats = self.storage.get_stats()
        self._send_json({
            "status": "running",
            "version": "1.0.0",
            "uptime": time.time() - SERVER_START_TIME,
            "storage": stats,
            "timestamp": time.time()
        })

    def _handle_ingest(self):
        """Ingest a single data point."""
        body = self._read_body()
        metric = body.get("metric")
        value = body.get("value")
        timestamp = body.get("timestamp", time.time())
        tags = body.get("tags", {}) or {}
        source = body.get("source", "default")

        # Environment can be supplied either as a top-level field or via tags.env
        env = body.get("env") or tags.get("env") or "default"
        tags = {**tags, "env": env}

        if not metric or value is None:
            self._send_error("Missing 'metric' or 'value'")
            return

        # Store the data point
        self.storage.write(metric, float(timestamp), float(value), tags, source)

        # Run anomaly detection (state isolated per environment)
        alerts = _run_rules_on_point(
            self.storage, self.detector, metric, float(value),
            float(timestamp), source, tags, env
        )
        anomaly_result = alerts[0] if alerts else None

        self._send_json({
            "success": True,
            "metric": metric,
            "env": env,
            "timestamp": float(timestamp),
            "anomaly_detected": anomaly_result is not None,
            "alert": anomaly_result
        })

    def _handle_batch_ingest(self):
        """Ingest multiple data points."""
        body = self._read_body()
        points = body.get("points", [])

        if not points:
            self._send_error("Missing 'points' array")
            return

        # Normalize env onto tags before storage
        for p in points:
            tags = p.get("tags", {}) or {}
            env = p.get("env") or tags.get("env") or "default"
            p["tags"] = {**tags, "env": env}
            p["env"] = env

        self.storage.write_batch(points)

        # Run anomaly detection on each point (per-environment state)
        anomalies = []
        for p in points:
            anomalies.extend(_run_rules_on_point(
                self.storage, self.detector,
                p.get("metric", ""), float(p.get("value", 0)),
                float(p.get("timestamp", time.time())),
                p.get("source", "default"), p.get("tags", {}), p["env"]
            ))

        self._send_json({
            "success": True,
            "ingested": len(points),
            "anomalies_detected": len(anomalies),
            "alerts": anomalies
        })

    def _handle_data_query(self, query: Dict):
        """Query time-series data, optionally across multiple environments."""
        metric = query.get("metric", [None])[0]
        start = float(query.get("start", [time.time() - 3600])[0])
        end = float(query.get("end", [time.time()])[0])
        max_points = int(query.get("max_points", [1000])[0])
        envs = _parse_envs(query)
        grouped = query.get("grouped", ["0"])[0] in ("1", "true", "yes")

        if not metric:
            self._send_error("Missing 'metric' parameter")
            return

        if grouped:
            # One series per environment (used for overlay charts)
            target_envs = envs or self._all_envs()
            groups = {}
            for env in target_envs:
                groups[env] = self.storage.query(
                    metric, start, end, envs=[env], max_points=max_points
                )
            self._send_json({
                "metric": metric,
                "start": start,
                "end": end,
                "grouped": True,
                "envs": target_envs,
                "groups": groups,
                "count": sum(len(v) for v in groups.values())
            })
            return

        data = self.storage.query(metric, start, end, envs=envs, max_points=max_points)
        self._send_json({
            "metric": metric,
            "start": start,
            "end": end,
            "envs": envs or self._all_envs(),
            "count": len(data),
            "data": data
        })

    def _handle_downsample(self, query: Dict):
        """Query with downsampling. Supports multi-env grouped overlay queries."""
        metric = query.get("metric", [None])[0]
        start = float(query.get("start", [time.time() - 3600])[0])
        end = float(query.get("end", [time.time()])[0])
        target = int(query.get("target", [200])[0])
        method = query.get("method", ["lttb"])[0]
        envs = _parse_envs(query)
        grouped = query.get("grouped", ["0"])[0] in ("1", "true", "yes")

        if not metric:
            self._send_error("Missing 'metric' parameter")
            return

        if grouped:
            target_envs = envs or self._all_envs()
            groups = {}
            original_total = 0
            for env in target_envs:
                raw = self.storage.query(metric, start, end, envs=[env], max_points=50000)
                original_total += len(raw)
                groups[env] = downsample_simple(raw, target, method)
            self._send_json({
                "metric": metric,
                "grouped": True,
                "method": method,
                "envs": target_envs,
                "original_count": original_total,
                "downsampled_count": sum(len(v) for v in groups.values()),
                "groups": groups
            })
            return

        data = self.storage.query(metric, start, end, envs=envs, max_points=50000)
        downsampled = downsample_simple(data, target, method)

        self._send_json({
            "metric": metric,
            "envs": envs or self._all_envs(),
            "original_count": len(data),
            "downsampled_count": len(downsampled),
            "method": method,
            "data": downsampled
        })

    def _handle_metrics(self, query: Dict):
        """Get available metrics, plus the env -> metrics breakdown."""
        env = query.get("env", [None])[0]
        metrics = self.storage.get_metrics(env)
        env_metrics = self.storage.get_environment_metrics()
        self._send_json({
            "metrics": metrics,
            "env_metrics": env_metrics,
            "envs": sorted(env_metrics.keys())
        })

    def _handle_environments(self):
        """List environments: static config merged with observed live data."""
        configured = self.storage.get_environments()
        observed = self.storage.get_environment_metrics()
        env_ids = list(dict.fromkeys(list(DEFAULT_ENVS) +
                                     list(configured.keys()) +
                                     list(observed.keys())))
        result = {}
        for env_id in env_ids:
            result[env_id] = {
                "id": env_id,
                "label": ENV_LABELS.get(env_id, env_id),
                "color": ENV_COLORS.get(env_id, "#3b82f6"),
                "metrics": observed.get(env_id, []),
                "metric_count": len(observed.get(env_id, [])),
                **configured.get(env_id, {})
            }
        self._send_json({"environments": result, "count": len(result)})

    def _all_envs(self) -> List[str]:
        """All known environment ids (configured + observed)."""
        configured = self.storage.get_environments()
        observed = self.storage.get_environment_metrics()
        return list(dict.fromkeys(list(DEFAULT_ENVS) +
                                  list(configured.keys()) +
                                  list(observed.keys())))

    def _handle_dashboard(self, query: Dict):
        """Get dashboard summary data.

        Query params:
          period   - lookback window in seconds
          env      - restrict to one environment
          grouped=1 - return per-environment groups for cross-env comparison
        """
        now = time.time()
        period = int(query.get("period", [300])[0])
        env = query.get("env", [None])[0]
        grouped = query.get("grouped", ["0"])[0] in ("1", "true", "yes")

        def _summarize(metric: str, envs=None):
            data = self.storage.query(metric, now - period, now,
                                      envs=envs, max_points=200)
            if not data:
                return None
            values = [p["v"] for p in data]
            return {
                "current": round(values[-1], 4),
                "min": round(min(values), 4),
                "max": round(max(values), 4),
                "avg": round(sum(values) / len(values), 4),
                "count": len(values),
                "data": data[-50:]
            }

        if grouped:
            # Per-environment summary of the same metrics, enabling
            # cross-environment comparison of a metric's value difference.
            target_envs = self._all_envs()
            all_metrics = self.storage.get_metrics()[:20]
            groups: Dict[str, Dict] = {}
            comparison = []

            for target_env in target_envs:
                env_metrics = {}
                for metric in all_metrics:
                    summary = _summarize(metric, envs=[target_env])
                    if summary:
                        env_metrics[metric] = summary
                groups[target_env] = env_metrics

            # Build metric-centric comparison rows (env current values + delta)
            baseline_env = "production" if "production" in target_envs else target_envs[0]
            for metric in all_metrics:
                row = {"metric": metric, "by_env": {}}
                base_current = None
                for target_env in target_envs:
                    summary = groups[target_env].get(metric)
                    if not summary:
                        continue
                    row["by_env"][target_env] = {
                        "current": summary["current"],
                        "avg": summary["avg"],
                        "min": summary["min"],
                        "max": summary["max"],
                    }
                if metric in groups.get(baseline_env, {}):
                    base_current = groups[baseline_env][metric]["current"]
                    row["baseline_env"] = baseline_env
                    for target_env, vals in row["by_env"].items():
                        vals["diff_vs_baseline"] = round(vals["current"] - base_current, 4)
                if row["by_env"]:
                    comparison.append(row)

            self._send_json({
                "timestamp": now,
                "period": period,
                "grouped": True,
                "envs": target_envs,
                "groups": groups,
                "comparison": comparison,
                "recent_alerts": self.storage.get_alerts(limit=10),
                "stats": self.storage.get_stats()
            })
            return

        metrics = self.storage.get_metrics(env)
        envs_filter = [env] if env else None
        dashboard_data = {}

        for metric in metrics[:20]:  # Limit to 20 metrics
            summary = _summarize(metric, envs=envs_filter)
            if summary:
                dashboard_data[metric] = summary

        # Recent alerts
        recent_alerts = self.storage.get_alerts(limit=10)
        if env:
            recent_alerts = [a for a in recent_alerts
                             if (a.get("env") or (a.get("tags") or {}).get("env", "default")) == env]

        self._send_json({
            "timestamp": now,
            "period": period,
            "env": env,
            "metrics": dashboard_data,
            "recent_alerts": recent_alerts,
            "stats": self.storage.get_stats()
        })

    def _handle_get_alerts(self, query: Dict):
        """Get alerts list."""
        status = query.get("status", [None])[0]
        severity = query.get("severity", [None])[0]
        env = query.get("env", [None])[0]
        limit = int(query.get("limit", [200])[0])

        alerts = self.storage.get_alerts(status=status, severity=severity, limit=limit)
        if env:
            alerts = [a for a in alerts
                      if (a.get("env") or (a.get("tags") or {}).get("env", "default")) == env]
        self._send_json({"alerts": alerts, "count": len(alerts)})

    def _handle_acknowledge_alert(self):
        """Acknowledge an alert."""
        body = self._read_body()
        alert_id = body.get("alert_id")
        if not alert_id:
            self._send_error("Missing 'alert_id'")
            return

        success = self.storage.acknowledge_alert(alert_id)
        self._send_json({"success": success})

    def _handle_resolve_alert(self):
        """Resolve an alert."""
        body = self._read_body()
        alert_id = body.get("alert_id")
        if not alert_id:
            self._send_error("Missing 'alert_id'")
            return

        success = self.storage.resolve_alert(alert_id)
        self._send_json({"success": success})

    def _handle_get_rules(self):
        """Get all rules."""
        rules = self.storage.get_rules()
        self._send_json({"rules": rules, "count": len(rules)})

    def _handle_add_rule(self):
        """Add or update a rule."""
        body = self._read_body()
        if not body.get("metric") or not body.get("algorithm"):
            self._send_error("Missing 'metric' or 'algorithm'")
            return

        rule = self.storage.add_rule(body)
        self._send_json({"success": True, "rule": rule})

    def _handle_delete_rule(self, rule_id: str):
        """Delete a rule."""
        success = self.storage.delete_rule(rule_id)
        self._send_json({"success": success})

    def _handle_get_sources(self):
        """Get all data sources."""
        sources = self.storage.get_sources()
        self._send_json({"sources": sources, "count": len(sources)})

    def _handle_add_source(self):
        """Add or update a data source."""
        body = self._read_body()
        source_id = body.get("id") or f"src_{int(time.time()*1000)}"
        source = self.storage.add_source(source_id, body)
        self._send_json({"success": True, "source": source})

    def _handle_delete_source(self, source_id: str):
        """Delete a data source."""
        success = self.storage.delete_source(source_id)
        self._send_json({"success": success})

    def _handle_shard_info(self):
        """Get shard file information."""
        info = self.storage.get_shard_info()
        self._send_json({"shards": info, "count": len(info)})

    def _handle_detector_state(self):
        """Get anomaly detector state."""
        state = self.detector.get_state_info()
        self._send_json({"state": state})

    def _handle_simulate(self):
        """Trigger data simulation into one or more environments."""
        body = self._read_body()
        duration = body.get("duration", 60)
        interval = body.get("interval", 1)
        metrics = body.get("metrics", ["cpu.usage", "memory.usage", "disk.io", "network.throughput"])
        # Optional: one environment or a list; defaults to all three
        envs = body.get("envs") or ([body["env"]] if body.get("env") else DEFAULT_ENVS)

        # Start simulation in background
        sim_thread = threading.Thread(
            target=_run_simulation,
            args=(self.storage, self.detector, metrics, duration, interval, envs),
            daemon=True
        )
        sim_thread.start()

        self._send_json({
            "success": True,
            "message": f"Simulation started for {duration}s",
            "metrics": metrics,
            "envs": envs
        })

    def _serve_frontend(self):
        """Serve the frontend HTML."""
        # Try multiple paths
        script_dir = os.path.dirname(os.path.abspath(__file__))
        candidates = [
            os.path.join(script_dir, "..", "ts_dashboard.html"),
            os.path.join(script_dir, "ts_dashboard.html"),
            os.path.join(os.getcwd(), "ts_dashboard.html"),
            os.path.join(os.getcwd(), "..", "ts_dashboard.html"),
        ]
        html_path = None
        for p in candidates:
            if os.path.exists(p):
                html_path = p
                break
        if html_path:
            with open(html_path, 'r') as f:
                content = f.read()
            self.send_response(200)
            self.send_header('Content-Type', 'text/html; charset=utf-8')
            self.end_headers()
            self.wfile.write(content.encode())
        else:
            self._send_error("Frontend not found", 404)


# ---- Data Simulator ----

# Per-environment simulation profiles so the three independent data
# sources visibly differ (production runs hottest, development coolest).
ENV_PROFILES = {
    "production":  {"base": (55, 75), "offset": 0},
    "staging":     {"base": (32, 48), "offset": 0},
    "development": {"base": (12, 25), "offset": 0},
}


class DataSimulator:
    """Generates realistic time-series data with anomalies.

    Passing ``seed`` gives each environment an independent RNG, so the
    three environments produce distinct but internally consistent series.
    """

    def __init__(self, seed: Optional[int] = None):
        self.rng = random.Random(seed)
        self.trends = {}
        self.seasonal = {}
        self.anomaly_injection = {}
        # Wall-clock origin; trend uses elapsed seconds so that the large
        # epoch timestamp never dominates the base value.
        self._t0 = time.time()

    def generate(self, metric: str, timestamp: float,
                 profile: Optional[Dict] = None) -> float:
        """Generate a data point for a metric."""
        profile = profile or {"base": (20, 80), "offset": 0}

        if metric not in self.trends:
            lo, hi = profile["base"]
            self.trends[metric] = {
                "base": self.rng.uniform(lo, hi) + profile.get("offset", 0),
                "trend": self.rng.uniform(-0.1, 0.1),
                "season_period": self.rng.choice([60, 300, 600, 1800]),
                "season_amplitude": self.rng.uniform(5, 20),
                "noise_std": self.rng.uniform(1, 5),
                "last_value": None
            }

        state = self.trends[metric]
        t = timestamp - self._t0

        # Base value with trend
        base = state["base"] + state["trend"] * t

        # Seasonal component
        seasonal = state["season_amplitude"] * math.sin(2 * math.pi * t / state["season_period"])

        # Random walk component
        if state["last_value"] is not None:
            walk = self.rng.gauss(0, state["noise_std"] * 0.3)
            value = state["last_value"] * 0.7 + (base + seasonal) * 0.3 + walk
        else:
            value = base + seasonal + self.rng.gauss(0, state["noise_std"])

        # Inject anomalies occasionally (2% chance)
        if self.rng.random() < 0.02:
            anomaly_type = self.rng.choice(["spike", "dip", "shift"])
            if anomaly_type == "spike":
                value += self.rng.uniform(20, 50)
            elif anomaly_type == "dip":
                value -= self.rng.uniform(20, 50)
            else:  # shift
                value += self.rng.uniform(-30, 30)

        # Clamp to reasonable range
        value = max(0, min(100, value))
        state["last_value"] = value

        return round(value, 4)


def _run_simulation(storage: TimeSeriesStorage, detector: AnomalyDetector,
                    metrics: list, duration: int, interval: float,
                    envs: Optional[List[str]] = None):
    """Run data simulation in background, one independent generator per env."""
    envs = envs or DEFAULT_ENVS
    # Independent RNG + trend state per environment
    sims = {env: DataSimulator(seed=abs(hash(("sim", env))) % (2**32))
            for env in envs}
    start = time.time()
    count = 0

    while time.time() - start < duration:
        ts = time.time()
        points = []

        for env in envs:
            sim = sims[env]
            profile = ENV_PROFILES.get(env, {"base": (20, 80), "offset": 0})
            for metric in metrics:
                value = sim.generate(metric, ts, profile)
                points.append({
                    "metric": metric,
                    "timestamp": ts,
                    "value": value,
                    "source": f"simulator-{env}",
                    "tags": {"env": env}
                })

        storage.write_batch(points)

        # Run anomaly detection (per-environment state)
        for p in points:
            _run_rules_on_point(
                storage, detector, p["metric"], p["value"], ts,
                p["source"], p["tags"], p["tags"]["env"]
            )

        count += len(points)
        time.sleep(interval)

    storage.force_flush()
    print(f"Simulation complete: {count} data points across {len(envs)} environments")


# ---- Server Startup ----

SERVER_START_TIME = time.time()


def create_default_rules(storage: TimeSeriesStorage):
    """Create default anomaly detection rules."""
    default_rules = [
        {
            "id": "rule_cpu_zscore",
            "name": "CPU Usage Z-Score",
            "metric": "cpu.usage",
            "algorithm": "zscore",
            "threshold": 3.0,
            "severity": "warning",
            "enabled": True,
            "dynamic_threshold": True,
            "params": {"window_size": 200},
            "description": "Detects CPU spikes using Z-score"
        },
        {
            "id": "rule_memory_ewma",
            "name": "Memory EWMA Alert",
            "metric": "memory.usage",
            "algorithm": "ewma",
            "threshold": 3.5,
            "severity": "warning",
            "enabled": True,
            "dynamic_threshold": False,
            "params": {"alpha": 0.3},
            "description": "Detects memory drift using EWMA"
        },
        {
            "id": "rule_disk_median",
            "name": "Disk I/O Moving Median",
            "metric": "disk.io",
            "algorithm": "moving_median",
            "threshold": 4.0,
            "severity": "critical",
            "enabled": True,
            "dynamic_threshold": True,
            "params": {"window_size": 100},
            "description": "Detects disk I/O anomalies using moving median"
        },
        {
            "id": "rule_network_zscore",
            "name": "Network Throughput Z-Score",
            "metric": "network.throughput",
            "algorithm": "zscore",
            "threshold": 2.5,
            "severity": "warning",
            "enabled": True,
            "dynamic_threshold": False,
            "params": {"window_size": 150},
            "description": "Detects network anomalies"
        }
    ]

    existing = {r["id"] for r in storage.get_rules()}
    for rule in default_rules:
        if rule["id"] not in existing:
            storage.add_rule(rule)


def create_default_sources(storage: TimeSeriesStorage):
    """Create default data sources: one independent source per environment."""
    default_sources = {
        "simulator-production": {
            "name": "生产环境数据源",
            "type": "simulator",
            "env": "production",
            "enabled": True,
            "metrics": ["cpu.usage", "memory.usage", "disk.io", "network.throughput"],
            "interval": 1,
            "description": "生产环境内置模拟器（独立数据源）"
        },
        "simulator-staging": {
            "name": "预发环境数据源",
            "type": "simulator",
            "env": "staging",
            "enabled": True,
            "metrics": ["cpu.usage", "memory.usage", "disk.io", "network.throughput"],
            "interval": 1,
            "description": "预发环境内置模拟器（独立数据源）"
        },
        "simulator-development": {
            "name": "开发环境数据源",
            "type": "simulator",
            "env": "development",
            "enabled": True,
            "metrics": ["cpu.usage", "memory.usage", "disk.io", "network.throughput"],
            "interval": 1,
            "description": "开发环境内置模拟器（独立数据源）"
        },
        "api": {
            "name": "API 数据摄入",
            "type": "api",
            "enabled": True,
            "endpoint": "/api/data/ingest",
            "description": "HTTP API 数据摄入端点（通过 tags.env 指定环境）"
        }
    }

    for sid, config in default_sources.items():
        if sid not in storage.get_sources():
            storage.add_source(sid, config)


def create_default_environments(storage: TimeSeriesStorage):
    """Register the three monitored environments."""
    for env_id in DEFAULT_ENVS:
        storage.add_environment(env_id, {
            "name": {"production": "生产环境", "staging": "预发环境",
                     "development": "开发环境"}[env_id],
            "label": ENV_LABELS[env_id],
            "color": ENV_COLORS[env_id],
            "source": f"simulator-{env_id}",
        })


def run_server(host: str = "0.0.0.0", port: int = 8080, data_dir: str = "./data"):
    """Start the time-series monitoring server."""
    import os

    # Initialize components
    storage = TimeSeriesStorage(data_dir)
    detector = AnomalyDetector()

    # Set class-level attributes
    TimeSeriesHandler.storage = storage
    TimeSeriesHandler.detector = detector

    # Create defaults
    create_default_rules(storage)
    create_default_environments(storage)
    create_default_sources(storage)

    # Create server
    server = ThreadedHTTPServer((host, port), TimeSeriesHandler)

    print(f"╔══════════════════════════════════════════════════════╗")
    print(f"║   Time-Series Monitoring Server v1.0.0              ║")
    print(f"╠══════════════════════════════════════════════════════╣")
    print(f"║   API:     http://{host}:{port}/api                ║")
    print(f"║   Status:  http://{host}:{port}/api/status         ║")
    print(f"║   Data:    {data_dir:<40s} ║")
    print(f"╚══════════════════════════════════════════════════════╝")

    # Start auto-simulation: one independent generator per environment
    def auto_simulate():
        time.sleep(2)  # Wait for server to start
        metrics = ["cpu.usage", "memory.usage", "disk.io", "network.throughput"]
        sims = {env: DataSimulator(seed=abs(hash(("auto", env))) % (2**32))
                for env in DEFAULT_ENVS}

        while True:
            ts = time.time()
            points = []
            for env in DEFAULT_ENVS:
                sim = sims[env]
                profile = ENV_PROFILES.get(env, {"base": (20, 80), "offset": 0})
                for metric in metrics:
                    value = sim.generate(metric, ts, profile)
                    points.append({
                        "metric": metric,
                        "timestamp": ts,
                        "value": value,
                        "source": f"simulator-{env}",
                        "tags": {"env": env}
                    })

            storage.write_batch(points)

            # Anomaly detection with per-environment state
            for p in points:
                _run_rules_on_point(
                    storage, detector, p["metric"], p["value"], ts,
                    p["source"], p["tags"], p["tags"]["env"]
                )

            time.sleep(1)

    sim_thread = threading.Thread(target=auto_simulate, daemon=True)
    sim_thread.start()

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nShutting down...")
        storage.force_flush()
        server.shutdown()


if __name__ == "__main__":
    import sys
    import os

    port = int(sys.argv[1]) if len(sys.argv) > 1 else 8080
    data_dir = sys.argv[2] if len(sys.argv) > 2 else "./data"
    run_server(port=port, data_dir=data_dir)