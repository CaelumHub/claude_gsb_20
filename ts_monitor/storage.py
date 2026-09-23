"""
Time-Series Storage Engine
- Hourly JSON shard files for time-series data
- Separate metadata and rules storage
- Cross-shard query with efficient merging
- Write-ahead buffer for high-throughput ingestion
"""

import json
import os
import time
import threading
from datetime import datetime, timezone, timedelta
from collections import defaultdict
from typing import List, Dict, Any, Optional, Tuple

class TimeSeriesStorage:
    """Manages time-series data with hourly JSON shard files."""

    def __init__(self, data_dir: str = "./data"):
        self.data_dir = data_dir
        self.ts_dir = os.path.join(data_dir, "timeseries")
        self.meta_file = os.path.join(data_dir, "metadata.json")
        self.rules_file = os.path.join(data_dir, "rules.json")
        self.alerts_file = os.path.join(data_dir, "alerts.json")

        os.makedirs(self.ts_dir, exist_ok=True)

        # Write buffer for high-throughput ingestion
        self._write_buffer: Dict[str, List[Dict]] = defaultdict(list)
        self._buffer_lock = threading.Lock()
        self._buffer_flush_interval = 2.0  # seconds
        self._last_flush = time.time()

        # In-memory cache for recent data (last 2 hours)
        self._cache: Dict[str, List[Dict]] = defaultdict(list)
        self._cache_lock = threading.Lock()
        self._max_cache_points = 50000

        # Environment index: env -> set(metrics). An environment is the
        # top-level grouping dimension (e.g. production/staging/development),
        # carried on every data point as tags.env.
        self._env_index: Dict[str, set] = defaultdict(set)
        self._env_index_dirty = False
        self._last_env_persist = 0.0

        # Load metadata and rules
        self.metadata = self._load_json(self.meta_file, {"sources": {}, "stats": {}})
        self.rules = self._load_json(self.rules_file, {"rules": []})
        self.alerts = self._load_json(self.alerts_file, {"alerts": [], "suppressed": {}})

        # Restore env index from metadata; lazily rebuilt from shards if missing
        for env, metrics in self.metadata.get("env_index", {}).items():
            self._env_index[env] = set(metrics)
        self._env_index_built = bool(self._env_index)

    def _load_json(self, path: str, default: Any) -> Any:
        """Load JSON file with fallback to default."""
        try:
            if os.path.exists(path):
                with open(path, 'r') as f:
                    return json.load(f)
        except (json.JSONDecodeError, IOError):
            pass
        return default

    def _save_json(self, path: str, data: Any):
        """Atomically save JSON file."""
        tmp_path = path + ".tmp"
        try:
            with open(tmp_path, 'w') as f:
                json.dump(data, f, indent=2, default=str)
            os.replace(tmp_path, path)
        except IOError as e:
            print(f"Error saving {path}: {e}")
            if os.path.exists(tmp_path):
                os.remove(tmp_path)

    def _get_shard_path(self, metric: str, timestamp: float) -> str:
        """Get the hourly shard file path for a metric and timestamp."""
        dt = datetime.fromtimestamp(timestamp, tz=timezone.utc)
        shard_key = dt.strftime("%Y%m%d_%H")
        # Keep dots in metric names (cpu.usage); only escape path separators
        safe_metric = metric.replace("/", "_").replace("\\", "_").replace(" ", "_")
        return os.path.join(self.ts_dir, f"{safe_metric}_{shard_key}.json")

    def _get_shard_key(self, metric: str, timestamp: float) -> str:
        """Get the shard key for caching."""
        dt = datetime.fromtimestamp(timestamp, tz=timezone.utc)
        return f"{metric}_{dt.strftime('%Y%m%d_%H')}"

    @staticmethod
    def _point_env(point: Dict[str, Any]) -> str:
        """Resolve the environment of a data point (defaults to 'default')."""
        return (point.get("tags") or {}).get("env") or point.get("env") or "default"

    def _index_point(self, metric: str, point: Dict[str, Any]):
        """Track a point's (env, metric) in the environment index."""
        env = self._point_env(point)
        if metric not in self._env_index[env]:
            self._env_index[env].add(metric)
            self._env_index_dirty = True

    def _maybe_persist_env_index(self, force: bool = False):
        """Persist env index to metadata, throttled to once per 5 seconds."""
        if not self._env_index_dirty:
            return
        now = time.time()
        if not force and now - self._last_env_persist < 5.0:
            return
        self.metadata["env_index"] = {
            env: sorted(metrics) for env, metrics in self._env_index.items()
        }
        self._save_json(self.meta_file, self.metadata)
        self._env_index_dirty = False
        self._last_env_persist = now

    def _rebuild_env_index(self):
        """Rebuild env -> metrics index by scanning shard files once."""
        import re
        shard_re = re.compile(r"^(?P<metric>.+)_(\d{8}_\d{2})\.json$")
        if os.path.exists(self.ts_dir):
            for fname in os.listdir(self.ts_dir):
                m = shard_re.match(fname)
                if not m:
                    continue
                metric = m.group("metric")
                fpath = os.path.join(self.ts_dir, fname)
                # Only open the shard's head to discover which envs it holds;
                # envs are few, and a single shard typically covers all of them.
                try:
                    with open(fpath, 'r') as f:
                        points = json.load(f)
                    envs = {self._point_env(p) for p in points}
                    for env in envs:
                        self._env_index[env].add(metric)
                except (json.JSONDecodeError, IOError):
                    pass
        with self._cache_lock:
            for metric, points in self._cache.items():
                for p in points:
                    self._env_index[self._point_env(p)].add(metric)
        self._env_index_built = True
        self._env_index_dirty = True
        self._maybe_persist_env_index(force=True)

    def write(self, metric: str, timestamp: float, value: float,
              tags: Optional[Dict[str, str]] = None, source: str = "default"):
        """Write a single data point to the write buffer."""
        point = {
            "t": round(timestamp, 3),
            "v": value,
            "tags": tags or {},
            "src": source
        }

        with self._buffer_lock:
            self._write_buffer[metric].append(point)
            self._index_point(metric, point)
            # Auto-flush if buffer is large enough
            if len(self._write_buffer[metric]) >= 1000 or \
               (time.time() - self._last_flush) > self._buffer_flush_interval:
                self._flush_buffer()

        # Update cache
        with self._cache_lock:
            self._cache[metric].append(point)
            # Trim cache if too large
            if len(self._cache[metric]) > self._max_cache_points:
                self._cache[metric] = self._cache[metric][-self._max_cache_points:]

    def write_batch(self, points: List[Dict[str, Any]]):
        """Write multiple data points efficiently."""
        with self._buffer_lock:
            for p in points:
                metric = p.get("metric", "unknown")
                point = {
                    "t": round(p.get("timestamp", time.time()), 3),
                    "v": p.get("value", 0),
                    "tags": p.get("tags", {}),
                    "src": p.get("source", "default")
                }
                self._write_buffer[metric].append(point)
                self._index_point(metric, point)

                with self._cache_lock:
                    self._cache[metric].append(point)

            if any(len(v) >= 500 for v in self._write_buffer.values()):
                self._flush_buffer()

        with self._cache_lock:
            for metric in list(self._cache.keys()):
                if len(self._cache[metric]) > self._max_cache_points:
                    self._cache[metric] = self._cache[metric][-self._max_cache_points:]

    def _flush_buffer(self):
        """Flush write buffer to shard files."""
        if not self._write_buffer:
            return

        shards_to_write: Dict[str, List[Dict]] = defaultdict(list)

        for metric, points in self._write_buffer.items():
            for point in points:
                shard_path = self._get_shard_path(metric, point["t"])
                shards_to_write[shard_path].append(point)

        for shard_path, points in shards_to_write.items():
            existing = []
            if os.path.exists(shard_path):
                try:
                    with open(shard_path, 'r') as f:
                        existing = json.load(f)
                except (json.JSONDecodeError, IOError):
                    existing = []

            existing.extend(points)
            # Sort by timestamp and deduplicate
            existing.sort(key=lambda x: x["t"])
            # Remove exact duplicates. The identity of a point includes its
            # environment and source, otherwise identical readings from
            # different environments would collapse into one.
            seen = set()
            unique = []
            for p in existing:
                key = (p["t"], p["v"], self._point_env(p), p.get("src"))
                if key not in seen:
                    seen.add(key)
                    unique.append(p)
            existing = unique

            # Keep only last 10000 points per shard to prevent unbounded growth
            if len(existing) > 10000:
                existing = existing[-10000:]

            try:
                tmp_path = shard_path + ".tmp"
                with open(tmp_path, 'w') as f:
                    json.dump(existing, f)
                os.replace(tmp_path, shard_path)
            except IOError as e:
                print(f"Error writing shard {shard_path}: {e}")

        self._write_buffer.clear()
        self._last_flush = time.time()

    def force_flush(self):
        """Force flush all buffered data."""
        with self._buffer_lock:
            self._flush_buffer()
        self._maybe_persist_env_index(force=True)

    def query(self, metric: str, start: float, end: float,
              tags: Optional[Dict[str, str]] = None,
              envs: Optional[List[str]] = None,
              max_points: int = 10000) -> List[Dict]:
        """Query time-series data across shards.

        ``envs`` restricts results to the given environments (tags.env).
        An empty/None list returns data from all environments.
        """
        self.force_flush()

        env_set = set(envs) if envs else None
        results = []

        def _match(p: Dict) -> bool:
            if env_set is not None and self._point_env(p) not in env_set:
                return False
            if tags and not all(p.get("tags", {}).get(k) == v for k, v in tags.items()):
                return False
            return True

        # Determine which hourly shards to read
        start_dt = datetime.fromtimestamp(start, tz=timezone.utc)
        end_dt = datetime.fromtimestamp(end, tz=timezone.utc)

        current = start_dt.replace(minute=0, second=0, microsecond=0)
        while current <= end_dt + timedelta(hours=1):
            shard_path = self._get_shard_path(metric, current.timestamp())
            if os.path.exists(shard_path):
                try:
                    with open(shard_path, 'r') as f:
                        points = json.load(f)
                    # Filter by time range, environment and tags
                    results.extend(p for p in points
                                   if start <= p["t"] <= end and _match(p))
                except (json.JSONDecodeError, IOError):
                    pass
            current += timedelta(hours=1)

        # Also check cache for very recent data
        with self._cache_lock:
            cache_points = list(self._cache.get(metric, []))
        results.extend(p for p in cache_points
                       if start <= p["t"] <= end and _match(p))

        # Deduplicate and sort
        seen = set()
        unique = []
        for p in sorted(results, key=lambda x: x["t"]):
            key = (p["t"], p["v"], self._point_env(p), p.get("src"))
            if key not in seen:
                seen.add(key)
                unique.append(p)

        # Downsample if too many points
        if len(unique) > max_points:
            step = len(unique) / max_points
            unique = [unique[int(i * step)] for i in range(max_points)]

        return unique

    def get_environment_metrics(self) -> Dict[str, List[str]]:
        """Return {env: [metrics]} discovered from ingested data."""
        if not self._env_index_built:
            self._rebuild_env_index()
        self._maybe_persist_env_index()
        return {env: sorted(metrics) for env, metrics in self._env_index.items()}

    def get_metrics(self, env: Optional[str] = None) -> List[str]:
        """Get list of all available metrics, optionally restricted to an env."""
        if env is not None:
            return self.get_environment_metrics().get(env, [])

        metrics = set()
        # Scan shard files
        if os.path.exists(self.ts_dir):
            for fname in os.listdir(self.ts_dir):
                if fname.endswith('.json'):
                    # Strip ".json", then split off the trailing "_YYYYMMDD_HH"
                    base = fname[:-5]
                    parts = base.rsplit('_', 2)
                    if len(parts) >= 3:
                        metrics.add(parts[0])
        # Also include cached metrics
        with self._cache_lock:
            metrics.update(self._cache.keys())
        return sorted(metrics)

    def get_shard_info(self) -> List[Dict]:
        """Get information about shard files."""
        info = []
        if os.path.exists(self.ts_dir):
            for fname in os.listdir(self.ts_dir):
                if fname.endswith('.json'):
                    fpath = os.path.join(self.ts_dir, fname)
                    stat = os.stat(fpath)
                    info.append({
                        "file": fname,
                        "size": stat.st_size,
                        "modified": datetime.fromtimestamp(stat.st_mtime).isoformat()
                    })
        return sorted(info, key=lambda x: x["file"])

    # ---- Metadata (Sources) ----

    def get_sources(self) -> Dict:
        """Get all configured data sources."""
        return self.metadata.get("sources", {})

    def add_source(self, source_id: str, config: Dict) -> Dict:
        """Add or update a data source."""
        self.metadata["sources"][source_id] = {
            **config,
            "id": source_id,
            "updated_at": datetime.now(timezone.utc).isoformat()
        }
        self._save_json(self.meta_file, self.metadata)
        return self.metadata["sources"][source_id]

    def delete_source(self, source_id: str) -> bool:
        """Delete a data source."""
        if source_id in self.metadata.get("sources", {}):
            del self.metadata["sources"][source_id]
            self._save_json(self.meta_file, self.metadata)
            return True
        return False

    # ---- Environments ----

    def get_environments(self) -> Dict[str, Dict]:
        """Get configured environments (production/staging/development/...)."""
        return self.metadata.get("environments", {})

    def add_environment(self, env_id: str, config: Dict) -> Dict:
        """Add or update an environment definition."""
        envs = self.metadata.setdefault("environments", {})
        envs[env_id] = {
            **config,
            "id": env_id,
            "updated_at": datetime.now(timezone.utc).isoformat()
        }
        self._save_json(self.meta_file, self.metadata)
        return envs[env_id]

    # ---- Rules ----

    def get_rules(self) -> List[Dict]:
        """Get all anomaly detection rules."""
        return self.rules.get("rules", [])

    def add_rule(self, rule: Dict) -> Dict:
        """Add or update an anomaly detection rule."""
        rule_id = rule.get("id", f"rule_{int(time.time()*1000)}")
        rule["id"] = rule_id
        rule["updated_at"] = datetime.now(timezone.utc).isoformat()

        # Update existing or add new
        existing = [r for r in self.rules["rules"] if r["id"] != rule_id]
        existing.append(rule)
        self.rules["rules"] = existing

        self._save_json(self.rules_file, self.rules)
        return rule

    def delete_rule(self, rule_id: str) -> bool:
        """Delete an anomaly detection rule."""
        before = len(self.rules["rules"])
        self.rules["rules"] = [r for r in self.rules["rules"] if r["id"] != rule_id]
        if len(self.rules["rules"]) < before:
            self._save_json(self.rules_file, self.rules)
            return True
        return False

    # ---- Alerts ----

    def get_alerts(self, status: Optional[str] = None,
                   severity: Optional[str] = None,
                   limit: int = 200) -> List[Dict]:
        """Get alerts with optional filtering."""
        alerts = self.alerts.get("alerts", [])
        if status:
            alerts = [a for a in alerts if a.get("status") == status]
        if severity:
            alerts = [a for a in alerts if a.get("severity") == severity]
        return sorted(alerts, key=lambda x: x.get("timestamp", 0), reverse=True)[:limit]

    def add_alert(self, alert: Dict) -> Dict:
        """Add a new alert with deduplication."""
        alert_id = alert.get("id", f"alert_{int(time.time()*1000)}")
        alert["id"] = alert_id
        alert["timestamp"] = alert.get("timestamp", time.time())
        alert["status"] = alert.get("status", "active")

        # Check for duplicate/suppressed alerts
        suppressed = self.alerts.get("suppressed", {})
        metric = alert.get("metric", "")
        rule_id = alert.get("rule_id", "")
        env = alert.get("env") or (alert.get("tags") or {}).get("env") or "default"
        alert["env"] = env
        # Same metric + rule in the SAME environment shares a suppression window
        suppress_key = f"{env}:{metric}:{rule_id}"

        # Suppress if same metric+rule had an alert in the last 5 minutes
        if suppress_key in suppressed:
            last_alert_time = suppressed[suppress_key]
            if time.time() - last_alert_time < 300:  # 5 min suppression
                alert["status"] = "suppressed"
                return alert

        suppressed[suppress_key] = time.time()
        self.alerts["suppressed"] = suppressed

        self.alerts["alerts"].append(alert)
        # Keep only last 1000 alerts
        if len(self.alerts["alerts"]) > 1000:
            self.alerts["alerts"] = self.alerts["alerts"][-1000:]

        self._save_json(self.alerts_file, self.alerts)
        return alert

    def acknowledge_alert(self, alert_id: str) -> bool:
        """Acknowledge an alert."""
        for alert in self.alerts.get("alerts", []):
            if alert.get("id") == alert_id:
                alert["status"] = "acknowledged"
                alert["acknowledged_at"] = time.time()
                self._save_json(self.alerts_file, self.alerts)
                return True
        return False

    def resolve_alert(self, alert_id: str) -> bool:
        """Resolve an alert."""
        for alert in self.alerts.get("alerts", []):
            if alert.get("id") == alert_id:
                alert["status"] = "resolved"
                alert["resolved_at"] = time.time()
                self._save_json(self.alerts_file, self.alerts)
                return True
        return False

    def cleanup_suppressed(self):
        """Clean up old suppression entries."""
        suppressed = self.alerts.get("suppressed", {})
        now = time.time()
        self.alerts["suppressed"] = {
            k: v for k, v in suppressed.items()
            if now - v < 600  # Keep 10 minutes of suppression history
        }
        self._save_json(self.alerts_file, self.alerts)

    def get_stats(self) -> Dict:
        """Get storage statistics."""
        total_size = 0
        shard_count = 0
        if os.path.exists(self.ts_dir):
            for fname in os.listdir(self.ts_dir):
                if fname.endswith('.json'):
                    fpath = os.path.join(self.ts_dir, fname)
                    total_size += os.path.getsize(fpath)
                    shard_count += 1

        return {
            "shard_count": shard_count,
            "total_size_bytes": total_size,
            "total_size_mb": round(total_size / (1024 * 1024), 2),
            "metric_count": len(self.get_metrics()),
            "source_count": len(self.get_sources()),
            "env_count": len(self.get_environment_metrics()),
            "rule_count": len(self.get_rules()),
            "alert_count": len(self.alerts.get("alerts", [])),
            "cache_size": sum(len(v) for v in self._cache.values())
        }