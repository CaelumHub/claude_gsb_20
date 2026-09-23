"""
Time-Series Storage Engine
- Hourly JSON shard files for time-series data
- Environment-aware series and metadata
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

        # Write buffer for high-throughput ingestion, keyed by (metric, environment)
        self._write_buffer: Dict[Tuple[str, str], List[Dict]] = defaultdict(list)
        self._buffer_lock = threading.Lock()
        self._buffer_flush_interval = 2.0  # seconds
        self._last_flush = time.time()

        # In-memory cache for recent data (last 2 hours)
        self._cache: Dict[Tuple[str, str], List[Dict]] = defaultdict(list)
        self._cache_lock = threading.Lock()
        self._max_cache_points = 50000

        # Load metadata and rules
        self.metadata = self._load_json(self.meta_file, {
            "sources": {},
            "stats": {},
            "series": {}
        })
        self.metadata.setdefault("sources", {})
        self.metadata.setdefault("stats", {})
        self.metadata.setdefault("series", {})
        self.rules = self._load_json(self.rules_file, {"rules": []})
        self.alerts = self._load_json(self.alerts_file, {"alerts": [], "suppressed": {}})

    @staticmethod
    def normalize_environment(environment: Optional[str]) -> str:
        """Normalize an environment identifier."""
        env = str(environment or "default").strip()
        return env or "default"

    @staticmethod
    def _safe_id(value: str) -> str:
        """Convert an identifier to a file-safe name."""
        safe = []
        for ch in str(value):
            if ch.isalnum() or ch in ("-", "_"):
                safe.append(ch)
            else:
                safe.append("_")
        return "".join(safe) or "default"

    def _series_key(self, metric: str, environment: Optional[str] = None) -> Tuple[str, str]:
        """Return the canonical in-memory series key."""
        return metric, self.normalize_environment(environment)

    def _register_series(self, metric: str, environment: str):
        """Record the original names represented by a file-safe shard key."""
        safe_metric = self._safe_id(metric.replace(".", "_").replace("/", "_").replace(" ", "_"))
        safe_env = self._safe_id(environment)
        key = f"{safe_metric}__{safe_env}"
        if key not in self.metadata["series"]:
            self.metadata["series"][key] = {
                "metric": metric,
                "environment": environment
            }
            self._save_json(self.meta_file, self.metadata)

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

    def _get_shard_path(self, metric: str, timestamp: float,
                        environment: Optional[str] = None) -> str:
        """Get the hourly shard file path for a metric, environment, and timestamp."""
        env = self.normalize_environment(environment)
        dt = datetime.fromtimestamp(timestamp, tz=timezone.utc)
        shard_key = dt.strftime("%Y%m%d_%H")
        safe_metric = self._safe_id(metric.replace("/", "_").replace(".", "_").replace(" ", "_"))
        safe_env = self._safe_id(env)
        return os.path.join(self.ts_dir, f"{safe_metric}__{safe_env}_{shard_key}.json")

    def write(self, metric: str, timestamp: float, value: float,
              tags: Optional[Dict[str, Any]] = None, source: str = "default",
              environment: Optional[str] = None):
        """Write a single data point to the write buffer."""
        env = self.normalize_environment(
            environment or (tags or {}).get("env") or (tags or {}).get("environment")
        )
        point_tags = dict(tags or {})
        point_tags["env"] = env

        point = {
            "t": round(float(timestamp), 3),
            "v": float(value),
            "tags": point_tags,
            "src": source
        }
        cache_key = self._series_key(metric, env)

        with self._buffer_lock:
            self._register_series(metric, env)
            self._write_buffer[cache_key].append(point)
            # Auto-flush if buffer is large enough
            if len(self._write_buffer[cache_key]) >= 1000 or \
               (time.time() - self._last_flush) > self._buffer_flush_interval:
                self._flush_buffer_locked()

        # Update cache
        with self._cache_lock:
            self._cache[cache_key].append(point)
            # Trim cache if too large
            if len(self._cache[cache_key]) > self._max_cache_points:
                self._cache[cache_key] = self._cache[cache_key][-self._max_cache_points:]

    def write_batch(self, points: List[Dict[str, Any]]):
        """Write multiple data points efficiently."""
        with self._buffer_lock:
            for p in points:
                metric = p.get("metric", "unknown")
                env = self.normalize_environment(
                    p.get("environment") or p.get("env") or
                    (p.get("tags") or {}).get("env") or
                    (p.get("tags") or {}).get("environment")
                )
                tags = dict(p.get("tags") or {})
                tags["env"] = env
                point = {
                    "t": round(float(p.get("timestamp", time.time())), 3),
                    "v": float(p.get("value", 0)),
                    "tags": tags,
                    "src": p.get("source", "default")
                }
                cache_key = self._series_key(metric, env)
                self._register_series(metric, env)
                self._write_buffer[cache_key].append(point)

                with self._cache_lock:
                    self._cache[cache_key].append(point)

            if any(len(v) >= 500 for v in self._write_buffer.values()):
                self._flush_buffer_locked()

    def _flush_buffer_locked(self):
        """Flush write buffer to shard files. Caller must hold the buffer lock."""
        if not self._write_buffer:
            return

        shards_to_write: Dict[str, List[Dict]] = defaultdict(list)

        for (metric, env), points in self._write_buffer.items():
            for point in points:
                shard_path = self._get_shard_path(metric, point["t"], env)
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
            # Sort by timestamp and deduplicate points from the same series/source.
            existing.sort(key=lambda x: x["t"])
            seen = set()
            unique = []
            for p in existing:
                tag_key = tuple(sorted((p.get("tags") or {}).items()))
                key = (p["t"], p["v"], p.get("src", ""), tag_key)
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

    def _flush_buffer(self):
        """Flush the buffer while acquiring the buffer lock."""
        with self._buffer_lock:
            self._flush_buffer_locked()

    def force_flush(self):
        """Force flush all buffered data."""
        self._flush_buffer()

    def query(self, metric: str, start: float, end: float,
              tags: Optional[Dict[str, str]] = None,
              max_points: int = 10000,
              environment: Optional[str] = None) -> List[Dict]:
        """Query one time-series across hourly shards."""
        self.force_flush()

        env = self.normalize_environment(environment)
        required_tags = dict(tags or {})
        required_tags["env"] = env
        results = []

        # Determine which hourly shards to read
        start_dt = datetime.fromtimestamp(start, tz=timezone.utc)
        end_dt = datetime.fromtimestamp(end, tz=timezone.utc)

        current = start_dt.replace(minute=0, second=0, microsecond=0)
        while current <= end_dt + timedelta(hours=1):
            shard_path = self._get_shard_path(metric, current.timestamp(), env)
            if os.path.exists(shard_path):
                try:
                    with open(shard_path, 'r') as f:
                        points = json.load(f)
                    filtered = [p for p in points if start <= p["t"] <= end]
                    filtered = [
                        p for p in filtered
                        if all(p.get("tags", {}).get(k) == v for k, v in required_tags.items())
                    ]
                    results.extend(filtered)
                except (json.JSONDecodeError, IOError):
                    pass
            current += timedelta(hours=1)

        # Also check cache for very recent data
        with self._cache_lock:
            cache_points = list(self._cache.get(self._series_key(metric, env), []))
        cache_filtered = [p for p in cache_points if start <= p["t"] <= end]
        cache_filtered = [
            p for p in cache_filtered
            if all(p.get("tags", {}).get(k) == v for k, v in required_tags.items())
        ]
        results.extend(cache_filtered)

        # Deduplicate and sort
        seen = set()
        unique = []
        for p in sorted(results, key=lambda x: x["t"]):
            tag_key = tuple(sorted((p.get("tags") or {}).items()))
            key = (p["t"], p["v"], p.get("src", ""), tag_key)
            if key not in seen:
                seen.add(key)
                unique.append(p)

        # Downsample if too many points
        if len(unique) > max_points:
            step = len(unique) / max_points
            unique = [unique[int(i * step)] for i in range(max_points)]

        return unique

    def get_metrics(self) -> List[str]:
        """Get list of all available metric names."""
        metrics = {item["metric"] for item in self.metadata.get("series", {}).values()}
        with self._cache_lock:
            metrics.update(metric for metric, _env in self._cache.keys())
        return sorted(metrics)

    def get_environments(self) -> List[str]:
        """Get all environments represented by configured sources or stored series."""
        environments = {
            item.get("environment")
            for item in self.metadata.get("series", {}).values()
            if item.get("environment")
        }
        environments.update(
            source.get("environment") or source.get("env")
            for source in self.get_sources().values()
            if source.get("environment") or source.get("env")
        )
        with self._cache_lock:
            environments.update(env for _metric, env in self._cache.keys())
        environments.discard(None)
        preferred = ["production", "staging", "development"]
        ordered = [env for env in preferred if env in environments]
        ordered.extend(sorted(environments - set(preferred)))
        return ordered

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
        env = self.normalize_environment(config.get("environment") or config.get("env"))
        self.metadata["sources"][source_id] = {
            **config,
            "id": source_id,
            "environment": env,
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
                   environment: Optional[str] = None,
                   limit: int = 200) -> List[Dict]:
        """Get alerts with optional filtering."""
        alerts = self.alerts.get("alerts", [])
        if status:
            alerts = [a for a in alerts if a.get("status") == status]
        if severity:
            alerts = [a for a in alerts if a.get("severity") == severity]
        if environment:
            alerts = [
                a for a in alerts
                if (a.get("environment") or a.get("env") or
                    (a.get("tags") or {}).get("env")) == environment
            ]
        return sorted(alerts, key=lambda x: x.get("timestamp", 0), reverse=True)[:limit]

    def add_alert(self, alert: Dict) -> Dict:
        """Add a new alert with deduplication."""
        alert_id = alert.get("id", f"alert_{int(time.time()*1000)}")
        env = self.normalize_environment(
            alert.get("environment") or alert.get("env") or
            (alert.get("tags") or {}).get("env")
        )
        alert["id"] = alert_id
        alert["environment"] = env
        alert.setdefault("tags", {})["env"] = env
        alert["timestamp"] = alert.get("timestamp", time.time())
        alert["status"] = alert.get("status", "active")

        # Check for duplicate/suppressed alerts
        suppressed = self.alerts.get("suppressed", {})
        metric = alert.get("metric", "")
        rule_id = alert.get("rule_id", "")
        suppress_key = f"{env}:{metric}:{rule_id}"

        # Suppress if same environment+metric+rule alerted in the last 5 minutes
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
            "environment_count": len(self.get_environments()),
            "source_count": len(self.get_sources()),
            "rule_count": len(self.get_rules()),
            "alert_count": len(self.alerts.get("alerts", [])),
            "cache_size": sum(len(v) for v in self._cache.values())
        }
