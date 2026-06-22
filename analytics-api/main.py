import logging
import math
import os
import threading
import time
from dataclasses import dataclass, field
from typing import Literal

from fastapi import FastAPI, HTTPException, Query, Request, Response
from pydantic import BaseModel, Field, ValidationError, field_validator

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("analytics-api")

app = FastAPI(title="PulseBoard Analytics API", version="1.0.0")

MAX_RECORDS = int(os.getenv("MAX_RECORDS", "10000"))
MAX_SERVICE_LENGTH = 100
MAX_RESPONSE_TIME_MS = 60_000.0
METRICS_DEFAULT_LIMIT = max(1, int(os.getenv("METRICS_DEFAULT_LIMIT", "100")))
METRICS_MAX_LIMIT = max(METRICS_DEFAULT_LIMIT, int(os.getenv("METRICS_MAX_LIMIT", "1000")))
BATCH_MAX_SIZE = max(1, int(os.getenv("METRICS_BATCH_MAX_SIZE", "500")))
ALLOWED_STATUSES = ("healthy", "unhealthy", "degraded", "unknown")
StatusLiteral = Literal["healthy", "unhealthy", "degraded", "unknown"]
SortFieldLiteral = Literal["timestamp", "service", "response_time_ms", "status"]
SortOrderLiteral = Literal["asc", "desc"]
ServiceSortFieldLiteral = Literal[
    "service",
    "total_checks",
    "healthy_checks",
    "uptime_pct",
    "last_seen",
    "first_seen",
    "latest_status",
    "latest_response_ms",
]


def _percentile(sorted_values: list[float], pct: float) -> float:
    if not sorted_values:
        return 0.0
    if len(sorted_values) == 1:
        return sorted_values[0]
    rank = (pct / 100.0) * (len(sorted_values) - 1)
    lower = int(math.floor(rank))
    upper = int(math.ceil(rank))
    if lower == upper:
        return sorted_values[lower]
    weight = rank - lower
    return sorted_values[lower] * (1 - weight) + sorted_values[upper] * weight


class MetricPayload(BaseModel):
    service: str = Field(..., min_length=1, max_length=MAX_SERVICE_LENGTH)
    status: StatusLiteral
    response_time_ms: float = Field(..., ge=0, le=MAX_RESPONSE_TIME_MS)
    timestamp: float | None = Field(default=None, gt=0)

    @field_validator("service")
    @classmethod
    def validate_service(cls, v: str) -> str:
        stripped = v.strip()
        if not stripped:
            raise ValueError("service must not be blank")
        if len(stripped) > MAX_SERVICE_LENGTH:
            raise ValueError(f"service must be at most {MAX_SERVICE_LENGTH} characters")
        return stripped

    @field_validator("response_time_ms")
    @classmethod
    def validate_response_time(cls, v: float) -> float:
        if not math.isfinite(v):
            raise ValueError("response_time_ms must be a finite number")
        return v

    @field_validator("timestamp")
    @classmethod
    def validate_timestamp(cls, v: float | None) -> float | None:
        if v is None:
            return None
        if not math.isfinite(v):
            raise ValueError("timestamp must be a finite number")
        return v


@dataclass
class MetricRecord:
    service: str
    status: str
    response_time_ms: float
    timestamp: float


@dataclass
class MetricsStore:
    records: list[MetricRecord] = field(default_factory=list)
    max_records: int = MAX_RECORDS
    _lock: threading.Lock = field(default_factory=threading.Lock, repr=False)

    def add(self, record: MetricRecord) -> MetricRecord:
        with self._lock:
            self.records.append(record)
            if len(self.records) > self.max_records:
                removed = len(self.records) - self.max_records
                del self.records[:removed]
                logger.info("Evicted %d old records (store capped at %d)", removed, self.max_records)
        logger.info("Recorded metric for service=%s status=%s", record.service, record.status)
        return record

    def get_all(self) -> list[MetricRecord]:
        with self._lock:
            return list(self.records)

    def get_by_service(self, service: str) -> list[MetricRecord]:
        with self._lock:
            return [r for r in self.records if r.service == service]

    def filter(
        self,
        service: str | None = None,
        status: str | None = None,
        since: float | None = None,
        until: float | None = None,
        q: str | None = None,
    ) -> list[MetricRecord]:
        with self._lock:
            results = list(self.records)
        if service is not None:
            results = [r for r in results if r.service == service]
        if status is not None:
            results = [r for r in results if r.status == status]
        if since is not None:
            results = [r for r in results if r.timestamp >= since]
        if until is not None:
            results = [r for r in results if r.timestamp <= until]
        if q is not None:
            # `q` は呼び出し側で trim 済みの想定。
            # 大文字小文字を無視するため lower() 比較。
            needle = q.lower()
            results = [r for r in results if needle in r.service.lower()]
        return results

    def delete_by_service(self, service: str) -> int:
        with self._lock:
            before = len(self.records)
            self.records = [r for r in self.records if r.service != service]
            deleted = before - len(self.records)
        if deleted > 0:
            logger.info("Deleted %d records for service=%s", deleted, service)
        return deleted

    def distinct_services(
        self,
        since: float | None = None,
        until: float | None = None,
        q: str | None = None,
    ) -> list[str]:
        """フィルタ後のレコードから重複排除した service 名一覧を返す（順不同）。

        `/metrics/services` のような per-service 集計（uptime / first_seen /
        last_seen / percentile 等）は一切行わないため、フィルタドロップダウンの
        populate のような「名前だけ欲しい」場面で /metrics/services より小さな
        ペイロード・低コストで応答できる。順序付けは呼び出し側で行う。
        """
        records_snapshot = self.filter(since=since, until=until, q=q)
        return list({r.service for r in records_snapshot})

    def delete(
        self,
        service: str | None = None,
        before: float | None = None,
        status: str | None = None,
    ) -> int:
        """Delete records matching the given filters.

        Records are removed only when they match every provided filter:
        - service: only records whose service equals this value
        - before:  only records whose timestamp is strictly less than this value
        - status:  only records whose status equals this value
        """
        if service is None and before is None and status is None:
            return 0
        with self._lock:
            initial = len(self.records)
            kept: list[MetricRecord] = []
            for r in self.records:
                if service is not None and r.service != service:
                    kept.append(r)
                    continue
                if before is not None and r.timestamp >= before:
                    kept.append(r)
                    continue
                if status is not None and r.status != status:
                    kept.append(r)
                    continue
                # falls through all filters → delete
            self.records = kept
            deleted = initial - len(self.records)
        if deleted > 0:
            logger.info(
                "Deleted %d records (service=%s, before=%s, status=%s)",
                deleted, service, before, status,
            )
        return deleted

    def summary(
        self,
        service: str | None = None,
        status: str | None = None,
        since: float | None = None,
        until: float | None = None,
        q: str | None = None,
    ) -> dict:
        records_snapshot = self.filter(
            service=service, status=status, since=since, until=until, q=q,
        )
        services: dict[str, dict] = {}
        for r in records_snapshot:
            if r.service not in services:
                services[r.service] = {"total": 0, "healthy": 0, "avg_response_ms": 0.0, "times": []}
            s = services[r.service]
            s["total"] += 1
            if r.status == "healthy":
                s["healthy"] += 1
            s["times"].append(r.response_time_ms)
        result = {}
        for svc, data in services.items():
            times = data["times"]
            avg = sum(times) / len(times) if times else 0
            sorted_times = sorted(times)
            result[svc] = {
                "total_checks": data["total"],
                "healthy_checks": data["healthy"],
                "uptime_pct": round(data["healthy"] / data["total"] * 100, 2) if data["total"] else 0,
                "avg_response_ms": round(avg, 2),
                "min_response_ms": round(sorted_times[0], 2) if sorted_times else 0.0,
                "max_response_ms": round(sorted_times[-1], 2) if sorted_times else 0.0,
                "p50_response_ms": round(_percentile(sorted_times, 50), 2),
                "p95_response_ms": round(_percentile(sorted_times, 95), 2),
                "p99_response_ms": round(_percentile(sorted_times, 99), 2),
            }
        return result

    def service_detail(
        self,
        service: str,
        since: float | None = None,
        until: float | None = None,
    ) -> dict | None:
        """単一サービスの集約結果を返す。レコードが 1 件も無ければ None。

        `summary()` の単一サービス相当の数値 (percentile 含む) に、
        `list_services()` 由来の `latest_*` / `first_seen` / `last_seen` を
        合わせて 1 オブジェクトで返す。ダッシュボードのサービス詳細画面の
        単一エンドポイント化を意図している。
        """
        records_snapshot = self.filter(service=service, since=since, until=until)
        if not records_snapshot:
            return None

        total = len(records_snapshot)
        healthy = 0
        # `ALLOWED_STATUSES` の全キーを 0 初期化することで、UI が
        # 存在チェックなしで `status_counts.degraded` 等を参照できる。
        # `/metrics/overview` の `status_counts` と同じ形を共有する。
        status_counts: dict[str, int] = {s: 0 for s in ALLOWED_STATUSES}
        times: list[float] = []
        first_seen: float | None = None
        last_seen: float | None = None
        latest_status = ""
        latest_response_ms = 0.0
        for r in records_snapshot:
            if r.status == "healthy":
                healthy += 1
            status_counts[r.status] = status_counts.get(r.status, 0) + 1
            times.append(r.response_time_ms)
            if first_seen is None or r.timestamp < first_seen:
                first_seen = r.timestamp
            if last_seen is None or r.timestamp >= last_seen:
                last_seen = r.timestamp
                latest_status = r.status
                latest_response_ms = r.response_time_ms

        sorted_times = sorted(times)
        avg = sum(times) / total
        return {
            "service": service,
            "total_checks": total,
            "healthy_checks": healthy,
            "uptime_pct": round(healthy / total * 100, 2),
            "status_counts": status_counts,
            "first_seen": first_seen,
            "last_seen": last_seen,
            "latest_status": latest_status,
            "latest_response_ms": round(latest_response_ms, 2),
            "avg_response_ms": round(avg, 2),
            "min_response_ms": round(sorted_times[0], 2),
            "max_response_ms": round(sorted_times[-1], 2),
            "p50_response_ms": round(_percentile(sorted_times, 50), 2),
            "p95_response_ms": round(_percentile(sorted_times, 95), 2),
            "p99_response_ms": round(_percentile(sorted_times, 99), 2),
        }

    def status_changes(
        self,
        service: str,
        since: float | None = None,
        until: float | None = None,
    ) -> list[dict]:
        """指定サービスの records を timestamp 昇順で走査し、
        `status` が直前 observation と異なるイベントだけを抽出して返す。

        各イベントの形:
            {
                "at": <変化が観測された Unix timestamp>,
                "from_status": <直前 observation の status>,
                "to_status": <現在 observation の status>,
                "response_time_ms": <現在 observation の response_time_ms (小数点 2 桁)>,
            }

        初回 observation (直前が無い) は `from_status` が定義できないため
        イベントとして扱わない。since/until は records レベルのフィルタ
        として適用してから走査するため、ウィンドウ境界で「直前」が範囲外
        にあっても結果が崩れない（その場合は「ウィンドウ内の最古」が
        ベースラインとして扱われる）。

        並び順は呼び元で `order` クエリ要求に従って必要なら反転する。
        ここでは常に timestamp 昇順で返す。
        """
        records_snapshot = self.filter(service=service, since=since, until=until)
        # filter() は records の挿入順（= 観測順）をそのまま保つが、
        # 別経路で順序が崩れた場合の保険として timestamp 昇順でソートしておく。
        records_snapshot.sort(key=lambda r: r.timestamp)
        events: list[dict] = []
        previous_status: str | None = None
        for r in records_snapshot:
            if previous_status is not None and r.status != previous_status:
                events.append({
                    "at": r.timestamp,
                    "from_status": previous_status,
                    "to_status": r.status,
                    "response_time_ms": round(r.response_time_ms, 2),
                })
            previous_status = r.status
        return events

    def service_by_status(
        self,
        service: str,
        since: float | None = None,
        until: float | None = None,
    ) -> dict | None:
        """単一サービスの観測をステータス別にグルーピングして集計結果を返す。

        `service_detail` は status_counts (件数のみ) を返すが、UI で
        「healthy のとき p95=80ms、degraded のとき p95=2000ms」のように
        ステータス別のレスポンス時間分布を見たい場合に使う。

        全 `ALLOWED_STATUSES` をキーに含み（観測 0 件のステータスも `count=0` で埋める）、
        UI 側で存在チェックなしで参照できるようにする。各ステータスのブロックは:
            count / avg_response_ms / min_response_ms / max_response_ms /
            p50_response_ms / p95_response_ms / p99_response_ms /
            first_seen / last_seen

        観測 0 件のステータスのレスポンス時間統計は `0.0`、`first_seen` / `last_seen`
        は `None` を返す（`/metrics/overview` の percentile が空集合で 0.0 を返す挙動と整合）。
        対象サービスのレコードが (since/until 範囲内に) 1 件も無い場合は `None` を返す。
        """
        records_snapshot = self.filter(service=service, since=since, until=until)
        if not records_snapshot:
            return None

        per_status: dict[str, dict] = {
            s: {"times": [], "first_seen": None, "last_seen": None}
            for s in ALLOWED_STATUSES
        }
        for r in records_snapshot:
            bucket = per_status.setdefault(
                r.status,
                {"times": [], "first_seen": None, "last_seen": None},
            )
            bucket["times"].append(r.response_time_ms)
            if bucket["first_seen"] is None or r.timestamp < bucket["first_seen"]:
                bucket["first_seen"] = r.timestamp
            if bucket["last_seen"] is None or r.timestamp > bucket["last_seen"]:
                bucket["last_seen"] = r.timestamp

        by_status: dict[str, dict] = {}
        for status_name, bucket in per_status.items():
            times = bucket["times"]
            sorted_times = sorted(times)
            count = len(sorted_times)
            avg = sum(times) / count if count else 0.0
            by_status[status_name] = {
                "count": count,
                "avg_response_ms": round(avg, 2),
                "min_response_ms": round(sorted_times[0], 2) if sorted_times else 0.0,
                "max_response_ms": round(sorted_times[-1], 2) if sorted_times else 0.0,
                "p50_response_ms": round(_percentile(sorted_times, 50), 2),
                "p95_response_ms": round(_percentile(sorted_times, 95), 2),
                "p99_response_ms": round(_percentile(sorted_times, 99), 2),
                "first_seen": bucket["first_seen"],
                "last_seen": bucket["last_seen"],
            }
        return {
            "service": service,
            "total": len(records_snapshot),
            "by_status": by_status,
        }

    def incidents(
        self,
        service: str,
        since: float | None = None,
        until: float | None = None,
    ) -> list[dict]:
        """対象サービスの records を timestamp 昇順で走査し、
        `healthy` 以外のステータスが連続する区間を 1 つのインシデントへ畳んで返す。

        各インシデントの形:
            {
                "started_at": <最初の非 healthy observation の timestamp>,
                "ended_at": <最後の非 healthy observation の timestamp>,
                "duration_seconds": <ended_at - started_at>,
                "ongoing": <ウィンドウ末端まで非 healthy が続いた場合 True>,
                "statuses": [<observation で観測された非 healthy ステータスの集合をソート>],
                "observation_count": <インシデント中の observation 件数>,
                "max_response_time_ms": <インシデント中の最大 response_time (小数点 2 桁)>,
            }

        ウィンドウ境界で「直前の healthy」が範囲外にあっても結果が崩れないよう、
        since/until は records レベルのフィルタとして適用してから走査する
        （= ウィンドウ内最古の observation がインシデント start として扱われる）。

        並び順は常に `started_at` 昇順。呼び元で `order` クエリ要求に従って反転する。
        """
        records_snapshot = self.filter(service=service, since=since, until=until)
        records_snapshot.sort(key=lambda r: r.timestamp)

        incidents_out: list[dict] = []
        cur_start: float | None = None
        cur_end: float | None = None
        cur_statuses: set[str] = set()
        cur_count = 0
        cur_max_rt = 0.0

        def _flush(ongoing: bool) -> None:
            # cur_start / cur_end が None のときには呼ばれない前提。
            duration = 0.0
            if cur_end is not None and cur_start is not None:
                duration = cur_end - cur_start
            incidents_out.append({
                "started_at": cur_start,
                "ended_at": cur_end,
                "duration_seconds": round(duration, 2),
                "ongoing": ongoing,
                "statuses": sorted(cur_statuses),
                "observation_count": cur_count,
                "max_response_time_ms": round(cur_max_rt, 2),
            })

        for r in records_snapshot:
            if r.status != "healthy":
                if cur_start is None:
                    cur_start = r.timestamp
                    cur_end = r.timestamp
                    cur_statuses = {r.status}
                    cur_count = 1
                    cur_max_rt = r.response_time_ms
                else:
                    cur_end = r.timestamp
                    cur_statuses.add(r.status)
                    cur_count += 1
                    if r.response_time_ms > cur_max_rt:
                        cur_max_rt = r.response_time_ms
            else:
                if cur_start is not None:
                    _flush(ongoing=False)
                    cur_start = None
                    cur_end = None
                    cur_statuses = set()
                    cur_count = 0
                    cur_max_rt = 0.0

        if cur_start is not None:
            _flush(ongoing=True)

        return incidents_out

    def all_incidents(
        self,
        service: str | None = None,
        q: str | None = None,
        since: float | None = None,
        until: float | None = None,
    ) -> list[dict]:
        """フィルタ条件に一致する全サービスのインシデントを横断的に返す。

        `incidents()` が単一サービスの非 healthy 連続区間を抽出するのに対し、
        本メソッドは複数サービスを横断して各インシデントに `service` フィールドを
        付与した辞書を返す。SRE ダッシュボードの「現在発生中・直近のインシデント」
        全体ビュー（特定サービスを跨いだ俯瞰）の単一リクエスト化を想定。

        フィルタの意味:
            - service: 指定された場合は単一サービスのみ対象（per-service と等価）
            - q: フィルタ後の service 名に対する部分一致（大文字小文字無視）
            - since/until: 観測の Unix timestamp ウィンドウ

        並び順は呼び出し側で決める前提でここでは `started_at` 昇順を返す
        （同一 `started_at` の場合は `service` 名の辞書順をタイブレーカに使う）。
        """
        if service is not None:
            services = [service]
        else:
            services = self.distinct_services(since=since, until=until, q=q)
        out: list[dict] = []
        for svc in services:
            for inc in self.incidents(service=svc, since=since, until=until):
                # `service` 列を先頭に置いて他のフィールドはそのまま残す。
                enriched = {"service": svc, **inc}
                out.append(enriched)
        out.sort(key=lambda d: (d["started_at"], d["service"]))
        return out

    def uptime(
        self,
        service: str,
        since: float | None = None,
        until: float | None = None,
    ) -> dict | None:
        """単一サービスの SLA 観点の集約値を返す。レコードが 1 件も無ければ None。

        `service_detail()` の `uptime_pct`（observation 件数ベース）に加えて、
        `incidents()` から得られるインシデント時間ベースの SLA 指標を 1 オブジェクトで返す。

        各値:
            - total_checks / healthy_checks / uptime_pct: チェック件数ベース
            - incident_count: 非 healthy の連続区間の数
            - ongoing_incident: ウィンドウ末端で incident が継続中か
            - total_incident_seconds: 全 incident の duration の合計（小数点 2 桁）
            - longest_incident_seconds: 最長 incident の duration
            - mean_incident_seconds: incident 平均長（MTTR 相当, incident_count==0 なら 0.0）

        ダッシュボードの SLA ウィジェット用に「件数ベースの uptime_pct と、
        インシデント単位の MTTR」を 1 リクエストで一括取得する用途を想定する。
        個別 incident のリストが必要な場合は `/incidents`、status_counts や percentile
        が必要な場合は `/services/{name}` を併用する（intentional 分離）。
        """
        records_snapshot = self.filter(service=service, since=since, until=until)
        if not records_snapshot:
            return None

        total = len(records_snapshot)
        healthy = sum(1 for r in records_snapshot if r.status == "healthy")
        incidents_list = self.incidents(service=service, since=since, until=until)
        incident_count = len(incidents_list)
        if incident_count == 0:
            total_incident_seconds = 0.0
            longest_incident_seconds = 0.0
            mean_incident_seconds = 0.0
            ongoing_incident = False
        else:
            durations = [float(inc["duration_seconds"]) for inc in incidents_list]
            total_incident_seconds = sum(durations)
            longest_incident_seconds = max(durations)
            mean_incident_seconds = total_incident_seconds / incident_count
            ongoing_incident = bool(incidents_list[-1]["ongoing"])
        return {
            "service": service,
            "total_checks": total,
            "healthy_checks": healthy,
            "uptime_pct": round(healthy / total * 100, 2),
            "incident_count": incident_count,
            "ongoing_incident": ongoing_incident,
            "total_incident_seconds": round(total_incident_seconds, 2),
            "longest_incident_seconds": round(longest_incident_seconds, 2),
            "mean_incident_seconds": round(mean_incident_seconds, 2),
        }

    def all_uptime(
        self,
        q: str | None = None,
        since: float | None = None,
        until: float | None = None,
    ) -> list[dict]:
        """全サービス横断の SLA 集約値リストを返す。

        `uptime()` が単一サービスを返すのに対し、本メソッドは `distinct_services()` で
        フィルタ通過後の service 名一覧を取り、各サービスに対し `uptime()` を呼んで
        結果を集約する。`uptime()` が None を返すサービス（範囲内にレコードが無い）は
        結果に含めない。

        SRE ダッシュボードの「サービス一覧 + 各サービスの uptime / MTTR / 進行中
        インシデント」全体ビューの単一リクエスト化を想定（`/metrics/incidents` の
        SLA 集約版に相当する）。

        並び順:
            uptime_pct 昇順をベースに、同 uptime_pct はサービス名昇順をタイブレーカ
            (= 悪い uptime を先頭に持ってくる方が SRE ダッシュボードでは有用)。
            呼び出し側で必要なら反転する。
        """
        services = self.distinct_services(since=since, until=until, q=q)
        out: list[dict] = []
        for svc in services:
            detail = self.uptime(service=svc, since=since, until=until)
            if detail is None:
                continue
            out.append(detail)
        out.sort(key=lambda d: (d["uptime_pct"], d["service"]))
        return out

    def latest_for_service(
        self,
        service: str,
        since: float | None = None,
        until: float | None = None,
    ) -> MetricRecord | None:
        """対象サービスの since/until 範囲内で最新（timestamp 最大）の observation を返す。

        範囲内にレコードが無ければ `None`。同 timestamp の重複時は、`add()` の挿入順を
        保つため後勝ち（= 同 timestamp の最後に追加された observation を返す）とする。
        ダッシュボードのバッジ表示など「直近 1 件だけ欲しい」用途向け。
        """
        with self._lock:
            snapshot = list(self.records)
        latest: MetricRecord | None = None
        for r in snapshot:
            if r.service != service:
                continue
            if since is not None and r.timestamp < since:
                continue
            if until is not None and r.timestamp > until:
                continue
            if latest is None or r.timestamp >= latest.timestamp:
                latest = r
        return latest

    def recent_for_service(
        self,
        service: str,
        limit: int,
        since: float | None = None,
        until: float | None = None,
    ) -> list[MetricRecord]:
        """対象サービスの since/until 範囲内で `timestamp` 降順の直近 `limit` 件を返す。

        ダッシュボードのサービス詳細画面の「最近の N 件のチェック履歴」用途向け。
        `service_detail` の percentile / status_counts / uptime_pct を計算しないため
        軽量で、`latest_for_service` の N 件版に相当する。

        同 timestamp の重複は `add()` の挿入順を保つ（`latest_for_service` の後勝ち
        セマンティクスと同じく、同 timestamp は「より後に追加された方が新しい」と扱う）。
        範囲内のレコードが `limit` 未満ならその分だけ返す。`limit <= 0` の場合は空配列。
        """
        if limit <= 0:
            return []
        with self._lock:
            snapshot = list(self.records)
        # filter は service / since / until のみ反映。q / status は本ヘルパーでは
        # 受け取らない（呼び元エンドポイントの仕様）。
        candidates: list[tuple[int, MetricRecord]] = []
        for idx, r in enumerate(snapshot):
            if r.service != service:
                continue
            if since is not None and r.timestamp < since:
                continue
            if until is not None and r.timestamp > until:
                continue
            candidates.append((idx, r))
        # timestamp 降順、同 timestamp は挿入順の後（= idx が大きい方）を先に。
        candidates.sort(key=lambda t: (t[1].timestamp, t[0]), reverse=True)
        return [r for _, r in candidates[:limit]]

    def has_records_for_service(
        self,
        service: str,
        since: float | None = None,
        until: float | None = None,
    ) -> bool:
        """指定サービスのレコードが、フィルタ範囲内に 1 件以上存在するかを返す。

        `/metrics/services/{service_name}/timeseries` のような「データ無しは 404」
        セマンティクスを実現するため、buckets を組み立てる前に存在チェックを行う。
        全件走査だが、フィルタ後の長さを評価する `filter()` と違って match を 1 件
        見つけ次第 break するため、データ量が多い場合に短絡できる。
        """
        with self._lock:
            snapshot = list(self.records)
        for r in snapshot:
            if r.service != service:
                continue
            if since is not None and r.timestamp < since:
                continue
            if until is not None and r.timestamp > until:
                continue
            return True
        return False

    def timeseries(
        self,
        bucket_seconds: int,
        service: str | None = None,
        status: str | None = None,
        since: float | None = None,
        until: float | None = None,
        q: str | None = None,
    ) -> list[dict]:
        """フィルタ後のレコードを `bucket_seconds` 秒幅の半開区間バケットに集約する。

        各バケットは `[bucket_start, bucket_start + bucket_seconds)` に属する観測を対象に、
        総件数 `total` / ステータス内訳 `by_status` / 平均応答時間 `avg_response_ms` を返す。
        レコードのない時刻のバケットは結果に含めない（スパース）。
        並び順は `bucket_start` の昇順。`by_status` は `ALLOWED_STATUSES` の全キーを
        0 初期化したマップで返すため、クライアントは存在チェックなしで参照できる。
        """
        records_snapshot = self.filter(
            service=service, status=status, since=since, until=until, q=q,
        )
        buckets: dict[int, dict] = {}
        for r in records_snapshot:
            # 整数演算でバケット境界を求める。負の timestamp は通常起こりえないが、
            # 万一来ても `int(floor(...))` 相当として正しく丸まる。
            bucket_start = int(r.timestamp // bucket_seconds) * bucket_seconds
            b = buckets.get(bucket_start)
            if b is None:
                b = {
                    "total": 0,
                    "by_status": {s: 0 for s in ALLOWED_STATUSES},
                    "times": [],
                }
                buckets[bucket_start] = b
            b["total"] += 1
            b["by_status"][r.status] = b["by_status"].get(r.status, 0) + 1
            b["times"].append(r.response_time_ms)

        result: list[dict] = []
        for bucket_start in sorted(buckets.keys()):
            b = buckets[bucket_start]
            times = b["times"]
            # スパース表現なのでバケットに最低 1 件は入っている前提だが、
            # 念のため空配列も 0.0 にフォールバックする。
            sorted_times = sorted(times)
            avg = sum(times) / len(times) if times else 0.0
            result.append({
                "bucket_start": float(bucket_start),
                "total": b["total"],
                "by_status": b["by_status"],
                "avg_response_ms": round(avg, 2),
                "min_response_ms": round(sorted_times[0], 2) if sorted_times else 0.0,
                "max_response_ms": round(sorted_times[-1], 2) if sorted_times else 0.0,
                "p50_response_ms": round(_percentile(sorted_times, 50), 2),
                "p95_response_ms": round(_percentile(sorted_times, 95), 2),
                "p99_response_ms": round(_percentile(sorted_times, 99), 2),
            })
        return result

    def overview(
        self,
        service: str | None = None,
        status: str | None = None,
        since: float | None = None,
        until: float | None = None,
        q: str | None = None,
    ) -> dict:
        """Return a single global aggregate across all (filtered) records.

        個別サービス単位ではなく、フィルタ後の全レコードを 1 つに集約した
        トップレベルの稼働サマリを返す。ダッシュボードのヘッダ表示など、
        「全体で今どうなっているか」を 1 リクエストで把握する用途を想定。
        """
        records_snapshot = self.filter(
            service=service, status=status, since=since, until=until, q=q,
        )
        status_counts: dict[str, int] = {s: 0 for s in ALLOWED_STATUSES}
        services: set[str] = set()
        times: list[float] = []
        earliest: float | None = None
        latest: float | None = None
        for r in records_snapshot:
            services.add(r.service)
            status_counts[r.status] = status_counts.get(r.status, 0) + 1
            times.append(r.response_time_ms)
            if earliest is None or r.timestamp < earliest:
                earliest = r.timestamp
            if latest is None or r.timestamp > latest:
                latest = r.timestamp

        total = len(records_snapshot)
        healthy = status_counts.get("healthy", 0)
        sorted_times = sorted(times)
        avg = sum(times) / len(times) if times else 0.0
        return {
            "total_records": total,
            "services_count": len(services),
            "status_counts": status_counts,
            "overall_uptime_pct": round(healthy / total * 100, 2) if total else 0.0,
            "response_time_ms": {
                "avg": round(avg, 2),
                "min": round(sorted_times[0], 2) if sorted_times else 0.0,
                "max": round(sorted_times[-1], 2) if sorted_times else 0.0,
                "p50": round(_percentile(sorted_times, 50), 2),
                "p95": round(_percentile(sorted_times, 95), 2),
                "p99": round(_percentile(sorted_times, 99), 2),
            },
            "earliest_timestamp": earliest,
            "latest_timestamp": latest,
        }


store = MetricsStore()


@app.get("/health")
def health():
    logger.debug("Health check requested")
    return {"status": "healthy", "service": "analytics-api", "timestamp": time.time()}


@app.post("/metrics", status_code=201)
def post_metric(payload: MetricPayload):
    record = MetricRecord(
        service=payload.service,
        status=payload.status,
        response_time_ms=payload.response_time_ms,
        timestamp=payload.timestamp or time.time(),
    )
    store.add(record)
    return {"recorded": True, "service": record.service, "timestamp": record.timestamp}


def _normalize_q_param(raw: str | None) -> tuple[str | None, str | None]:
    """`q` クエリパラメータを正規化する。

    戻り値は (正規化後の値, エラーメッセージ)。
    - None → (None, None) : 未指定（フィルタしない）
    - trim 後が空 → (None, "q must not be blank") : 400 を返す対象
    - 上限超過 → (None, "q is too long ...") : 400 を返す対象
    - 正常 → (trimmed, None)
    """
    if raw is None:
        return None, None
    stripped = raw.strip()
    if not stripped:
        return None, "q must not be blank"
    if len(stripped) > MAX_SERVICE_LENGTH:
        return None, f"q must be at most {MAX_SERVICE_LENGTH} characters"
    return stripped, None


def _format_validation_error(exc: ValidationError) -> str:
    parts = []
    for err in exc.errors():
        loc = ".".join(str(p) for p in err.get("loc", ()) if p != "")
        msg = err.get("msg", "invalid")
        if loc:
            parts.append(f"{loc}: {msg}")
        else:
            parts.append(msg)
    return "; ".join(parts) if parts else "invalid payload"


@app.post("/metrics/batch")
async def post_metrics_batch(request: Request, response: Response):
    try:
        body = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="Request body must be valid JSON")
    if not isinstance(body, dict):
        raise HTTPException(status_code=400, detail="Request body must be a JSON object")
    metrics = body.get("metrics")
    if not isinstance(metrics, list):
        raise HTTPException(status_code=400, detail="Field 'metrics' must be an array")
    if len(metrics) == 0:
        raise HTTPException(status_code=400, detail="Field 'metrics' must not be empty")
    if len(metrics) > BATCH_MAX_SIZE:
        raise HTTPException(
            status_code=400,
            detail=f"Field 'metrics' must contain at most {BATCH_MAX_SIZE} items",
        )

    accepted: list[dict] = []
    rejected: list[dict] = []
    for index, item in enumerate(metrics):
        if not isinstance(item, dict):
            rejected.append({"index": index, "error": "item must be a JSON object"})
            continue
        try:
            payload = MetricPayload.model_validate(item)
        except ValidationError as e:
            rejected.append({"index": index, "error": _format_validation_error(e)})
            continue
        record = MetricRecord(
            service=payload.service,
            status=payload.status,
            response_time_ms=payload.response_time_ms,
            timestamp=payload.timestamp or time.time(),
        )
        store.add(record)
        accepted.append({
            "index": index,
            "service": record.service,
            "timestamp": record.timestamp,
        })

    total = len(metrics)
    if len(rejected) == total:
        status_code = 400
    elif rejected:
        status_code = 207  # Multi-Status: partial success
    else:
        status_code = 201

    logger.info(
        "Batch ingest: total=%d accepted=%d rejected=%d",
        total, len(accepted), len(rejected),
    )

    response.status_code = status_code
    return {
        "total": total,
        "accepted_count": len(accepted),
        "rejected_count": len(rejected),
        "accepted": accepted,
        "rejected": rejected,
    }


@app.get("/metrics")
def get_metrics(
    service: str | None = None,
    status: StatusLiteral | None = Query(
        default=None,
        description=f"ステータスで絞り込み（{', '.join(ALLOWED_STATUSES)}）",
    ),
    since: float | None = Query(
        default=None,
        ge=0,
        description="この Unix timestamp 以降（>=）のレコードに絞り込む",
    ),
    until: float | None = Query(
        default=None,
        ge=0,
        description="この Unix timestamp 以前（<=）のレコードに絞り込む",
    ),
    limit: int = Query(
        default=METRICS_DEFAULT_LIMIT,
        ge=1,
        le=METRICS_MAX_LIMIT,
        description=f"返却件数上限（最大 {METRICS_MAX_LIMIT}）",
    ),
    offset: int = Query(
        default=0,
        ge=0,
        description="先頭から読み飛ばす件数",
    ),
    sort: SortFieldLiteral = Query(
        default="timestamp",
        description="ソートフィールド（timestamp / service / response_time_ms / status）",
    ),
    order: SortOrderLiteral = Query(
        default="asc",
        description="ソート順（asc / desc）",
    ),
    q: str | None = Query(
        default=None,
        description="service 名に対する大文字小文字無視の部分一致検索",
    ),
):
    if since is not None and until is not None and since > until:
        raise HTTPException(
            status_code=400,
            detail="since must be less than or equal to until",
        )
    if since is not None and not math.isfinite(since):
        raise HTTPException(status_code=400, detail="since must be a finite number")
    if until is not None and not math.isfinite(until):
        raise HTTPException(status_code=400, detail="until must be a finite number")
    q_value, q_err = _normalize_q_param(q)
    if q_err is not None:
        raise HTTPException(status_code=400, detail=q_err)

    records = store.filter(service=service, status=status, since=since, until=until, q=q_value)
    reverse = order == "desc"
    records = sorted(records, key=lambda r: getattr(r, sort), reverse=reverse)
    total = len(records)
    page = records[offset:offset + limit]
    return {
        "count": len(page),
        "total": total,
        "limit": limit,
        "offset": offset,
        "sort": sort,
        "order": order,
        "metrics": [r.__dict__ for r in page],
    }


@app.delete("/metrics")
def delete_metrics(
    service: str | None = Query(
        default=None,
        description="削除対象のサービス名（省略時は service で絞り込まない）",
        min_length=1,
        max_length=MAX_SERVICE_LENGTH,
    ),
    before: float | None = Query(
        default=None,
        gt=0,
        description=(
            "この Unix timestamp より前（<）のレコードを削除。"
            "service / status と組み合わせると AND 条件になる"
        ),
    ),
    status: StatusLiteral | None = Query(
        default=None,
        description=(
            f"削除対象のステータス（{', '.join(ALLOWED_STATUSES)}）。"
            "service / before と組み合わせると AND 条件になる"
        ),
    ),
):
    if before is not None and not math.isfinite(before):
        raise HTTPException(status_code=400, detail="before must be a finite number")
    normalized = service.strip() if service is not None else None
    if normalized is not None and not normalized:
        return {"error": "service must not be blank", "deleted_count": 0}
    if normalized is None and before is None and status is None:
        raise HTTPException(
            status_code=400,
            detail="At least one of 'service', 'before' or 'status' must be provided",
        )
    deleted = store.delete(service=normalized, before=before, status=status)
    if deleted == 0:
        message = "No metrics matched the given filters"
        return {
            "error": message,
            "deleted_count": 0,
            "service": normalized,
            "before": before,
            "status": status,
        }
    return {
        "message": "Metrics deleted",
        "service": normalized,
        "before": before,
        "status": status,
        "deleted_count": deleted,
    }


@app.get("/metrics/summary")
def get_summary(
    service: str | None = None,
    status: StatusLiteral | None = Query(
        default=None,
        description=f"ステータスで絞り込み（{', '.join(ALLOWED_STATUSES)}）",
    ),
    since: float | None = Query(
        default=None,
        ge=0,
        description="この Unix timestamp 以降（>=）のレコードに絞り込む",
    ),
    until: float | None = Query(
        default=None,
        ge=0,
        description="この Unix timestamp 以前（<=）のレコードに絞り込む",
    ),
    q: str | None = Query(
        default=None,
        description="service 名に対する大文字小文字無視の部分一致検索",
    ),
):
    if since is not None and until is not None and since > until:
        raise HTTPException(
            status_code=400,
            detail="since must be less than or equal to until",
        )
    if since is not None and not math.isfinite(since):
        raise HTTPException(status_code=400, detail="since must be a finite number")
    if until is not None and not math.isfinite(until):
        raise HTTPException(status_code=400, detail="until must be a finite number")
    q_value, q_err = _normalize_q_param(q)
    if q_err is not None:
        raise HTTPException(status_code=400, detail=q_err)
    return store.summary(
        service=service, status=status, since=since, until=until, q=q_value,
    )


@app.get("/metrics/overview")
def get_overview(
    service: str | None = None,
    status: StatusLiteral | None = Query(
        default=None,
        description=f"ステータスで絞り込み（{', '.join(ALLOWED_STATUSES)}）",
    ),
    since: float | None = Query(
        default=None,
        ge=0,
        description="この Unix timestamp 以降（>=）のレコードに絞り込む",
    ),
    until: float | None = Query(
        default=None,
        ge=0,
        description="この Unix timestamp 以前（<=）のレコードに絞り込む",
    ),
    q: str | None = Query(
        default=None,
        description="service 名に対する大文字小文字無視の部分一致検索",
    ),
):
    if since is not None and until is not None and since > until:
        raise HTTPException(
            status_code=400,
            detail="since must be less than or equal to until",
        )
    if since is not None and not math.isfinite(since):
        raise HTTPException(status_code=400, detail="since must be a finite number")
    if until is not None and not math.isfinite(until):
        raise HTTPException(status_code=400, detail="until must be a finite number")
    q_value, q_err = _normalize_q_param(q)
    if q_err is not None:
        raise HTTPException(status_code=400, detail=q_err)
    return store.overview(
        service=service, status=status, since=since, until=until, q=q_value,
    )


@app.get("/metrics/count")
def get_metrics_count(
    service: str | None = None,
    status: StatusLiteral | None = Query(
        default=None,
        description=f"ステータスで絞り込み（{', '.join(ALLOWED_STATUSES)}）",
    ),
    since: float | None = Query(
        default=None,
        ge=0,
        description="この Unix timestamp 以降（>=）のレコードに絞り込む",
    ),
    until: float | None = Query(
        default=None,
        ge=0,
        description="この Unix timestamp 以前（<=）のレコードに絞り込む",
    ),
    q: str | None = Query(
        default=None,
        description="service 名に対する大文字小文字無視の部分一致検索",
    ),
):
    """フィルタ条件に合致するレコードの件数のみを返す軽量エンドポイント。

    `/metrics?limit=1` 相当のメタデータだけが必要な UI（バッジ表示・ページャ初期化等）
    向け。レコード本体を返さないため、転送量と JSON 直列化コストを抑えられる。
    `by_status` は `ALLOWED_STATUSES` の全キーを 0 で初期化して返すため、
    クライアントは存在チェックなしで各ステータスにアクセスできる。
    `services` は該当レコードに登場した service 名のユニーク数で、
    ダッシュボードの「X サービス × Y チェック」サマリーで利用する。
    """
    if since is not None and until is not None and since > until:
        raise HTTPException(
            status_code=400,
            detail="since must be less than or equal to until",
        )
    if since is not None and not math.isfinite(since):
        raise HTTPException(status_code=400, detail="since must be a finite number")
    if until is not None and not math.isfinite(until):
        raise HTTPException(status_code=400, detail="until must be a finite number")
    q_value, q_err = _normalize_q_param(q)
    if q_err is not None:
        raise HTTPException(status_code=400, detail=q_err)

    records = store.filter(
        service=service, status=status, since=since, until=until, q=q_value,
    )
    by_status: dict[str, int] = {s: 0 for s in ALLOWED_STATUSES}
    distinct_services: set[str] = set()
    for r in records:
        by_status[r.status] = by_status.get(r.status, 0) + 1
        distinct_services.add(r.service)
    return {
        "total": len(records),
        "services": len(distinct_services),
        "by_status": by_status,
    }


@app.get("/metrics/timeseries")
def get_timeseries(
    bucket_seconds: int = Query(
        default=60,
        ge=1,
        le=86400,
        description="バケット幅（秒）。1秒〜1日（86400秒）の範囲で指定。",
    ),
    service: str | None = None,
    status: StatusLiteral | None = Query(
        default=None,
        description=f"ステータスで絞り込み（{', '.join(ALLOWED_STATUSES)}）",
    ),
    since: float | None = Query(
        default=None,
        ge=0,
        description="この Unix timestamp 以降（>=）のレコードに絞り込む",
    ),
    until: float | None = Query(
        default=None,
        ge=0,
        description="この Unix timestamp 以前（<=）のレコードに絞り込む",
    ),
    q: str | None = Query(
        default=None,
        description="service 名に対する大文字小文字無視の部分一致検索",
    ),
):
    """フィルタ条件に合致するレコードを `bucket_seconds` 秒幅の時系列バケットに集約して返す。

    各バケットは `[bucket_start, bucket_start + bucket_seconds)` の半開区間で、
    観測のないバケットは結果に含めない（スパース表現）。並び順は `bucket_start` 昇順。
    ダッシュボードの時系列チャートなど、サーバ側でビニング済みのデータを必要とする
    用途を想定している。
    """
    if since is not None and until is not None and since > until:
        raise HTTPException(
            status_code=400,
            detail="since must be less than or equal to until",
        )
    if since is not None and not math.isfinite(since):
        raise HTTPException(status_code=400, detail="since must be a finite number")
    if until is not None and not math.isfinite(until):
        raise HTTPException(status_code=400, detail="until must be a finite number")
    q_value, q_err = _normalize_q_param(q)
    if q_err is not None:
        raise HTTPException(status_code=400, detail=q_err)

    buckets = store.timeseries(
        bucket_seconds=bucket_seconds,
        service=service, status=status, since=since, until=until, q=q_value,
    )
    return {
        "bucket_seconds": bucket_seconds,
        "count": len(buckets),
        "buckets": buckets,
    }


@app.get("/metrics/services")
def list_services(
    service: str | None = Query(
        default=None,
        description="サービス名で絞り込み（指定時は該当サービスのみ集計）",
        min_length=1,
        max_length=MAX_SERVICE_LENGTH,
    ),
    status: StatusLiteral | None = Query(
        default=None,
        description=f"最新ステータスで絞り込み（{', '.join(ALLOWED_STATUSES)}）",
    ),
    since: float | None = Query(
        default=None,
        ge=0,
        description="この Unix timestamp 以降（>=）の観測のみ集計",
    ),
    until: float | None = Query(
        default=None,
        ge=0,
        description="この Unix timestamp 以前（<=）の観測のみ集計",
    ),
    sort: ServiceSortFieldLiteral = Query(
        default="service",
        description=(
            "ソートフィールド（service / total_checks / healthy_checks / uptime_pct / "
            "last_seen / first_seen / latest_status / latest_response_ms）"
        ),
    ),
    order: SortOrderLiteral = Query(
        default="asc",
        description="ソート順（asc / desc）",
    ),
    limit: int = Query(
        default=METRICS_DEFAULT_LIMIT,
        ge=1,
        le=METRICS_MAX_LIMIT,
        description=f"返却件数上限（最大 {METRICS_MAX_LIMIT}）",
    ),
    offset: int = Query(
        default=0,
        ge=0,
        description="先頭から読み飛ばす件数",
    ),
    q: str | None = Query(
        default=None,
        description="service 名に対する大文字小文字無視の部分一致検索",
    ),
):
    if since is not None and until is not None and since > until:
        raise HTTPException(
            status_code=400,
            detail="since must be less than or equal to until",
        )
    if since is not None and not math.isfinite(since):
        raise HTTPException(status_code=400, detail="since must be a finite number")
    if until is not None and not math.isfinite(until):
        raise HTTPException(status_code=400, detail="until must be a finite number")
    q_value, q_err = _normalize_q_param(q)
    if q_err is not None:
        raise HTTPException(status_code=400, detail=q_err)

    # service は POST 時に strip 保存されるため、ここでも strip して照合する。
    # 空白のみの場合はフィルタなし扱い（None）とする。
    normalized_service = service.strip() if service is not None else None
    if not normalized_service:
        normalized_service = None

    records = store.filter(service=normalized_service, since=since, until=until, q=q_value)
    by_service: dict[str, dict] = {}
    for r in records:
        existing = by_service.get(r.service)
        is_healthy = 1 if r.status == "healthy" else 0
        if existing is None:
            by_service[r.service] = {
                "service": r.service,
                "total_checks": 1,
                "healthy_checks": is_healthy,
                "first_seen": r.timestamp,
                "last_seen": r.timestamp,
                "latest_status": r.status,
                "latest_response_ms": round(r.response_time_ms, 2),
            }
            continue
        existing["total_checks"] += 1
        existing["healthy_checks"] += is_healthy
        if r.timestamp < existing["first_seen"]:
            existing["first_seen"] = r.timestamp
        if r.timestamp >= existing["last_seen"]:
            existing["last_seen"] = r.timestamp
            existing["latest_status"] = r.status
            existing["latest_response_ms"] = round(r.response_time_ms, 2)

    # uptime_pct は集計確定後に一度だけ計算する（小数点 2 桁丸め）
    for s in by_service.values():
        total = s["total_checks"]
        s["uptime_pct"] = (
            round(s["healthy_checks"] / total * 100, 2) if total else 0.0
        )

    services = list(by_service.values())
    if status is not None:
        services = [s for s in services if s["latest_status"] == status]

    reverse = order == "desc"
    services.sort(key=lambda s: s[sort], reverse=reverse)
    total = len(services)
    page = services[offset:offset + limit]
    return {
        "count": len(page),
        "total": total,
        "limit": limit,
        "offset": offset,
        "sort": sort,
        "order": order,
        "services": page,
    }


@app.get("/metrics/services/names")
def list_service_names(
    since: float | None = Query(
        default=None,
        ge=0,
        description="この Unix timestamp 以降（>=）の観測のみを対象にする",
    ),
    until: float | None = Query(
        default=None,
        ge=0,
        description="この Unix timestamp 以前（<=）の観測のみを対象にする",
    ),
    q: str | None = Query(
        default=None,
        description="service 名に対する大文字小文字無視の部分一致検索",
    ),
    order: SortOrderLiteral = Query(
        default="asc",
        description="サービス名の並び順（asc / desc）",
    ),
    limit: int = Query(
        default=METRICS_DEFAULT_LIMIT,
        ge=1,
        le=METRICS_MAX_LIMIT,
        description=f"返却件数上限（最大 {METRICS_MAX_LIMIT}）",
    ),
    offset: int = Query(
        default=0,
        ge=0,
        description="先頭から読み飛ばす件数",
    ),
):
    """フィルタ後のレコードに含まれる distinct な service 名一覧のみを返す軽量エンドポイント。

    `/metrics/services` は per-service の uptime / first_seen / last_seen /
    latest_status / percentile などフル集計を返すため、フィルタドロップダウンの
    populate のように「名前だけ欲しい」用途では過剰。本エンドポイントは集計を
    一切行わず、重複排除した service 名のみをサービス名昇順（または降順）に
    並べてページネーションして返す。
    """
    if since is not None and until is not None and since > until:
        raise HTTPException(
            status_code=400,
            detail="since must be less than or equal to until",
        )
    if since is not None and not math.isfinite(since):
        raise HTTPException(status_code=400, detail="since must be a finite number")
    if until is not None and not math.isfinite(until):
        raise HTTPException(status_code=400, detail="until must be a finite number")
    q_value, q_err = _normalize_q_param(q)
    if q_err is not None:
        raise HTTPException(status_code=400, detail=q_err)

    names = store.distinct_services(since=since, until=until, q=q_value)
    names.sort(reverse=(order == "desc"))
    total = len(names)
    page = names[offset:offset + limit]
    return {
        "count": len(page),
        "total": total,
        "limit": limit,
        "offset": offset,
        "order": order,
        "names": page,
    }


@app.get("/metrics/services/{service_name}")
def get_service_detail(
    service_name: str,
    since: float | None = Query(
        default=None,
        ge=0,
        description="この Unix timestamp 以降（>=）の観測のみ集計",
    ),
    until: float | None = Query(
        default=None,
        ge=0,
        description="この Unix timestamp 以前（<=）の観測のみ集計",
    ),
):
    """単一サービスの詳細集計を返す。データが無ければ 404。

    `/metrics/services?service=X` は単一要素の配列を返すため UI 側で unwrap
    が必要だったが、こちらは単一オブジェクトを返す。`latest_*` と
    percentile (p50/p95/p99) を同時に返すため、サービス詳細画面で 1
    リクエストにまとめられる。
    """
    if since is not None and until is not None and since > until:
        raise HTTPException(
            status_code=400,
            detail="since must be less than or equal to until",
        )
    if since is not None and not math.isfinite(since):
        raise HTTPException(status_code=400, detail="since must be a finite number")
    if until is not None and not math.isfinite(until):
        raise HTTPException(status_code=400, detail="until must be a finite number")

    normalized = service_name.strip()
    if not normalized:
        raise HTTPException(status_code=400, detail="service_name must not be blank")
    if len(normalized) > MAX_SERVICE_LENGTH:
        raise HTTPException(
            status_code=400,
            detail=f"service_name must be at most {MAX_SERVICE_LENGTH} characters",
        )

    detail = store.service_detail(service=normalized, since=since, until=until)
    if detail is None:
        raise HTTPException(
            status_code=404,
            detail=f"No metrics found for service '{normalized}'",
        )
    return detail


@app.get("/metrics/services/{service_name}/latest")
def get_service_latest(
    service_name: str,
    since: float | None = Query(
        default=None,
        ge=0,
        description="この Unix timestamp 以降（>=）の observation のみ対象",
    ),
    until: float | None = Query(
        default=None,
        ge=0,
        description="この Unix timestamp 以前（<=）の observation のみ対象",
    ),
):
    """単一サービスの最新 observation 1 件を `{service, status, response_time_ms, timestamp}` で返す。

    `GET /metrics/services/{service_name}` はフル集約 (percentile / status_counts / uptime_pct)
    を返すため「現在のステータス・直近の応答時間だけを見たい」UI 用途では過剰。
    本エンドポイントはレスポンス時間統計や集計を計算せず、対象範囲内で `timestamp` 最大
    の 1 件をそのまま返す軽量エンドポイント。

    `since` / `until` クエリで対象範囲を絞れる（他の `/metrics/services/{name}/*` と整合）。
    範囲内にレコードが 1 件も無い場合は 404。
    """
    if since is not None and until is not None and since > until:
        raise HTTPException(
            status_code=400,
            detail="since must be less than or equal to until",
        )
    if since is not None and not math.isfinite(since):
        raise HTTPException(status_code=400, detail="since must be a finite number")
    if until is not None and not math.isfinite(until):
        raise HTTPException(status_code=400, detail="until must be a finite number")

    normalized = service_name.strip()
    if not normalized:
        raise HTTPException(status_code=400, detail="service_name must not be blank")
    if len(normalized) > MAX_SERVICE_LENGTH:
        raise HTTPException(
            status_code=400,
            detail=f"service_name must be at most {MAX_SERVICE_LENGTH} characters",
        )

    latest = store.latest_for_service(service=normalized, since=since, until=until)
    if latest is None:
        raise HTTPException(
            status_code=404,
            detail=f"No metrics found for service '{normalized}'",
        )
    return {
        "service": latest.service,
        "status": latest.status,
        "response_time_ms": round(latest.response_time_ms, 2),
        "timestamp": latest.timestamp,
    }


@app.get("/metrics/services/{service_name}/recent")
def get_service_recent(
    service_name: str,
    limit: int = Query(
        default=10,
        ge=1,
        le=200,
        description="返す observation 件数の上限（1〜200）。",
    ),
    since: float | None = Query(
        default=None,
        ge=0,
        description="この Unix timestamp 以降（>=）の observation のみ対象",
    ),
    until: float | None = Query(
        default=None,
        ge=0,
        description="この Unix timestamp 以前（<=）の observation のみ対象",
    ),
):
    """単一サービスの直近 N 件の observation を `timestamp` 降順で返す。

    `/metrics/services/{service_name}` はフル集約 (percentile / uptime_pct / status_counts)
    を計算するため、ダッシュボードの「最近のチェック履歴」一覧用途には過剰。
    本エンドポイントは生 observation を新しい順に返すだけで、集計は一切しない。

    `since` / `until` クエリで対象範囲を絞れる（他の `/metrics/services/{name}/*` と整合）。
    範囲内にレコードが 1 件も無い場合は 404。
    """
    if since is not None and until is not None and since > until:
        raise HTTPException(
            status_code=400,
            detail="since must be less than or equal to until",
        )
    if since is not None and not math.isfinite(since):
        raise HTTPException(status_code=400, detail="since must be a finite number")
    if until is not None and not math.isfinite(until):
        raise HTTPException(status_code=400, detail="until must be a finite number")

    normalized = service_name.strip()
    if not normalized:
        raise HTTPException(status_code=400, detail="service_name must not be blank")
    if len(normalized) > MAX_SERVICE_LENGTH:
        raise HTTPException(
            status_code=400,
            detail=f"service_name must be at most {MAX_SERVICE_LENGTH} characters",
        )

    items = store.recent_for_service(
        service=normalized,
        limit=limit,
        since=since,
        until=until,
    )
    if not items:
        raise HTTPException(
            status_code=404,
            detail=f"No metrics found for service '{normalized}'",
        )
    return {
        "service": normalized,
        "count": len(items),
        "items": [
            {
                "status": r.status,
                "response_time_ms": round(r.response_time_ms, 2),
                "timestamp": r.timestamp,
            }
            for r in items
        ],
    }


@app.get("/metrics/services/{service_name}/timeseries")
def get_service_timeseries(
    service_name: str,
    bucket_seconds: int = Query(
        default=60,
        ge=1,
        le=86400,
        description="バケット幅（秒）。1秒〜1日（86400秒）の範囲で指定。",
    ),
    status: StatusLiteral | None = Query(
        default=None,
        description=f"ステータスで絞り込み（{', '.join(ALLOWED_STATUSES)}）",
    ),
    since: float | None = Query(
        default=None,
        ge=0,
        description="この Unix timestamp 以降（>=）のレコードに絞り込む",
    ),
    until: float | None = Query(
        default=None,
        ge=0,
        description="この Unix timestamp 以前（<=）のレコードに絞り込む",
    ),
):
    """単一サービスを対象とした時系列バケット集計を返す。

    `/metrics/timeseries?service=X` と同じ buckets 形を返すが、

    - service 名をパスから受け取ることで URL が宣言的になり、UI 側でのクエリ生成が不要
    - 該当サービスのレコードが (since/until 範囲内に) 1 件も無い場合は 404 を返す
      （`/metrics/services/{service_name}` 詳細エンドポイントと同じ「存在しない service は
      404」セマンティクスを共有）

    という点が異なる。response には `service` フィールドを付与して、UI 側で
    どのサービスの結果かを混同しないようにする。
    """
    if since is not None and until is not None and since > until:
        raise HTTPException(
            status_code=400,
            detail="since must be less than or equal to until",
        )
    if since is not None and not math.isfinite(since):
        raise HTTPException(status_code=400, detail="since must be a finite number")
    if until is not None and not math.isfinite(until):
        raise HTTPException(status_code=400, detail="until must be a finite number")

    normalized = service_name.strip()
    if not normalized:
        raise HTTPException(status_code=400, detail="service_name must not be blank")
    if len(normalized) > MAX_SERVICE_LENGTH:
        raise HTTPException(
            status_code=400,
            detail=f"service_name must be at most {MAX_SERVICE_LENGTH} characters",
        )

    # status フィルタは buckets の中身を空に絞り込むだけで、サービス自体は存在しうるため、
    # 存在チェックは since/until だけで判定する（status フィルタ後の空 buckets は通常応答）。
    if not store.has_records_for_service(service=normalized, since=since, until=until):
        raise HTTPException(
            status_code=404,
            detail=f"No metrics found for service '{normalized}'",
        )

    buckets = store.timeseries(
        bucket_seconds=bucket_seconds,
        service=normalized,
        status=status,
        since=since,
        until=until,
    )
    return {
        "service": normalized,
        "bucket_seconds": bucket_seconds,
        "count": len(buckets),
        "buckets": buckets,
    }


@app.get("/metrics/services/{service_name}/status_changes")
def get_service_status_changes(
    service_name: str,
    since: float | None = Query(
        default=None,
        ge=0,
        description="この Unix timestamp 以降（>=）の観測のみを対象にする",
    ),
    until: float | None = Query(
        default=None,
        ge=0,
        description="この Unix timestamp 以前（<=）の観測のみを対象にする",
    ),
    limit: int = Query(
        default=METRICS_DEFAULT_LIMIT,
        ge=1,
        le=METRICS_MAX_LIMIT,
        description=f"返却件数上限（最大 {METRICS_MAX_LIMIT}）",
    ),
    offset: int = Query(
        default=0,
        ge=0,
        description="先頭から読み飛ばす件数",
    ),
    order: SortOrderLiteral = Query(
        default="asc",
        description="ソート順（at の昇順 / 降順）",
    ),
):
    """単一サービスのステータス遷移イベントを時系列順に返す。

    `/metrics?service=X&sort=timestamp` をクライアント側で順次比較すれば等価な結果は
    得られるが、ペイロード（全 observation）の転送コストが高く、ロジックが各 UI
    に分散するのを避けるためサーバ側で集約する。

    レスポンス:
        {
          "service": <service_name>,
          "count": <ページ内件数>,
          "total": <since/until 範囲内の遷移総数>,
          "limit": <limit>,
          "offset": <offset>,
          "order": <"asc" or "desc">,
          "changes": [
            {"at": ..., "from_status": ..., "to_status": ..., "response_time_ms": ...},
            ...
          ]
        }

    変化点が 1 件も無い場合（観測が全て同じステータス、または observation が 1 件のみ）は
    `total: 0` / `changes: []` を返す。ただし、対象サービスのレコードがウィンドウ範囲内
    に 1 件も無い場合は 404 を返す（`/metrics/services/{service_name}` 等とセマンティクス
    を揃える）。
    """
    if since is not None and until is not None and since > until:
        raise HTTPException(
            status_code=400,
            detail="since must be less than or equal to until",
        )
    if since is not None and not math.isfinite(since):
        raise HTTPException(status_code=400, detail="since must be a finite number")
    if until is not None and not math.isfinite(until):
        raise HTTPException(status_code=400, detail="until must be a finite number")

    normalized = service_name.strip()
    if not normalized:
        raise HTTPException(status_code=400, detail="service_name must not be blank")
    if len(normalized) > MAX_SERVICE_LENGTH:
        raise HTTPException(
            status_code=400,
            detail=f"service_name must be at most {MAX_SERVICE_LENGTH} characters",
        )

    if not store.has_records_for_service(service=normalized, since=since, until=until):
        raise HTTPException(
            status_code=404,
            detail=f"No metrics found for service '{normalized}'",
        )

    events = store.status_changes(service=normalized, since=since, until=until)
    if order == "desc":
        events.reverse()
    total = len(events)
    page = events[offset:offset + limit]
    return {
        "service": normalized,
        "count": len(page),
        "total": total,
        "limit": limit,
        "offset": offset,
        "order": order,
        "changes": page,
    }


@app.get("/metrics/services/{service_name}/by_status")
def get_service_by_status(
    service_name: str,
    since: float | None = Query(
        default=None,
        ge=0,
        description="この Unix timestamp 以降（>=）の観測のみ集計",
    ),
    until: float | None = Query(
        default=None,
        ge=0,
        description="この Unix timestamp 以前（<=）の観測のみ集計",
    ),
):
    """単一サービスの観測を `status` 別にグルーピングしてレスポンス時間統計を返す。

    既存 `/metrics/services/{service_name}` は `status_counts`（件数のみ）と全体の
    percentile を返すが、「healthy のとき p95=80ms / degraded のとき p95=2000ms」のように
    ステータスごとのレスポンス時間分布を可視化したい場面では情報が足りなかった。
    本エンドポイントは各ステータスについて count / avg / min / max / p50 / p95 / p99 /
    first_seen / last_seen を返し、UI 側で 1 リクエストでドリルダウン表示を可能にする。

    レスポンス:
        {
          "service": <service_name>,
          "total": <since/until 範囲内の全 observation 件数>,
          "by_status": {
            "healthy":   {count, avg_response_ms, min, max, p50, p95, p99, first_seen, last_seen},
            "unhealthy": {...},
            "degraded":  {...},
            "unknown":   {...}
          }
        }

    `by_status` は `ALLOWED_STATUSES` の全キーを必ず含み、観測 0 件のステータスでも
    count=0 / 統計 0.0 / first_seen=last_seen=null を埋めるため、UI は存在チェック不要。
    対象サービスのレコードが範囲内に 1 件も無い場合は 404 を返す
    （`/metrics/services/{service_name}` 等とセマンティクスを揃える）。
    """
    if since is not None and until is not None and since > until:
        raise HTTPException(
            status_code=400,
            detail="since must be less than or equal to until",
        )
    if since is not None and not math.isfinite(since):
        raise HTTPException(status_code=400, detail="since must be a finite number")
    if until is not None and not math.isfinite(until):
        raise HTTPException(status_code=400, detail="until must be a finite number")

    normalized = service_name.strip()
    if not normalized:
        raise HTTPException(status_code=400, detail="service_name must not be blank")
    if len(normalized) > MAX_SERVICE_LENGTH:
        raise HTTPException(
            status_code=400,
            detail=f"service_name must be at most {MAX_SERVICE_LENGTH} characters",
        )

    detail = store.service_by_status(service=normalized, since=since, until=until)
    if detail is None:
        raise HTTPException(
            status_code=404,
            detail=f"No metrics found for service '{normalized}'",
        )
    return detail


@app.get("/metrics/services/{service_name}/incidents")
def get_service_incidents(
    service_name: str,
    since: float | None = Query(
        default=None,
        ge=0,
        description="この Unix timestamp 以降（>=）の観測のみ対象",
    ),
    until: float | None = Query(
        default=None,
        ge=0,
        description="この Unix timestamp 以前（<=）の観測のみ対象",
    ),
    limit: int = Query(
        default=METRICS_DEFAULT_LIMIT,
        ge=1,
        le=METRICS_MAX_LIMIT,
        description=f"返却件数上限（最大 {METRICS_MAX_LIMIT}）",
    ),
    offset: int = Query(
        default=0,
        ge=0,
        description="先頭から読み飛ばす件数",
    ),
    order: SortOrderLiteral = Query(
        default="asc",
        description="ソート順（started_at の昇順 / 降順）",
    ),
):
    """単一サービスの「`healthy` 以外が連続した期間」をインシデントとして時系列順に返す。

    `/metrics/services/{service_name}/status_changes` は遷移点のリストを返すが、
    各 UI で隣り合う `healthy→non-healthy→healthy` を 1 イベントへ畳む必要があり、
    集計ロジックが各クライアントに分散していた。本エンドポイントはサーバ側で
    インシデント単位に集約し、`started_at` / `ended_at` / `duration_seconds` /
    `ongoing` / `statuses` / `observation_count` / `max_response_time_ms` を返す。

    レスポンス:
        {
          "service": <service_name>,
          "count":  <ページ内件数>,
          "total":  <ウィンドウ内のインシデント総数>,
          "limit":  <limit>,
          "offset": <offset>,
          "order":  <"asc" or "desc">,
          "incidents": [
            {"started_at", "ended_at", "duration_seconds", "ongoing",
             "statuses", "observation_count", "max_response_time_ms"},
            ...
          ]
        }

    インシデントが 1 件も無い（全 observation が `healthy`）場合は `total: 0`,
    `incidents: []`。対象サービスのレコードが範囲内に 1 件も無い場合は 404 を返す
    （他の `/metrics/services/{name}/*` とセマンティクスを揃える）。

    `ongoing` はウィンドウ末端まで非 healthy が続いた場合に True。ウィンドウ外で
    のちに healthy へ戻った可能性は判定できないため、「ウィンドウ内では未終了」
    という意味で扱う。
    """
    if since is not None and until is not None and since > until:
        raise HTTPException(
            status_code=400,
            detail="since must be less than or equal to until",
        )
    if since is not None and not math.isfinite(since):
        raise HTTPException(status_code=400, detail="since must be a finite number")
    if until is not None and not math.isfinite(until):
        raise HTTPException(status_code=400, detail="until must be a finite number")

    normalized = service_name.strip()
    if not normalized:
        raise HTTPException(status_code=400, detail="service_name must not be blank")
    if len(normalized) > MAX_SERVICE_LENGTH:
        raise HTTPException(
            status_code=400,
            detail=f"service_name must be at most {MAX_SERVICE_LENGTH} characters",
        )

    if not store.has_records_for_service(service=normalized, since=since, until=until):
        raise HTTPException(
            status_code=404,
            detail=f"No metrics found for service '{normalized}'",
        )

    incidents_list = store.incidents(service=normalized, since=since, until=until)
    if order == "desc":
        incidents_list.reverse()
    total = len(incidents_list)
    page = incidents_list[offset:offset + limit]
    return {
        "service": normalized,
        "count": len(page),
        "total": total,
        "limit": limit,
        "offset": offset,
        "order": order,
        "incidents": page,
    }


@app.get("/metrics/incidents")
def get_all_incidents(
    service: str | None = Query(
        default=None,
        description="単一サービスのみに絞り込む場合に指定（per-service 版と等価）",
    ),
    q: str | None = Query(
        default=None,
        description="service 名に対する部分一致フィルタ（大文字小文字無視）",
    ),
    since: float | None = Query(
        default=None,
        ge=0,
        description="この Unix timestamp 以降（>=）の観測のみ対象",
    ),
    until: float | None = Query(
        default=None,
        ge=0,
        description="この Unix timestamp 以前（<=）の観測のみ対象",
    ),
    ongoing_only: bool = Query(
        default=False,
        description="True ならウィンドウ末端で継続中（ongoing）のインシデントのみを返す",
    ),
    limit: int = Query(
        default=METRICS_DEFAULT_LIMIT,
        ge=1,
        le=METRICS_MAX_LIMIT,
        description=f"返却件数上限（最大 {METRICS_MAX_LIMIT}）",
    ),
    offset: int = Query(
        default=0,
        ge=0,
        description="先頭から読み飛ばす件数",
    ),
    order: SortOrderLiteral = Query(
        default="asc",
        description="ソート順（started_at の昇順 / 降順）",
    ),
):
    """全サービス横断のインシデント一覧。

    `/metrics/services/{service_name}/incidents` は単一サービス単位だが、
    SRE ダッシュボードの「今どこかで障害が起きているか」を見たい場面では
    クライアント側で `/metrics/services/names` → 各サービス毎の
    `/metrics/services/{name}/incidents` を fan-out する必要があった。
    本エンドポイントはサーバ側で全サービスを横断集計し、各インシデントに
    `service` 列を付けて返す。

    レスポンス:
        {
          "count":  <ページ内件数>,
          "total":  <フィルタ後のインシデント総数>,
          "limit":  <limit>,
          "offset": <offset>,
          "order":  <"asc" or "desc">,
          "incidents": [
            {"service", "started_at", "ended_at", "duration_seconds", "ongoing",
             "statuses", "observation_count", "max_response_time_ms"},
            ...
          ]
        }

    `service` クエリを指定した場合は単一サービスのみ対象（`/metrics/services/
    {service_name}/incidents` と同等の絞り込み）。`q` は service 名への部分一致
    フィルタ（`/metrics/overview` 等と同じセマンティクス）。`ongoing_only=true`
    の場合はウィンドウ末端まで非 healthy が続いている現在進行中のインシデント
    だけを返す。

    並びは `started_at` 昇順をベースに、同 timestamp は `service` 名でタイブレーク。
    `order=desc` を指定するとリストを反転して返す。
    """
    if since is not None and until is not None and since > until:
        raise HTTPException(
            status_code=400,
            detail="since must be less than or equal to until",
        )
    if since is not None and not math.isfinite(since):
        raise HTTPException(status_code=400, detail="since must be a finite number")
    if until is not None and not math.isfinite(until):
        raise HTTPException(status_code=400, detail="until must be a finite number")

    normalized_service: str | None = None
    if service is not None:
        normalized_service = service.strip()
        if not normalized_service:
            raise HTTPException(status_code=400, detail="service must not be blank")
        if len(normalized_service) > MAX_SERVICE_LENGTH:
            raise HTTPException(
                status_code=400,
                detail=f"service must be at most {MAX_SERVICE_LENGTH} characters",
            )

    normalized_q, q_error = _normalize_q_param(q)
    if q_error is not None:
        raise HTTPException(status_code=400, detail=q_error)

    incidents_list = store.all_incidents(
        service=normalized_service,
        q=normalized_q,
        since=since,
        until=until,
    )
    if ongoing_only:
        incidents_list = [inc for inc in incidents_list if inc.get("ongoing")]
    if order == "desc":
        incidents_list.reverse()
    total = len(incidents_list)
    page = incidents_list[offset:offset + limit]
    return {
        "count": len(page),
        "total": total,
        "limit": limit,
        "offset": offset,
        "order": order,
        "incidents": page,
    }


@app.get("/metrics/services/{service_name}/uptime")
def get_service_uptime(
    service_name: str,
    since: float | None = Query(
        default=None,
        ge=0,
        description="この Unix timestamp 以降（>=）の観測のみ集計",
    ),
    until: float | None = Query(
        default=None,
        ge=0,
        description="この Unix timestamp 以前（<=）の観測のみ集計",
    ),
):
    """単一サービスの SLA 集約値を返す。

    既存:
        - `/metrics/services/{service_name}` は uptime_pct (件数ベース) のみ
        - `/metrics/services/{service_name}/incidents` は incident のリスト
    の 2 つを SRE ダッシュボードで結合する利用が増えたため、件数ベースの
    uptime_pct とインシデント単位の SLA 指標 (incident_count / total_incident_seconds /
    longest_incident_seconds / mean_incident_seconds / ongoing_incident) を 1 リクエストに
    集約する。

    レスポンス:
        {
          "service": <service_name>,
          "total_checks": <int>,
          "healthy_checks": <int>,
          "uptime_pct": <float>,
          "incident_count": <int>,
          "ongoing_incident": <bool>,
          "total_incident_seconds": <float>,
          "longest_incident_seconds": <float>,
          "mean_incident_seconds": <float>
        }

    対象サービスのレコードが範囲内に 1 件も無い場合は 404 を返す
    （他の `/metrics/services/{name}/*` とセマンティクスを揃える）。
    """
    if since is not None and until is not None and since > until:
        raise HTTPException(
            status_code=400,
            detail="since must be less than or equal to until",
        )
    if since is not None and not math.isfinite(since):
        raise HTTPException(status_code=400, detail="since must be a finite number")
    if until is not None and not math.isfinite(until):
        raise HTTPException(status_code=400, detail="until must be a finite number")

    normalized = service_name.strip()
    if not normalized:
        raise HTTPException(status_code=400, detail="service_name must not be blank")
    if len(normalized) > MAX_SERVICE_LENGTH:
        raise HTTPException(
            status_code=400,
            detail=f"service_name must be at most {MAX_SERVICE_LENGTH} characters",
        )

    detail = store.uptime(service=normalized, since=since, until=until)
    if detail is None:
        raise HTTPException(
            status_code=404,
            detail=f"No metrics found for service '{normalized}'",
        )
    return detail


@app.get("/metrics/uptime")
def get_all_uptime(
    q: str | None = Query(
        default=None,
        description="service 名に対する部分一致フィルタ（大文字小文字無視）",
    ),
    since: float | None = Query(
        default=None,
        ge=0,
        description="この Unix timestamp 以降（>=）の観測のみ集計",
    ),
    until: float | None = Query(
        default=None,
        ge=0,
        description="この Unix timestamp 以前（<=）の観測のみ集計",
    ),
    ongoing_only: bool = Query(
        default=False,
        description="True ならウィンドウ末端で進行中インシデントを持つサービスのみ返す",
    ),
    limit: int = Query(
        default=METRICS_DEFAULT_LIMIT,
        ge=1,
        le=METRICS_MAX_LIMIT,
        description=f"返却件数上限（最大 {METRICS_MAX_LIMIT}）",
    ),
    offset: int = Query(
        default=0,
        ge=0,
        description="先頭から読み飛ばす件数",
    ),
    order: SortOrderLiteral = Query(
        default="asc",
        description="ソート順（uptime_pct 昇順 = asc は worst-first、降順 = desc は best-first）",
    ),
):
    """全サービス横断の SLA 集約一覧。

    `/metrics/services/{service_name}/uptime` は単一サービス単位だが、
    SRE ダッシュボードの「全サービスの uptime / MTTR / 進行中インシデント」
    ビューでは `/metrics/services/names` → 各サービス毎の `/uptime` の fan-out が
    必要だった。本エンドポイントはサーバ側で全サービスを横断集計し、各サービスの
    SLA 集約値を 1 リクエストで返す（`/metrics/incidents` の SLA 集約版）。

    レスポンス:
        {
          "count":  <ページ内件数>,
          "total":  <フィルタ後のサービス数>,
          "limit":  <limit>,
          "offset": <offset>,
          "order":  <"asc" or "desc">,
          "services": [
            {"service", "total_checks", "healthy_checks", "uptime_pct",
             "incident_count", "ongoing_incident", "total_incident_seconds",
             "longest_incident_seconds", "mean_incident_seconds"},
            ...
          ]
        }

    並びは uptime_pct 昇順をベースに、同 uptime_pct は service 名でタイブレーク。
    `asc`（既定）は worst-uptime 先頭、`desc` は best-uptime 先頭。
    `ongoing_only=true` でフィルタすると `ongoing_incident=true` のサービスだけを返す。
    範囲内にレコードが 1 件も無いサービスは結果に含めない（`/uptime` の per-service
    版が 404 を返すケースに相当）。
    """
    if since is not None and until is not None and since > until:
        raise HTTPException(
            status_code=400,
            detail="since must be less than or equal to until",
        )
    if since is not None and not math.isfinite(since):
        raise HTTPException(status_code=400, detail="since must be a finite number")
    if until is not None and not math.isfinite(until):
        raise HTTPException(status_code=400, detail="until must be a finite number")

    normalized_q, q_error = _normalize_q_param(q)
    if q_error is not None:
        raise HTTPException(status_code=400, detail=q_error)

    services_list = store.all_uptime(q=normalized_q, since=since, until=until)
    if ongoing_only:
        services_list = [s for s in services_list if s.get("ongoing_incident")]
    if order == "desc":
        services_list = list(reversed(services_list))
    total = len(services_list)
    page = services_list[offset:offset + limit]
    return {
        "count": len(page),
        "total": total,
        "limit": limit,
        "offset": offset,
        "order": order,
        "services": page,
    }


if __name__ == "__main__":
    import uvicorn

    port = int(os.getenv("ANALYTICS_PORT", "8001"))
    logger.info("Starting Analytics API on port %d", port)
    uvicorn.run(app, host="0.0.0.0", port=port)
