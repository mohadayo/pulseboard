import express, { Request, Response, NextFunction } from "express";
import axios, { AxiosError } from "axios";
import { createLogger, format, transports } from "winston";

const logger = createLogger({
  level: process.env.LOG_LEVEL || "info",
  format: format.combine(format.timestamp(), format.json()),
  transports: [new transports.Console()],
});

const ANALYTICS_URL = process.env.ANALYTICS_URL || "http://localhost:8001";
const CHECKER_URL = process.env.CHECKER_URL || "http://localhost:8002";
const PROXY_TIMEOUT = parseInt(process.env.PROXY_TIMEOUT || "5000", 10);

// JSON ペイロードの最大サイズ。明示しないと express.json の既定 100kb で動くため、
// 環境変数で上書きできる形で明示する。analytics-api の /metrics/batch
// （最大 500 件）の実用サイズも収まる 256kb を既定値に置く。
const MAX_REQUEST_BODY = process.env.MAX_REQUEST_BODY || "256kb";

const app = express();
app.use(express.json({ limit: MAX_REQUEST_BODY }));

app.use((req: Request, _res: Response, next: NextFunction) => {
  logger.info(`${req.method} ${req.path}`, { ip: req.ip });
  next();
});

// allowedParams に列挙したクエリのみを上流向け URLSearchParams に詰める。
// 未指定 (undefined) と空文字は除外する。GET / DELETE プロキシで共有する。
function buildUpstreamParams(
  req: Request,
  allowedParams: readonly string[],
): URLSearchParams {
  const params = new URLSearchParams();
  for (const key of allowedParams) {
    const value = req.query[key];
    if (value !== undefined && value !== "") {
      params.set(key, String(value));
    }
  }
  return params;
}

// 上流呼び出しで発生したエラーを 4xx/5xx 伝播 or 502 へ丸めて応答する。
// `proxyAnalyticsGet` / `proxyAnalyticsDelete` の共通エラー出力。
function respondUpstreamError(
  res: Response,
  err: unknown,
  label: string,
): void {
  if (err instanceof AxiosError && err.response) {
    logger.warn("Analytics returned error", {
      label,
      status: err.response.status,
      data: err.response.data,
    });
    res.status(err.response.status).json(err.response.data);
    return;
  }
  const message = err instanceof AxiosError ? err.message : "Unknown error";
  logger.error(`Failed to fetch ${label}`, { error: message });
  res.status(502).json({ error: "Analytics service unavailable", detail: message });
}

// analytics-api への GET プロキシ共通処理。
// allowedParams に列挙したクエリのみを転送し（未指定・空文字は除外）、
// 上流の応答はそのまま返す。上流の HTTP エラー(4xx/5xx)はステータス込みで
// 伝播し、接続不能などの transport エラーは 502 に丸めて返す。
async function proxyAnalyticsGet(
  req: Request,
  res: Response,
  upstreamPath: string,
  allowedParams: readonly string[],
  label: string,
): Promise<void> {
  try {
    const qs = buildUpstreamParams(req, allowedParams).toString();
    const url = qs
      ? `${ANALYTICS_URL}${upstreamPath}?${qs}`
      : `${ANALYTICS_URL}${upstreamPath}`;
    const resp = await axios.get(url, { timeout: PROXY_TIMEOUT });
    res.json(resp.data);
  } catch (err) {
    respondUpstreamError(res, err, label);
  }
}

// analytics-api への DELETE プロキシ共通処理。
// GET と同じ allowlist ベースの転送と空文字除外を適用する。
// 上流の応答ステータス（200 でなくても）を踏襲して返す。
async function proxyAnalyticsDelete(
  req: Request,
  res: Response,
  upstreamPath: string,
  allowedParams: readonly string[],
  label: string,
): Promise<void> {
  try {
    const qs = buildUpstreamParams(req, allowedParams).toString();
    const url = qs
      ? `${ANALYTICS_URL}${upstreamPath}?${qs}`
      : `${ANALYTICS_URL}${upstreamPath}`;
    const resp = await axios.delete(url, { timeout: PROXY_TIMEOUT });
    res.status(resp.status).json(resp.data);
  } catch (err) {
    respondUpstreamError(res, err, label);
  }
}

app.get("/health", (_req: Request, res: Response) => {
  res.json({ status: "healthy", service: "api-gateway" });
});

app.get("/api/metrics", (req: Request, res: Response) =>
  proxyAnalyticsGet(
    req,
    res,
    "/metrics",
    ["service", "status", "since", "until", "limit", "offset", "sort", "order", "q"],
    "metrics",
  ),
);

app.get("/api/metrics/summary", (req: Request, res: Response) =>
  proxyAnalyticsGet(
    req,
    res,
    "/metrics/summary",
    ["service", "status", "since", "until", "q"],
    "summary",
  ),
);

app.get("/api/metrics/overview", (req: Request, res: Response) =>
  proxyAnalyticsGet(
    req,
    res,
    "/metrics/overview",
    ["service", "status", "since", "until", "q"],
    "overview",
  ),
);

app.get("/api/metrics/count", (req: Request, res: Response) =>
  proxyAnalyticsGet(
    req,
    res,
    "/metrics/count",
    ["service", "status", "since", "until", "q"],
    "count",
  ),
);

app.get("/api/metrics/services", (req: Request, res: Response) =>
  proxyAnalyticsGet(
    req,
    res,
    "/metrics/services",
    ["service", "status", "since", "until", "sort", "order", "limit", "offset", "q"],
    "services",
  ),
);

// 時系列バケット集計エンドポイントを analytics-api にプロキシする。
// allowedParams は analytics-api 側 `/metrics/timeseries` のクエリと一致させる。
app.get("/api/metrics/timeseries", (req: Request, res: Response) =>
  proxyAnalyticsGet(
    req,
    res,
    "/metrics/timeseries",
    ["service", "status", "since", "until", "q", "bucket_seconds"],
    "timeseries",
  ),
);

// distinct な service 名一覧のみを返す軽量エンドポイントを analytics-api にプロキシする。
// `:name` パラメタ付きルート (`/api/metrics/services/:name`) より前に登録する必要がある
// — Express は登録順にマッチするため、後ろに置くと `name = "names"` の単一サービス
// 詳細リクエストとして解釈されてしまう。
app.get("/api/metrics/services/names", (req: Request, res: Response) =>
  proxyAnalyticsGet(
    req,
    res,
    "/metrics/services/names",
    ["since", "until", "q", "order", "limit", "offset"],
    "service-names",
  ),
);

// 単一サービスの詳細を返すエンドポイント。analytics-api 側で 404 が返るため、
// proxy 経由でもそのまま 404 を伝播する。
app.get(
  "/api/metrics/services/:name",
  (req: Request<{ name: string }>, res: Response) =>
    proxyAnalyticsGet(
      req,
      res,
      `/metrics/services/${encodeURIComponent(req.params.name)}`,
      ["since", "until"],
      "service-detail",
    ),
);

app.post("/api/metrics", async (req: Request, res: Response) => {
  try {
    const resp = await axios.post(`${ANALYTICS_URL}/metrics`, req.body, { timeout: PROXY_TIMEOUT });
    res.status(resp.status).json(resp.data);
  } catch (err) {
    if (err instanceof AxiosError && err.response) {
      logger.warn("Analytics returned error on post", {
        status: err.response.status,
        data: err.response.data,
      });
      res.status(err.response.status).json(err.response.data);
      return;
    }
    const message = err instanceof AxiosError ? err.message : "Unknown error";
    logger.error("Failed to post metric", { error: message });
    res.status(502).json({ error: "Analytics service unavailable", detail: message });
  }
});

app.post("/api/metrics/batch", async (req: Request, res: Response) => {
  try {
    const resp = await axios.post(
      `${ANALYTICS_URL}/metrics/batch`,
      req.body,
      {
        timeout: PROXY_TIMEOUT,
        validateStatus: (status: number) => status >= 200 && status < 300,
      }
    );
    res.status(resp.status).json(resp.data);
  } catch (err) {
    if (err instanceof AxiosError && err.response) {
      logger.warn("Analytics returned error on batch", {
        status: err.response.status,
        data: err.response.data,
      });
      res.status(err.response.status).json(err.response.data);
      return;
    }
    const message = err instanceof AxiosError ? err.message : "Unknown error";
    logger.error("Failed to post metric batch", { error: message });
    res.status(502).json({ error: "Analytics service unavailable", detail: message });
  }
});

app.delete("/api/metrics", (req: Request, res: Response) =>
  proxyAnalyticsDelete(
    req,
    res,
    "/metrics",
    ["service", "before", "status"],
    "delete-metrics",
  ),
);

app.get("/api/check", async (_req: Request, res: Response) => {
  try {
    const resp = await axios.get(`${CHECKER_URL}/check`, { timeout: PROXY_TIMEOUT });
    res.json(resp.data);
  } catch (err) {
    const message =
      err instanceof AxiosError
        ? err.message
        : "Unknown error";
    logger.error("Failed to run health check", { error: message });
    res.status(502).json({ error: "Health checker unavailable", detail: message });
  }
});

app.get("/api/status", async (_req: Request, res: Response) => {
  const services = [
    { name: "analytics-api", url: `${ANALYTICS_URL}/health` },
    { name: "health-checker", url: `${CHECKER_URL}/health` },
  ];

  const statuses = await Promise.all(
    services.map(async (svc) => {
      try {
        const resp = await axios.get(svc.url, { timeout: 3000 });
        return { service: svc.name, status: resp.data.status || "healthy" };
      } catch {
        return { service: svc.name, status: "unhealthy" };
      }
    })
  );

  statuses.push({ service: "api-gateway", status: "healthy" });
  res.json({ services: statuses });
});

app.use((_req: Request, res: Response) => {
  res.status(404).json({ error: "Not found" });
});

// express.json の limit 超過は SyntaxError ではなく entity.too.large になる。
// 既定の Express エラーハンドラに任せると HTML を返してしまうため、
// JSON で 413 を返す専用ハンドラを 500 ハンドラの前段に置く。
// 同様に、構文不正な JSON ボディは body-parser が SyntaxError
// (`type === 'entity.parse.failed'`) を投げる。これを 500 ハンドラまで
// 落とすと "Internal server error" として 5xx を返してしまい、
// SRE の 5xx アラートが誤発火し原因（クライアント側の不正リクエスト）も
// 分からない。413 と並列で 400 ハンドラを用意し、JSON で明示的に返す。
app.use(
  (
    err: Error & { type?: string; status?: number; statusCode?: number },
    req: Request,
    res: Response,
    next: NextFunction,
  ) => {
    const status = err.status ?? err.statusCode;
    if (err && (err.type === "entity.too.large" || status === 413)) {
      logger.warn("Request body too large", { limit: MAX_REQUEST_BODY });
      res.status(413).json({ error: "request body too large" });
      return;
    }
    if (err instanceof SyntaxError && err.type === "entity.parse.failed") {
      logger.warn("Malformed JSON body", { path: req.path });
      res.status(400).json({ error: "invalid JSON body" });
      return;
    }
    next(err);
  },
);

app.use((err: Error, _req: Request, res: Response, _next: NextFunction) => {
  logger.error("Unhandled error", { error: err.message });
  res.status(500).json({ error: "Internal server error" });
});

export { app, ANALYTICS_URL, CHECKER_URL, MAX_REQUEST_BODY };
