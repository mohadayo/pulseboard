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

const app = express();
app.use(express.json());

app.use((req: Request, _res: Response, next: NextFunction) => {
  logger.info(`${req.method} ${req.path}`, { ip: req.ip });
  next();
});

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
    const params = new URLSearchParams();
    for (const key of allowedParams) {
      const value = req.query[key];
      if (value !== undefined && value !== "") {
        params.set(key, String(value));
      }
    }
    const qs = params.toString();
    const url = qs
      ? `${ANALYTICS_URL}${upstreamPath}?${qs}`
      : `${ANALYTICS_URL}${upstreamPath}`;
    const resp = await axios.get(url, { timeout: PROXY_TIMEOUT });
    res.json(resp.data);
  } catch (err) {
    if (err instanceof AxiosError && err.response) {
      logger.warn("Analytics returned error", {
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
}

app.get("/health", (_req: Request, res: Response) => {
  res.json({ status: "healthy", service: "api-gateway" });
});

app.get("/api/metrics", (req: Request, res: Response) =>
  proxyAnalyticsGet(
    req,
    res,
    "/metrics",
    ["service", "status", "since", "until", "limit", "offset", "sort", "order"],
    "metrics",
  ),
);

app.get("/api/metrics/summary", (req: Request, res: Response) =>
  proxyAnalyticsGet(
    req,
    res,
    "/metrics/summary",
    ["service", "status", "since", "until"],
    "summary",
  ),
);

app.get("/api/metrics/overview", (req: Request, res: Response) =>
  proxyAnalyticsGet(
    req,
    res,
    "/metrics/overview",
    ["service", "status", "since", "until"],
    "overview",
  ),
);

app.get("/api/metrics/services", (req: Request, res: Response) =>
  proxyAnalyticsGet(
    req,
    res,
    "/metrics/services",
    ["service", "status", "since", "until", "sort", "order", "limit", "offset"],
    "services",
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

app.delete("/api/metrics", async (req: Request, res: Response) => {
  try {
    const params = new URLSearchParams();
    if (req.query.service !== undefined) params.set("service", String(req.query.service));
    if (req.query.before !== undefined) params.set("before", String(req.query.before));
    const qs = params.toString();
    const url = qs
      ? `${ANALYTICS_URL}/metrics?${qs}`
      : `${ANALYTICS_URL}/metrics`;
    const resp = await axios.delete(url, { timeout: PROXY_TIMEOUT });
    res.status(resp.status).json(resp.data);
  } catch (err) {
    if (err instanceof AxiosError && err.response) {
      logger.warn("Analytics returned error on delete", {
        status: err.response.status,
        data: err.response.data,
      });
      res.status(err.response.status).json(err.response.data);
      return;
    }
    const message = err instanceof AxiosError ? err.message : "Unknown error";
    logger.error("Failed to delete metrics", { error: message });
    res.status(502).json({ error: "Analytics service unavailable", detail: message });
  }
});

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

app.use((err: Error, _req: Request, res: Response, _next: NextFunction) => {
  logger.error("Unhandled error", { error: err.message });
  res.status(500).json({ error: "Internal server error" });
});

export { app, ANALYTICS_URL, CHECKER_URL };
