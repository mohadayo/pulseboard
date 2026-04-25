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

const app = express();
app.use(express.json());

app.use((req: Request, _res: Response, next: NextFunction) => {
  logger.info(`${req.method} ${req.path}`, { ip: req.ip });
  next();
});

app.get("/health", (_req: Request, res: Response) => {
  res.json({ status: "healthy", service: "api-gateway" });
});

app.get("/api/metrics", async (_req: Request, res: Response) => {
  try {
    const resp = await axios.get(`${ANALYTICS_URL}/metrics`);
    res.json(resp.data);
  } catch (err) {
    const message =
      err instanceof AxiosError
        ? err.message
        : "Unknown error";
    logger.error("Failed to fetch metrics", { error: message });
    res.status(502).json({ error: "Analytics service unavailable", detail: message });
  }
});

app.get("/api/metrics/summary", async (_req: Request, res: Response) => {
  try {
    const resp = await axios.get(`${ANALYTICS_URL}/metrics/summary`);
    res.json(resp.data);
  } catch (err) {
    const message =
      err instanceof AxiosError
        ? err.message
        : "Unknown error";
    logger.error("Failed to fetch summary", { error: message });
    res.status(502).json({ error: "Analytics service unavailable", detail: message });
  }
});

app.post("/api/metrics", async (req: Request, res: Response) => {
  try {
    const resp = await axios.post(`${ANALYTICS_URL}/metrics`, req.body);
    res.status(resp.status).json(resp.data);
  } catch (err) {
    const message =
      err instanceof AxiosError
        ? err.message
        : "Unknown error";
    logger.error("Failed to post metric", { error: message });
    res.status(502).json({ error: "Analytics service unavailable", detail: message });
  }
});

app.get("/api/check", async (_req: Request, res: Response) => {
  try {
    const resp = await axios.get(`${CHECKER_URL}/check`);
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
