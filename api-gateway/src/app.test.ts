import request from "supertest";
import { app } from "./app";

describe("API Gateway", () => {
  describe("GET /health", () => {
    it("returns healthy status", async () => {
      const res = await request(app).get("/health");
      expect(res.status).toBe(200);
      expect(res.body.status).toBe("healthy");
      expect(res.body.service).toBe("api-gateway");
    });
  });

  describe("GET /api/metrics", () => {
    it("returns 502 when analytics is down", async () => {
      const res = await request(app).get("/api/metrics");
      expect(res.status).toBe(502);
      expect(res.body.error).toBe("Analytics service unavailable");
    });
  });

  describe("GET /api/metrics/summary", () => {
    it("returns 502 when analytics is down", async () => {
      const res = await request(app).get("/api/metrics/summary");
      expect(res.status).toBe(502);
      expect(res.body.error).toBe("Analytics service unavailable");
    });
  });

  describe("POST /api/metrics", () => {
    it("returns 502 when analytics is down", async () => {
      const res = await request(app)
        .post("/api/metrics")
        .send({ service: "test", status: "healthy", response_time_ms: 10 });
      expect(res.status).toBe(502);
      expect(res.body.error).toBe("Analytics service unavailable");
    });
  });

  describe("GET /api/check", () => {
    it("returns 502 when checker is down", async () => {
      const res = await request(app).get("/api/check");
      expect(res.status).toBe(502);
      expect(res.body.error).toBe("Health checker unavailable");
    });
  });

  describe("GET /api/status", () => {
    it("returns status for all services", async () => {
      const res = await request(app).get("/api/status");
      expect(res.status).toBe(200);
      expect(res.body.services).toHaveLength(3);
      const gateway = res.body.services.find(
        (s: { service: string }) => s.service === "api-gateway"
      );
      expect(gateway.status).toBe("healthy");
    });
  });

  describe("404 handler", () => {
    it("returns 404 for unknown routes", async () => {
      const res = await request(app).get("/unknown");
      expect(res.status).toBe(404);
      expect(res.body.error).toBe("Not found");
    });
  });
});
