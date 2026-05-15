import request from "supertest";
import axios, { AxiosError } from "axios";
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

    it("forwards status, service, since, until, limit, offset to analytics", async () => {
      const spy = jest
        .spyOn(axios, "get")
        .mockResolvedValueOnce({ status: 200, data: { metrics: [], total: 0 } } as never);
      const res = await request(app).get(
        "/api/metrics?service=web&status=unhealthy&since=1700000000&until=1800000000&limit=10&offset=2"
      );
      expect(res.status).toBe(200);
      const calledUrl = spy.mock.calls[0][0] as string;
      expect(calledUrl).toContain("service=web");
      expect(calledUrl).toContain("status=unhealthy");
      expect(calledUrl).toContain("since=1700000000");
      expect(calledUrl).toContain("until=1800000000");
      expect(calledUrl).toContain("limit=10");
      expect(calledUrl).toContain("offset=2");
      spy.mockRestore();
    });
  });

  describe("GET /api/metrics/summary", () => {
    it("returns 502 when analytics is down", async () => {
      const res = await request(app).get("/api/metrics/summary");
      expect(res.status).toBe(502);
      expect(res.body.error).toBe("Analytics service unavailable");
    });

    it("forwards service/status/since/until to analytics", async () => {
      const spy = jest
        .spyOn(axios, "get")
        .mockResolvedValueOnce({ status: 200, data: { web: { total_checks: 1 } } } as never);
      const res = await request(app).get(
        "/api/metrics/summary?service=web&status=healthy&since=100&until=200"
      );
      expect(res.status).toBe(200);
      const calledUrl = spy.mock.calls[0][0] as string;
      expect(calledUrl).toContain("service=web");
      expect(calledUrl).toContain("status=healthy");
      expect(calledUrl).toContain("since=100");
      expect(calledUrl).toContain("until=200");
      spy.mockRestore();
    });

    it("propagates 4xx errors from analytics", async () => {
      const err = new AxiosError("Bad Request");
      err.response = {
        status: 400,
        statusText: "Bad Request",
        headers: {},
        config: {} as never,
        data: { detail: "since must be less than or equal to until" },
      };
      const spy = jest.spyOn(axios, "get").mockRejectedValueOnce(err);
      const res = await request(app).get("/api/metrics/summary?since=200&until=100");
      expect(res.status).toBe(400);
      expect(res.body.detail).toContain("since must be less than or equal to until");
      spy.mockRestore();
    });
  });

  describe("GET /api/metrics/services", () => {
    it("returns 502 when analytics is down", async () => {
      const res = await request(app).get("/api/metrics/services");
      expect(res.status).toBe(502);
      expect(res.body.error).toBe("Analytics service unavailable");
    });

    it("forwards status/since/until/sort/order/limit/offset to analytics", async () => {
      const spy = jest
        .spyOn(axios, "get")
        .mockResolvedValueOnce({ status: 200, data: { services: [], total: 0 } } as never);
      const res = await request(app).get(
        "/api/metrics/services?status=healthy&since=100&until=200&sort=last_seen&order=desc&limit=5&offset=1"
      );
      expect(res.status).toBe(200);
      const calledUrl = spy.mock.calls[0][0] as string;
      expect(calledUrl).toContain("status=healthy");
      expect(calledUrl).toContain("since=100");
      expect(calledUrl).toContain("until=200");
      expect(calledUrl).toContain("sort=last_seen");
      expect(calledUrl).toContain("order=desc");
      expect(calledUrl).toContain("limit=5");
      expect(calledUrl).toContain("offset=1");
      spy.mockRestore();
    });

    it("propagates 4xx errors from analytics", async () => {
      const err = new AxiosError("Unprocessable Entity");
      err.response = {
        status: 422,
        statusText: "Unprocessable Entity",
        headers: {},
        config: {} as never,
        data: { detail: "Input should be one of" },
      };
      const spy = jest.spyOn(axios, "get").mockRejectedValueOnce(err);
      const res = await request(app).get("/api/metrics/services?sort=bogus");
      expect(res.status).toBe(422);
      spy.mockRestore();
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

    it("propagates 4xx validation errors from analytics", async () => {
      const err = new AxiosError("Unprocessable Entity");
      err.response = {
        status: 422,
        statusText: "Unprocessable Entity",
        headers: {},
        config: {} as never,
        data: { detail: [{ msg: "field required", loc: ["body", "status"] }] },
      };
      const spy = jest.spyOn(axios, "post").mockRejectedValueOnce(err);
      const res = await request(app)
        .post("/api/metrics")
        .send({ service: "test", response_time_ms: 10 });
      expect(res.status).toBe(422);
      expect(res.body.detail).toBeDefined();
      spy.mockRestore();
    });
  });

  describe("POST /api/metrics/batch", () => {
    it("forwards body to analytics and propagates 207 partial success", async () => {
      const spy = jest.spyOn(axios, "post").mockResolvedValueOnce({
        status: 207,
        data: {
          total: 2,
          accepted_count: 1,
          rejected_count: 1,
          accepted: [{ index: 0, service: "web", timestamp: 1700000000 }],
          rejected: [{ index: 1, error: "service: must not be blank" }],
        },
      } as never);
      const res = await request(app)
        .post("/api/metrics/batch")
        .send({
          metrics: [
            { service: "web", status: "healthy", response_time_ms: 10 },
            { service: "", status: "healthy", response_time_ms: 5 },
          ],
        });
      expect(res.status).toBe(207);
      expect(res.body.accepted_count).toBe(1);
      expect(res.body.rejected_count).toBe(1);
      const calledUrl = spy.mock.calls[0][0] as string;
      expect(calledUrl).toContain("/metrics/batch");
      spy.mockRestore();
    });

    it("propagates 201 when all entries accepted", async () => {
      const spy = jest.spyOn(axios, "post").mockResolvedValueOnce({
        status: 201,
        data: { total: 1, accepted_count: 1, rejected_count: 0, accepted: [], rejected: [] },
      } as never);
      const res = await request(app)
        .post("/api/metrics/batch")
        .send({ metrics: [{ service: "web", status: "healthy", response_time_ms: 1 }] });
      expect(res.status).toBe(201);
      expect(res.body.accepted_count).toBe(1);
      spy.mockRestore();
    });

    it("propagates 400 from analytics on invalid batch", async () => {
      const err = new AxiosError("Bad Request");
      err.response = {
        status: 400,
        statusText: "Bad Request",
        headers: {},
        config: {} as never,
        data: { detail: "Field 'metrics' must not be empty" },
      };
      const spy = jest.spyOn(axios, "post").mockRejectedValueOnce(err);
      const res = await request(app)
        .post("/api/metrics/batch")
        .send({ metrics: [] });
      expect(res.status).toBe(400);
      spy.mockRestore();
    });

    it("returns 502 when analytics is down", async () => {
      const res = await request(app)
        .post("/api/metrics/batch")
        .send({ metrics: [{ service: "web", status: "healthy", response_time_ms: 1 }] });
      expect(res.status).toBe(502);
      expect(res.body.error).toBe("Analytics service unavailable");
    });
  });

  describe("DELETE /api/metrics", () => {
    it("forwards service query parameter and result to analytics", async () => {
      const spy = jest.spyOn(axios, "delete").mockResolvedValueOnce({
        status: 200,
        data: { message: "Metrics deleted", service: "web", deleted_count: 3 },
      } as never);
      const res = await request(app).delete("/api/metrics?service=web");
      expect(res.status).toBe(200);
      expect(res.body.deleted_count).toBe(3);
      const calledUrl = spy.mock.calls[0][0] as string;
      expect(calledUrl).toContain("service=web");
      spy.mockRestore();
    });

    it("propagates 4xx errors from analytics", async () => {
      const err = new AxiosError("Bad Request");
      err.response = {
        status: 422,
        statusText: "Unprocessable Entity",
        headers: {},
        config: {} as never,
        data: { detail: "service is required" },
      };
      const spy = jest.spyOn(axios, "delete").mockRejectedValueOnce(err);
      const res = await request(app).delete("/api/metrics");
      expect(res.status).toBe(422);
      expect(res.body.detail).toContain("service is required");
      spy.mockRestore();
    });

    it("returns 502 when analytics is down", async () => {
      const res = await request(app).delete("/api/metrics?service=web");
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
