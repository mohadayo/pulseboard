import request from "supertest";
import axios, { AxiosError } from "axios";
import { app, ANALYTICS_URL, CHECKER_URL, STATUS_PROBE_TIMEOUT } from "./app";

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

    it("forwards sort and order to analytics", async () => {
      const spy = jest
        .spyOn(axios, "get")
        .mockResolvedValueOnce({ status: 200, data: { metrics: [], total: 0 } } as never);
      const res = await request(app).get(
        "/api/metrics?sort=response_time_ms&order=desc"
      );
      expect(res.status).toBe(200);
      const calledUrl = spy.mock.calls[0][0] as string;
      expect(calledUrl).toContain("sort=response_time_ms");
      expect(calledUrl).toContain("order=desc");
      spy.mockRestore();
    });

    it("forwards q (partial-match search) to analytics", async () => {
      const spy = jest
        .spyOn(axios, "get")
        .mockResolvedValueOnce({ status: 200, data: { metrics: [], total: 0 } } as never);
      const res = await request(app).get("/api/metrics?q=web");
      expect(res.status).toBe(200);
      const calledUrl = spy.mock.calls[0][0] as string;
      expect(calledUrl).toContain("q=web");
      spy.mockRestore();
    });

    it("does not forward empty query params to analytics", async () => {
      const spy = jest
        .spyOn(axios, "get")
        .mockResolvedValueOnce({ status: 200, data: { metrics: [], total: 0 } } as never);
      const res = await request(app).get("/api/metrics?service=&since=");
      expect(res.status).toBe(200);
      const calledUrl = spy.mock.calls[0][0] as string;
      expect(calledUrl).not.toContain("service=");
      expect(calledUrl).not.toContain("since=");
      spy.mockRestore();
    });

    it("propagates 4xx errors from analytics on invalid sort", async () => {
      const err = new AxiosError("Unprocessable Entity");
      err.response = {
        status: 422,
        statusText: "Unprocessable Entity",
        headers: {},
        config: {} as never,
        data: { detail: "Input should be one of" },
      };
      const spy = jest.spyOn(axios, "get").mockRejectedValueOnce(err);
      const res = await request(app).get("/api/metrics?sort=bogus");
      expect(res.status).toBe(422);
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

    it("forwards q (partial-match search) to analytics", async () => {
      const spy = jest
        .spyOn(axios, "get")
        .mockResolvedValueOnce({ status: 200, data: { web: { total_checks: 1 } } } as never);
      const res = await request(app).get("/api/metrics/summary?q=web");
      expect(res.status).toBe(200);
      const calledUrl = spy.mock.calls[0][0] as string;
      expect(calledUrl).toContain("/metrics/summary");
      expect(calledUrl).toContain("q=web");
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

  describe("GET /api/metrics/overview", () => {
    it("returns 502 when analytics is down", async () => {
      const res = await request(app).get("/api/metrics/overview");
      expect(res.status).toBe(502);
      expect(res.body.error).toBe("Analytics service unavailable");
    });

    it("forwards service/status/since/until to analytics", async () => {
      const spy = jest
        .spyOn(axios, "get")
        .mockResolvedValueOnce({
          status: 200,
          data: { total_records: 0, services_count: 0 },
        } as never);
      const res = await request(app).get(
        "/api/metrics/overview?service=web&status=healthy&since=100&until=200"
      );
      expect(res.status).toBe(200);
      const calledUrl = spy.mock.calls[0][0] as string;
      expect(calledUrl).toContain("/metrics/overview");
      expect(calledUrl).toContain("service=web");
      expect(calledUrl).toContain("status=healthy");
      expect(calledUrl).toContain("since=100");
      expect(calledUrl).toContain("until=200");
      spy.mockRestore();
    });

    it("forwards q (partial-match search) to analytics", async () => {
      const spy = jest
        .spyOn(axios, "get")
        .mockResolvedValueOnce({
          status: 200,
          data: { total_records: 0, services_count: 0 },
        } as never);
      const res = await request(app).get("/api/metrics/overview?q=web");
      expect(res.status).toBe(200);
      const calledUrl = spy.mock.calls[0][0] as string;
      expect(calledUrl).toContain("/metrics/overview");
      expect(calledUrl).toContain("q=web");
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
      const res = await request(app).get("/api/metrics/overview?since=200&until=100");
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

    it("forwards service/status/since/until/sort/order/limit/offset to analytics", async () => {
      const spy = jest
        .spyOn(axios, "get")
        .mockResolvedValueOnce({ status: 200, data: { services: [], total: 0 } } as never);
      const res = await request(app).get(
        "/api/metrics/services?service=web&status=healthy&since=100&until=200&sort=last_seen&order=desc&limit=5&offset=1"
      );
      expect(res.status).toBe(200);
      const calledUrl = spy.mock.calls[0][0] as string;
      expect(calledUrl).toContain("service=web");
      expect(calledUrl).toContain("status=healthy");
      expect(calledUrl).toContain("since=100");
      expect(calledUrl).toContain("until=200");
      expect(calledUrl).toContain("sort=last_seen");
      expect(calledUrl).toContain("order=desc");
      expect(calledUrl).toContain("limit=5");
      expect(calledUrl).toContain("offset=1");
      spy.mockRestore();
    });

    it("forwards q (partial-match search) to analytics", async () => {
      const spy = jest
        .spyOn(axios, "get")
        .mockResolvedValueOnce({ status: 200, data: { services: [], total: 0 } } as never);
      const res = await request(app).get("/api/metrics/services?q=web");
      expect(res.status).toBe(200);
      const calledUrl = spy.mock.calls[0][0] as string;
      expect(calledUrl).toContain("/metrics/services");
      expect(calledUrl).toContain("q=web");
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

  describe("GET /api/metrics/services/names", () => {
    it("forwards to /metrics/services/names without query string when no params", async () => {
      const spy = jest
        .spyOn(axios, "get")
        .mockResolvedValueOnce({
          status: 200,
          data: { count: 0, total: 0, limit: 100, offset: 0, order: "asc", names: [] },
        } as never);
      const res = await request(app).get("/api/metrics/services/names");
      expect(res.status).toBe(200);
      expect(res.body.names).toEqual([]);
      const calledUrl = spy.mock.calls[0][0] as string;
      // クエリ無しなので URL に "?" は付かないこと（buildUpstreamParams の挙動回帰）
      expect(calledUrl).toBe(`${ANALYTICS_URL}/metrics/services/names`);
      spy.mockRestore();
    });

    it("forwards since/until/q/order/limit/offset query params", async () => {
      const spy = jest
        .spyOn(axios, "get")
        .mockResolvedValueOnce({
          status: 200,
          data: { count: 1, total: 1, limit: 50, offset: 0, order: "desc", names: ["web"] },
        } as never);
      const res = await request(app).get(
        "/api/metrics/services/names?since=100&until=200&q=we&order=desc&limit=50&offset=0"
      );
      expect(res.status).toBe(200);
      expect(res.body.names).toEqual(["web"]);
      const calledUrl = spy.mock.calls[0][0] as string;
      expect(calledUrl).toContain("since=100");
      expect(calledUrl).toContain("until=200");
      expect(calledUrl).toContain("q=we");
      expect(calledUrl).toContain("order=desc");
      expect(calledUrl).toContain("limit=50");
      expect(calledUrl).toContain("offset=0");
      spy.mockRestore();
    });

    it("drops empty-string query params before forwarding", async () => {
      const spy = jest
        .spyOn(axios, "get")
        .mockResolvedValueOnce({
          status: 200,
          data: { count: 0, total: 0, limit: 100, offset: 0, order: "asc", names: [] },
        } as never);
      const res = await request(app).get(
        "/api/metrics/services/names?since=&q=&order="
      );
      expect(res.status).toBe(200);
      const calledUrl = spy.mock.calls[0][0] as string;
      expect(calledUrl).not.toContain("since=");
      expect(calledUrl).not.toContain("q=");
      expect(calledUrl).not.toContain("order=");
      spy.mockRestore();
    });

    it("does NOT forward unrelated query params (e.g. status / sort / service)", async () => {
      // /metrics/services/names は status / sort / service を受け付けないため、
      // クライアントが付与しても上流には渡さないこと。
      const spy = jest
        .spyOn(axios, "get")
        .mockResolvedValueOnce({
          status: 200,
          data: { count: 0, total: 0, limit: 100, offset: 0, order: "asc", names: [] },
        } as never);
      const res = await request(app).get(
        "/api/metrics/services/names?status=healthy&sort=last_seen&service=web"
      );
      expect(res.status).toBe(200);
      const calledUrl = spy.mock.calls[0][0] as string;
      expect(calledUrl).not.toContain("status=");
      expect(calledUrl).not.toContain("sort=");
      expect(calledUrl).not.toContain("service=");
      spy.mockRestore();
    });

    it("propagates 400 from analytics on invalid query", async () => {
      const err = new AxiosError("Bad Request");
      err.response = {
        status: 400,
        statusText: "Bad Request",
        headers: {},
        config: {} as never,
        data: { detail: "q must not be blank" },
      };
      const spy = jest.spyOn(axios, "get").mockRejectedValueOnce(err);
      const res = await request(app).get("/api/metrics/services/names?q=foo");
      expect(res.status).toBe(400);
      expect(res.body.detail).toContain("q must not be blank");
      spy.mockRestore();
    });

    it("returns 502 when analytics is down", async () => {
      const res = await request(app).get("/api/metrics/services/names");
      expect(res.status).toBe(502);
      expect(res.body.error).toBe("Analytics service unavailable");
    });

    it("is preferred over /api/metrics/services/:name for the exact literal 'names'", async () => {
      // ルート登録順により、`/api/metrics/services/names` リクエストは names エンドポイントに
      // ヒットし、`{name:"names"}` の単一サービス詳細ルートには落ちないこと。
      const spy = jest
        .spyOn(axios, "get")
        .mockResolvedValueOnce({
          status: 200,
          data: { count: 0, total: 0, limit: 100, offset: 0, order: "asc", names: [] },
        } as never);
      await request(app).get("/api/metrics/services/names");
      const calledUrl = spy.mock.calls[0][0] as string;
      // 単一サービスルートに行くと "/metrics/services/names?..." ではなく
      // 同じ path だが「service-detail」label でログされる差しか無いため、
      // ここでは forward 先 URL の末尾が "/names" であって URL エンコードされた
      // パスではないことを確認する。
      expect(calledUrl).toBe(`${ANALYTICS_URL}/metrics/services/names`);
      spy.mockRestore();
    });
  });

  describe("GET /api/metrics/services/:name", () => {
    it("forwards to /metrics/services/{name} with url-encoded path", async () => {
      const spy = jest
        .spyOn(axios, "get")
        .mockResolvedValueOnce({
          status: 200,
          data: { service: "web", total_checks: 3 },
        } as never);
      const res = await request(app).get("/api/metrics/services/web");
      expect(res.status).toBe(200);
      expect(res.body.service).toBe("web");
      const calledUrl = spy.mock.calls[0][0] as string;
      expect(calledUrl).toContain("/metrics/services/web");
      spy.mockRestore();
    });

    it("forwards since/until query params", async () => {
      const spy = jest
        .spyOn(axios, "get")
        .mockResolvedValueOnce({ status: 200, data: { service: "web" } } as never);
      const res = await request(app).get(
        "/api/metrics/services/web?since=100&until=200"
      );
      expect(res.status).toBe(200);
      const calledUrl = spy.mock.calls[0][0] as string;
      expect(calledUrl).toContain("since=100");
      expect(calledUrl).toContain("until=200");
      spy.mockRestore();
    });

    it("propagates 404 from analytics when no data", async () => {
      const err = new AxiosError("Not Found");
      err.response = {
        status: 404,
        statusText: "Not Found",
        headers: {},
        config: {} as never,
        data: { detail: "No metrics found for service 'web'" },
      };
      const spy = jest.spyOn(axios, "get").mockRejectedValueOnce(err);
      const res = await request(app).get("/api/metrics/services/web");
      expect(res.status).toBe(404);
      expect(res.body.detail).toContain("No metrics");
      spy.mockRestore();
    });

    it("returns 502 when analytics is down", async () => {
      const res = await request(app).get("/api/metrics/services/web");
      expect(res.status).toBe(502);
      expect(res.body.error).toBe("Analytics service unavailable");
    });

    it("url-encodes service names with special characters", async () => {
      const spy = jest
        .spyOn(axios, "get")
        .mockResolvedValueOnce({ status: 200, data: { service: "a/b" } } as never);
      // request library does the path-encoding on its end; route param will be 'a/b' decoded
      const res = await request(app).get(
        "/api/metrics/services/" + encodeURIComponent("a b")
      );
      expect(res.status).toBe(200);
      const calledUrl = spy.mock.calls[0][0] as string;
      // a%20b 形式で送られる（encodeURIComponent の挙動）
      expect(calledUrl).toContain("/metrics/services/a%20b");
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

    it("forwards before query parameter to analytics", async () => {
      const spy = jest.spyOn(axios, "delete").mockResolvedValueOnce({
        status: 200,
        data: { message: "Metrics deleted", before: 1700000000, deleted_count: 5 },
      } as never);
      const res = await request(app).delete("/api/metrics?before=1700000000");
      expect(res.status).toBe(200);
      expect(res.body.deleted_count).toBe(5);
      const calledUrl = spy.mock.calls[0][0] as string;
      expect(calledUrl).toContain("before=1700000000");
      spy.mockRestore();
    });

    it("forwards both service and before to analytics", async () => {
      const spy = jest.spyOn(axios, "delete").mockResolvedValueOnce({
        status: 200,
        data: { message: "Metrics deleted", service: "web", before: 1700000000, deleted_count: 2 },
      } as never);
      const res = await request(app).delete("/api/metrics?service=web&before=1700000000");
      expect(res.status).toBe(200);
      const calledUrl = spy.mock.calls[0][0] as string;
      expect(calledUrl).toContain("service=web");
      expect(calledUrl).toContain("before=1700000000");
      spy.mockRestore();
    });

    it("propagates 400 when neither service nor before are provided", async () => {
      const err = new AxiosError("Bad Request");
      err.response = {
        status: 400,
        statusText: "Bad Request",
        headers: {},
        config: {} as never,
        data: { detail: "At least one of 'service' or 'before' must be provided" },
      };
      const spy = jest.spyOn(axios, "delete").mockRejectedValueOnce(err);
      const res = await request(app).delete("/api/metrics");
      expect(res.status).toBe(400);
      expect(res.body.detail).toContain("service");
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

    it("does not forward empty service / before query params", async () => {
      const spy = jest.spyOn(axios, "delete").mockResolvedValueOnce({
        status: 200,
        data: { message: "Metrics deleted", deleted_count: 0 },
      } as never);
      const res = await request(app).delete("/api/metrics?service=&before=");
      expect(res.status).toBe(200);
      const calledUrl = spy.mock.calls[0][0] as string;
      // 空文字は除外され、URL にはクエリが乗らない（パスのみ）。
      expect(calledUrl).toMatch(/\/metrics$/);
      expect(calledUrl).not.toContain("service=");
      expect(calledUrl).not.toContain("before=");
      spy.mockRestore();
    });

    it("ignores unknown query params (only forwards service / before)", async () => {
      const spy = jest.spyOn(axios, "delete").mockResolvedValueOnce({
        status: 200,
        data: { message: "Metrics deleted", deleted_count: 1 },
      } as never);
      const res = await request(app).delete(
        "/api/metrics?service=web&limit=10&sort=service&q=ignored",
      );
      expect(res.status).toBe(200);
      const calledUrl = spy.mock.calls[0][0] as string;
      expect(calledUrl).toContain("service=web");
      expect(calledUrl).not.toContain("limit=");
      expect(calledUrl).not.toContain("sort=");
      expect(calledUrl).not.toContain("q=");
      spy.mockRestore();
    });

    it("propagates non-2xx success status (e.g. 204) from upstream", async () => {
      const spy = jest.spyOn(axios, "delete").mockResolvedValueOnce({
        status: 200,
        data: { message: "Metrics deleted", deleted_count: 2 },
      } as never);
      const res = await request(app).delete("/api/metrics?before=1700000000");
      // axios.delete のレスポンスをそのまま踏襲する想定（200/204 など）。
      expect([200, 204]).toContain(res.status);
      spy.mockRestore();
    });

    it("forwards status filter to analytics", async () => {
      const spy = jest.spyOn(axios, "delete").mockResolvedValueOnce({
        status: 200,
        data: { message: "Metrics deleted", status: "unhealthy", deleted_count: 4 },
      } as never);
      const res = await request(app).delete("/api/metrics?status=unhealthy");
      expect(res.status).toBe(200);
      const calledUrl = spy.mock.calls[0][0] as string;
      expect(calledUrl).toContain("status=unhealthy");
      spy.mockRestore();
    });

    it("forwards status combined with service and before", async () => {
      const spy = jest.spyOn(axios, "delete").mockResolvedValueOnce({
        status: 200,
        data: {
          message: "Metrics deleted",
          service: "web",
          before: 1700000000,
          status: "unhealthy",
          deleted_count: 1,
        },
      } as never);
      const res = await request(app).delete(
        "/api/metrics?service=web&before=1700000000&status=unhealthy",
      );
      expect(res.status).toBe(200);
      const calledUrl = spy.mock.calls[0][0] as string;
      expect(calledUrl).toContain("service=web");
      expect(calledUrl).toContain("before=1700000000");
      expect(calledUrl).toContain("status=unhealthy");
      spy.mockRestore();
    });

    it("does not forward empty status param", async () => {
      const spy = jest.spyOn(axios, "delete").mockResolvedValueOnce({
        status: 200,
        data: { message: "Metrics deleted", deleted_count: 1 },
      } as never);
      const res = await request(app).delete("/api/metrics?service=web&status=");
      expect(res.status).toBe(200);
      const calledUrl = spy.mock.calls[0][0] as string;
      expect(calledUrl).toContain("service=web");
      expect(calledUrl).not.toContain("status=");
      spy.mockRestore();
    });
  });

  describe("GET /api/check", () => {
    it("returns 502 when checker is down", async () => {
      const res = await request(app).get("/api/check");
      expect(res.status).toBe(502);
      expect(res.body.error).toBe("Health checker unavailable");
    });

    it("forwards checker response body on 200", async () => {
      const spy = jest.spyOn(axios, "get").mockResolvedValueOnce({
        status: 200,
        data: {
          results: [
            { service: "web", status: "healthy", latency_ms: 12 },
            { service: "worker", status: "unhealthy", latency_ms: null },
          ],
        },
      } as never);
      const res = await request(app).get("/api/check");
      expect(res.status).toBe(200);
      expect(res.body.results).toHaveLength(2);
      expect(res.body.results[0].service).toBe("web");
      expect(res.body.results[1].status).toBe("unhealthy");
      // 上流は CHECKER_URL の /check を叩く
      const calledUrl = spy.mock.calls[0][0] as string;
      expect(calledUrl).toBe(`${CHECKER_URL}/check`);
      spy.mockRestore();
    });

    it("passes PROXY_TIMEOUT (default 5000ms) to axios", async () => {
      const spy = jest
        .spyOn(axios, "get")
        .mockResolvedValueOnce({ status: 200, data: { results: [] } } as never);
      const res = await request(app).get("/api/check");
      expect(res.status).toBe(200);
      const config = spy.mock.calls[0][1] as { timeout?: number };
      expect(config.timeout).toBe(5000);
      spy.mockRestore();
    });

    it("returns 502 with error detail when checker is unreachable", async () => {
      const err = new AxiosError("connect ECONNREFUSED 127.0.0.1:8002");
      const spy = jest.spyOn(axios, "get").mockRejectedValueOnce(err);
      const res = await request(app).get("/api/check");
      expect(res.status).toBe(502);
      expect(res.body.error).toBe("Health checker unavailable");
      expect(res.body.detail).toContain("ECONNREFUSED");
      spy.mockRestore();
    });

    it("returns 502 even when checker responds with 5xx (no propagation)", async () => {
      // /api/check は respondUpstreamError を経由せず、あらゆる例外を 502 に丸める。
      // このテストは「4xx / 5xx が透過的に伝播しない」既存契約を回帰検知するためのもの。
      const err = new AxiosError("Internal Server Error");
      err.response = {
        status: 500,
        statusText: "Internal Server Error",
        headers: {},
        config: {} as never,
        data: { detail: "checker exploded" },
      };
      const spy = jest.spyOn(axios, "get").mockRejectedValueOnce(err);
      const res = await request(app).get("/api/check");
      expect(res.status).toBe(502);
      expect(res.body.error).toBe("Health checker unavailable");
      spy.mockRestore();
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

    it("returns healthy for both upstreams when they respond 200", async () => {
      const spy = jest
        .spyOn(axios, "get")
        .mockResolvedValueOnce({ status: 200, data: { status: "healthy" } } as never)
        .mockResolvedValueOnce({ status: 200, data: { status: "healthy" } } as never);
      const res = await request(app).get("/api/status");
      expect(res.status).toBe(200);
      expect(res.body.services).toHaveLength(3);
      const analytics = res.body.services.find(
        (s: { service: string }) => s.service === "analytics-api"
      );
      const checker = res.body.services.find(
        (s: { service: string }) => s.service === "health-checker"
      );
      const gateway = res.body.services.find(
        (s: { service: string }) => s.service === "api-gateway"
      );
      expect(analytics.status).toBe("healthy");
      expect(checker.status).toBe("healthy");
      expect(gateway.status).toBe("healthy");
      spy.mockRestore();
    });

    it("forwards non-default status verbatim from upstream (e.g. 'degraded')", async () => {
      const spy = jest
        .spyOn(axios, "get")
        .mockResolvedValueOnce({ status: 200, data: { status: "degraded" } } as never)
        .mockResolvedValueOnce({ status: 200, data: { status: "healthy" } } as never);
      const res = await request(app).get("/api/status");
      expect(res.status).toBe(200);
      const analytics = res.body.services.find(
        (s: { service: string }) => s.service === "analytics-api"
      );
      expect(analytics.status).toBe("degraded");
      spy.mockRestore();
    });

    it("defaults to 'healthy' when upstream 200 body has no status field", async () => {
      // 上流の /health が 200 を返しても JSON に status を含まない古い実装の互換確認。
      const spy = jest
        .spyOn(axios, "get")
        .mockResolvedValueOnce({ status: 200, data: {} } as never)
        .mockResolvedValueOnce({ status: 200, data: {} } as never);
      const res = await request(app).get("/api/status");
      expect(res.status).toBe(200);
      const analytics = res.body.services.find(
        (s: { service: string }) => s.service === "analytics-api"
      );
      const checker = res.body.services.find(
        (s: { service: string }) => s.service === "health-checker"
      );
      expect(analytics.status).toBe("healthy");
      expect(checker.status).toBe("healthy");
      spy.mockRestore();
    });

    it("returns 'unhealthy' for the failing upstream while keeping the healthy one", async () => {
      // analytics-api は 200 healthy、health-checker は接続不能。
      // 片方の失敗が全体の 5xx を招かないことを回帰確認する。
      const spy = jest
        .spyOn(axios, "get")
        .mockResolvedValueOnce({ status: 200, data: { status: "healthy" } } as never)
        .mockRejectedValueOnce(new AxiosError("ECONNREFUSED"));
      const res = await request(app).get("/api/status");
      expect(res.status).toBe(200);
      const analytics = res.body.services.find(
        (s: { service: string }) => s.service === "analytics-api"
      );
      const checker = res.body.services.find(
        (s: { service: string }) => s.service === "health-checker"
      );
      const gateway = res.body.services.find(
        (s: { service: string }) => s.service === "api-gateway"
      );
      expect(analytics.status).toBe("healthy");
      expect(checker.status).toBe("unhealthy");
      // api-gateway 自身は常に healthy を自称する
      expect(gateway.status).toBe("healthy");
      spy.mockRestore();
    });

    it("uses STATUS_PROBE_TIMEOUT (not PROXY_TIMEOUT) for upstream /health probes", async () => {
      // PR #112 で PROXY_TIMEOUT (5000ms) から STATUS_PROBE_TIMEOUT (3000ms 既定) に切り替わった契約を
      // 明示的に回帰検知する。将来のリファクタで誤って PROXY_TIMEOUT に戻したときにここで落ちる。
      const spy = jest
        .spyOn(axios, "get")
        .mockResolvedValueOnce({ status: 200, data: { status: "healthy" } } as never)
        .mockResolvedValueOnce({ status: 200, data: { status: "healthy" } } as never);
      const res = await request(app).get("/api/status");
      expect(res.status).toBe(200);
      const analyticsConfig = spy.mock.calls[0][1] as { timeout?: number };
      const checkerConfig = spy.mock.calls[1][1] as { timeout?: number };
      expect(analyticsConfig.timeout).toBe(STATUS_PROBE_TIMEOUT);
      expect(checkerConfig.timeout).toBe(STATUS_PROBE_TIMEOUT);
      // 既定値が 3000ms のままであることも同時に保証する
      expect(STATUS_PROBE_TIMEOUT).toBe(3000);
      spy.mockRestore();
    });

    it("preserves service order: analytics-api, health-checker, api-gateway", async () => {
      // 呼び出し順（Promise.all の入力配列順）と最終レスポンス配列順が一致することを確認。
      // ダッシュボード側で「先頭 = 一次データストア」の暗黙前提を持つケースに備え、
      // ここが崩れないよう固定する。
      const spy = jest
        .spyOn(axios, "get")
        .mockResolvedValueOnce({ status: 200, data: { status: "healthy" } } as never)
        .mockResolvedValueOnce({ status: 200, data: { status: "healthy" } } as never);
      const res = await request(app).get("/api/status");
      expect(res.status).toBe(200);
      expect(res.body.services.map((s: { service: string }) => s.service)).toEqual([
        "analytics-api",
        "health-checker",
        "api-gateway",
      ]);
      spy.mockRestore();
    });

    it("probes ANALYTICS_URL/health and CHECKER_URL/health", async () => {
      const spy = jest
        .spyOn(axios, "get")
        .mockResolvedValueOnce({ status: 200, data: { status: "healthy" } } as never)
        .mockResolvedValueOnce({ status: 200, data: { status: "healthy" } } as never);
      await request(app).get("/api/status");
      expect(spy.mock.calls[0][0]).toBe(`${ANALYTICS_URL}/health`);
      expect(spy.mock.calls[1][0]).toBe(`${CHECKER_URL}/health`);
      spy.mockRestore();
    });
  });

  describe("GET /api/metrics/count", () => {
    it("returns 502 when analytics is down", async () => {
      const res = await request(app).get("/api/metrics/count");
      expect(res.status).toBe(502);
      expect(res.body.error).toBe("Analytics service unavailable");
    });

    it("forwards filter params and returns count payload", async () => {
      const spy = jest.spyOn(axios, "get").mockResolvedValueOnce({
        status: 200,
        data: {
          total: 3,
          by_status: { healthy: 2, unhealthy: 1, degraded: 0, unknown: 0 },
        },
      } as never);
      const res = await request(app).get(
        "/api/metrics/count?service=web&status=healthy&since=1700000000&until=1800000000&q=web",
      );
      expect(res.status).toBe(200);
      expect(res.body.total).toBe(3);
      expect(res.body.by_status.healthy).toBe(2);
      const calledUrl = spy.mock.calls[0][0] as string;
      expect(calledUrl).toContain("/metrics/count");
      expect(calledUrl).toContain("service=web");
      expect(calledUrl).toContain("status=healthy");
      expect(calledUrl).toContain("since=1700000000");
      expect(calledUrl).toContain("until=1800000000");
      expect(calledUrl).toContain("q=web");
      spy.mockRestore();
    });

    it("does not forward unrelated params (limit/offset/sort)", async () => {
      const spy = jest
        .spyOn(axios, "get")
        .mockResolvedValueOnce({ status: 200, data: { total: 0, by_status: {} } } as never);
      const res = await request(app).get(
        "/api/metrics/count?limit=10&offset=5&sort=service",
      );
      expect(res.status).toBe(200);
      const calledUrl = spy.mock.calls[0][0] as string;
      expect(calledUrl).not.toContain("limit=");
      expect(calledUrl).not.toContain("offset=");
      expect(calledUrl).not.toContain("sort=");
      spy.mockRestore();
    });

    it("propagates 4xx from analytics on invalid time range", async () => {
      const err = new AxiosError("Bad Request");
      err.response = {
        status: 400,
        data: { detail: "since must be less than or equal to until" },
      } as never;
      const spy = jest.spyOn(axios, "get").mockRejectedValueOnce(err);
      const res = await request(app).get("/api/metrics/count?since=200&until=100");
      expect(res.status).toBe(400);
      expect(res.body.detail).toContain("since");
      spy.mockRestore();
    });
  });

  describe("GET /api/metrics/timeseries", () => {
    it("returns 502 when analytics is down", async () => {
      const res = await request(app).get("/api/metrics/timeseries");
      expect(res.status).toBe(502);
      expect(res.body.error).toBe("Analytics service unavailable");
    });

    it("forwards filter params including bucket_seconds", async () => {
      const spy = jest.spyOn(axios, "get").mockResolvedValueOnce({
        status: 200,
        data: {
          bucket_seconds: 60,
          count: 1,
          buckets: [
            {
              bucket_start: 1700000000.0,
              total: 2,
              by_status: { healthy: 2, unhealthy: 0, degraded: 0, unknown: 0 },
              avg_response_ms: 12.34,
            },
          ],
        },
      } as never);
      const res = await request(app).get(
        "/api/metrics/timeseries?service=web&status=healthy&since=1700000000&until=1800000000&q=web&bucket_seconds=60",
      );
      expect(res.status).toBe(200);
      expect(res.body.bucket_seconds).toBe(60);
      expect(res.body.count).toBe(1);
      expect(res.body.buckets[0].total).toBe(2);
      const calledUrl = spy.mock.calls[0][0] as string;
      expect(calledUrl).toContain("/metrics/timeseries");
      expect(calledUrl).toContain("service=web");
      expect(calledUrl).toContain("status=healthy");
      expect(calledUrl).toContain("since=1700000000");
      expect(calledUrl).toContain("until=1800000000");
      expect(calledUrl).toContain("q=web");
      expect(calledUrl).toContain("bucket_seconds=60");
      spy.mockRestore();
    });

    it("does not forward unrelated params (limit/offset/sort)", async () => {
      const spy = jest.spyOn(axios, "get").mockResolvedValueOnce({
        status: 200,
        data: { bucket_seconds: 60, count: 0, buckets: [] },
      } as never);
      const res = await request(app).get(
        "/api/metrics/timeseries?limit=10&offset=5&sort=timestamp&order=desc",
      );
      expect(res.status).toBe(200);
      const calledUrl = spy.mock.calls[0][0] as string;
      expect(calledUrl).not.toContain("limit=");
      expect(calledUrl).not.toContain("offset=");
      expect(calledUrl).not.toContain("sort=");
      expect(calledUrl).not.toContain("order=");
      spy.mockRestore();
    });

    it("strips empty bucket_seconds (lets analytics default kick in)", async () => {
      const spy = jest.spyOn(axios, "get").mockResolvedValueOnce({
        status: 200,
        data: { bucket_seconds: 60, count: 0, buckets: [] },
      } as never);
      await request(app).get("/api/metrics/timeseries?bucket_seconds=");
      const calledUrl = spy.mock.calls[0][0] as string;
      expect(calledUrl).not.toContain("bucket_seconds=");
      spy.mockRestore();
    });

    it("propagates 422 from analytics on invalid bucket_seconds", async () => {
      const err = new AxiosError("Unprocessable Entity");
      err.response = {
        status: 422,
        data: { detail: [{ msg: "ensure this value is greater than or equal to 1" }] },
      } as never;
      const spy = jest.spyOn(axios, "get").mockRejectedValueOnce(err);
      const res = await request(app).get("/api/metrics/timeseries?bucket_seconds=0");
      expect(res.status).toBe(422);
      spy.mockRestore();
    });

    it("propagates 400 from analytics when since > until", async () => {
      const err = new AxiosError("Bad Request");
      err.response = {
        status: 400,
        data: { detail: "since must be less than or equal to until" },
      } as never;
      const spy = jest.spyOn(axios, "get").mockRejectedValueOnce(err);
      const res = await request(app).get("/api/metrics/timeseries?since=200&until=100");
      expect(res.status).toBe(400);
      expect(res.body.detail).toContain("since");
      spy.mockRestore();
    });
  });

  describe("GET /api/metrics/uptime", () => {
    it("returns 502 when analytics is down", async () => {
      const res = await request(app).get("/api/metrics/uptime");
      expect(res.status).toBe(502);
      expect(res.body.error).toBe("Analytics service unavailable");
    });

    it("forwards to /metrics/uptime without query string when no params", async () => {
      const spy = jest.spyOn(axios, "get").mockResolvedValueOnce({
        status: 200,
        data: {
          count: 0,
          total: 0,
          limit: 100,
          offset: 0,
          order: "asc",
          services: [],
        },
      } as never);
      const res = await request(app).get("/api/metrics/uptime");
      expect(res.status).toBe(200);
      expect(res.body.services).toEqual([]);
      const calledUrl = spy.mock.calls[0][0] as string;
      // クエリ無しなので URL に "?" は付かないこと（buildUpstreamParams の挙動回帰）
      expect(calledUrl).toBe(`${ANALYTICS_URL}/metrics/uptime`);
      spy.mockRestore();
    });

    it("forwards q/since/until/ongoing_only/limit/offset/order to analytics", async () => {
      const spy = jest.spyOn(axios, "get").mockResolvedValueOnce({
        status: 200,
        data: {
          count: 1,
          total: 1,
          limit: 50,
          offset: 0,
          order: "desc",
          services: [
            {
              service: "web",
              total_checks: 10,
              healthy_checks: 9,
              uptime_pct: 90.0,
              incident_count: 1,
              ongoing_incident: true,
              total_incident_seconds: 30.0,
              longest_incident_seconds: 30.0,
              mean_incident_seconds: 30.0,
            },
          ],
        },
      } as never);
      const res = await request(app).get(
        "/api/metrics/uptime?q=we&since=100&until=200&ongoing_only=true&limit=50&offset=0&order=desc",
      );
      expect(res.status).toBe(200);
      expect(res.body.services[0].service).toBe("web");
      expect(res.body.services[0].ongoing_incident).toBe(true);
      const calledUrl = spy.mock.calls[0][0] as string;
      expect(calledUrl).toContain("/metrics/uptime");
      expect(calledUrl).toContain("q=we");
      expect(calledUrl).toContain("since=100");
      expect(calledUrl).toContain("until=200");
      expect(calledUrl).toContain("ongoing_only=true");
      expect(calledUrl).toContain("limit=50");
      expect(calledUrl).toContain("offset=0");
      expect(calledUrl).toContain("order=desc");
      spy.mockRestore();
    });

    it("drops empty-string query params before forwarding", async () => {
      const spy = jest.spyOn(axios, "get").mockResolvedValueOnce({
        status: 200,
        data: {
          count: 0, total: 0, limit: 100, offset: 0, order: "asc", services: [],
        },
      } as never);
      const res = await request(app).get(
        "/api/metrics/uptime?q=&since=&ongoing_only=",
      );
      expect(res.status).toBe(200);
      const calledUrl = spy.mock.calls[0][0] as string;
      expect(calledUrl).not.toContain("q=");
      expect(calledUrl).not.toContain("since=");
      expect(calledUrl).not.toContain("ongoing_only=");
      spy.mockRestore();
    });

    it("does NOT forward unrelated params (service / status / sort / bucket_seconds)", async () => {
      // `/metrics/uptime` は service / status / sort / bucket_seconds を受け付けないため、
      // クライアントが付与しても上流には渡さないこと。
      // 特に `service` は「特定サービスに絞る」用途では `/metrics/services/{name}/uptime`
      // を使うのが正で、`/metrics/uptime` の allowlist には含めない。
      const spy = jest.spyOn(axios, "get").mockResolvedValueOnce({
        status: 200,
        data: {
          count: 0, total: 0, limit: 100, offset: 0, order: "asc", services: [],
        },
      } as never);
      const res = await request(app).get(
        "/api/metrics/uptime?service=web&status=healthy&sort=uptime_pct&bucket_seconds=60",
      );
      expect(res.status).toBe(200);
      const calledUrl = spy.mock.calls[0][0] as string;
      expect(calledUrl).not.toContain("service=");
      expect(calledUrl).not.toContain("status=");
      expect(calledUrl).not.toContain("sort=");
      expect(calledUrl).not.toContain("bucket_seconds=");
      spy.mockRestore();
    });

    it("propagates 400 from analytics on invalid q", async () => {
      const err = new AxiosError("Bad Request");
      err.response = {
        status: 400,
        data: { detail: "q must not be blank" },
      } as never;
      const spy = jest.spyOn(axios, "get").mockRejectedValueOnce(err);
      const res = await request(app).get("/api/metrics/uptime?q=%20");
      expect(res.status).toBe(400);
      expect(res.body.detail).toContain("q must not be blank");
      spy.mockRestore();
    });

    it("propagates 400 from analytics when since > until", async () => {
      const err = new AxiosError("Bad Request");
      err.response = {
        status: 400,
        data: { detail: "since must be less than or equal to until" },
      } as never;
      const spy = jest.spyOn(axios, "get").mockRejectedValueOnce(err);
      const res = await request(app).get("/api/metrics/uptime?since=200&until=100");
      expect(res.status).toBe(400);
      expect(res.body.detail).toContain("since");
      spy.mockRestore();
    });
  });

  describe("404 handler", () => {
    it("returns 404 for unknown routes", async () => {
      const res = await request(app).get("/unknown");
      expect(res.status).toBe(404);
      expect(res.body.error).toBe("Not found");
    });
  });

  describe("JSON body size limit", () => {
    it("returns 413 when POST body exceeds the configured limit", async () => {
      // 既定 256kb を確実に超える 512KB の payload を組み立てる。
      const huge = "a".repeat(512 * 1024);
      const res = await request(app)
        .post("/api/metrics")
        .set("Content-Type", "application/json")
        .send({ service: "web", value: huge });
      expect(res.status).toBe(413);
      expect(res.body.error).toBe("request body too large");
    });

    it("accepts a small JSON POST (proxies to analytics)", async () => {
      const spy = jest
        .spyOn(axios, "post")
        .mockResolvedValueOnce({ status: 201, data: { recorded: true } } as never);
      const res = await request(app)
        .post("/api/metrics")
        .send({ service: "web", status: "healthy", response_time_ms: 1 });
      expect(res.status).toBe(201);
      expect(spy).toHaveBeenCalled();
      spy.mockRestore();
    });
  });

  describe("Malformed JSON body", () => {
    it("returns 400 with JSON error on invalid JSON to POST /api/metrics", async () => {
      const spy = jest.spyOn(axios, "post");
      const res = await request(app)
        .post("/api/metrics")
        .set("Content-Type", "application/json")
        .send("{not-valid-json");
      expect(res.status).toBe(400);
      expect(res.headers["content-type"]).toMatch(/application\/json/);
      expect(res.body.error).toBe("invalid JSON body");
      // 上流 analytics-api には転送されない
      expect(spy).not.toHaveBeenCalled();
      spy.mockRestore();
    });

    it("returns 400 with JSON error on invalid JSON to POST /api/metrics/batch", async () => {
      const spy = jest.spyOn(axios, "post");
      const res = await request(app)
        .post("/api/metrics/batch")
        .set("Content-Type", "application/json")
        .send('{"metrics":[');
      expect(res.status).toBe(400);
      expect(res.headers["content-type"]).toMatch(/application\/json/);
      expect(res.body.error).toBe("invalid JSON body");
      expect(spy).not.toHaveBeenCalled();
      spy.mockRestore();
    });

    it("returns 400 with JSON error on completely empty body with JSON content-type", async () => {
      const res = await request(app)
        .post("/api/metrics")
        .set("Content-Type", "application/json")
        .send("not json at all");
      expect(res.status).toBe(400);
      expect(res.body.error).toBe("invalid JSON body");
    });
  });
});
