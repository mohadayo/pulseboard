import { app, logger } from "./app";

const PORT = parseInt(process.env.GATEWAY_PORT || "8000", 10);
// SIGTERM 受信後、進行中リクエストの完了を待つ最大時間 (ms)。
// Kubernetes の既定 grace period (30s) より短めに設定し、
// SIGKILL が飛ぶ前に確実にプロセスを終了させる。
const SHUTDOWN_TIMEOUT_MS = parseInt(
  process.env.SHUTDOWN_TIMEOUT_MS || "10000",
  10,
);

const server = app.listen(PORT, () => {
  logger.info("API Gateway listening", { port: PORT });
});

// SIGTERM / SIGINT を受けたら新規接続の受付を止め、進行中リクエストを
// 完了させたうえで終了する。タイムアウト超過時は強制終了 (exit 1)。
// Docker/Kubernetes 環境でのローリングアップデート中に、リクエストの
// 途中切断や 5xx を発生させないための実装。
function shutdown(signal: string): void {
  logger.info("Received shutdown signal", { signal });
  const forceExit = setTimeout(() => {
    logger.error("Graceful shutdown timed out, forcing exit", {
      timeout_ms: SHUTDOWN_TIMEOUT_MS,
    });
    process.exit(1);
  }, SHUTDOWN_TIMEOUT_MS);
  // 待機用タイマー自体でイベントループを塞がないよう unref する
  // — 進行中リクエストが早期に完了した際、タイマーが原因でプロセスが
  // 生き残り続けるのを防ぐ。
  forceExit.unref();

  server.close((err) => {
    clearTimeout(forceExit);
    if (err) {
      logger.error("Error during server shutdown", { error: err.message });
      process.exit(1);
    }
    logger.info("Server closed cleanly");
    process.exit(0);
  });
}

process.on("SIGTERM", () => shutdown("SIGTERM"));
process.on("SIGINT", () => shutdown("SIGINT"));
