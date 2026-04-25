import { app } from "./app";

const PORT = process.env.GATEWAY_PORT || 8000;

app.listen(PORT, () => {
  console.log(`[INFO] API Gateway listening on port ${PORT}`);
});
