import { CynapsaClient } from "../src/index.js";

const libraryPath = process.argv[2];
if (!libraryPath) {
  throw new Error("usage: npm run example -- /absolute/path/to/libcynapsacore");
}

const client = new CynapsaClient({ libraryPath });
try {
  client.start();
  const admission = client.initialize();
  const completion = await client.nextCompletion(5_000);
  const status = client.status();

  console.log(`accepted=${admission.accepted} command_id=${admission.commandId}`);
  console.log(`completion_ok=${completion?.ok ?? false}`);
  console.log(`lifecycle=${status.lifecycle}`);
} finally {
  await client.close();
}
