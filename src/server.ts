import { config } from './config.js';
import { buildServer } from './routes.js';

const server = await buildServer(config);

try {
  await server.listen({ host: config.host, port: config.port });
} catch (error) {
  server.log.error(error);
  process.exit(1);
}
