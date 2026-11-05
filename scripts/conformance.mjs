// Adapter for the platform's HTTP-only contract suite. The suite's agent-pi `harness.json` runs
// unmodified; this writes only the host file around it. The runtime has no conformance mode.
import { writeFileSync, mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join, resolve } from 'node:path';
import { spawn } from 'node:child_process';
const binary = resolve(process.env.AGENT_TI_BIN ?? 'target/debug/agent-ti');
const dir = mkdtempSync(join(tmpdir(), 'agent-ti-contract-'));
const url = process.env.CONFORMANCE_URL;
const host = {
  port: process.env.PORT,
  instanceId: 'r0',
  logLevel: 'warn',
  storage: { kind: 'sqlite', dir: process.env.CONFORMANCE_DATA },
  auth: { algorithms: 'ES256', jwksUrl: `${url}/jwks`, issuer: url, audience: 't10', tenantClaim: 'tid' },
  agents: process.env.AGENT_CONFIG,
};
writeFileSync(join(dir, 'agent-ti.json'), JSON.stringify(host));
const child = spawn(binary, ['serve', join(dir, 'agent-ti.json')], { stdio: 'inherit' });
for (const signal of ['SIGTERM', 'SIGINT']) process.on(signal, () => child.kill(signal));
child.on('exit', code => { rmSync(dir, { recursive: true, force: true }); process.exit(code ?? 0); });
