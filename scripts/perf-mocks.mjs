// The model and token issuer for scripts/perf-compare.mjs, in a process of their own so that their
// work isn't charged to the runtime or the client. Uses t10-perf's mocks with its cluster key, the
// key its token minting signs with.
import {createMockAuth} from '../../t10-perf/src/mocks/auth.mjs';
import {createMockLlm} from '../../t10-perf/src/mocks/llm.mjs';
const llm=createMockLlm({ttftMs:Number(process.env.PERF_TTFT_MS??300),tokenDelayMs:Number(process.env.PERF_TOKEN_DELAY_MS??20),tokenCount:Number(process.env.PERF_TOKENS??30),toolRounds:0,thinkingTokens:0,preToolTokens:0});
const auth=createMockAuth({useClusterKey:true});
await llm.listen(Number(process.env.PERF_LLM_PORT),'127.0.0.1');
await auth.listen(Number(process.env.PERF_AUTH_PORT),'127.0.0.1');
console.log('ready');
