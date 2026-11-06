// t10-perf's text scenario on one runtime at rising concurrency, with memory sampled throughout:
// agent-pi's assistant `agent.json`, unchanged, on either. Run for each and compare.
//   node scripts/perf-compare.mjs <agent-ti|agent-pi> [steps, default 10,25,50,100,200] [turns, default 3]
// Build agent-ti with `cargo build --release` first. Results go to stdout as a table and JSON.
import {spawn,execFileSync} from 'node:child_process';
import {existsSync,mkdtempSync,rmSync,writeFileSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {join,resolve} from 'node:path';
import {runBenchmark} from '../../t10-perf/src/runner/scenarios.mjs';
import {createMockAuth} from '../../t10-perf/src/mocks/auth.mjs';
const runtime=process.argv[2];
if(runtime!=='agent-ti'&&runtime!=='agent-pi')throw new Error('name a runtime: agent-ti or agent-pi');
const steps=(process.argv[3]??'10,25,50,100,200').split(',').map(Number);
const turns=Number(process.argv[4]??3);
const root=resolve(new URL('..',import.meta.url).pathname);
const pi=resolve(root,'../agent-pi');
const base=Number(process.env.TEST_BASE_PORT??29170);
const appPort=base,llmPort=base+1,authPort=base+2;
const url=port=>`http://127.0.0.1:${port}`;
const issuer='http://localhost:18000';
const dir=mkdtempSync(join(tmpdir(),'agent-ti-perf-'));
// High enough that neither runtime's limits decide the result. PERF_LIMITS narrows agent-ti's, as
// JSON, or `defaults` runs it on the host file's own.
const capacity=Number(process.env.PERF_MAX_RUNS??1000);
const env={PATH:process.env.PATH,HOME:process.env.HOME,PORT:String(appPort),LOG_LEVEL:'error',AGENT_DATA_DIR:dir,AGENT_INSTANCE_ID:'r0',
 AUTH_KIND:'jwt',JWT_ALGORITHMS:'ES256',JWT_JWKS_URL:`${url(authPort)}/.well-known/jwks.json`,JWT_ISSUER:issuer,JWT_AUDIENCE:'t10',JWT_TENANT_CLAIM:'tid',
 LLM_BASE_URL:`${url(llmPort)}/v1`,LLM_API_KEY:'perf',LLM_API:'chat-completions',FAST_MODEL_ID:'mock',SMART_MODEL_ID:'mock'};
const agentJson=join(pi,'apps/assistant/config/agent.json');
let command,args,cwd;
if(runtime==='agent-ti'){
 const binary=join(root,'target/release/agent-ti');
 if(!existsSync(binary))throw new Error('run cargo build --release first');
 const host=join(dir,'agent-ti.json');
 writeFileSync(host,JSON.stringify({port:String(appPort),instanceId:'r0',logLevel:'error',storage:{kind:'sqlite',dir},
  auth:{algorithms:'ES256',jwksUrl:env.JWT_JWKS_URL,issuer,audience:'t10',tenantClaim:'tid'},
  limits:process.env.PERF_LIMITS==='defaults'?{}:{maxConcurrentRuns:capacity,maxConcurrentRunsPerTenant:capacity,maxQueuedPerTenant:10000,maxStreams:20000,...JSON.parse(process.env.PERF_LIMITS??'{}')},agents:agentJson}));
 [command,args,cwd]=[binary,['serve',host],root];
}else{
 env.AGENT_CONFIG=agentJson;env.AGENT_ENV_FILE='/nonexistent';
 [command,args,cwd]=[process.execPath,['--disable-warning=ExperimentalWarning',join(pi,'packages/agent-pi/src/main.ts')],pi];
}
const children=[];
const start=(c,a,e,o={})=>{const child=spawn(c,a,{env:e,stdio:['ignore','ignore','inherit'],...o});children.push(child);return child;};
const ready=async target=>{for(let i=0;i<400;i++){try{if((await fetch(target)).ok)return;}catch{}await new Promise(r=>setTimeout(r,50));}throw new Error('not ready: '+target);};
const rssOf=pid=>Number(execFileSync('ps',['-o','rss=','-p',String(pid)]).toString().trim())/1024;
const cpuOf=pid=>Number(execFileSync('ps',['-o','%cpu=','-p',String(pid)]).toString().trim());
const scrape=async()=>{const out={};for(const line of (await(await fetch(url(appPort)+'/metrics')).text()).split('\n')){const m=/^(\w+_(?:sum|count))\{agent="assistant"\} ([\d.e+-]+)$/.exec(line);if(m)out[m[1]]=Number(m[2]);}return out;};
const rows=[];
try{
 start(process.execPath,[join(root,'scripts/perf-mocks.mjs')],{...process.env,PERF_LLM_PORT:String(llmPort),PERF_AUTH_PORT:String(authPort)});
 await ready(url(authPort)+'/healthz');
 const app=start(command,args,env,{cwd});
 await ready(url(appPort)+'/readyz');
 const idle=rssOf(app.pid);
 const auth=createMockAuth({useClusterKey:true});
 const token=worker=>auth.mintToken(`usr_perf_${worker}`,'ten_acme',{audience:['t10']});
 const noisy=console.log;
 for(const concurrency of steps){
  let peak=rssOf(app.pid),cpu=0;
  const sampler=setInterval(()=>{try{peak=Math.max(peak,rssOf(app.pid));cpu=Math.max(cpu,cpuOf(app.pid));}catch{}},250);
  const before=runtime==='agent-ti'?await scrape():{};
  console.log=()=>{};
  const summary=await runBenchmark({targetUrl:url(appPort),token,agentName:'assistant',concurrency,turnsPerSession:turns,promptText:'say performance benchmark verification',customHeaders:{},durationSeconds:null});
  console.log=noisy;
  clearInterval(sampler);
  const l=summary.latencies;
  // From inside agent-ti: a model call's own wait for its first token, and a run from claim to end,
  // so what the client sees beyond these is queueing and the client's own load.
  const after=runtime==='agent-ti'?await scrape():{};
  const mean=name=>{const n=(after[name+'_count']??0)-(before[name+'_count']??0);return n>0?Math.round(1000*((after[name+'_sum']??0)-(before[name+'_sum']??0))/n):undefined;};
  const row={runtime,concurrency,turns:summary.totalTurns,failed:summary.failedTurns,throughput:+summary.throughputTurnsPerSec.toFixed(1),ttftP50:Math.round(l.timeToFirstToken.p50),ttftP95:Math.round(l.timeToFirstToken.p95),totalP50:Math.round(l.turnTotal.p50),totalP95:Math.round(l.turnTotal.p95),overheadP95:Math.round(l.platformOverhead.p95),peakRssMiB:Math.round(peak),peakCpuPct:Math.round(cpu),innerTtftMean:mean('agent_ti_first_token_seconds'),innerRunMean:mean('agent_ti_run_seconds')};
  rows.push(row);noisy(JSON.stringify(row));if(summary.errors?.length){const kinds={};for(const e of summary.errors){const k=String(e.message??e).replace(/[0-9a-f]{8,}/g,'#').slice(0,140);kinds[k]=(kinds[k]??0)+1;}noisy('errors '+JSON.stringify(kinds));}
 }
 noisy(JSON.stringify({runtime,idleRssMiB:+idle.toFixed(1),finalRssMiB:+rssOf(app.pid).toFixed(1)}));
}finally{
 for(const child of children)if(child.exitCode===null)child.kill('SIGKILL');
 rmSync(dir,{recursive:true,force:true});
}
