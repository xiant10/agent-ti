// Three real agent-ti processes behind a real ai-gateway-agent built from the sibling repo, with
// the smoke overlay's host file and agents and the scripted fixture.
import {spawn,execFileSync} from 'node:child_process';
import {existsSync,mkdirSync,mkdtempSync,rmSync,writeFileSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {join,resolve} from 'node:path';
const root=resolve(new URL('..',import.meta.url).pathname);
const gatewayRepo=resolve(root,'../ai-gateway');
if(!existsSync(gatewayRepo))throw new Error(`needs the gateway at ${gatewayRepo}`);
const dir=mkdtempSync(join(tmpdir(),'agent-ti-replicas-'));
const base=Number(process.env.TEST_BASE_PORT??29140);
const fixturePort=base,gatewayPort=base+1,ports=[base+2,base+3,base+4];
const url=port=>`http://127.0.0.1:${port}`;
const smoke=join(root,'deploy/k8s/overlays/smoke');
const names=['agent-ti-0','agent-ti-1','agent-ti-2'];
const children=[];
const start=(command,args,env,options={})=>{const child=spawn(command,args,{env:{...process.env,LOG_LEVEL:'warn',...env},stdio:'inherit',...options});children.push(child);return child;};
const ready=async target=>{for(let i=0;i<200;i++){try{if((await fetch(target)).ok)return;}catch{}await new Promise(r=>setTimeout(r,50));}throw new Error('not ready: '+target);};
const launch=index=>{
 const data=join(dir,names[index]);mkdirSync(data,{recursive:true});
 return start(join(root,'target/debug/agent-ti'),['serve',join(smoke,'agent-ti.json')],{PORT:String(ports[index]),FIXTURE_URL:url(fixturePort),AGENT_DATA_DIR:data,AGENTS_DIR:join(smoke,'agents'),AGENT_INSTANCE_ID:names[index]});
};
const gatewayBinary=join(dir,'gateway');
execFileSync('go',['build','-o',gatewayBinary,'./cmd/gateway'],{cwd:gatewayRepo,stdio:'inherit'});
const config=join(dir,'config.agent.yaml');
writeFileSync(config,`server: {role: agent, port: ${gatewayPort}, read_timeout: 30s, write_timeout: 300s, shutdown_timeout: 2s}
agent_route:
  agents: "${names.map((name,i)=>`ti-demo.${name}=${url(ports[i])}`).join(',')}"
  default_agent: ti-demo
  ready_interval: 1s
  ready_timeout: 1s
  list_timeout: 1s
  auth: {issuer: agent-ti-fixture, url: "${url(fixturePort)}", service: ai-gateway, client_secret: test-only}
`);
const env={...process.env,GATEWAY_URL:url(gatewayPort),AGENT_TI_FIXTURE_URL:url(fixturePort),REPLICA_URLS:ports.map(url).join(',')};
const state=join(dir,'state.json');
const part=name=>new Promise((done,fail)=>{const child=spawn(process.execPath,['scripts/replicas.mjs',name,state],{cwd:root,env,stdio:'inherit'});child.on('exit',code=>code===0?done():fail(new Error(`replicas ${name} exited ${code}`)));});
try{
 start(process.execPath,[join(root,'scripts/fixture.mjs')],{PORT:String(fixturePort)},{cwd:root});await ready(url(fixturePort)+'/healthz');
 const apps=ports.map((_,i)=>launch(i));for(const port of ports)await ready(url(port)+'/readyz');
 const gw=start(gatewayBinary,['-config',config],{},{stdio:['ignore','ignore','ignore']});gw.once('exit',code=>{if(code)console.error('gateway exited',code);});
 await ready(url(gatewayPort)+'/health');
 await part('up');
 await new Promise(done=>{apps[2].once('exit',done);apps[2].kill('SIGKILL');});
 await part('down');
 launch(2);await ready(url(ports[2])+'/readyz');
 await part('back');
 console.log('PASS three replicas behind the gateway');
}finally{
 for(const child of children)if(child.exitCode===null)child.kill('SIGKILL');
 rmSync(dir,{recursive:true,force:true});
}
