// Runs the smoke against a real child process with the smoke overlay's host file and agents, then
// kills it with SIGKILL during a model call and during an unsafe tool call, and checks recovery.
import {spawn} from 'node:child_process';
import {mkdtempSync,rmSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {join,resolve} from 'node:path';
const root=resolve(new URL('..',import.meta.url).pathname);
const dir=mkdtempSync(join(tmpdir(),'agent-ti-smoke-'));
const appPort=Number(process.env.TEST_APP_PORT??29120),fixturePort=Number(process.env.TEST_FIXTURE_PORT??29121);
const smoke=join(root,'deploy/k8s/overlays/smoke');
const env={...process.env,AGENT_TI_URL:`http://127.0.0.1:${appPort}`,AGENT_TI_FIXTURE_URL:`http://127.0.0.1:${fixturePort}`,
 PORT:String(appPort),FIXTURE_URL:`http://127.0.0.1:${fixturePort}`,AGENT_DATA_DIR:dir,AGENTS_DIR:join(smoke,'agents'),LOG_LEVEL:'warn'};
const fixture=spawn(process.execPath,['scripts/fixture.mjs'],{cwd:root,env:{...env,PORT:String(fixturePort)},stdio:'inherit'});
let app;
const launch=()=>{app=spawn(join(root,'target/debug/agent-ti'),['serve',join(smoke,'agent-ti.json')],{env,stdio:'inherit'});};
async function ready(url){for(let i=0;i<200;i++){try{if((await fetch(url)).ok)return;}catch{}await new Promise(r=>setTimeout(r,50));}throw new Error('not ready: '+url);}
const run=(args)=>new Promise((done,fail)=>{const child=spawn(process.execPath,['scripts/smoke.mjs',...args],{cwd:root,env,stdio:'inherit'});child.on('exit',code=>code===0?done():fail(new Error('smoke exit '+code)));});
async function crash(name){
 const state=join(dir,`${name}.json`);await run([`prepare-${name}`,state]);
 await new Promise(done=>{app.once('exit',done);app.kill('SIGKILL');});
 launch();await ready(env.AGENT_TI_URL+'/readyz');await run([`verify-${name}`,state]);
}
try{
 await ready(env.AGENT_TI_FIXTURE_URL+'/healthz');launch();await ready(env.AGENT_TI_URL+'/readyz');await run([]);
 await crash('restart');
 await crash('tool-restart');
 await crash('child-restart');
 await crash('task-restart');
 await crash('flow-restart');
 console.log('PASS real process SIGKILL recovery during a model call, an unsafe tool call, a sub-agent, a task and a flow');
}finally{
 if(app&&app.exitCode===null){await new Promise(done=>{app.once('exit',done);app.kill('SIGTERM');setTimeout(()=>app.kill('SIGKILL'),3000).unref();});}
 fixture.kill('SIGTERM');rmSync(dir,{recursive:true,force:true});
}
