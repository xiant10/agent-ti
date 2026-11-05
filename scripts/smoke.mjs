// HTTP smoke against a running agent-ti with the smoke overlay's host file and agents
// (deploy/k8s/overlays/smoke) and the scripted fixture. `prepare-*` and `verify-*` bracket a SIGKILL.
import assert from 'node:assert/strict';
import {readFileSync,writeFileSync} from 'node:fs';
import {randomUUID} from 'node:crypto';
const base=process.env.AGENT_TI_URL??'http://127.0.0.1:28120';
const fixture=process.env.AGENT_TI_FIXTURE_URL??'http://127.0.0.1:28121';
const sleep=ms=>new Promise(r=>setTimeout(r,ms));
async function token(params={}){const r=await fetch(fixture+'/token?'+new URLSearchParams(params));assert.equal(r.status,200);return (await r.json()).token;}
const alice=await token();
async function rpc(method,params={},session,auth=alice,path='/agent',extra={}){
 const response=await fetch(base+path,{method:'POST',headers:{authorization:`Bearer ${auth}`,'content-type':'application/json',...(session?{'agent-session-id':session}:{}),...extra},body:JSON.stringify({jsonrpc:'2.0',id:1,method,params})});
 return {status:response.status,...await response.json()};
}
async function create(auth=alice,path='/agent'){const r=await rpc('session.create',{},undefined,auth,path);assert.equal(r.status,200,JSON.stringify(r));return r.result.sessionId;}
async function follow(session,{cursor,stop=e=>e.type==='run.finished',auth=alice,timeout=20000,path='/agent'}={}){
 const controller=new AbortController();const timer=setTimeout(()=>controller.abort(),timeout);const events=[];
 try{
  const r=await fetch(base+path,{headers:{authorization:`Bearer ${auth}`,'agent-session-id':session,...(cursor?{'last-event-id':cursor}:{})},signal:controller.signal});assert.equal(r.status,200);
  let buffer='';const decoder=new TextDecoder();
  for await(const chunk of r.body){buffer+=decoder.decode(chunk,{stream:true});let end;
   while((end=buffer.indexOf('\n\n'))>=0){const frame=buffer.slice(0,end);buffer=buffer.slice(end+2);const line=frame.split('\n').find(l=>l.startsWith('data: '));if(!line)continue;const event=JSON.parse(line.slice(6)).params;events.push(event);if(stop(event,events))return events;}
  }
 }finally{clearTimeout(timer);controller.abort();}
 return events;
}
async function prompt(session,input,{key=randomUUID(),auth=alice,path='/agent',stop}={}){
 const start=await follow(session,{stop:e=>e.type==='snapshot',auth,path});const cursor=start[0].eventId;
 const r=await rpc('session.prompt',{input,requestId:key,wait:false},session,auth,path);assert.equal(r.status,200,JSON.stringify(r));
 // A plugin's reply ends with its message; a model run with run.finished.
 const done=stop??(r.result.answeredBy?e=>e.type==='message.end':e=>e.type==='run.finished');
 const events=await follow(session,{cursor,auth,path,stop:done});assert.ok(done(events.at(-1)),JSON.stringify(events.at(-1)));events.result=r.result;return events;
}
const stats=async()=>await(await fetch(fixture+'/stats')).json();
const end=events=>events.find(e=>e.type==='message.end');
const mode=process.argv[2];
if(mode==='prepare-restart'||mode==='prepare-tool-restart'){
 const id=await create();const key=randomUUID();const snapshot=await follow(id,{stop:e=>e.type==='snapshot'});
 const input=mode==='prepare-restart'?'slow restart':'slow tool';const tools=(await stats()).tools;
 assert.equal((await rpc('session.prompt',{input,requestId:key,wait:false},id)).status,200);
 // Killed during the model call, or while the unsafe tool call is in flight after its prepared record.
 await follow(id,{cursor:snapshot[0].eventId,stop:e=>e.type===(mode==='prepare-restart'?'run.started':'tool.started')});
 if(mode==='prepare-tool-restart'){for(let i=0;i<50&&((await stats()).tools.slow_write??0)===(tools.slow_write??0);i++)await sleep(50);}
 writeFileSync(process.argv[3],JSON.stringify({session:id,key,input,cursor:snapshot[0].eventId,tools}));
 console.log(`accepted and ${mode==='prepare-restart'?'generating':'in an unsafe tool call'} before restart`);process.exit(0);
}
if(mode==='verify-restart'||mode==='verify-tool-restart'){
 const state=JSON.parse(readFileSync(process.argv[3]));
 const events=await follow(state.session,{cursor:state.cursor,timeout:30000});
 const r=await rpc('session.prompt',{input:state.input,requestId:state.key,wait:false},state.session);assert.equal(r.status,200);assert.equal(r.result.duplicate,true);
 const messages=await rpc('session.messages',{},state.session);
 // Deltas aren't kept across a restart: a delta cursor gets a snapshot.
 const stale=await follow(state.session,{cursor:`${state.cursor}.1`,stop:e=>e.type==='snapshot'||e.type==='run.finished'});
 assert.equal(stale[0].type,'snapshot');
 if(mode==='verify-restart'){
  assert.ok(events.some(e=>e.type==='message.end'&&e.text.includes('recovered after restart')));
  assert.deepEqual(messages.result.messages.map(m=>m.role),['user','assistant']);
  console.log('PASS restart during a model call: accepted work recovered; one transcript exchange; retry deduplicated');
 }else{
  assert.ok(end(events).text.includes('interrupted'),end(events).text);
  assert.equal(events.at(-1).status,'completed');
  assert.equal((await stats()).tools.slow_write,(state.tools.slow_write??0)+1,'the unsafe call was repeated');
  assert.deepEqual(messages.result.messages.map(m=>m.role),['user','assistant','tool','assistant']);
  assert.equal(messages.result.messages[2].isError,true);
  console.log('PASS restart during an unsafe tool call: not repeated; returned to the model as interrupted; the run completed');
 }
 process.exit(0);
}

assert.equal((await fetch(base+'/healthz')).status,200);assert.equal((await fetch(base+'/readyz')).status,200);
const manifest=await(await fetch(base+'/v1/runtime')).json();assert.equal(manifest.agent,'ti-demo');assert.match(manifest.bundle,/^sha256:/);
assert.equal((await(await fetch(base+'/agents/ti-plain/v1/runtime')).json()).agent,'ti-plain');
assert.equal((await rpc('agents.list',{},undefined,'invalid')).status,401);
assert.equal((await rpc('agents.list',{},undefined,await token({aud:'wrong'}))).status,401);
assert.equal((await rpc('agents.list',{},undefined,await token({ttl:'-120'}))).status,401);
assert.equal((await rpc('agents.list',{},undefined,await token({scope:'other'}))).status,403);
const session=await create();
for(const auth of [await token({user:'bob'}),await token({tenant:'ten_b'})]){
 for(const method of ['session.get','session.messages','session.prompt','session.cancel','session.update'])assert.equal((await rpc(method,{input:'say forged',requestId:'foreign'},session,auth)).status,404,method);
 const list=await rpc('session.list',{},undefined,auth);assert.ok(!list.result.sessions.some(s=>s.sessionId===session));
 const r=await fetch(base+'/agent',{headers:{authorization:`Bearer ${auth}`,'agent-session-id':session,'last-event-id':`${session}:0`}});assert.equal(r.status,404);
}
const plain=await create(alice,'/agents/ti-plain/agent');
assert.equal((await rpc('session.get',{},plain,alice,'/agents/ti-demo/agent')).status,421);
const before=await stats();const events=await prompt(session,'say Hello 😀é',{key:'once'});
assert.equal(events.find(e=>e.type==='text.delta').offset,0);
assert.equal(end(events).utf16Length,end(events).text.length);
const duplicate=await rpc('session.prompt',{input:'say Hello 😀é',requestId:'once',wait:false},session);assert.equal(duplicate.result.duplicate,true);
assert.equal((await rpc('session.prompt',{input:'changed',requestId:'once',wait:false},session)).status,409);
assert.equal((await stats()).modelCalls,before.modelCalls+1);
assert.equal((await rpc('session.get',{},session)).result.configVersion,manifest.bundle);
const lastStart=events.findIndex(e=>e.type==='run.started');
const replay=await follow(session,{cursor:events[lastStart].eventId});assert.deepEqual(replay.map(e=>e.type),events.slice(lastStart+1).map(e=>e.type));
const gap=await follow(session,{cursor:'foreign:999',stop:e=>e.type==='snapshot'});assert.equal(gap[0].type,'snapshot');
// Plugins answer before admission, without the model; the exchange is in the transcript.
let calls=(await stats()).modelCalls;
const shortcut=await create();const help=await prompt(shortcut,'/help');
assert.equal(help.result.answeredBy,'shortcuts');assert.equal(help.result.placement,'placed');
assert.equal(end(help).text,'Agent Ti is ready.');assert.equal(end(help).source,'shortcuts');
assert.equal(end(await prompt(shortcut,'Hello!')).text,"Hi, I'm Agent Ti. What can I do for you?");
assert.equal((await stats()).modelCalls,calls);
await prompt(shortcut,'say followup');const seen=(await stats()).requests.at(-1);
assert.ok(seen.messages>=6,JSON.stringify(seen));assert.ok(seen.system.includes('<context>\nDeployment context marker: ti-context-v1.'));
// guardrails: the rewrite is what the model sees and what is stored.
const card=await create();await prompt(card,'say card 4111 1111 1111 1111');
assert.equal((await stats()).requests.at(-1).input,'say card [card number removed]');
assert.equal((await rpc('session.messages',{},card)).result.messages[0].text,'say card [card number removed]');
const toolEvents=await prompt(await create(),'tool');
assert.ok(toolEvents.some(e=>e.type==='tool.finished'&&e.isError===false&&e.name==='tools_echo'));
// A tool error goes back to the model, which carries on.
const badTool=await prompt(await create(),'bad tool');
assert.ok(badTool.some(e=>e.type==='tool.finished'&&e.isError===true));
assert.ok(end(badTool).text.startsWith('tool done: Invalid arguments'),end(badTool).text);assert.equal(badTool.at(-1).status,'completed');
const zero=await prompt(plain,'say No plugins',{path:'/agents/ti-plain/agent'});assert.ok(zero.some(e=>e.type==='message.end'&&e.text.includes('No plugins')));
const forged=await create();await rpc('session.prompt',{input:'say verified identity',requestId:'forged',wait:false},forged,alice,'/agent',{'x-tenant-id':'forged','x-user-id':'forged'});await follow(forged,{cursor:`${forged}:0`});const identity=(await stats()).requests.at(-1);assert.equal(identity.tenant,'ten_a');assert.equal(identity.user,'alice');
// Cancellation after a real started generation, signalled in-process.
const slow=await create();const snap=await follow(slow,{stop:e=>e.type==='snapshot'});await rpc('session.prompt',{input:'slow cancel',requestId:'slow',wait:false},slow);await follow(slow,{cursor:snap[0].eventId,stop:e=>e.type==='run.started'});await sleep(100);
const cancelAt=Date.now();assert.equal((await rpc('session.cancel',{requestId:'cancel'},slow)).status,200);const cancelled=await follow(slow,{cursor:snap[0].eventId});
assert.equal(end(cancelled).stopReason,'aborted');assert.ok(Date.now()-cancelAt<2000,'cancellation took '+(Date.now()-cancelAt)+' ms');
// Live text: deltas arrive while the model is still generating, and a stream resumes mid-reply.
const live=await create();const liveStart=(await follow(live,{stop:e=>e.type==='snapshot'}))[0].eventId;
const began=Date.now();assert.equal((await rpc('session.prompt',{input:'stream',requestId:'stream',wait:false},live)).status,200);
const head=await follow(live,{cursor:liveStart,stop:(e,all)=>all.filter(x=>x.type==='text.delta').length>=2});
const firstDeltaAt=Date.now()-began;
const mid=await follow(live,{stop:e=>e.type==='snapshot'});
assert.ok(mid[0].partial?.text?.length>0,'snapshot mid-reply has no partial text: '+JSON.stringify(mid[0]));
const resumeAt=head.filter(e=>e.type==='text.delta').at(-1);assert.match(resumeAt.eventId,/:\d+\.\d+$/);
const rest=await follow(live,{cursor:resumeAt.eventId});const finishedAt=Date.now()-began;
const deltas=[...head,...rest].filter(e=>e.type==='text.delta');
assert.equal(new Set(deltas.map(e=>e.eventId)).size,deltas.length,'a delta was sent twice');
const liveEnd=end(rest);assert.equal(deltas.map(e=>e.text).join(''),liveEnd.text);
assert.ok(liveEnd.text.startsWith(mid[0].partial.text),'partial is not a prefix of the reply');
assert.equal(liveEnd.messageId,resumeAt.messageId);
let units=0;for(const d of deltas){assert.equal(d.offset,units);units+=d.text.length;}assert.equal(units,liveEnd.utf16Length);
assert.ok(firstDeltaAt<finishedAt-800,`first delta at ${firstDeltaAt} ms, reply done at ${finishedAt} ms`);
assert.ok(deltas.length>=5,'expected the reply in several batches, got '+deltas.length);
// The committed record has no deltas: a replay from before the run gets the deltas still in memory.
const replayLive=await follow(live,{cursor:liveStart});assert.equal(replayLive.filter(e=>e.type==='text.delta').map(e=>e.text).join(''),liveEnd.text);
// Cancelled mid-reply: the message ends with the text already shown, marked aborted.
const cut=await create();const cutStart=(await follow(cut,{stop:e=>e.type==='snapshot'}))[0].eventId;
await rpc('session.prompt',{input:'stream',requestId:'cut',wait:false},cut);
const shown=await follow(cut,{cursor:cutStart,stop:(e,all)=>all.filter(x=>x.type==='text.delta').length>=2});
await rpc('session.cancel',{requestId:'cut-cancel'},cut);
const cutEnd=end(await follow(cut,{cursor:shown.at(-1).eventId}));
assert.equal(cutEnd.stopReason,'aborted');assert.ok(cutEnd.text.startsWith(shown.filter(e=>e.type==='text.delta').map(e=>e.text).join('')),JSON.stringify(cutEnd));
assert.equal((await rpc('session.messages',{},cut)).result.messages.length,1,'aborted text must not be stored as a message');
console.log('PASS HTTP smoke: JWT, tenant/user/agent isolation, deduplication, plugin replies, guardrails, tools and tool errors, UTF-16, replay, cancellation, config version, live text and resume');
