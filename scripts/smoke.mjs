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
 }catch(e){
  if(e.name==='AbortError')throw new Error(`follow timed out on ${session} waiting for ${stop}; saw ${JSON.stringify(events.map(v=>[v.type,v.status??v.phase??String(v.text??'').slice(0,60)]))}`);
  throw e;
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
if(mode==='prepare-child-restart'){
 const id=await create();const key=randomUUID();const snapshot=await follow(id,{stop:e=>e.type==='snapshot'});const calls=(await stats()).modelCalls;
 assert.equal((await rpc('session.prompt',{input:'delegate slow',requestId:key,wait:false},id)).status,200);
 // Killed while the child's model call is in flight, inside the parent's tool call.
 await follow(id,{cursor:snapshot[0].eventId,stop:e=>e.type==='tool.started'&&e.name==='agent_helper'});
 for(let i=0;i<50&&(await stats()).modelCalls<calls+2;i++)await sleep(50);
 writeFileSync(process.argv[3],JSON.stringify({session:id,key,cursor:snapshot[0].eventId}));
 console.log('accepted and in a sub-agent before restart');process.exit(0);
}
if(mode==='prepare-flow-restart'){
 const id=await create();const snapshot=await follow(id,{stop:e=>e.type==='snapshot'});const tools=(await stats()).tools;
 assert.equal((await rpc('session.prompt',{input:'flow slow',requestId:randomUUID(),wait:false},id)).status,200);
 // Killed with a flow's first tool finished and its second, unsafe one in flight.
 for(let i=0;i<100&&((await stats()).tools.slow_write??0)===(tools.slow_write??0);i++)await sleep(50);
 writeFileSync(process.argv[3],JSON.stringify({session:id,cursor:snapshot[0].eventId,tools}));
 console.log('accepted and inside a flow\'s unsafe tool call before restart');process.exit(0);
}
if(mode==='verify-flow-restart'){
 const state=JSON.parse(readFileSync(process.argv[3]));
 await follow(state.session,{cursor:state.cursor,timeout:30000});
 const {tools}=await stats();
 assert.equal(tools.echo,(state.tools.echo??0)+1,'the finished lookup ran again');
 assert.equal(tools.slow_write,(state.tools.slow_write??0)+1,'the cut-off write ran again');
 const result=(await rpc('session.messages',{},state.session)).result.messages.find(m=>m.role==='tool');
 assert.match(result.text,/<step id="found" status="not_found">\nechoed/);
 assert.match(result.text,/<step id="save" status="failed">\ninterrupted/);
 const flow=(await rpc('session.get',{},state.session)).result.flow;
 assert.equal(flow.status,'completed');assert.equal(flow.steps.find(s=>s.stepId==='save').status,'failed');
 console.log('PASS restart inside a flow: the finished call reused, the cut-off write not repeated, the model told');process.exit(0);
}
if(mode==='prepare-task-restart'){
 const id=await create();const snapshot=await follow(id,{stop:e=>e.type==='snapshot'});const calls=(await stats()).modelCalls;
 await prompt(id,'start slow task');
 // Killed while the background child's model call is in flight, with the parent idle.
 for(let i=0;i<50&&(await stats()).modelCalls<calls+3;i++)await sleep(50);
 writeFileSync(process.argv[3],JSON.stringify({session:id,cursor:snapshot[0].eventId}));
 console.log('a background task is working before restart');process.exit(0);
}
if(mode==='verify-task-restart'){
 const state=JSON.parse(readFileSync(process.argv[3]));
 const events=await follow(state.session,{cursor:state.cursor,timeout:30000,stop:e=>e.type==='message.end'&&e.text.includes('recovered after restart')});
 assert.ok(events.length>0);await sleep(1000);
 const lines=(await rpc('session.messages',{},state.session)).result.messages.filter(m=>m.role==='task');
 assert.equal(lines.length,1,'reports after the restart: '+JSON.stringify(lines));
 assert.equal((await rpc('session.get',{},state.session)).result.tasks[0].status,'idle');
 console.log('PASS restart during a background task: the supervisor resumed; the parent got one report');
 process.exit(0);
}
if(mode==='verify-child-restart'){
 const state=JSON.parse(readFileSync(process.argv[3]));
 const events=await follow(state.session,{cursor:state.cursor,timeout:30000});
 assert.ok(end(events)?.text.includes('tool done: recovered after restart'),JSON.stringify(end(events)));
 const children=(await rpc('session.get',{},state.session)).result.subagents;
 assert.equal(children.length,1,'the rerun made a second child: '+JSON.stringify(children));
 assert.deepEqual((await rpc('session.messages',{},children[0])).result.messages.map(m=>m.role),['user','assistant'],'the child answered more than once');
 console.log('PASS restart during a sub-agent: the parent\'s tool found the same child and submission; the child answered once');
 process.exit(0);
}
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
// note: the model reads it ahead of the input, redacted like the input; the transcript leaves it out.
const noted=await create();assert.equal((await rpc('session.prompt',{input:'say noted',note:'spoken reply; heard 4111 1111 1111 1111',requestId:'noted',wait:false},noted)).status,200);await follow(noted,{cursor:`${noted}:0`});
assert.equal((await stats()).requests.at(-1).input,'<turn-note>spoken reply; heard [card number removed]</turn-note>\n\nsay noted');
assert.equal((await rpc('session.messages',{},noted)).result.messages[0].text,'say noted');
assert.equal((await rpc('session.prompt',{input:'x',note:7,requestId:'bad-note',wait:false},noted)).status,400);
const toolEvents=await prompt(await create(),'tool');
assert.ok(toolEvents.some(e=>e.type==='tool.finished'&&e.isError===false&&e.name==='tools_echo'));
// delegation: a sub-agent and the expert run as child sessions of the caller's session.
const parent=await create();
const delegated=await prompt(parent,'delegate');
assert.ok(delegated.some(e=>e.type==='tool.finished'&&e.name==='agent_helper'&&!e.isError),JSON.stringify(delegated.filter(e=>e.type.startsWith('tool'))));
assert.ok(end(delegated).text.includes('tool done: helped'),end(delegated).text);
const helperRequest=(await stats()).requests.findLast(r=>r.input==='say helped');
assert.ok(helperRequest.system.includes('Marker: ti-helper-v1.'),'helper instructions');
assert.deepEqual(helperRequest.tools,[],'a sub-agent with no tools gets none: '+JSON.stringify(helperRequest.tools));
const consulted=await prompt(parent,'ask expert');
assert.ok(end(consulted).text.includes('tool done: expert view'),end(consulted).text);
const expertRequest=(await stats()).requests.findLast(r=>r.input==='say expert view');
assert.ok(!expertRequest.tools.includes('consult_expert')&&!expertRequest.tools.includes('agent_helper')&&expertRequest.tools.includes('tools_echo'),JSON.stringify(expertRequest.tools));
const children=(await rpc('session.get',{},parent)).result.subagents;
assert.equal(children.length,2,JSON.stringify(children));
assert.ok(!(await rpc('session.list')).result.sessions.some(x=>children.includes(x.sessionId)),'children are not listed');
assert.equal((await rpc('session.get',{},children[0])).result.parent,parent);
assert.ok((await rpc('session.messages',{},children[0])).result.messages.at(-1).text.startsWith('helped'),'the child keeps its own transcript');
assert.equal((await rpc('session.prompt',{input:'drive it',requestId:'drive',wait:false},children[0])).status,404,'a child cannot be driven');
assert.equal((await rpc('session.get',{},children[0],await token({user:'bob'}))).status,404,'another user cannot read it');
// Cancelling the parent while its child works cancels the child.
const busyParent=await create();const busyStart=await follow(busyParent,{stop:e=>e.type==='snapshot'});
await rpc('session.prompt',{input:'delegate slow',requestId:'delegate-slow',wait:false},busyParent);
await follow(busyParent,{cursor:busyStart[0].eventId,stop:e=>e.type==='tool.started'&&e.name==='agent_helper'});await sleep(300);
const slowChild=(await rpc('session.get',{},busyParent)).result.subagents[0];
assert.equal((await rpc('session.get',{},slowChild)).result.busy,true,'the child is working');
assert.equal((await rpc('session.cancel',{requestId:'cancel-parent'},busyParent)).status,200);
let childIdle=false;for(let i=0;i<40&&!childIdle;i++){await sleep(100);childIdle=(await rpc('session.get',{},slowChild)).result.busy===false;}
assert.ok(childIdle,'the child was not cancelled with its parent');
// background tasks: the tool returns at once, and the child's turn ending wakes the parent.
const until=(session,cursor,stop)=>follow(session,{cursor,stop,timeout:20000});
const scouted=await create();const scoutStart=await follow(scouted,{stop:e=>e.type==='snapshot'});
const started=await prompt(scouted,'start task');
assert.ok(end(started).text.includes('Started task t1'),end(started).text);
const woken=await until(scouted,scoutStart[0].eventId,e=>e.type==='message.end'&&e.text.includes('scouted'));
assert.ok(woken.some(e=>e.type==='task.updated'&&e.taskId==='t1'&&e.status==='working'),'task.updated working');
assert.ok(woken.some(e=>e.type==='task.updated'&&e.status==='idle'),'task.updated idle');
const taskSeen=await stats();
const report=taskSeen.requests.findLast(r=>r.session===scouted&&r.input.startsWith('<task_report id="t1" name="scout" status="idle"'));
assert.ok(report,'the parent read the report');assert.ok(report.system.includes('<tasks>\nBackground tasks you start report'),'parent section');
const scoutRequest=taskSeen.requests.findLast(r=>r.input==='say scouted');
assert.ok(scoutRequest.system.includes('You work in the background for another agent'),'child section');
assert.deepEqual(scoutRequest.tools,['task_note'],JSON.stringify(scoutRequest.tools));
const scoutedDetails=(await rpc('session.get',{},scouted)).result;
assert.deepEqual(scoutedDetails.tasks,[{taskId:'t1',name:'scout',status:'idle',usage:scoutedDetails.tasks[0].usage}]);
assert.ok(scoutedDetails.tasks[0].usage.totalTokens>0);assert.equal(scoutedDetails.taskUsage.totalTokens,scoutedDetails.tasks[0].usage.totalTokens);
assert.ok((await rpc('session.messages',{},scouted)).result.messages.some(m=>m.role==='task'&&m.kind==='report'&&m.taskId==='t1'&&m.text.startsWith('scouted')),'a task line in the transcript');
// task_send starts the child's next run, whose answer reports again; task_list reads the last one.
await prompt(scouted,'send to task');
await until(scouted,scoutStart[0].eventId,e=>e.type==='message.end'&&e.text.includes('again'));
const listed=await prompt(scouted,'list tasks');assert.ok(end(listed).text.includes('tool done: <task id=\"t1\"')||end(listed).text.includes('<task id="t1"'),end(listed).text);
// task_stop ends it, and a send to it is refused to the model.
await prompt(scouted,'stop task');
assert.equal((await rpc('session.get',{},scouted)).result.tasks[0].status,'stopped');
const refused=await prompt(scouted,'send to task');assert.ok(refused.some(e=>e.type==='tool.finished'&&e.isError===true),'a send to an ended task');assert.ok(end(refused).text.includes('has ended'),end(refused).text);
// A child's task_note reaches the parent without starting a run, and its turn still reports.
const noted2=await create();const notedStart=await follow(noted2,{stop:e=>e.type==='snapshot'});
await prompt(noted2,'start note task');
await until(noted2,notedStart[0].eventId,e=>e.type==='message.end'&&e.text.includes('Noted'));
assert.ok((await rpc('session.messages',{},noted2)).result.messages.some(m=>m.role==='task'&&m.kind==='note'&&m.text==='found 3'),'the note is in the transcript');
// A profile with taskReports "note": the report waits for the next request, with no run of its own.
const quiet=(await rpc('session.create',{profile:'quiet'})).result.sessionId;
await prompt(quiet,'start task');
let quietLine;for(let i=0;i<100&&!quietLine;i++){await sleep(100);quietLine=(await rpc('session.messages',{},quiet)).result.messages.find(m=>m.role==='task');}
assert.ok(quietLine,'the note reached the transcript');
assert.ok(!(await stats()).requests.some(r=>r.session===quiet&&r.input.startsWith('<task_report')),'a note starts no run');
await prompt(quiet,'say next');
assert.equal((await stats()).requests.findLast(r=>r.session===quiet).reports,1,'the next request carries the note');
// A run past its deadline is stopped and reported; the task can take more work.
const late=await create();const lateStart=await follow(late,{stop:e=>e.type==='snapshot'});
await prompt(late,'start timeout task');
await until(late,lateStart[0].eventId,e=>e.type==='message.end'&&e.text.includes('ran out of time'));
assert.equal((await rpc('session.get',{},late)).result.tasks[0].status,'failed');
// A report queued behind a busy parent that an interrupt withdraws comes back as a note, once.
const barged=await create();
await prompt(barged,'start hold task');
await rpc('session.prompt',{input:'slow parent',requestId:'slow-parent',wait:false},barged);
await sleep(2000);
const bargeStart=await follow(barged,{stop:e=>e.type==='snapshot'});
assert.equal((await rpc('session.interrupt',{input:'say after barge',requestId:'barge',wait:false},barged)).status,200);
// The interrupting input is what the model answers, whatever the supervisor is doing meanwhile.
await until(barged,bargeStart[0].eventId,e=>e.type==='message.end'&&e.text.includes('after barge'));
let bargedLines=[];for(let i=0;i<50&&bargedLines.length===0;i++){await sleep(100);bargedLines=(await rpc('session.messages',{},barged)).result.messages.filter(m=>m.role==='task');}
assert.equal(bargedLines.length,1,JSON.stringify(bargedLines));assert.ok(bargedLines[0].text.startsWith('hold scouted'),bargedLines[0].text);
assert.ok(!(await stats()).requests.some(r=>r.session===barged&&r.input.startsWith('<task_report')),'a withdrawn report starts no run');
// maxBackgroundRunsPerUser (3): a fourth child waits for a slot, and its 500 ms deadline waits with it.
const carol=await token({user:'carol'});const carolBusy=await create(carol),carolLate=await create(carol);
for(let i=0;i<3;i++)await prompt(carolBusy,'start slow task',{auth:carol});
const lateFrom=Date.now();const carolStart=await follow(carolLate,{stop:e=>e.type==='snapshot',auth:carol});
await prompt(carolLate,'start timeout task',{auth:carol});
await follow(carolLate,{cursor:carolStart[0].eventId,auth:carol,timeout:20000,stop:e=>e.type==='message.end'&&e.text.includes('ran out of time')});
assert.ok(Date.now()-lateFrom>=3000,`the fourth child ran beside three others, or its deadline ran while it waited: ${Date.now()-lateFrom} ms`);
// A plain cancel leaves tasks working; cancel with tasks ends them; the live limit refuses a fourth.
const fleet=await create();
for(let i=0;i<3;i++)await prompt(fleet,'start slow task');
const fourth=await prompt(fleet,'start slow task');assert.ok(end(fourth).text.includes('Too many background tasks'),end(fourth).text);
await rpc('session.cancel',{requestId:'plain'},fleet);await sleep(200);
assert.ok((await rpc('session.get',{},fleet)).result.tasks.every(t=>t.status==='working'),'a plain cancel left the tasks alone');
await rpc('session.cancel',{requestId:'all',tasks:true},fleet);
assert.ok((await rpc('session.get',{},fleet)).result.tasks.every(t=>t.status==='stopped'),'cancel with tasks ended them');
// A tool error goes back to the model, which carries on.
const badTool=await prompt(await create(),'bad tool');
assert.ok(badTool.some(e=>e.type==='tool.finished'&&e.isError===true));
assert.ok(end(badTool).text.startsWith('tool done: Invalid arguments'),end(badTool).text);assert.equal(badTool.at(-1).status,'completed');
const zero=await prompt(plain,'say No plugins',{path:'/agents/ti-plain/agent'});assert.ok(zero.some(e=>e.type==='message.end'&&e.text.includes('No plugins')));
// compaction (ti-plain: a 3,000-token window): a long session's requests stay the same size, start
// from a summary, and the transcript keeps every message.
const long=await create(alice,'/agents/ti-plain/agent');const seenCompaction=[];
for(let i=0;i<12;i++){
 const events=await prompt(long,`say long-${i} `+'lorem ipsum '.repeat(50),{path:'/agents/ti-plain/agent'});
 seenCompaction.push(...events.filter(e=>e.type==='compaction'));await sleep(300);
}
const longRequests=(await stats()).requests.filter(r=>r.session===long&&r.input.startsWith('say long-'));
const sizes=longRequests.map(r=>r.chars);
assert.equal(longRequests.length,12,JSON.stringify(sizes));
assert.ok((await stats()).summaries>0,'no summary was made');
// Uncompacted, the twelfth request would carry all eleven earlier exchanges.
const tail=sizes.slice(-6);assert.ok(Math.max(...tail)-Math.min(...tail)<200&&Math.max(...tail)<6*sizes[0],`requests kept growing: ${sizes}`);
assert.ok(longRequests.at(-1).first.startsWith('The conversation history before this point was compacted'),JSON.stringify(longRequests.at(-1).first));
assert.ok(seenCompaction.some(e=>e.phase==='start')&&seenCompaction.some(e=>e.phase==='end'&&!e.error),JSON.stringify(seenCompaction));
assert.equal((await rpc('session.messages',{},long,alice,'/agents/ti-plain/agent')).result.messages.length,24);
// A provider's context-overflow error compacts and runs once more.
const over=await create(alice,'/agents/ti-plain/agent');
for(let i=0;i<2;i++)await prompt(over,`say over-${i} `+'lorem ipsum '.repeat(75),{path:'/agents/ti-plain/agent'});
const overflowed=await prompt(over,'overflow then answer',{path:'/agents/ti-plain/agent'});
const overflowRequests=(await stats()).requests.filter(r=>r.input==='overflow then answer').slice(-2);
assert.ok(overflowRequests.length===2&&overflowRequests[0].overflow,JSON.stringify(overflowRequests));
assert.ok(end(overflowed)?.text.includes('overflow then answer'),JSON.stringify(overflowed.map(e=>[e.type,e.phase??e.message??''])));
assert.ok(overflowed.some(e=>e.type==='compaction'&&e.phase==='end'&&!e.error),'no compaction during the overflowed run');
assert.ok(!overflowRequests.at(-1).overflow,'the retry overflowed too');
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
// Usage: a session's own spend, in agent-pi's shape.
const spender=await create();await prompt(spender,'say spend');
const spending=(await rpc('session.get',{},spender)).result;
assert.equal(Object.values(spending.usage.models)[0].totalTokens,15,JSON.stringify(spending.usage));assert.equal(spending.taskUsage.totalTokens,0);
// A foreground child's spend counts as the parent's; a task's stands apart.
const helped=await create();await prompt(helped,'delegate');
const helping=(await rpc('session.get',{},helped)).result;
assert.equal(helping.usage.tools.subagents.totalTokens,15,JSON.stringify(helping.usage));
// A cancelled run keeps what its finished model calls spent.
const spendCut=await create();const cancelledStart=(await follow(spendCut,{stop:e=>e.type==='snapshot'}))[0].eventId;
await rpc('session.prompt',{input:'slow tool',requestId:'spend-cancel',wait:false},spendCut);
const inTool=await follow(spendCut,{cursor:cancelledStart,stop:e=>e.type==='tool.started'});
await rpc('session.cancel',{requestId:'spend-cancel-2'},spendCut);
const cancelEnd=await follow(spendCut,{cursor:inTool.at(-1).eventId,stop:e=>e.type==='run.finished'});
assert.equal(cancelEnd.at(-1).status,'cancelled',JSON.stringify(cancelEnd.at(-1)));
assert.equal(Object.values((await rpc('session.get',{},spendCut)).result.usage.models)[0].totalTokens,15);
// Metrics: what ran, how long it took, how much it cost, and how big the store is.
const scraped=await(await fetch(base+'/metrics')).text();
assert.match(scraped,/agent_ti_runs_total\{agent="ti-demo",status="completed"\} \d+/);
assert.match(scraped,/agent_ti_run_seconds_count\{agent="ti-demo"\} \d+/);
assert.match(scraped,/agent_ti_first_token_seconds_bucket\{agent="ti-demo",le="\+Inf"\} \d+/);
assert.match(scraped,/agent_ti_tool_seconds_count\{agent="ti-demo",server="tools"\} \d+/);
assert.match(scraped,/agent_ti_tokens_total\{agent="ti-demo",kind="input"\} \d+/);
assert.match(scraped,/agent_ti_store_bytes\{agent="ti-demo"\} [1-9]/);
assert.match(scraped,/agent_ti_runs_active\{\} \d+/);
// Flows: `load_skill` with `query` runs a deterministic flow without the model.
const flowed=await create();const beforeFlow=await stats();
const flowRun=await prompt(flowed,'flow lookup');
const traces=flowRun.filter(e=>e.type==='flow.updated').map(e=>e.trace);
assert.ok(traces.length>4,JSON.stringify(flowRun.map(e=>e.type)));
const last=traces.at(-1);assert.equal(last.flowId,'lookup');assert.equal(last.status,'completed');assert.equal(last.llmCallsSaved,1);
assert.deepEqual(Object.fromEntries(last.steps.map(s=>[s.stepId,s.status])),{start:'completed',extract:'completed',check:'completed',ask:'skipped',find:'completed',gate:'completed',save:'skipped',reply:'completed'});
const afterFlow=await stats();assert.equal(afterFlow.tools.echo,beforeFlow.tools.echo+1);assert.equal(afterFlow.tools.slow_write??0,beforeFlow.tools.slow_write??0);
assert.equal(afterFlow.modelCalls,beforeFlow.modelCalls+2,'one call for load_skill, one for the answer');
const evidence=(await rpc('session.messages',{},flowed)).result.messages.find(m=>m.role==='tool').text;
assert.match(evidence,/Answer from what the lookup found/);assert.match(evidence,/<flow_evidence skill="lookup" mode="deterministic"/);assert.match(evidence,/"subject":"find acme"/);
assert.equal((await rpc('session.get',{},flowed)).result.flow.status,'completed');
assert.equal((await follow(flowed,{stop:e=>e.type==='snapshot'}))[0].flow.flowId,'lookup');
// A step that asks stops the flow, with nothing called; the model is given the question.
const asked=await create();const echoes=(await stats()).tools.echo;
const askRun=await prompt(asked,'flow ask');
assert.equal(askRun.filter(e=>e.type==='flow.updated').at(-1).trace.status,'needs_input');
assert.equal((await stats()).tools.echo,echoes);
assert.match((await rpc('session.messages',{},asked)).result.messages.find(m=>m.role==='tool').text,/<flow_needs_input>\nWhich ambiguous acme do you mean\?\n<\/flow_needs_input>/);
// Without a query, the trace follows the model: done when it answers.
const guided=await create();
const guidedRun=await prompt(guided,'guided skill');
const guidedTraces=guidedRun.filter(e=>e.type==='flow.updated').map(e=>e.trace);
assert.equal(guidedTraces[0].mode,'guided');assert.equal(guidedTraces[0].status,'running');assert.equal(guidedTraces.at(-1).status,'completed');
// skills.list shows flows, and skills.checkFlow checks an edit without saving it.
const skillList=(await rpc('skills.list')).result.skills;
const lookupSkill=skillList.find(s=>s.name==='lookup');assert.equal(lookupSkill.flow.mode,'deterministic');assert.equal(skillList.find(s=>s.name==='guided').flow.mode,'guided');
const edited=await rpc('skills.checkFlow',{skill:'lookup',flow:{...lookupSkill.flow,title:'Edited'}});
assert.equal(edited.status,200,JSON.stringify(edited));assert.equal(edited.result.flow.title,'Edited');assert.ok(edited.result.tools.includes('echo'));
const badNode={id:'bad',kind:'tool',label:'Bad',tools:['delete_everything']};
assert.equal((await rpc('skills.checkFlow',{skill:'lookup',flow:{...lookupSkill.flow,nodes:[...lookupSkill.flow.nodes,badNode]}})).status,400);
const customFlow={id:'mine',title:'Mine',mode:'deterministic',nodes:[{id:'start',kind:'start',label:'S'},{id:'look',kind:'tool',label:'L',call:{tool:'tools_echo'}}],edges:[{id:'e',source:'start',target:'look'}]};
assert.equal((await rpc('skills.checkFlow',{skill:'mine',flow:customFlow})).status,400);
assert.equal((await rpc('skills.checkFlow',{skill:'mine',flow:customFlow,tools:['tools:echo']})).status,200);
assert.equal((await rpc('skills.list')).result.skills.find(s=>s.name==='lookup').flow.title,'Lookup');
assert.equal((await rpc('skills.list')).result.skills.some(s=>s.name==='mine'),false);
// Acknowledgements (src/progress.rs; ti-ack: on, 3 s deadline, `waiting` after 1 s quiet). A stream opens
// before the prompt, as Workbench's does; `watch` returns the events up to the run's end.
const ackPath='/agents/ti-ack/agent';
const acks=async()=>(await stats()).acks;
async function watch(session,input,params={},stop=e=>e.type==='run.finished'){
 let opened;const ready=new Promise(r=>opened=r);
 const events=follow(session,{path:ackPath,stop:(e,all)=>{if(e.type==='snapshot')opened();return stop(e,all);}});
 await ready;
 const r=await rpc('session.prompt',{input,requestId:randomUUID(),wait:false,...params},session,alice,ackPath);assert.equal(r.status,200,JSON.stringify(r));
 return {events:await events,result:r.result};
}
const working=events=>events.filter(e=>e.type==='progress'&&e.phase==='working');
// Shown ahead of the answer, for its submission, then cleared before the run ends; never in history; tokens counted.
const acked=await create(alice,ackPath);
const ackFirst=await watch(acked,'hold ack this');
const ackShown=working(ackFirst.events);assert.equal(ackShown.length,1,JSON.stringify(ackFirst.events.map(e=>e.type)));
assert.equal(ackShown[0].text,'Looking into: hold ack this');assert.deepEqual(ackShown[0].submissionIds,[ackFirst.result.submissionId]);
const ackOrder=t=>ackFirst.events.findIndex(t);
assert.ok(ackOrder(e=>e.type==='progress'&&e.phase==='working')<ackOrder(e=>e.type==='text.delta'),'acknowledged before the answer');
assert.ok(ackOrder(e=>e.type==='progress'&&e.phase==='finished')<ackOrder(e=>e.type==='run.finished'),'cleared before the run ends');
const firstAck=(await acks()).at(-1);
assert.equal(firstAck.reasoning,null,"thinking off");assert.equal(firstAck.maxTokens,60);assert.equal(firstAck.tools,0);assert.equal(firstAck.previous,false);assert.equal(firstAck.session,acked);
assert.ok((await rpc('session.messages',{},acked,alice,ackPath)).result.messages.every(m=>!String(m.text).includes('Looking into')));
assert.equal(Object.values((await rpc('session.get',{},acked,alice,ackPath)).result.usage.models)[0].totalTokens,25,'the answer\'s 15 and the acknowledgement\'s 10');
// The next one reads the previous reply, so "yes, go ahead" makes sense.
await watch(acked,'say go ahead');assert.equal((await acks()).at(-1).previous,true);
// Nothing for a greeting, for an acknowledgement slower than the answer, or for one that fails.
for(const input of ['hi there','ack late','ack fail']){
 const before=(await acks()).length;const run=await watch(await create(alice,ackPath),input);
 assert.equal(working(run.events).length,0,input);assert.equal((await acks()).length,before+1,input);
}
// None when nobody is watching, when the prompt asks for none, or when the profile turns it off.
{
 const before=(await acks()).length;
 const unwatched=await create(alice,ackPath);
 assert.equal((await rpc('session.prompt',{input:'say unseen',requestId:randomUUID(),wait:false},unwatched,alice,ackPath)).status,200);
 // Polled, not followed: a stream opened now could count as watching the run.
 for(let i=0;i<200&&!(await rpc('session.messages',{},unwatched,alice,ackPath)).result.messages.some(m=>m.role==='assistant');i++)await sleep(25);
 assert.equal(working((await watch(await create(alice,ackPath),'say asked for none',{progress:false})).events).length,0);
 const quietSession=(await rpc('session.create',{profile:'quiet'},undefined,alice,ackPath)).result.sessionId;
 assert.equal(working((await watch(quietSession,'say quiet profile')).events).length,0);
 assert.equal((await acks()).length,before,'no acknowledgement requested: '+JSON.stringify((await acks()).slice(before).map(a=>a.said)));
}
// A client that will open the stream next (Workbench's first message) says `progress: true`: acknowledged before anyone follows.
{
 const fresh=await create(alice,ackPath);
 const r=await rpc('session.prompt',{input:'hold first message',requestId:randomUUID(),wait:false,progress:true},fresh,alice,ackPath);assert.equal(r.status,200,JSON.stringify(r));
 const later=await follow(fresh,{cursor:`${fresh}:0`,path:ackPath});
 assert.equal(working(later)[0]?.text,'Looking into: hold first message',JSON.stringify(later.map(e=>e.type)));
}
// A quiet run sends `waiting` once, still carrying the acknowledgement; a fresh stream's snapshot has it.
{
 const paused=await create(alice,ackPath);
 const run=watch(paused,'pause ack wait');
 for(let i=0;i<40&&!(await acks()).some(a=>a.session===paused);i++)await sleep(25);
 await sleep(300);
 const snapshot=(await follow(paused,{path:ackPath,stop:e=>e.type==='snapshot'}))[0];
 assert.equal(snapshot.busy,true);assert.equal(snapshot.progress?.text,'Looking into: pause ack wait',JSON.stringify(snapshot));
 const {events}=await run;
 const waiting=events.filter(e=>e.type==='progress'&&e.phase==='waiting');
 assert.equal(waiting.length,1,JSON.stringify(events.map(e=>[e.type,e.phase])));assert.equal(waiting[0].text,'Looking into: pause ack wait');
 assert.equal((await follow(paused,{path:ackPath,stop:e=>e.type==='snapshot'}))[0].progress,undefined,'gone once the run ended');
}
// A message steered into the running turn is acknowledged too, under both submissions.
{
 const steering=await create(alice,ackPath);
 let base;
 const run=watch(steering,'pause ack steer',{},(e,all)=>e.type==='run.finished'&&all.filter(v=>v.type==='run.finished').length===2);
 for(let i=0;i<40&&!(await acks()).some(a=>a.session===steering);i++)await sleep(25);
 await sleep(200);
 const steered=await rpc('session.prompt',{input:'also this',requestId:randomUUID(),wait:false,whenBusy:'steer'},steering,alice,ackPath);assert.equal(steered.status,200,JSON.stringify(steered));
 const {events}=await run;
 const steerAck=working(events).find(e=>e.text==='Looking into: also this');
 assert.ok(steerAck,JSON.stringify(working(events)));assert.equal(steerAck.submissionIds.length,2);assert.ok(steerAck.submissionIds.includes(steered.result.submissionId));
 assert.equal((await acks()).find(a=>a.session===steering&&a.said==='also this').steered,true);
}
const ackMetrics=await(await fetch(base+'/metrics')).text();
for(const outcome of ['shown','empty','late','failed','skipped'])assert.match(ackMetrics,new RegExp(`agent_ti_progress_total\\{agent="ti-ack",outcome="${outcome}"\\}`),outcome);
console.log('PASS HTTP smoke: usage, metrics, acknowledgements, flows, background tasks, delegation, compaction, JWT, tenant/user/agent isolation, deduplication, plugin replies, guardrails, tools and tool errors, UTF-16, replay, cancellation, config version, live text and resume');
