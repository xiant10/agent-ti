// Three replicas behind the real ai-gateway-agent (agent-pi's docs/replicas.md). Driven in three
// parts, around taking the last replica down, by scripts/test-replicas.mjs and test-replicas-k8s.sh.
// GATEWAY_URL is the gateway; REPLICA_URLS are the replicas in order, for calling them directly.
import assert from 'node:assert/strict';
import {readFileSync,writeFileSync} from 'node:fs';
const gateway=process.env.GATEWAY_URL,fixture=process.env.AGENT_TI_FIXTURE_URL;
const replicas=process.env.REPLICA_URLS.split(',');
const names=['agent-ti-0','agent-ti-1','agent-ti-2'];
const [part,stateFile]=process.argv.slice(2);
const sleep=ms=>new Promise(r=>setTimeout(r,ms));
const token=async user=>{const r=await fetch(`${fixture}/token?${new URLSearchParams({user})}`);assert.equal(r.status,200);return (await r.json()).token;};
async function call(base,auth,method,params={},session,extra={}){
 const response=await fetch(base+'/agent',{method:'POST',headers:{authorization:`Bearer ${auth}`,'content-type':'application/json','agent-method':method,...(session?{'agent-session-id':session}:{}),...extra},body:JSON.stringify({jsonrpc:'2.0',id:1,method,params})});
 const text=await response.text();let body={};try{body=JSON.parse(text);}catch{}
 return {status:response.status,headers:response.headers,...body,text};
}
const replicaOf=id=>id.split('.')[1];
// A prompt through the gateway, answered by waiting on the session's event stream through it too.
async function ask(auth,session,input){
 const controller=new AbortController();const timer=setTimeout(()=>controller.abort(),20000);
 try{
  const stream=await fetch(gateway+'/agent',{headers:{authorization:`Bearer ${auth}`,'agent-session-id':session},signal:controller.signal});
  assert.equal(stream.status,200);
  const sent=await call(gateway,auth,'session.prompt',{input,requestId:crypto.randomUUID(),wait:false},session);
  assert.equal(sent.status,200,sent.text);
  let buffer='';const decoder=new TextDecoder();
  for await(const chunk of stream.body){
   buffer+=decoder.decode(chunk,{stream:true});let end;
   while((end=buffer.indexOf('\n\n'))>=0){
    const line=buffer.slice(0,end).split('\n').find(l=>l.startsWith('data: '));buffer=buffer.slice(end+2);if(!line)continue;
    const event=JSON.parse(line.slice(6)).params;
    if(event.type==='message.end')return event.text;
   }
  }
 }finally{clearTimeout(timer);controller.abort();}
 throw new Error('stream ended without an answer');
}
async function createVia(base,auth){const r=await call(base,auth,'session.create');assert.equal(r.status,200,r.text);return r.result.sessionId;}

if(part==='up'){
 for(let i=0;i<120;i++){const r=await call(gateway,await token('probe'),'session.list');if(r.status===200)break;await sleep(250);}
 // People spread over the replicas, each staying on theirs.
 const placed=new Map();
 for(let i=0;i<30;i++){
  const user=`user-${i}`,auth=await token(user);const id=await createVia(gateway,auth);
  assert.match(id,/^ti-demo\.agent-ti-[012]\.[0-9a-f]+$/);
  assert.equal(replicaOf(await createVia(gateway,auth)),replicaOf(id),'one person moved between replicas');
  if(!placed.has(replicaOf(id)))placed.set(replicaOf(id),{user,id});
 }
 assert.deepEqual([...placed.keys()].sort(),names,'not every replica was given sessions');
 console.log('PASS placement: 30 people over 3 replicas, each staying on theirs');
 // Each replica runs its own sessions, with follow-ups and streams routed by the ID.
 for(const [replica,{user,id}] of placed){
  const auth=await token(user);
  assert.ok((await ask(auth,id,`say hello from ${replica}`)).includes(`hello from ${replica}`));
  assert.ok((await ask(auth,id,'say again')).includes('again'));
  const messages=await call(gateway,auth,'session.messages',{},id);
  assert.deepEqual(messages.result.messages.map(m=>m.role),['user','assistant','user','assistant']);
 }
 console.log('PASS routing: prompts, streams and transcripts reach the replica in the ID');
 // A wrong replica says so and names the right one, whichever way it's called.
 const {user,id}=placed.get('agent-ti-0');const auth=await token(user);
 const expected=`ti-demo.agent-ti-0`;
 const wrong=await call(replicas[1],auth,'session.get',{},id);
 assert.equal(wrong.status,421);assert.equal(wrong.headers.get('agent-instance'),expected);assert.equal(wrong.error.code,-32021);
 const stream=await fetch(replicas[1]+'/agent',{headers:{authorization:`Bearer ${auth}`,'agent-session-id':id}});
 assert.equal(stream.status,421);assert.equal(stream.headers.get('agent-instance'),expected);await stream.body?.cancel();
 const closed=await fetch(replicas[2]+'/agent',{method:'DELETE',headers:{authorization:`Bearer ${auth}`,'agent-session-id':id}});
 assert.equal(closed.status,421);assert.equal(closed.headers.get('agent-instance'),expected);
 assert.equal((await call(replicas[0],auth,'session.get',{},id)).status,200);
 assert.equal((await call(replicas[1],auth,'session.get',{},'nonsense')).status,404);
 console.log('PASS 421 with Agent-Instance on POST, GET and DELETE; the owner still answers');
 // A person's sessions on several replicas list as one, newest first.
 const lister=await token('lister');const own=[];
 for(const replica of replicas){own.push(await createVia(replica,lister));await sleep(20);}
 const merged=await call(gateway,lister,'session.list');
 assert.equal(merged.status,200,merged.text);
 assert.deepEqual(merged.result.sessions.map(s=>s.sessionId),[...own].reverse());
 assert.ok(!merged.result.partial);
 const paged=await call(gateway,lister,'session.list',{limit:2});
 assert.equal(paged.result.sessions.length,2);assert.ok(paged.result.nextCursor,JSON.stringify(paged.result));
 const rest=await call(gateway,lister,'session.list',{limit:2,cursor:paged.result.nextCursor});
 assert.deepEqual([...paged.result.sessions,...rest.result.sessions].map(s=>s.sessionId),[...own].reverse());
 console.log('PASS merged listing across replicas, with a cursor');
 // Closing through the gateway reaches the owner.
 const toClose=await createVia(gateway,lister);
 const gone=await fetch(gateway+'/agent',{method:'DELETE',headers:{authorization:`Bearer ${lister}`,'agent-session-id':toClose}});
 assert.equal(gone.status,204,await gone.text());
 assert.equal((await call(gateway,lister,'session.get',{},toClose)).status,404);
 console.log('PASS DELETE through the gateway closes the session');
 // Someone placed on the replica about to go down, and someone who isn't.
 const onLast=[...Array(200).keys()].map(i=>`last-${i}`);
 let last,other;
 for(const candidate of onLast){
  const a=await token(candidate);const s=await createVia(gateway,a);
  if(replicaOf(s)===names[2]&&!last)last={user:candidate,session:s};
  if(replicaOf(s)!==names[2]&&!other)other={user:candidate,session:s};
  if(last&&other)break;
 }
 assert.ok(last&&other);
 assert.ok((await ask(await token(last.user),last.session,'say before the outage')).includes('before the outage'));
 writeFileSync(stateFile,JSON.stringify({last,other,lister:own}));
 process.exit(0);
}
const state=JSON.parse(readFileSync(stateFile));
if(part==='down'){
 const lastAuth=await token(state.last.user),otherAuth=await token(state.other.user);
 // The gateway notices within its probe interval.
 let unavailable;
 for(let i=0;i<80;i++){unavailable=await call(gateway,lastAuth,'session.get',{},state.last.session);if(unavailable.status>=500)break;await sleep(250);}
 assert.ok(unavailable.status>=500,`a lost replica's session answered ${unavailable.status}`);
 assert.ok((await ask(otherAuth,state.other.session,'say still here')).includes('still here'));
 console.log('PASS a lost replica: its sessions stop, and the others carry on');
 // New sessions go to a ready replica, once the gateway has seen it's gone.
 let moved;
 for(let i=0;i<80;i++){const r=await call(gateway,lastAuth,'session.create');if(r.status===200&&replicaOf(r.result.sessionId)!==names[2]){moved=r.result.sessionId;break;}await sleep(250);}
 assert.ok(moved,'a new session was not placed on a ready replica');
 const partial=await call(gateway,await token('lister'),'session.list');
 assert.equal(partial.status,200,partial.text);assert.equal(partial.result.partial,true);
 assert.deepEqual(partial.result.sessions.map(s=>s.sessionId).sort(),state.lister.filter(id=>replicaOf(id)!==names[2]).sort());
 console.log('PASS new sessions placed elsewhere; the list says partial');
 process.exit(0);
}
if(part==='back'){
 const auth=await token(state.last.user);
 let answered;
 for(let i=0;i<120;i++){answered=await call(gateway,auth,'session.messages',{},state.last.session);if(answered.status===200)break;await sleep(250);}
 assert.equal(answered.status,200,answered.text);
 assert.deepEqual(answered.result.messages.map(m=>m.role),['user','assistant'],'the conversation is lost');
 assert.ok((await ask(auth,state.last.session,'say after the outage')).includes('after the outage'));
 const whole=await call(gateway,await token('lister'),'session.list');
 assert.ok(!whole.result.partial);assert.equal(whole.result.sessions.length,3);
 console.log('PASS the replica came back with its conversations; the list is whole again');
 process.exit(0);
}
throw new Error(`unknown part ${part}`);
