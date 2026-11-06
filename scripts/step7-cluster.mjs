// Usage, from the workspace, after deploying Ti to namespace t10 with port-forwards:
//   kubectl -n t10 port-forward svc/auth-server 29100:18000
//   kubectl -n t10 port-forward svc/t10-agent-control 29110:18010
//   kubectl -n t10 port-forward pod/agent-ti-0 29130:18120
//   node agent-ti/scripts/step7-cluster.mjs
// It uses the sample users (dev mode), calls the real model, and removes what it publishes.
// Step 7 on the local cluster: a tenant skill and a tenant connector for Ti, published through control.
import assert from 'node:assert/strict';
const AUTH='http://127.0.0.1:29100',CTL='http://127.0.0.1:29110',TI='http://127.0.0.1:29130/agent';
const sleep=ms=>new Promise(r=>setTimeout(r,ms));
async function token(user,tenant){const form=new URLSearchParams({grant_type:'password',client_id:'t10-workbench',username:user,password:'t10-sample-password',...(tenant?{tenant}:{})});const r=await fetch(AUTH+'/v1/token',{method:'POST',body:form});const j=await r.json();assert.ok(j.access_token,user+': '+JSON.stringify(j));return j.access_token;}
async function ctl(tok,method,path,body){const r=await fetch(CTL+path,{method,headers:{authorization:'Bearer '+tok,'content-type':'application/json'},body:body&&JSON.stringify(body)});const t=await r.text();let j;try{j=JSON.parse(t)}catch{j=t}return {status:r.status,body:j};}
async function rpc(tok,method,params={},session){const r=await fetch(TI,{method:'POST',headers:{authorization:'Bearer '+tok,'content-type':'application/json',...(session?{'agent-session-id':session}:{})},body:JSON.stringify({jsonrpc:'2.0',id:1,method,params})});return {status:r.status,...await r.json()};}
async function turn(tok,session,input){
  const ctrl=new AbortController();const events=[];
  const done=fetch(TI,{headers:{authorization:'Bearer '+tok,accept:'text/event-stream','agent-session-id':session},signal:ctrl.signal}).then(async r=>{const dec=new TextDecoder();let buf='';for await(const c of r.body){buf+=dec.decode(c,{stream:true});let i;while((i=buf.indexOf('\n\n'))>=0){const b=buf.slice(0,i);buf=buf.slice(i+2);const data=b.split('\n').filter(l=>l.startsWith('data:')).map(l=>l.slice(5).trim()).join('');if(!data)continue;const m=JSON.parse(data);const e=m.params?.event??m.params??m;events.push(e);if(e.type==='run.finished')ctrl.abort();}}}).catch(()=>{});
  await sleep(300);const p=await rpc(tok,'session.prompt',{input,requestId:crypto.randomUUID(),wait:false},session);assert.equal(p.status,200,JSON.stringify(p));
  await Promise.race([done,sleep(90000)]);return events;
}
const tag=Date.now().toString(36);
const acmeAdmin=await token('sample-acme-admin','ten_sample-acme'),globexAdmin=await token('sample-globex-admin','ten_sample-globex');
const acme=await token('sample-member','ten_sample-acme'),globex=await token('sample-multi','ten_sample-globex');
const skill='ti-check',code1='TI-'+tag+'-ONE',code2='TI-'+tag+'-TWO';
const put=(code,version,tools)=>ctl(acmeAdmin,'PUT','/v1/admin/skills/ti/'+skill,{kind:'custom',content:{description:'Answers questions about the deployment check code, and echoes text back.',body:`When asked for the deployment check code, answer with exactly ${code} and nothing else.${tools?' To echo text back, call this skill\'s echo tool with the text.':''}`,keywords:['deployment check code'],...(tools?{tools}:{})},...(version===undefined?{}:{version}),reason:'agent-ti step 7 check'});
const version=async path=>{const r=await ctl(acmeAdmin,'GET',path);return r.body.skill?.version??r.body.connector?.version??r.body.version;};
let cleanup=[];
try{
  // 1. A skill published for acme reaches acme's next turn, and never globex's.
  const existing=await ctl(acmeAdmin,'GET','/v1/admin/skills/ti/'+skill);
  let r;for(const base of [existing.body?.skill?.version,undefined,0]){r=await put(code1,base);if(r.status===200)break;}assert.equal(r.status,200,JSON.stringify(r));cleanup.push(async()=>ctl(acmeAdmin,'POST','/v1/admin/skills/ti/'+skill+'/remove',{version:await version('/v1/admin/skills/ti/'+skill),reason:'cleanup'}));
  const published=Date.now();
  const sA=(await rpc(acme,'session.create')).result.sessionId,sG=(await rpc(globex,'session.create')).result.sessionId;
  let seenAt;for(let i=0;i<60;i++){const l=await rpc(acme,'skills.list',{},sA);if(JSON.stringify(l.result).includes(skill)){seenAt=Date.now();break;}await sleep(250);}
  assert.ok(seenAt,'acme never saw the skill');console.log(`skill visible to acme ${seenAt-published} ms after publish`);
  assert.ok(!JSON.stringify((await rpc(globex,'skills.list',{},sG)).result).includes(skill),'globex sees acme skill');console.log('globex does not see it');
  let ev=await turn(acme,sA,'What is the deployment check code? Use your skills.');
  let end=ev.filter(e=>e.type==='message.end').at(-1);console.log('acme answer:',JSON.stringify(end?.text),'tools:',ev.filter(e=>e.type==='tool.started').map(e=>e.name).join(','));
  assert.ok(end?.text.includes(code1),'answer lacks code');
  // 2. A new version reaches the next turn of the same session.
  const cur=await ctl(acmeAdmin,'GET','/v1/admin/skills/ti/'+skill);
  r=await put(code2,cur.body.skill?.version??cur.body.version);assert.equal(r.status,200,JSON.stringify(r));const republished=Date.now();
  await sleep(1500);
  const sA2=(await rpc(acme,'session.create')).result.sessionId;
  ev=await turn(acme,sA2,'What is the deployment check code? Use your skills.');end=ev.filter(e=>e.type==='message.end').at(-1);
  console.log(`after republish (+${Date.now()-republished} ms) acme answer:`,JSON.stringify(end?.text));assert.ok(end?.text.includes(code2),'new version not used');
  // 3. A connector for acme's Ti is offered to acme only, and runs.
  const name='tic'+tag.slice(-5).replace(/[^a-z0-9]/g,'');
  r=await ctl(acmeAdmin,'POST','/v1/admin/connectors',{name,title:'Ti check',url:'http://mcp-fixture.t10.svc.cluster.local:18099/open/mcp',auth:{kind:'none'}});assert.equal(r.status,201,JSON.stringify(r));
  const id=r.body.connector.id;cleanup.push(async()=>{await ctl(acmeAdmin,'POST','/v1/admin/connectors/'+id+'/disable',{version:await version('/v1/admin/connectors/'+id),reason:'cleanup'});return ctl(acmeAdmin,'POST','/v1/admin/connectors/'+id+'/remove',{version:await version('/v1/admin/connectors/'+id),reason:'cleanup'});});
  r=await ctl(acmeAdmin,'POST','/v1/admin/connectors/'+id+'/discover');assert.ok(r.body.connector?.discovered?.tools?.length>=2,JSON.stringify(r.body));
  r=await ctl(acmeAdmin,'PUT','/v1/admin/connectors/'+id,{title:'Ti check',url:'http://mcp-fixture.t10.svc.cluster.local:18099/open/mcp',auth:{kind:'none'},tools:['echo'],agents:['ti'],version:r.body.connector.version});assert.equal(r.status,200,JSON.stringify(r));
  r=await ctl(acmeAdmin,'POST','/v1/admin/connectors/'+id+'/publish',{version:r.body.connector.version,reason:'agent-ti step 7 check'});assert.equal(r.status,200,JSON.stringify(r));
  // A skill that names the connector's tools activates them, as Workbench's skill editor writes it.
  r=await put(code2,await version('/v1/admin/skills/ti/'+skill),[name+':*']);assert.equal(r.status,200,JSON.stringify(r));
  await sleep(1500);
  const sA3=(await rpc(acme,'session.create')).result.sessionId;
  ev=await turn(acme,sA3,`Use the ${skill} skill to echo "hello from ti", then tell me what came back.`);
  const started=ev.filter(e=>e.type==='tool.started').map(e=>e.name),fin=ev.filter(e=>e.type==='tool.finished');end=ev.filter(e=>e.type==='message.end').at(-1);
  console.log('acme connector turn tools:',started.join(','),'errors:',fin.map(e=>e.isError).join(','),'answer:',JSON.stringify(end?.text).slice(0,200));
  assert.ok(started.includes(name+'_echo'),'connector tool not called');assert.ok(fin.some(e=>e.name===name+'_echo'&&!e.isError),'connector call failed');
  const sG2=(await rpc(globex,'session.create')).result.sessionId;
  ev=await turn(globex,sG2,`Call the ${name}_echo tool with text "leak?", then tell me what it returned.`);
  const gstarted=ev.filter(e=>e.type==='tool.started').map(e=>e.name);console.log('globex tools:',gstarted.join(',')||'(none)','answer:',JSON.stringify(ev.filter(e=>e.type==='message.end').at(-1)?.text).slice(0,160));
  assert.ok(!gstarted.includes(name+'_echo')||ev.filter(e=>e.type==='tool.finished').every(e=>e.isError),'globex ran acme connector');
  console.log('PASS step 7 on the cluster');
} finally { for(const c of cleanup.reverse()){const r=await c();console.log('cleanup',r.status);} }
