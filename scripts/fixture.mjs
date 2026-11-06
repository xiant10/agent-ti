// Test-only issuer, model and MCP service. Never deploy on a production route.
import {createServer} from 'node:http';
import {generateKeyPairSync,sign} from 'node:crypto';
const keys=generateKeyPairSync('ec',{namedCurve:'P-256'});
const jwk={...keys.publicKey.export({format:'jwk'}),kid:'fixture',use:'sig',alg:'ES256'};
const b64=v=>Buffer.from(JSON.stringify(v)).toString('base64url');
let modelCalls=0,toolCalls=0,summaries=0;
const tools={};
const requests=[];
const server=createServer(async(req,res)=>{
 try {
    const url=new URL(req.url,'http://fixture');
    const json=value=>{res.writeHead(200,{'content-type':'application/json'});res.end(JSON.stringify(value));};
    if(url.pathname==='/healthz')return json({ok:true});
    if(url.pathname==='/jwks'||url.pathname==='/.well-known/jwks.json')return json({keys:[jwk]});
    if(url.pathname==='/token'){
      const now=Math.floor(Date.now()/1000);
      const payload={iss:'agent-ti-fixture',aud:url.searchParams.get('aud')??'t10',sub:url.searchParams.get('user')??'alice',sid:'fixture',tid:url.searchParams.get('tenant')??'ten_a',exp:now+Number(url.searchParams.get('ttl')??'600'),scope:url.searchParams.get('scope')??'agent:use config:publish'};
      const input=`${b64({alg:'ES256',kid:'fixture',typ:'JWT'})}.${b64(payload)}`;
      return json({token:input+'.'+sign('sha256',Buffer.from(input),{key:keys.privateKey,dsaEncoding:'ieee-p1363'}).toString('base64url')});
    }
    if(url.pathname==='/stats')return json({modelCalls,toolCalls,tools,requests,summaries});
    const chunks=[];let size=0;
    for await(const chunk of req){size+=chunk.length;if(size>1024*1024){res.writeHead(413).end();return;}chunks.push(chunk);}
    const body=JSON.parse(Buffer.concat(chunks).toString());
    if(url.pathname==='/mcp'){
      if(body.id===undefined){res.writeHead(202).end();return;}
      if(body.method==='tools/list')return json({jsonrpc:'2.0',id:body.id,result:{tools:[
        {name:'echo',description:'Echoes a value',inputSchema:{type:'object',properties:{value:{type:'string'}},required:['value'],additionalProperties:false}},
        {name:'slow_write',description:'Writes slowly; not safe to repeat',inputSchema:{type:'object',properties:{value:{type:'string'}},required:['value']}}]}});
      if(body.method==='tools/call'){
        toolCalls++;tools[body.params.name]=(tools[body.params.name]??0)+1;
        if(body.params.name==='slow_write'){await new Promise(r=>setTimeout(r,5000));if(res.destroyed)return;}
        return json({jsonrpc:'2.0',id:body.id,result:{content:[{type:'text',text:'echoed'}],isError:false}});
      }
      return json({jsonrpc:'2.0',id:body.id,result:{protocolVersion:'2025-03-26',capabilities:{tools:{}},serverInfo:{name:'fixture',version:'1'}}});
    }
    if(url.pathname!=='/v1/chat/completions'){res.writeHead(404).end();return;}
    modelCalls++;
    const send0=(text)=>{res.writeHead(200,{'content-type':'text/event-stream'});res.write('data: '+JSON.stringify({choices:[{index:0,delta:{content:text},finish_reason:null}]})+'\n\n');res.write('data: '+JSON.stringify({choices:[{index:0,delta:{},finish_reason:'stop'}]})+'\n\n');res.end('data: [DONE]\n\n');};
    // A compaction's summarizer request: answered with a short summary that names what it read.
    if(String(body.messages[0]?.content??'').startsWith('You are a context summarization assistant')){
      summaries++;const conversation=String(body.messages.at(-1)?.content??'');
      return send0(`## Goal\nSummary ${summaries} of ${conversation.length} characters; earlier summary ${conversation.includes('<summary>')?'folded in':'none'}.`);
    }
    const input=body.messages.findLast(m=>m.role==='user')?.content??'';
    // A provider's context-overflow error, for a long request of an input starting "overflow".
    if(input.startsWith('overflow')&&JSON.stringify(body.messages).length>2500){
      requests.push({input,chars:JSON.stringify(body.messages).length,overflow:true});
      res.writeHead(400,{'content-type':'application/json'});return res.end(JSON.stringify({error:{message:"This model's maximum context length is 3000 tokens"}}));
    }
    requests.push({input,reports:body.messages.filter(m=>String(m.content).startsWith('<task_')).length,tools:(body.tools??[]).map(t=>t.function.name),chars:JSON.stringify(body.messages).length,first:String(body.messages[1]?.content??'').slice(0,80),system:body.messages[0]?.content,tenant:req.headers['x-tenant-id'],user:req.headers['x-user-id'],session:req.headers['x-session-id'],messages:body.messages.length});if(requests.length>2048)requests.shift();
    res.writeHead(200,{'content-type':'text/event-stream'});
    const send=(delta,finish=null)=>res.write('data: '+JSON.stringify({choices:[{index:0,delta,finish_reason:finish}]})+'\n\n');
    const calls={'start hold task':['agent_scout','{"task":"hold scouted"}'],'start task':['agent_scout','{"task":"say scouted"}'],'start note task':['agent_scout','{"task":"note parent"}'],'start slow task':['agent_scout','{"task":"slow scouted"}'],'start timeout task':['agent_slowpoke','{"task":"slow poke"}'],'send to task':['task_send','{"taskId":"t1","message":"say again"}'],'stop task':['task_stop','{"taskId":"t1"}'],'list tasks':['task_list','{}'],'note parent':['task_note','{"message":"found 3"}'],delegate:['agent_helper','{"task":"say helped"}'],'delegate slow':['agent_helper','{"task":"slow child"}'],'ask expert':['consult_expert','{"question":"say expert view"}'],'flow lookup':['load_skill','{"name":"lookup","query":"find acme"}'],'flow ask':['load_skill','{"name":"lookup","query":"ambiguous acme"}'],'flow slow':['load_skill','{"name":"lookup","query":"slowly acme"}'],'guided skill':['load_skill','{"name":"guided"}'],tool:['echo','{"value":"x"}'],'bad tool':['echo','{"wrong":1}'],'slow tool':['slow_write','{"value":"x"}']};
    if(body.messages.at(-1)?.role==='tool'){send({content:'tool done: '+body.messages.at(-1).content.slice(0,120)});}
    else if(calls[input]){
      const [want,args]=calls[input];const name=body.tools?.find(t=>t.function.name.endsWith(want))?.function.name;
      send({tool_calls:[{index:0,id:'call-1',type:'function',function:{name,arguments:args}}]});res.write('data: '+JSON.stringify({choices:[{index:0,delta:{},finish_reason:'tool_calls'}],usage:{prompt_tokens:10,completion_tokens:5}})+'\n\n');return res.end('data: [DONE]\n\n');
    }else if(input==='stream'){
      for(const word of ['one ','two ','three ','four ','five ','six ','seven ','eight ','nine ','ten 😀']){send({content:word});await new Promise(r=>setTimeout(r,150));if(res.destroyed)return;}
    }else{
      if(input.startsWith('hold')){await new Promise(r=>setTimeout(r,1000));if(res.destroyed)return;}
      if(input.startsWith('slow')){await new Promise(r=>setTimeout(r,5000));if(res.destroyed)return;}
      send({content:input.startsWith('slow')?'recovered after restart':input.replace(/^say /,'')+' 😀é'});
    }
    res.write('data: '+JSON.stringify({choices:[{index:0,delta:{},finish_reason:'stop'}],usage:{prompt_tokens:10,completion_tokens:5}})+'\n\n');res.end('data: [DONE]\n\n');
 }catch(e){console.error(e.message);res.destroy();}
});
server.listen(Number(process.env.PORT??8080),'0.0.0.0');
