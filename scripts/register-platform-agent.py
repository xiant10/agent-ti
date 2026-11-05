#!/usr/bin/env python3
"""Publish only the Ti agent/pool through reviewed, version-checked drafts."""
import argparse,json,urllib.request,urllib.error
from pathlib import Path
parser=argparse.ArgumentParser(description=__doc__)
parser.add_argument('--control-url',required=True)
parser.add_argument('--manifest-url',required=True)
parser.add_argument('--token-file',type=Path,required=True)
parser.add_argument('--reason',required=True)
args=parser.parse_args()
root=Path(__file__).resolve().parents[1]
registration=json.loads((root/'deploy/agent-registration.json').read_text())
token=args.token_file.read_text().strip()
def request(path,body=None,method='GET'):
    headers={'Authorization':'Bearer '+token,'Content-Type':'application/json'}
    req=urllib.request.Request(args.control_url.rstrip('/')+path,data=None if body is None else json.dumps(body).encode(),method=method,headers=headers)
    try:
        with urllib.request.urlopen(req,timeout=30) as r:return None if r.status==204 else json.load(r)
    except urllib.error.HTTPError as e:
        raise SystemExit(f'{method} {path}: {e.code}: {e.read().decode()}')
def publish(method,path,body):
    draft=request(path,body,method)['id']
    review=request('/v1/platform/drafts/'+draft)
    if review['stale'] or review['findings'].get('problems'):
        request('/v1/platform/drafts/'+draft,method='DELETE')
        raise SystemExit('Draft refused or superseded: '+json.dumps(review['findings']))
    result=request('/v1/platform/drafts/'+draft+'/publish',{'reason':args.reason},'POST')
    print(path,'published revision',result['revision'])
with urllib.request.urlopen(args.manifest_url,timeout=10) as r:manifest=json.load(r)
name=registration['name']
if manifest['pool']!=name or manifest['agent']!=name or manifest['harness']!=registration['pool']['harness'] or set(manifest['capabilities'])!=set(registration['pool']['capabilities']):
    raise SystemExit('Running manifest does not match the proposed pool')
config=request('/v1/platform/config')['config']
if name not in config['agents']:
    publish('POST','/v1/platform/agents',{'name':name,'title':registration['agent']['title']})
config=request('/v1/platform/config')['config']
current=config['pools'].get(name)
if current!=registration['pool']:
    if current is not None and current['state']!='active':raise SystemExit('Existing Ti pool is not active; review its state before editing')
    publish('PUT','/v1/platform/pools/'+name,registration['pool'])
config=request('/v1/platform/config')['config']
if config['agents'][name]!=registration['agent']:
    publish('PUT','/v1/platform/agents/'+name,{**registration['agent'],'default':config.get('default_agent')==name})
print('Ti Assistant registered; routers refresh their catalog on the next poll')
