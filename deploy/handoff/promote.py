#!/usr/bin/env python3
"""Promote an accepted immutable image using a prewarmed, temporary ECS task.

Only the release process invokes this tool. Credentials are inherited; never
printed or copied into its journal. A failure after activation retains the warm
serving task. Re-run with --resume to reconcile, or promote an accepted protocol-1
rollback image with --recover-journal pointing at the interrupted run.
"""
import argparse
import base64
import json
import os
from pathlib import Path
import re
import subprocess
import time
import threading
import uuid

REQUIRED = {'two_process_handoff','crash_boundaries','lost_rpc_response','stale_owner',
            'database_reconnect','replacement_cancel','legacy_bootstrap','compatible_rollback',
            'api_journal_boundary','downstream_no_refund_or_duplicate_credit','dev_ecs_handoff'}
LOCAL_REQUIRED=REQUIRED-{'dev_ecs_handoff','downstream_no_refund_or_duplicate_credit'}


def validate_acceptance(receipt, source, digest, *, dev_validation=False):
    if not re.fullmatch(r'[0-9a-f]{40}',source) or not re.fullmatch(r'sha256:[0-9a-f]{64}',digest):
        raise ValueError('Full source SHA and immutable image digest required')
    if receipt.get('source') != source or receipt.get('image_digest') != digest or receipt.get('protocol') != 1:
        raise ValueError('Acceptance does not identify this exact source/image/protocol')
    passed={name for name,result in receipt.get('checks',{}).items() if result.get('passed') is True and result.get('evidence')}
    required=LOCAL_REQUIRED if dev_validation else REQUIRED
    if not required <= passed:
        raise ValueError('Missing exact-image acceptance: '+', '.join(sorted(required-passed)))
    if receipt.get('downstream_image_digest') is None:
        raise ValueError('Downstream API compatibility image must be recorded')


def source_of(definition, container):
    selected=next(c for c in definition['containerDefinitions'] if c['name']==container)
    env={e['name']:e['value'] for e in selected.get('environment',[])}
    return env.get('RRELAYER_RELEASE_ID'),selected['image']


def compatible_definition(previous,candidate,container):
    # No secrets, roles, networking, resources, sidecars, commands or runtime
    # configuration may silently change as part of an image-only handoff.
    generated={'taskDefinitionArn','revision','status','requiresAttributes','compatibilities','registeredAt','registeredBy','deregisteredAt'}
    def normalize(value):
        value=json.loads(json.dumps(value))
        for k in generated:value.pop(k,None)
        for c in value['containerDefinitions']:
            if c['name']==container:
                c['image']='IMAGE'
                c['environment']=sorted([e for e in c.get('environment',[]) if e['name']!='RRELAYER_RELEASE_ID'],key=lambda e:e['name'])
        return value
    if normalize(previous)!=normalize(candidate):raise ValueError('Task configuration changed beyond image/release ID; review separately')


class Controller:
    def __init__(self,args):
        self.a=args;self.path=Path(args.journal);self.state={}
        if self.path.exists():
            if not args.resume:raise ValueError('Journal exists; reconcile with --resume')
            self.state=json.loads(self.path.read_text())
            if self.state['source']!=args.source or self.state['digest']!=args.digest:raise ValueError('Resume candidate changed')
            if any(self.state.get(k)!=getattr(args,k) for k in ['cluster','service','container','region']):raise ValueError('Resume deployment target changed')
        else:
            self.state={'cluster':args.cluster,'service':args.service,'container':args.container,'region':args.region,'source':args.source,'digest':args.digest,'id':str(uuid.uuid4()),'stage':'prepared'}
    def save(self,**values):
        self.state.update(values);self.path.parent.mkdir(parents=True,exist_ok=True)
        temp=self.path.with_suffix('.tmp');temp.write_text(json.dumps(self.state,indent=2)+'\n');temp.replace(self.path)
    def aws(self,*args):
        result=subprocess.run(['aws','--region',self.a.region,*args,'--output','json'],capture_output=True,text=True,timeout=90)
        if result.returncode:raise RuntimeError('AWS operation failed: '+' '.join(args[:2]))
        return json.loads(result.stdout or '{}')
    def tasks(self,service=None):
        args=['ecs','list-tasks','--cluster',self.a.cluster,'--desired-status','RUNNING']
        if service:args+=['--service-name',service]
        ids=self.aws(*args)['taskArns']
        return self.aws('ecs','describe-tasks','--cluster',self.a.cluster,'--tasks',*ids)['tasks'] if ids else []
    def task(self,arn):
        return self.aws('ecs','describe-tasks','--cluster',self.a.cluster,'--tasks',arn)['tasks'][0]
    def api(self,task,path,body=None):
        # Authentication stays inside the container. Never echo environment values.
        script='curl --silent --show-error --fail --max-time 8 '
        if body is not None:
            encoded=base64.b64encode(json.dumps(body).encode()).decode()
            script+='--user "$RRELAYER_AUTH_USERNAME:$RRELAYER_AUTH_PASSWORD" -H "Content-Type: application/json" --data "$(echo '+encoded+' | base64 -d)" '
        script+='http://127.0.0.1:3000'+path
        encoded=base64.b64encode(script.encode()).decode()
        command="sh -c 'echo HANDOFF_BEGIN; echo "+encoded+" | base64 -d | sh; echo; echo HANDOFF_END'"
        proc=subprocess.Popen(['aws','--region',self.a.region,'ecs','execute-command','--cluster',self.a.cluster,'--task',task,'--container',self.a.container,'--interactive','--command',command],stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True)
        try:
            timer=threading.Timer(30,proc.kill);timer.start()
            lines=[];capturing=False;complete=False
            try:
                for line in proc.stdout:
                    if line.strip()=='HANDOFF_BEGIN':capturing=True;continue
                    if line.strip()=='HANDOFF_END':complete=True;break
                    if capturing:lines.append(line)
            finally:timer.cancel()
            proc.stdin.close()
            try:proc.wait(timeout=5)
            except subprocess.TimeoutExpired:proc.kill();proc.wait()
            if not complete:raise RuntimeError('Incomplete ECS Exec response')
            return json.loads(''.join(lines))
        except Exception:
            proc.kill();proc.wait()
            raise RuntimeError('Task API unavailable: '+path) from None
    def wait(self,predicate,label,seconds=600):
        until=time.monotonic()+seconds
        while time.monotonic()<until:
            try:
                value=predicate()
                if value:return value
            except (RuntimeError,KeyError,IndexError):pass
            time.sleep(2)
        raise RuntimeError('Timed out: '+label)
    def target(self,task):
        data=self.task(task)
        ip=next(d['value'] for a in data['attachments'] if a['type']=='ElasticNetworkInterface' for d in a['details'] if d['name']=='privateIPv4Address')
        return {'Id':ip,'Port':3000}
    def healthy(self,target,group):
        rows=self.aws('elbv2','describe-target-health','--target-group-arn',group,'--targets',json.dumps([target]))['TargetHealthDescriptions']
        return rows and rows[0]['TargetHealth']['State']=='healthy'
    def ready(self,arn,source,digest):
        task=self.task(arn)
        containers=[c for c in task.get('containers',[]) if c['name']==self.a.container]
        if task['lastStatus']!='RUNNING' or not containers or containers[0].get('imageDigest')!=digest:return False
        status=self.api(arn,'/ready')
        if status.get('protocol')!=1 or status.get('release')!=source or status.get('queuesReady') is not True:return False
        return status
    def run(self):
        a=self.a
        dev_validation=getattr(a,'dev_validation',False)
        if dev_validation and (a.cluster,a.service,a.container,a.region)!=('rrelayer-dev-cluster','rrelayer-dev','rrelayer-dev','ap-northeast-1'):
            raise ValueError('Dev validation mode is restricted to the named dev service')
        receipt=json.loads(Path(a.acceptance).read_text());validate_acceptance(receipt,a.source,a.digest,dev_validation=dev_validation)
        service=self.aws('ecs','describe-services','--cluster',a.cluster,'--services',a.service)['services'][0]
        if service['deploymentConfiguration']['maximumPercent']!=100 or service['deploymentConfiguration']['minimumHealthyPercent']!=0:
            raise ValueError('Keep the service stop-before-start; temporary warm task supplies continuity')
        if service['desiredCount']!=1 and not (a.resume and self.state.get('legacy') and service['desiredCount']==0):raise ValueError('Expected one service task')
        current=self.aws('ecs','describe-task-definition','--task-definition',service['taskDefinition'])['taskDefinition']
        candidate=self.aws('ecs','describe-task-definition','--task-definition',a.task_definition)['taskDefinition']
        a.task_definition=candidate['taskDefinitionArn']
        source,image=source_of(candidate,a.container)
        if source!=a.source or not image.endswith('@'+a.digest):raise ValueError('Candidate definition must pin the accepted digest and release ID')
        compatible_definition(current,candidate,a.container)
        groups=service.get('loadBalancers',[])
        if len(groups)!=1 or groups[0]['containerPort']!=3000:raise ValueError('Expected one IP target group on port 3000')
        group=groups[0]['targetGroupArn']
        target_group=self.aws('elbv2','describe-target-groups','--target-group-arns',group)['TargetGroups'][0]
        if target_group['TargetType']!='ip':raise ValueError('Expected IP target group')
        running=self.tasks(a.service)
        recovery=json.loads(Path(a.recover_journal).read_text()) if a.recover_journal else None
        allowed={t['taskArn'] for t in running}|{x for x in [self.state.get('warm_task'),recovery and recovery.get('warm_task')] if x}
        unexpected=[t['taskArn'] for t in self.tasks() if t['taskDefinitionArn'].split('/')[-1].split(':')[0]==current['family'] and t['taskArn'] not in allowed]
        if unexpected:raise ValueError('Unaccounted relayer tasks; reconcile before deployment')
        if 'previous_definition' not in self.state:
            if len(running)!=1 and not recovery:raise ValueError('Service is not steady; use its recovery journal')
            previous_source,_=source_of(current,a.container)
            old_api=None
            if previous_source and running:old_api=self.api(running[0]['taskArn'],'/ready')
            legacy=not old_api or old_api.get('protocol')!=1
            if legacy and not a.bootstrap and not recovery:raise ValueError('Legacy binary requires explicit bootstrap')
            if recovery:
                old_api=self.api(recovery['warm_task'],'/ready');legacy=False
            active=old_api and old_api.get('activeRelease')
            self.save(previous_definition=service['taskDefinition'],previous_tasks=[t['taskArn'] for t in running],previous_release=active,legacy=legacy,target_group=group)
        if self.state['stage']=='complete':return {'stage':'complete','source':a.source,'digest':a.digest}
        if not a.execute:
            return {'plan':'prewarm → verify → activate → replace service → retire warm task','legacy':self.state['legacy']}
        if 'warm_task' not in self.state:
            self.save(stage='launching')
            result=self.aws('ecs','run-task','--cluster',a.cluster,'--task-definition',a.task_definition,'--launch-type','FARGATE','--enable-execute-command','--network-configuration',json.dumps(service['networkConfiguration']),'--client-token',self.state['id'],'--group','handoff-'+self.state['id'])
            if result.get('failures') or len(result.get('tasks',[]))!=1:raise RuntimeError('Candidate launch failed; existing service unchanged')
            self.save(warm_task=result['tasks'][0]['taskArn'],stage='warming')
        warm=self.state['warm_task'];status=self.wait(lambda:self.ready(warm,a.source,a.digest),'candidate readiness')
        if status['activeRelease'] not in (self.state['previous_release'],a.source):raise ValueError('Active release changed outside this handoff')
        target=self.target(warm)
        if self.state['legacy'] and not status['durableIntakeReady']:
            # Ingress producers must already be quiesced with durable retry. Verify
            # the candidate observes no unresolved legacy rows before AND after stop.
            if not receipt.get('legacy_ingress_quiesced_evidence') or status['unresolvedTransactions']!=0:
                raise ValueError('Legacy ingress must be quiesced and accepted work fully drained')
            self.save(stage='stopping-legacy')
            self.aws('ecs','update-service','--cluster',a.cluster,'--service',a.service,'--desired-count','0')
            self.wait(lambda:all(self.task(t)['lastStatus']=='STOPPED' for t in self.state['previous_tasks']),'legacy tasks stopped')
            self.save(stage='legacy-stopped')
        if status['durableIntakeReady']:
            self.aws('elbv2','register-targets','--target-group-arn',group,'--targets',json.dumps([target]))
            self.wait(lambda:self.healthy(target,group),'prewarmed target health')
        # ECS cannot roll back to an unfenced legacy image after activation.
        # Recovery always uses this controller and an accepted protocol-1 image.
        config=dict(service['deploymentConfiguration'])
        config['deploymentCircuitBreaker']={'enable':True,'rollback':False}
        self.aws('ecs','update-service','--cluster',a.cluster,'--service',a.service,'--deployment-configuration',json.dumps(config))
        self.save(stage='activating')
        if status['activeRelease']!=a.source:
            self.api(warm,'/handoff/activate',{'expected_release':self.state['previous_release'],'bootstrap':self.state['legacy']})
        status=self.api(warm,'/ready')
        if not status['processing'] or not status['durableIntakeReady']:raise RuntimeError('Activation unverified; preserve both tasks')
        self.save(stage='activated')
        self.aws('elbv2','register-targets','--target-group-arn',group,'--targets',json.dumps([target]))
        self.wait(lambda:self.healthy(target,group),'active warm target health')
        self.aws('ecs','update-service','--cluster',a.cluster,'--service',a.service,'--task-definition',a.task_definition,'--desired-count','1','--enable-execute-command')
        self.save(stage='replacing-service')
        def service_ready():
            tasks=self.tasks(a.service)
            if len(tasks)!=1 or tasks[0]['taskDefinitionArn']!=a.task_definition:return False
            status=self.ready(tasks[0]['taskArn'],a.source,a.digest)
            return status and status['processing'] and status['durableIntakeReady'] and self.healthy(self.target(tasks[0]['taskArn']),group)
        self.wait(service_ready,'replacement service health',900)
        self.save(stage='service-ready')
        retiring=[warm]+([recovery['warm_task']] if recovery else [])
        for arn in retiring:
            if self.task(arn)['lastStatus']=='STOPPED':continue
            target=self.target(arn)
            self.aws('elbv2','deregister-targets','--target-group-arn',group,'--targets',json.dumps([target]))
            def drained():
                rows=self.aws('elbv2','describe-target-health','--target-group-arn',group,'--targets',json.dumps([target]))['TargetHealthDescriptions']
                return not rows or rows[0]['TargetHealth']['State']=='unused'
            self.wait(drained,'target drain')
            self.aws('ecs','stop-task','--cluster',a.cluster,'--task',arn,'--reason','Verified handoff complete')
        self.save(stage='complete')
        return {'stage':'complete','source':a.source,'digest':a.digest}


def main():
    p=argparse.ArgumentParser(description=__doc__)
    for name in ['cluster','service','container','task-definition','source','digest','acceptance','journal']:p.add_argument('--'+name,required=True)
    p.add_argument('--dev-validation',action='store_true');p.add_argument('--region',default='ap-northeast-1');p.add_argument('--execute',action='store_true');p.add_argument('--bootstrap',action='store_true');p.add_argument('--resume',action='store_true');p.add_argument('--recover-journal')
    args=p.parse_args();controller=Controller(args)
    try:print(json.dumps(controller.run()))
    except Exception as error:
        controller.save(error=str(error))
        raise SystemExit(f'{error}. Preserve warm task; journal: {args.journal}')

if __name__=='__main__':main()
