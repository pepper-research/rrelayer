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
import pty
from pathlib import Path
import re
import select
import subprocess
import time
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
            if args.resume:
                raise ValueError('Cannot resume without the existing deployment journal')
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
        tasks = self.aws('ecs','describe-tasks','--cluster',self.a.cluster,'--tasks',arn)['tasks']
        if len(tasks) != 1 or tasks[0]['taskArn'] != arn:
            raise RuntimeError('Task status could not be verified: '+arn)
        return tasks[0]
    def api(self,task,path,body=None):
        # Authentication stays inside the container. Never echo environment values.
        script='curl --silent --show-error --fail --max-time 8 '
        if body is not None:
            encoded=base64.b64encode(json.dumps(body).encode()).decode()
            script+='--user "$RRELAYER_AUTH_USERNAME:$RRELAYER_AUTH_PASSWORD" -H "Content-Type: application/json" --data "$(echo '+encoded+' | base64 -d)" '
        script+='http://127.0.0.1:3000'+path
        encoded=base64.b64encode(script.encode()).decode()
        command="sh -c 'echo HANDOFF_BEGIN; echo "+encoded+" | base64 -d | sh; echo; echo HANDOFF_END'"
        master,slave=pty.openpty()
        try:
            proc=subprocess.Popen(['aws','--region',self.a.region,'ecs','execute-command','--cluster',self.a.cluster,'--task',task,'--container',self.a.container,'--interactive','--command',command],stdin=slave,stdout=slave,stderr=slave,start_new_session=True)
        except Exception:
            os.close(master)
            raise
        finally:
            os.close(slave)
        try:
            data=b''
            deadline=time.monotonic()+35
            while b'HANDOFF_END' not in data and time.monotonic()<deadline:
                ready,_,_=select.select([master],[],[],min(1,max(0,deadline-time.monotonic())))
                if ready:
                    try:chunk=os.read(master,65536)
                    except OSError:break
                    if not chunk:break
                    data+=chunk
            if b'HANDOFF_BEGIN' not in data or b'HANDOFF_END' not in data:
                raise RuntimeError('Incomplete ECS Exec response')
            body=data.split(b'HANDOFF_BEGIN',1)[1].split(b'HANDOFF_END',1)[0].replace(b'\r',b'').strip()
            return json.loads(body)
        except Exception:
            raise RuntimeError('Task API unavailable: '+path) from None
        finally:
            os.close(master)
            try:proc.terminate()
            except ProcessLookupError:pass
            try:proc.wait(timeout=3)
            except subprocess.TimeoutExpired:
                try:proc.kill()
                except ProcessLookupError:pass
                proc.wait()
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
        recovery = self.state.get('recovery')
        if a.recover_journal:
            original = json.loads(Path(a.recover_journal).read_text())
            if any(original.get(k) != getattr(a,k) for k in ('cluster','service','container','region')):
                raise ValueError('Recovery deployment target changed')
            if original['id'] == self.state['id'] or not original.get('warm_task'):
                raise ValueError('Expected an independent recovery journal with a warm task')
            reference = {k:original[k] for k in ('id','source','digest','warm_task','legacy','stage','target_group','previous_tasks','previous_definition')}
            confirmed = set(original.get('stopped_legacy_tasks', []))
            if original['legacy'] and original['stage'] in ('legacy-stopped','activating','activated','replacing-service'):
                confirmed.update(original['previous_tasks'])
            if not confirmed.issubset(original['previous_tasks']):
                raise ValueError('Shutdown confirmation does not belong to original task inventory')
            reference['stopped_legacy_tasks'] = sorted(confirmed)
            reference['warm_retired'] = original['warm_task'] in original.get('retired_tasks', [])
            if recovery:
                required = {'id','source','digest','warm_task','legacy','stage','target_group'}
                if not required.issubset(recovery) or any(reference.get(k) != v for k,v in recovery.items()):
                    raise ValueError('Recovery journal changed')
                if recovery != reference:
                    self.save(recovery=reference)
            recovery = reference
        interrupted_bootstrap = recovery and recovery['legacy'] and recovery['stage'] in (
            'stopping-legacy','legacy-stopped','activating','activated','replacing-service')
        if service['desiredCount'] != 1 and not (service['desiredCount'] == 0
                and (interrupted_bootstrap or a.resume and self.state.get('legacy'))):
            raise ValueError('Expected one service task')
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
        if recovery and recovery['target_group'] != group:
            raise ValueError('Recovery target group changed')
        running=self.tasks(a.service)
        allowed={t['taskArn'] for t in running}|{x for x in [self.state.get('warm_task'),recovery and recovery.get('warm_task')] if x}
        launched = None
        extras = [task for task in self.tasks() if task['taskArn'] not in allowed
                  and (task['taskDefinitionArn'].split('/')[-1].split(':')[0] == current['family']
                       or task['taskDefinitionArn'] == a.task_definition)]
        if extras:
            if a.resume and self.state['stage'] == 'launching' and len(extras) == 1:
                task = extras[0]
                if (task.get('group') == 'handoff-' + self.state['id']
                        and task['taskDefinitionArn'] == a.task_definition
                        and task.get('startedBy') == self.state['id']):
                    launched = task['taskArn']
            if not launched:
                raise ValueError('Unaccounted relayer tasks; reconcile before deployment')
        if 'previous_definition' not in self.state:
            if len(running)!=1 and not recovery:raise ValueError('Service is not steady; use its recovery journal')
            previous_source,_=source_of(current,a.container)
            old_api=None
            if not recovery and previous_source and running:old_api=self.api(running[0]['taskArn'],'/ready')
            legacy=not old_api or old_api.get('protocol')!=1
            if legacy and not a.bootstrap and not recovery:raise ValueError('Legacy binary requires explicit bootstrap')
            if recovery:
                counts = [service.get(k) for k in ('desiredCount','runningCount','pendingCount')]
                if counts[0] == 0 and counts != [0,0,0]:
                    raise ValueError('Legacy service has not stopped')
                stopped_bootstrap = (counts == [0,0,0] and interrupted_bootstrap
                                     and self.task(recovery['warm_task'])['lastStatus'] == 'STOPPED'
                                     and all(t['lastStatus'] == 'STOPPED' for t in running))
                before_activation = recovery['stage'] in ('stopping-legacy', 'legacy-stopped')
                live_legacy = (recovery['legacy'] and recovery['stage'] in ('prepared','launching','warming','stopping-legacy')
                               and counts == [1,1,0] and service['taskDefinition'] == recovery['previous_definition']
                               and len(running) == 1 and running[0]['lastStatus'] == 'RUNNING'
                               and running[0]['taskDefinitionArn'] == service['taskDefinition'])
                if live_legacy:
                    active = None
                elif interrupted_bootstrap and before_activation and counts == [0,0,0]:
                    active = None
                elif stopped_bootstrap:
                    # Only the recovery standby can now read the durable release fence.
                    # Its observation is checked before target registration or activation.
                    active = recovery['source']
                else:
                    old_api = None
                    can_use_service = recovery['stage'] in ('activated','replacing-service','service-ready','retiring','complete')
                    if (can_use_service and counts == [1,1,0] and len(running) == 1
                            and running[0]['taskDefinitionArn'] == service['taskDefinition']
                            and source_of(current,a.container)[0] == recovery['source']
                            and self.healthy(self.target(running[0]['taskArn']),group)):
                        try:
                            old_api = self.ready(running[0]['taskArn'], recovery['source'], recovery['digest'])
                        except RuntimeError:
                            if recovery.get('warm_retired'):
                                raise
                            # The journaled warm sender must independently prove ownership below.
                    if not old_api and not recovery.get('warm_retired'):
                        old_api = self.ready(recovery['warm_task'], recovery['source'], recovery['digest'])
                    if not old_api or not old_api['durableIntakeReady'] or not old_api['processing'] or old_api['activeRelease'] != recovery['source']:
                        raise ValueError('Interrupted warm sender is not the verified active release')
                    active = old_api['activeRelease']
                legacy=live_legacy
            else:
                active=old_api and old_api.get('activeRelease')
            self.save(previous_definition=service['taskDefinition'],previous_tasks=[t['taskArn'] for t in running],previous_release=active,legacy=legacy,target_group=group,recovery=recovery)
        if self.state['stage']=='complete':return {'stage':'complete','source':a.source,'digest':a.digest}
        if not a.execute:
            return {'plan':'prewarm → verify → activate → replace service → retire warm task','legacy':self.state['legacy']}
        if self.state['stage'] in ('replacing-service', 'service-ready', 'retiring'):
            return self.finish_replacement(group, recovery)
        if launched:
            self.save(warm_task=launched, stage='warming')
        if 'warm_task' not in self.state:
            self.save(stage='launching')
            result=self.aws('ecs','run-task','--cluster',a.cluster,'--task-definition',a.task_definition,'--launch-type','FARGATE','--enable-execute-command','--network-configuration',json.dumps(service['networkConfiguration']),'--client-token',self.state['id'],'--started-by',self.state['id'],'--group','handoff-'+self.state['id'])
            if result.get('failures') or len(result.get('tasks',[]))!=1:raise RuntimeError('Candidate launch failed; existing service unchanged')
            self.save(warm_task=result['tasks'][0]['taskArn'],stage='warming')
        warm=self.state['warm_task'];status=self.wait(lambda:self.ready(warm,a.source,a.digest),'candidate readiness')
        if (not self.state['legacy'] and recovery and recovery['legacy'] and recovery['stage'] in ('stopping-legacy','legacy-stopped','activating')
                and status['activeRelease'] is None and not status['durableIntakeReady']):
            service_state = self.aws('ecs','describe-services','--cluster',a.cluster,'--services',a.service)['services'][0]
            if (not receipt.get('legacy_ingress_quiesced_evidence') or status['unresolvedTransactions'] != 0
                    or any(service_state.get(k) != 0 for k in ('desiredCount','runningCount','pendingCount'))
                    or (recovery['stage'] == 'activating' and self.task(recovery['warm_task'])['lastStatus'] != 'STOPPED')
                    or any(t['lastStatus'] != 'STOPPED' for t in self.tasks(a.service))
                    or (recovery['stage'] == 'stopping-legacy' and
                        any(self.task(t)['lastStatus'] != 'STOPPED' for t in recovery['previous_tasks']
                            if t not in recovery.get('stopped_legacy_tasks', [])))):
                raise ValueError('Cannot verify drained legacy shutdown for recovery bootstrap')
            self.save(legacy=True, previous_release=None, previous_tasks=recovery['previous_tasks'],
                      stopped_legacy_tasks=recovery.get('stopped_legacy_tasks', []))
        expected_owners = {self.state['previous_release']}
        if self.state['stage'] in ('activating','activated'):
            expected_owners.add(a.source)
        if status['activeRelease'] not in expected_owners:
            raise ValueError('Active release changed outside this handoff')
        target=self.target(warm)
        if self.state['legacy'] and not status['durableIntakeReady']:
            # Ingress producers must already be quiesced with durable retry. Verify
            # the candidate observes no unresolved legacy rows before AND after stop.
            if not receipt.get('legacy_ingress_quiesced_evidence') or status['unresolvedTransactions']!=0:
                raise ValueError('Legacy ingress must be quiesced and accepted work fully drained')
            confirmed = set(self.state.get('stopped_legacy_tasks', []))
            if self.state['stage'] in ('legacy-stopped','activating'):
                confirmed.update(self.state['previous_tasks'])
            observed = set(self.state['previous_tasks'])
            observed.update(t['taskArn'] for t in self.tasks(a.service))
            self.save(stage='stopping-legacy', previous_tasks=sorted(observed), stopped_legacy_tasks=sorted(confirmed))
            self.aws('ecs','update-service','--cluster',a.cluster,'--service',a.service,'--desired-count','0')
            def legacy_stopped():
                service = self.aws('ecs','describe-services','--cluster',a.cluster,'--services',a.service)['services'][0]
                current = {task['taskArn'] for task in self.tasks(a.service)}
                if current - observed:
                    observed.update(current)
                    self.save(previous_tasks=sorted(observed))
                if any(service.get(k) != 0 for k in ('desiredCount','runningCount','pendingCount')):
                    return False
                for task in sorted(observed - confirmed):
                    if self.task(task)['lastStatus'] != 'STOPPED':
                        return False
                    confirmed.add(task)
                    self.save(stopped_legacy_tasks=sorted(confirmed))
                return True
            self.wait(legacy_stopped,'legacy tasks stopped')
            status = self.ready(warm, a.source, a.digest)
            if not status or status['unresolvedTransactions'] != 0:
                raise ValueError('Legacy work remains after service shutdown')
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
        return self.finish_replacement(group, recovery)

    def finish_replacement(self, group, recovery):
        a = self.a
        warm = self.state["warm_task"]
        def service_ready():
            tasks=self.tasks(a.service)
            if len(tasks)!=1 or tasks[0]['taskDefinitionArn']!=a.task_definition:return False
            status=self.ready(tasks[0]['taskArn'],a.source,a.digest)
            return status and status['processing'] and status['durableIntakeReady'] and self.healthy(self.target(tasks[0]['taskArn']),group)
        self.wait(service_ready,'replacement service health',900)
        self.save(stage='retiring')
        retiring=[warm]+([recovery['warm_task']] if recovery and not recovery.get('warm_retired') else [])
        stopped = set(self.state.get('retired_tasks', []))
        for arn in retiring:
            if arn in stopped:
                continue
            task = self.task(arn)
            if task['lastStatus'] != 'STOPPED':
                if task.get('desiredStatus') != 'STOPPED':
                    target=self.target(arn)
                    self.aws('elbv2','deregister-targets','--target-group-arn',group,'--targets',json.dumps([target]))
                    def drained():
                        rows=self.aws('elbv2','describe-target-health','--target-group-arn',group,'--targets',json.dumps([target]))['TargetHealthDescriptions']
                        return not rows or rows[0]['TargetHealth']['State']=='unused'
                    self.wait(drained,'target drain')
                    self.aws('ecs','stop-task','--cluster',a.cluster,'--task',arn,'--reason','Verified handoff complete')
                self.wait(lambda: self.task(arn)['lastStatus']=='STOPPED', 'task retirement')
            stopped.add(arn)
            self.save(retired_tasks=sorted(stopped))
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
