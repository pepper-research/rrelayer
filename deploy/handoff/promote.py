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

from registration_window import MESSAGE as REGISTRATION_PAUSED, registration_window

REQUIRED = {'two_process_handoff','crash_boundaries','lost_rpc_response','stale_owner',
            'database_reconnect','replacement_cancel','legacy_bootstrap','compatible_rollback',
            'api_journal_boundary','downstream_no_refund_or_duplicate_credit','dev_ecs_handoff'}
LOCAL_REQUIRED=REQUIRED-{'dev_ecs_handoff','downstream_no_refund_or_duplicate_credit'}


class TaskNotFound(RuntimeError):
    pass


class ReadinessNotFound(RuntimeError):
    pass


def report_reference(value):
    return isinstance(value, str) and bool(value.strip())


def protocol_one(value):
    return type(value) is int and value == 1


def validate_acceptance(receipt, source, digest, *, dev_validation=False):
    if not re.fullmatch(r'[0-9a-f]{40}',source) or not re.fullmatch(r'sha256:[0-9a-f]{64}',digest):
        raise ValueError('Full source SHA and immutable image digest required')
    if receipt.get('source') != source or receipt.get('image_digest') != digest or not protocol_one(receipt.get('protocol')):
        raise ValueError('Acceptance does not identify this exact source/image/protocol')
    passed = {name for name, result in receipt.get('checks', {}).items()
              if isinstance(result, dict) and result.get('passed') is True
              and report_reference(result.get('evidence'))}
    required=LOCAL_REQUIRED if dev_validation else REQUIRED
    if not required <= passed:
        raise ValueError('Missing exact-image acceptance: '+', '.join(sorted(required-passed)))
    downstream = receipt.get('downstream_image_digest')
    if not isinstance(downstream, str) or not re.fullmatch(r'sha256:[0-9a-f]{64}', downstream):
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


def retirement_inventory(journal):
    previous, warm, retired = set(), set(), set()
    # A recovery can itself be recovered. Keep its sender identities and prior
    # STOPPED observations together across each import, including old journals.
    chain = []
    while journal:
        chain.append(journal)
        journal = journal.get('recovery')
    for journal in reversed(chain):
        previous.update(journal.get('previous_tasks', []))
        if journal.get('warm_task'):
            warm.add(journal['warm_task'])
            if journal.get('warm_retired'):
                retired.add(journal['warm_task'])
        retired.update(journal.get('retired_tasks', []))
        retired.update(journal.get('stopped_legacy_tasks', []))
        if not retired.issubset(previous | warm):
            raise ValueError('Retirement receipt does not identify a tracked sender')
    return previous, warm, retired


def historical_retirements(journal, shutdown, receipt_path):
    keys = ('id','cluster','region','service','warm_task','source','digest')
    if (journal['stage'] != 'complete' or not isinstance(shutdown, dict)
            or any(shutdown.get(k) != journal[k] for k in keys)
            or shutdown.get('lastStatus') != 'STOPPED' or not report_reference(shutdown.get('evidence'))):
        raise ValueError('Historical shutdown evidence does not match the completed original journal')
    confirmed = {journal['warm_task']}
    # Older receipts attest to one warm sender and may reference a text report.
    # A production verdict additionally identifies stopped predecessors by ARN.
    path = Path(receipt_path).parent / shutdown['evidence']
    if not path.is_file():
        return confirmed
    try:
        verdict = json.loads(path.read_text())
    except (UnicodeDecodeError, json.JSONDecodeError):
        return confirmed
    if not isinstance(verdict, dict) or 'retiredTasks' not in verdict:
        return confirmed
    if (verdict.get('passed') is not True or verdict.get('source') != journal['source']
            or verdict.get('imageDigest') != journal['digest'] or not isinstance(verdict['retiredTasks'], list)):
        raise ValueError('Historical retirement verdict source or image mismatch')
    previous, warm, _ = retirement_inventory(journal)
    seen = set()
    for row in verdict['retiredTasks']:
        if not isinstance(row, dict) or row.get('taskArn') not in previous | warm:
            raise ValueError('Historical retirement verdict names an unrelated sender')
        task = row['taskArn']
        if task in seen or row.get('lastStatus') != 'STOPPED':
            raise ValueError('Historical retirement verdict has conflicting or nonterminal evidence')
        seen.add(task)
        confirmed.add(task)
    return confirmed


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
        args=['ecs','list-tasks','--cluster',self.a.cluster]
        if service:args+=['--service-name',service]
        ids = sorted({arn for desired in ('RUNNING','STOPPED')
                      for arn in self.aws(*args,'--desired-status',desired)['taskArns']})
        tasks = []
        for start in range(0, len(ids), 100):
            batch = ids[start:start+100]
            response = self.aws('ecs','describe-tasks','--cluster',self.a.cluster,'--tasks',*batch)
            rows = response['tasks']
            if response.get('failures') or sorted(t['taskArn'] for t in rows) != sorted(batch):
                raise RuntimeError('Incomplete ECS task inventory')
            tasks.extend(row for row in rows if row['lastStatus'] != 'STOPPED')
        return tasks
    def task(self,arn):
        response = self.aws('ecs','describe-tasks','--cluster',self.a.cluster,'--tasks',arn)
        tasks, failures = response['tasks'], response.get('failures', [])
        if not tasks and len(failures) == 1 and failures[0].get('arn') == arn and failures[0].get('reason') == 'MISSING':
            raise TaskNotFound('Task record expired or missing: '+arn)
        if failures or len(tasks) != 1 or tasks[0]['taskArn'] != arn:
            raise RuntimeError('Task status could not be verified: '+arn)
        return tasks[0]
    def api(self,task,path,body=None):
        # Authentication stays inside the container. Never echo environment values.
        script='curl --silent --show-error --fail --max-time 8 '
        if body is not None:
            encoded=base64.b64encode(json.dumps(body).encode()).decode()
            script+='--user "$RRELAYER_AUTH_USERNAME:$RRELAYER_AUTH_PASSWORD" -H "Content-Type: application/json" --data "$(echo '+encoded+' | base64 -d)" '
        probe = path == '/ready' and body is None
        if probe:
            script = "curl --silent --max-time 8 --write-out '\nHANDOFF_HTTP:%{http_code}' "
        script+='http://127.0.0.1:3000'+path
        return self.task_json(task, script, path, probe)

    def task_json(self, task, script, path, probe=False):
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
            if probe:
                body, marker, status = body.rpartition(b'HANDOFF_HTTP:')
                if not marker or status != b'200':
                    if marker and status == b'404':
                        raise ReadinessNotFound('Readiness endpoint returned HTTP 404')
                    raise RuntimeError('Readiness HTTP status unavailable or unsuccessful')
            return json.loads(body)
        except ReadinessNotFound:
            raise
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
    def task_running(self, task):
        return (task.get('lastStatus') == 'RUNNING' and task.get('desiredStatus') == 'RUNNING'
                and task.get('healthStatus') != 'UNHEALTHY'
                and all(c.get('healthStatus') != 'UNHEALTHY' and c.get('lastStatus', 'RUNNING') == 'RUNNING'
                        for c in task.get('containers', []) if c.get('name') == self.a.container))

    def ready(self,arn,source,digest):
        task=self.task(arn)
        containers=[c for c in task.get('containers',[]) if c['name']==self.a.container]
        if not self.task_running(task) or not containers or containers[0].get('imageDigest')!=digest:return False
        status=self.api(arn,'/ready')
        if not protocol_one(status.get('protocol')) or status.get('release')!=source or status.get('queuesReady') is not True:return False
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
        if recovery and not a.recover_journal and recovery.get('legacy') and not recovery.get('inventory_reconciled'):
            raise ValueError('Resume legacy recovery with the original --recover-journal to reconcile sender inventory')
        if a.recover_journal:
            original = json.loads(Path(a.recover_journal).read_text())
            if any(original.get(k) != getattr(a,k) for k in ('cluster','service','container','region')):
                raise ValueError('Recovery deployment target changed')
            if original['id'] == self.state['id'] or not original.get('warm_task'):
                raise ValueError('Expected an independent recovery journal with a warm task')
            reference = {k:original[k] for k in ('id','source','digest','warm_task','legacy','stage','target_group','previous_tasks','previous_definition')}
            confirmed = set(original.get('stopped_legacy_tasks', []))
            if original['legacy'] and original['stage'] in ('legacy-stopped','activating','activated','staging-at-zero','replacing-service'):
                confirmed.update(original['previous_tasks'])
            if not confirmed.issubset(original['previous_tasks']):
                raise ValueError('Shutdown confirmation does not belong to original task inventory')
            direct_inventory = set(reference['previous_tasks'])
            direct_confirmed = set(confirmed)
            inventory = set(direct_inventory)
            ancestor = original.get('recovery')
            while ancestor and ancestor.get('legacy'):
                if 'previous_tasks' not in ancestor:
                    raise ValueError('Historical rollback sender inventory is incomplete')
                prior_tasks = set(ancestor['previous_tasks'])
                prior_stops = set(ancestor.get('stopped_legacy_tasks', []))
                if not prior_stops.issubset(prior_tasks):
                    raise ValueError('Historical shutdown confirmation has no sender')
                inventory.update(prior_tasks)
                confirmed.update(prior_stops)
                ancestor = ancestor.get('recovery')
            if original.get('registration_window_owner') and original['stage'] != 'complete':
                reference['registration_window_owner'] = original['registration_window_owner']
            reference['inventory_reconciled'] = True
            reference['previous_tasks'] = sorted(inventory)
            reference['stopped_legacy_tasks'] = sorted(confirmed)
            reference['warm_retired'] = original['warm_task'] in original.get('retired_tasks', [])
            historical = set()
            shutdown = receipt.get('historical_warm_shutdown')
            if shutdown is not None:
                historical = historical_retirements(original, shutdown, a.acceptance)
                reference['warm_retired'] = True
            retirement_inventory(original)
            reference['retired_tasks'] = sorted(set(original.get('retired_tasks', [])) | historical)
            if original.get('recovery'):
                reference['recovery'] = original['recovery']
            if recovery:
                required = {'id','source','digest','warm_task','legacy','stage','target_group'}
                prior_reference = dict(recovery)
                for key, direct in [('previous_tasks', direct_inventory), ('stopped_legacy_tasks', direct_confirmed)]:
                    if key in prior_reference and set(prior_reference[key]) in (direct, set(reference[key])):
                        prior_reference[key] = reference[key]
                if ('retired_tasks' in prior_reference and
                        set(prior_reference['retired_tasks']).issubset(reference['retired_tasks'])):
                    prior_reference['retired_tasks'] = reference['retired_tasks']
                if prior_reference.get('warm_retired') is False and reference['warm_retired']:
                    # Retirement is monotonic. Persist it after checking the live inventory.
                    prior_reference['warm_retired'] = True
                if not required.issubset(recovery) or any(reference.get(k) != v for k,v in prior_reference.items()):
                    raise ValueError('Recovery journal changed')
            recovery = reference
        interrupted_bootstrap = recovery and recovery['legacy'] and recovery['stage'] in (
            'stopping-legacy','legacy-stopped','activating','activated','staging-at-zero','replacing-service')
        if service['desiredCount'] != 1 and not (service['desiredCount'] == 0
                and (interrupted_bootstrap or a.resume and (self.state.get('legacy') or self.state['stage'] == 'staging-at-zero'))):
            raise ValueError('Expected one service task')
        current=self.aws('ecs','describe-task-definition','--task-definition',service['taskDefinition'])['taskDefinition']
        candidate=self.aws('ecs','describe-task-definition','--task-definition',a.task_definition)['taskDefinition']
        a.task_definition=candidate['taskDefinitionArn']
        source,image=source_of(candidate,a.container)
        if source!=a.source or not image.endswith('@'+a.digest):raise ValueError('Candidate definition must pin the accepted digest and release ID')
        compatible_definition(current,candidate,a.container)
        groups=service.get('loadBalancers',[])
        if len(groups)!=1 or groups[0].get('containerPort')!=3000 or groups[0].get('containerName')!=a.container:raise ValueError('Expected one IP target group on port 3000')
        group=groups[0]['targetGroupArn']
        if self.state.get('target_group', group) != group:
            raise ValueError('Service target group changed since the original handoff')
        target_group=self.aws('elbv2','describe-target-groups','--target-group-arns',group)['TargetGroups'][0]
        if target_group['TargetType']!='ip':raise ValueError('Expected IP target group')
        if recovery and recovery['target_group'] != group:
            raise ValueError('Recovery target group changed')
        running=self.tasks(a.service)
        allowed={t['taskArn'] for t in running}|{x for x in [self.state.get('warm_task'),recovery and recovery.get('warm_task')] if x}
        launched = None
        inventory = self.tasks()
        if recovery and recovery.get('warm_retired') and any(
                task['taskArn'] == recovery['warm_task'] and task['lastStatus'] != 'STOPPED' for task in inventory):
            raise ValueError('Original warm sender is live despite retirement evidence')
        if a.recover_journal and self.state.get('recovery') and self.state['recovery'] != recovery:
            self.save(recovery=recovery)
        extras = [task for task in inventory if task['taskArn'] not in allowed
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
        if not recovery and self.state['stage'] in ('prepared','launching','warming'):
            baseline = self.state.get('previous_definition', service['taskDefinition'])
            if (not self.steady_deployment(service, baseline)
                    or len(running) != 1 or not self.task_running(running[0])
                    or running[0]['taskDefinitionArn'] != baseline):
                raise ValueError('Service is not steady; use its recovery journal')
        if 'previous_definition' not in self.state:
            previous_source,_=source_of(current,a.container)
            old_api=None
            if not recovery and previous_source and running:
                arn = running[0]['taskArn']
                try:
                    old_api = self.api(arn, '/ready')
                except ReadinessNotFound:
                    if not a.bootstrap or not self.healthy(self.target(arn), group):
                        raise
                else:
                    if not isinstance(old_api, dict) or not protocol_one(old_api.get('protocol')):
                        raise ValueError('Tagged sender returned an unknown readiness protocol')
            legacy=not old_api or not protocol_one(old_api.get('protocol'))
            if legacy and not a.bootstrap and not recovery:raise ValueError('Legacy binary requires explicit bootstrap')
            possible_previous_owner = None
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
                elif (not recovery['legacy'] and recovery['stage'] in ('activating','activated','replacing-service')
                      and counts == [1,1,0] and len(running) == 1
                      and running[0]['taskArn'] in recovery['previous_tasks']
                      and running[0]['taskDefinitionArn'] == service['taskDefinition'] == recovery['previous_definition']
                      and self.task(recovery['warm_task'])['lastStatus'] == 'STOPPED'):
                    # Only prewarm against this expectation. The standby must read
                    # the matching durable fence before registration or activation.
                    active = recovery['source']
                    if recovery['stage'] == 'activating':
                        possible_previous_owner = previous_source
                elif stopped_bootstrap:
                    # Only the recovery standby can now read the durable release fence.
                    # Its observation is checked before target registration or activation.
                    active = recovery['source']
                else:
                    old_api = None
                    can_use_service = recovery['stage'] in ('activated','staging-at-zero','replacing-service','service-ready','retiring','complete')
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
                        try:
                            old_api = self.ready(recovery['warm_task'], recovery['source'], recovery['digest'])
                        except RuntimeError:
                            if recovery['stage'] not in ('prepared','launching','warming','activating'):
                                raise
                            old_api = None
                    expected_active = recovery['source']
                    standby_confirms_prior = (old_api and old_api.get('durableIntakeReady')
                                              and not old_api.get('processing')
                                              and old_api.get('activeRelease') == previous_source)
                    if ((standby_confirms_prior or not old_api) and previous_source
                            and not recovery['legacy'] and recovery['stage'] in ('prepared','launching','warming','activating')
                            and counts == [1,1,0] and len(running) == 1
                            and running[0]['taskArn'] in recovery['previous_tasks']
                            and running[0]['taskDefinitionArn'] == service['taskDefinition'] == recovery['previous_definition']):
                        old_api = self.api(running[0]['taskArn'], '/ready')
                        if not protocol_one(old_api.get('protocol')) or old_api.get('release') != previous_source:
                            raise ValueError('Previous sender identity changed')
                        expected_active = previous_source
                    if not old_api or not old_api['durableIntakeReady'] or not old_api['processing'] or old_api['activeRelease'] != expected_active:
                        raise ValueError('Interrupted warm sender is not the verified active release')
                    active = old_api['activeRelease']
                legacy=live_legacy
            else:
                active=old_api and old_api.get('activeRelease')
            inventory = {t['taskArn'] for t in running}
            stopped = []
            if legacy and recovery:
                inventory.update(recovery['previous_tasks'])
                stopped = recovery.get('stopped_legacy_tasks', [])
            # A pre-discovery sender cannot observe registrations accepted by another process.
            # Independent recovery inherits a live ingress rule even if ownership already moved.
            window_owner = (recovery or {}).get('registration_window_owner')
            if not old_api or old_api.get('queueDiscovery') is not True or old_api.get('registrationReady') is not True:
                window_owner = window_owner or self.state['id']
            self.save(registration_window_owner=window_owner, possible_previous_owner=possible_previous_owner, previous_definition=service['taskDefinition'],
                      previous_tasks=sorted(inventory), stopped_legacy_tasks=stopped, previous_release=active,
                      legacy=legacy, target_group=group, recovery=recovery)
        shutdown = receipt.get('historical_warm_shutdown')
        if self.state['stage']=='complete' and not a.recover_journal and shutdown is not None:
            retired = set(self.state.get('retired_tasks', []))
            verified = historical_retirements(self.state, shutdown, a.acceptance)
            if verified - retired:
                self.save(retired_tasks=sorted(retired | verified))
        if self.state['stage']=='complete':
            self.verify_completion(group)
            return {'stage':'complete','source':a.source,'digest':a.digest}
        if not a.execute:
            return {'plan':'prewarm → verify → activate → replace service → retire warm task','legacy':self.state['legacy']}
        if 'registration_window_owner' not in self.state and self.state['stage'] not in ('prepared','launching','warming'):
            raise ValueError('Historical in-flight deployment requires independent recovery with a registration-compatible image')
        if self.state['stage'] == 'staging-at-zero':
            return self.stage_legacy_replacement(group, recovery, service)
        replacement_applied = (self.state['stage'] == 'activated' and service['taskDefinition'] == a.task_definition)
        if replacement_applied or self.state['stage'] in ('replacing-service', 'service-ready', 'retiring'):
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
            if (not report_reference(receipt.get('legacy_ingress_quiesced_evidence')) or status['unresolvedTransactions'] != 0
                    or any(service_state.get(k) != 0 for k in ('desiredCount','runningCount','pendingCount'))
                    or (recovery['stage'] == 'activating' and self.task(recovery['warm_task'])['lastStatus'] != 'STOPPED')
                    or any(t['lastStatus'] != 'STOPPED' for t in self.tasks(a.service))
                    or (recovery['stage'] == 'stopping-legacy' and
                        any(self.task(t)['lastStatus'] != 'STOPPED' for t in recovery['previous_tasks']
                            if t not in recovery.get('stopped_legacy_tasks', [])))):
                raise ValueError('Cannot verify drained legacy shutdown for recovery bootstrap')
            self.save(legacy=True, previous_release=None, previous_tasks=recovery['previous_tasks'],
                      stopped_legacy_tasks=recovery.get('stopped_legacy_tasks', []))
        prior = self.state.get('possible_previous_owner')
        if prior and self.state['stage'] == 'warming' and status['durableIntakeReady'] and status['activeRelease'] == prior:
            self.save(previous_release=prior, possible_previous_owner=None)
        expected_owners = {self.state['previous_release']}
        if self.state['stage'] in ('activating','activated'):
            expected_owners.add(a.source)
        if status['activeRelease'] not in expected_owners:
            raise ValueError('Active release changed outside this handoff')
        if recovery and not self.state['legacy'] and not status['durableIntakeReady']:
            raise ValueError('Recovery standby cannot verify durable intake')
        if status.get('queueDiscovery') is not True:
            raise ValueError('Candidate and rollback images must support registration queue discovery')
        if 'registration_window_owner' not in self.state:
            self.save(registration_window_owner=self.state['id'])
        if self.state['registration_window_owner']:
            verify_incumbent = not self.state['legacy'] and not recovery and self.state['stage'] == 'warming'
            if verify_incumbent:
                self.wait(lambda: self.registration_incumbent(group), 'incumbent before registration pause')
            self.registration_window(group, close=True)
            if verify_incumbent:
                self.wait(lambda: self.registration_incumbent(group), 'incumbent after registration pause')
        target=self.target(warm)
        if self.state['legacy'] and not status['durableIntakeReady']:
            # Ingress producers must already be quiesced with durable retry. Verify
            # the candidate observes no unresolved legacy rows before AND after stop.
            if not report_reference(receipt.get('legacy_ingress_quiesced_evidence')) or status['unresolvedTransactions']!=0:
                raise ValueError('Legacy ingress must be quiesced and accepted work fully drained')
            confirmed = set(self.state.get('stopped_legacy_tasks', []))
            if self.state['stage'] in ('legacy-stopped','activating'):
                confirmed.update(self.state['previous_tasks'])
            observed = set(self.state['previous_tasks'])
            if recovery and recovery.get('legacy'):
                observed.update(recovery['previous_tasks'])
                confirmed.update(recovery.get('stopped_legacy_tasks', []))
            listed = {t['taskArn'] for t in self.tasks(a.service)}
            observed.update(listed)
            self.save(previous_tasks=sorted(observed))
            live_service = self.aws('ecs','describe-services','--cluster',a.cluster,'--services',a.service)['services'][0]
            if live_service['desiredCount'] != 0:
                for task in sorted(observed - listed - confirmed):
                    if self.task(task)['lastStatus'] != 'STOPPED':
                        raise RuntimeError('Cannot verify legacy tasks stopped before scale-down')
                    confirmed.add(task)
                    self.save(stopped_legacy_tasks=sorted(confirmed))
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
        if self.state['legacy'] or (recovery and service['desiredCount'] == 0):
            self.save(stage='staging-at-zero')
            return self.stage_legacy_replacement(group, recovery)
        self.aws('ecs','update-service','--cluster',a.cluster,'--service',a.service,'--task-definition',a.task_definition,'--desired-count','1','--enable-execute-command')
        self.save(stage='replacing-service')
        return self.finish_replacement(group, recovery)

    def stage_legacy_replacement(self, group, recovery, service=None):
        a = self.a
        def current_service():
            return self.aws('ecs','describe-services','--cluster',a.cluster,'--services',a.service)['services'][0]
        service = service or current_service()
        if service['desiredCount'] == 0:
            if service['taskDefinition'] != a.task_definition:
                self.aws('ecs','update-service','--cluster',a.cluster,'--service',a.service,
                         '--task-definition',a.task_definition,'--desired-count','0','--enable-execute-command')
            def staged():
                service = current_service()
                deployments = service.get('deployments', [])
                return (service['taskDefinition'] == a.task_definition and len(deployments) == 1
                        and deployments[0]['taskDefinition'] == a.task_definition
                        and all(service[k] == 0 and deployments[0][k] == 0
                                for k in ('desiredCount','pendingCount','runningCount'))
                        and all(t['lastStatus'] == 'STOPPED' for t in self.tasks(a.service)))
            self.wait(staged,'sole candidate deployment at zero')
            status = self.ready(self.state['warm_task'], a.source, a.digest)
            if not status or not status['processing'] or not status['durableIntakeReady'] or status['activeRelease'] != a.source:
                raise ValueError('Warm sender ownership changed before scale-up')
            self.save(zero_verified=True)
            self.aws('ecs','update-service','--cluster',a.cluster,'--service',a.service,'--desired-count','1')
        elif not self.state.get('zero_verified') or service['taskDefinition'] != a.task_definition:
            raise ValueError('Service changed before verified scale-up')
        self.save(stage='replacing-service')
        return self.finish_replacement(group, recovery)

    @staticmethod
    def steady_deployment(service, definition):
        deployments = service.get('deployments', [])
        return (len(deployments) == 1 and deployments[0].get('status') == 'PRIMARY'
                and deployments[0].get('rolloutState') == 'COMPLETED'
                and all(record.get('taskDefinition') == definition and
                        [record.get(k) for k in ('desiredCount','runningCount','pendingCount')] == [1,1,0]
                        for record in (service, deployments[0])))

    def service_ready(self, group):
        return bool(self.serving_task(group))

    def serving_task(self, group, *, sole_target=False, registration_ready=False):
        a = self.a
        service = self.aws('ecs','describe-services','--cluster',a.cluster,'--services',a.service)['services'][0]
        config = service.get('deploymentConfiguration', {})
        breaker = config.get('deploymentCircuitBreaker', {})
        if (config.get('maximumPercent') != 100 or config.get('minimumHealthyPercent') != 0
                or breaker.get('enable') is not True or breaker.get('rollback') is not False):
            return False
        if (not self.steady_deployment(service, a.task_definition)
                or [(lb.get('targetGroupArn'), lb.get('containerName'), lb.get('containerPort'))
                    for lb in service.get('loadBalancers', [])] != [(group, a.container, 3000)]):
            return False
        tasks=self.tasks(a.service)
        if (len(tasks) != 1 or tasks[0]['taskDefinitionArn'] != a.task_definition
                or not self.task_running(tasks[0])):
            return False
        arn = tasks[0]['taskArn']
        status = self.ready(arn, a.source, a.digest)
        if (not status or status.get('queueDiscovery') is not True or status['activeRelease'] != a.source
                or not status['processing'] or not status['durableIntakeReady']
                or (registration_ready and status.get('registrationReady') is not True)):
            return False
        target = self.target(arn)
        if sole_target:
            rows = self.aws('elbv2','describe-target-health','--target-group-arn',group)['TargetHealthDescriptions']
            if (len(rows) != 1 or rows[0]['Target']['Id'] != target['Id']
                    or rows[0]['Target']['Port'] != target['Port'] or rows[0]['TargetHealth']['State'] != 'healthy'):
                return False
        elif not self.healthy(target, group):
            return False
        return arn

    def registration_incumbent(self, group):
        definition = self.state['previous_definition']
        service = self.aws('ecs','describe-services','--cluster',self.a.cluster,'--services',self.a.service)['services'][0]
        tasks = self.tasks(self.a.service)
        if (not self.steady_deployment(service, definition) or len(tasks) != 1
                or tasks[0]['taskDefinitionArn'] != definition or not self.task_running(tasks[0])):
            return False
        arn = tasks[0]['taskArn']
        if not self.healthy(self.target(arn), group):
            return False
        status = self.api(arn, '/ready')
        previous = self.state['previous_release']
        if not (protocol_one(status.get('protocol')) and status.get('release') == previous
                and status.get('activeRelease') == previous and status.get('processing') is True
                and status.get('queuesReady') is True and status.get('durableIntakeReady') is True):
            return False
        if arn not in self.state['previous_tasks']:
            self.save(previous_tasks=sorted({*self.state['previous_tasks'], arn}))
        return True

    def registration_window(self, group, close):
        registration_window(self, group, close)

    def completed_service_task(self, group, *, registration_ready=True):
        arn = self.serving_task(group, sole_target=True, registration_ready=registration_ready)
        if not arn:
            return False
        previous, warm, retired = retirement_inventory(self.state)
        families = {td.rsplit('/', 1)[-1].split(':')[0] for td in
                    (self.state['previous_definition'], self.a.task_definition)}
        tracked = previous | warm | {arn}
        live = {t['taskArn'] for t in self.tasks() if t['lastStatus'] != 'STOPPED'
                and (t['taskArn'] in tracked
                     or t['taskDefinitionArn'].rsplit('/', 1)[-1].split(':')[0] in families)}
        if live != {arn} or arn in warm:
            return False
        # Historical completed journals can predate retirement receipts. An
        # absent task-list entry alone does not establish its terminal status.
        for task in sorted((previous | warm) - {arn}):
            try:
                if self.task(task)['lastStatus'] != 'STOPPED':
                    return False
            except TaskNotFound:
                if task not in retired:
                    return False
            if task not in retired:
                retired.add(task)
                self.save(retired_tasks=sorted(retired))
        return arn

    def verify_completion(self, group):
        def confirmed():
            arn = self.completed_service_task(group)
            if not arn:
                return False
            self.verify_public_ingress(group)
            return self.completed_service_task(group) == arn
        self.wait(confirmed, 'sole accepted service and public ingress at completion')

    def verify_public_ingress(self, group):
        registration_window(self, group, close=False, verify_only=True)

    def verify_registration_response(self, hostname, port):
        return self.probe_registration(self.state['warm_task'], hostname, port, paused=True)

    def verify_registration_reopened(self, hostname, port):
        group = self.state['target_group']
        arn = self.completed_service_task(group)
        if not arn or not self.probe_registration(arn, hostname, port, paused=False):
            return False
        return self.completed_service_task(group) == arn

    def probe_registration(self, task, hostname, port, paused):
        # Malformed, unauthenticated POSTs cannot allocate a key or sender.
        if not re.fullmatch(r'[A-Za-z0-9.-]+', hostname) or not isinstance(port, int):
            raise ValueError('Unexpected load balancer address')
        expected = (f"expected='{REGISTRATION_PAUSED} 503'" if paused else
                    '''expected=$(probe "http://127.0.0.1:3000$path")
    case "$expected" in *" 400"|*" 401"|*" 403"|*" 422") ;; *) exit 1 ;; esac''')
        script = f"""set -eu
host={hostname}
probe() {{
  curl --noproxy '*' --silent --show-error --max-time 3 -H 'Content-Type: application/json' --data '{{' -w ' %{{http_code}}' "$@"
}}
ips=$(getent ahostsv4 "$host" | cut -d' ' -f1 | sort -u)
test -n "$ips"
for path in /relayers/1/new /relayers/1/import /relayers/00000000-0000-0000-0000-000000000000/clone; do
  {expected}
  for ip in $ips; do
    response=$(probe --resolve "$host:{port}:$ip" "http://$host:{port}$path")
    test "$response" = "$expected"
  done
done
echo true"""
        if paused:
            return self.task_json(task, script, 'registration ingress') is True
        # A different revision can return the same authentication error. Read
        # the serving release on those same nodes before accepting their replies.
        script = script.removesuffix('echo true') + '''printf '['
sep=''
for ip in $ips; do
  printf '%s' "$sep"
  curl --noproxy '*' --silent --fail --max-time 3 --resolve "$host:PORT:$ip" "http://$host:PORT/ready"
  sep=','
done
printf ']'
'''.replace('PORT', str(port))
        statuses = self.task_json(task, script, 'registration ingress')
        return (isinstance(statuses, list) and bool(statuses) and
                all(isinstance(s, dict) and protocol_one(s.get('protocol')) and
                    s.get('release') == self.a.source and s.get('activeRelease') == self.a.source and
                    all(s.get(k) is True for k in ('queueDiscovery','registrationReady','queuesReady','processing','durableIntakeReady'))
                    for s in statuses))

    def finish_replacement(self, group, recovery):
        a = self.a
        warm = self.state["warm_task"]
        self.wait(lambda: self.service_ready(group),'replacement service health',900)
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
                    self.wait(lambda: self.service_ready(group),'replacement service health',900)
                    self.aws('ecs','stop-task','--cluster',a.cluster,'--task',arn,'--reason','Verified handoff complete')
                self.wait(lambda: self.task(arn)['lastStatus']=='STOPPED', 'task retirement')
            stopped.add(arn)
            self.save(retired_tasks=sorted(stopped))
        self.wait(lambda: self.service_ready(group),'replacement service health after retirement',900)
        if self.state.get('registration_window_owner'):
            def reopen_service():
                arn = self.completed_service_task(group, registration_ready=False)
                if not arn:
                    return False
                self.api(arn, '/handoff/resume-registration', {})
                status = self.ready(arn, a.source, a.digest)
                return status and status.get('activeRelease') == a.source and status.get('processing') and status.get('registrationReady')
            self.wait(reopen_service, 'sole healthy registration-ready public sender')
            self.registration_window(group, close=False)
        self.verify_completion(group)
        self.save(stage='complete')
        return {'stage':'complete','source':a.source,'digest':a.digest}


def main():
    p=argparse.ArgumentParser(description=__doc__)
    for name in ['cluster','service','container','task-definition','source','digest','acceptance','journal']:p.add_argument('--'+name,required=True)
    p.add_argument('--dev-validation',action='store_true');p.add_argument('--region',default='ap-northeast-1');p.add_argument('--execute',action='store_true');p.add_argument('--bootstrap',action='store_true');p.add_argument('--resume',action='store_true');p.add_argument('--recover-journal')
    args=p.parse_args();controller=Controller(args)
    try:print(json.dumps(controller.run()))
    except Exception as error:
        if controller.state.get('stage') != 'complete':
            controller.save(error=str(error))
        raise SystemExit(f'{error}. Preserve warm task; journal: {args.journal}')

if __name__=='__main__':main()
