import copy,json,os,tempfile,unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch
from promote import historical_retirements,TaskNotFound,retirement_inventory,ReadinessNotFound,Controller,REQUIRED,validate_acceptance,compatible_definition
SHA='a'*40;DIGEST='sha256:'+'b'*64

def receipt():return {'source':SHA,'image_digest':DIGEST,'protocol':1,'downstream_image_digest':DIGEST,'legacy_ingress_quiesced_evidence':'fixture ingress closed','checks':{k:{'passed':True,'evidence':'test fixture'} for k in REQUIRED}}

class Fake(Controller):
 def __init__(self,args,legacy=False):
  super().__init__(args);self.calls=[];self.active=None if legacy else 'old';self.enabled=not legacy;self.legacy=legacy;self.fail=None;self.oldStopped=False;self.warmStopped=False;self.replaced=False;self.staged=False;self.unresolved=0
 def api(self,task,path,body=None):
  self.calls.append(('api',task,path))
  if path=='/handoff/activate':
   if self.legacy:assert self.oldStopped
   assert body['expected_release']==self.active;self.active=SHA;self.enabled=True
  return {'queuesReady':True,'queueDiscovery':True,'registrationReady':True,'protocol':1,'release':SHA if task!='old-task' else 'old','activeRelease':self.active,'durableIntakeReady':self.enabled,'processing':self.enabled and (task!='old-task' or self.active=='old'),'unresolvedTransactions':self.unresolved}
 def ready(self,*args):return self.api(args[0],'/ready')
 def wait(self,predicate,label,seconds=600):
  self.calls.append(('wait',label))
  if self.fail==label:raise RuntimeError('injected '+label)
  result=predicate()
  if not result:raise RuntimeError('fixture condition false: '+label)
  return result
 def verify_public_ingress(self,group):self.calls.append(('verify_public_ingress',group))
 def registration_window(self,group,close):self.calls.append(('registration_window',close))
 def task(self,arn):return {'taskArn':arn,'desiredStatus':'STOPPED' if (arn=='old-task' and self.oldStopped) or (arn=='warm-task' and self.warmStopped) else 'RUNNING','taskDefinitionArn':'candidate:1' if arn!='old-task' else 'previous:1','lastStatus':'STOPPED' if (arn=='old-task' and self.oldStopped) or (arn=='warm-task' and self.warmStopped) else 'RUNNING'}
 def tasks(self,service=None):return [self.task('service-new' if self.replaced else 'old-task')] if service or 'warm_task' not in self.state else [self.task('service-new' if self.replaced else 'old-task'),self.task('warm-task')]
 def target(self,arn):return {'Id':arn,'Port':3000}
 def healthy(self,*args):return True
 def aws(self,*args):
  self.calls.append(args);op=args[1]
  if op=='describe-services':return {'services':[{'deploymentConfiguration':{'maximumPercent':100,'minimumHealthyPercent':0,'deploymentCircuitBreaker':{'enable':True,'rollback':not self.replaced}},'runningCount':0 if self.oldStopped and not self.replaced else 1,'pendingCount':0,'desiredCount':0 if self.oldStopped and not self.replaced else 1,'taskDefinition':'candidate:1' if self.replaced or self.staged else 'previous:1','loadBalancers':[{'containerName':self.a.container,'containerPort':3000,'targetGroupArn':'tg'}],'networkConfiguration':{},'deployments':[dict(taskDefinition='candidate:1',desiredCount=0,pendingCount=0,runningCount=0)] if self.staged else [dict(status='PRIMARY',rolloutState='COMPLETED',taskDefinition='candidate:1' if self.replaced else 'previous:1',desiredCount=1,runningCount=1,pendingCount=0)]}]}
  if op=='describe-task-definition':
   candidate=args[3]=='candidate:1';return {'taskDefinition':{'taskDefinitionArn':args[3],'family':'rrelayer','containerDefinitions':[{'name':'relayer','image':'image@'+DIGEST if candidate else 'image:old','environment':[{'name':'RRELAYER_RELEASE_ID','value':SHA if candidate else 'old'}] if candidate or not self.legacy else []}]}}
  if op=='describe-target-groups':return {'TargetGroups':[{'TargetType':'ip'}]}
  if op=='run-task':return {'tasks':[{'taskArn':'warm-task'}]}
  if op=='update-service':
   if '--desired-count' in args and args[args.index('--desired-count')+1]=='0':self.oldStopped=True
   if '--task-definition' in args:
    self.staged='--desired-count' in args and args[args.index('--desired-count')+1]=='0'
    self.replaced=not self.staged;self.oldStopped=True
   if self.staged and '--desired-count' in args and args[args.index('--desired-count')+1]=='1':
    self.replaced=True;self.staged=False
  if op=='stop-task':self.warmStopped=True
  if op=='describe-target-health' and '--targets' not in args:return {'TargetHealthDescriptions':[{'Target':self.target('service-new'),'TargetHealth':{'State':'healthy'}}]}
  if op=='describe-target-health':return {'TargetHealthDescriptions':[{'TargetHealth':{'State':'unused'}}]}
  return {}

class PromotionTests(unittest.TestCase):
 def setUp(self):
  self.temp=tempfile.TemporaryDirectory();self.path=Path(self.temp.name);(self.path/'acceptance.json').write_text(json.dumps(receipt()))
  self.args=SimpleNamespace(cluster='cluster',service='service',container='relayer',region='region',journal=str(self.path/'journal.json'),resume=False,source=SHA,digest=DIGEST,acceptance=str(self.path/'acceptance.json'),task_definition='candidate:1',bootstrap=False,recover_journal=None,execute=True)
 def tearDown(self):self.temp.cleanup()
 def test_protocol_version_is_an_integer(self):
  for version in (True, 1.0, '1'):
   with self.subTest(version=version), self.assertRaises(ValueError):
    validate_acceptance(dict(receipt(), protocol=version), SHA, DIGEST)

 def test_required_check_needs_a_nonblank_report_reference(self):
  for evidence in (True, '   ', '', None):
   candidate = receipt()
   candidate['checks']['dev_ecs_handoff']['evidence'] = evidence
   with self.subTest(evidence=evidence), self.assertRaisesRegex(ValueError, 'dev_ecs_handoff'):
    validate_acceptance(candidate, SHA, DIGEST)

 def test_downstream_image_must_have_an_immutable_digest(self):
  for value in (None, '', 'latest', 'sha256:abc', 123):
   with self.subTest(value=value), self.assertRaisesRegex(ValueError, 'Downstream'):
    validate_acceptance(dict(receipt(), downstream_image_digest=value), SHA, DIGEST)
  validate_acceptance(receipt(), SHA, DIGEST)

 def test_recovery_plan_retains_original_and_rotated_legacy_inventory(self):
  self.args.bootstrap = True
  original = Fake(self.args, legacy=True)
  original.fail = 'candidate readiness'
  with self.assertRaises(RuntimeError): original.run()
  original.save(stage='stopping-legacy', previous_tasks=['old-task','retired-task'], stopped_legacy_tasks=['retired-task'])
  self.args.recover_journal = self.args.journal
  self.args.journal = str(self.path / 'rollback.json')
  self.args.execute = False
  recovery = Fake(self.args, legacy=True)
  recovery.tasks = lambda service=None: [dict(recovery.task('old-task'), taskArn='rotated')]
  recovery.run()
  self.assertEqual(recovery.state['previous_tasks'], ['old-task','retired-task','rotated'])
  self.assertEqual(recovery.state['stopped_legacy_tasks'], ['retired-task'])

 def test_refuses_unfinished_baseline_before_launch(self):
  controller = Fake(self.args)
  tasks = controller.tasks
  controller.tasks = lambda service=None: [dict(t, taskDefinitionArn='previous:0') for t in tasks(service)]
  with self.assertRaisesRegex(ValueError, 'not steady'):
   controller.run()
  self.assertNotIn('previous_definition', controller.state)
  self.assertFalse(any(call[:2] == ('ecs','run-task') for call in controller.calls))

 def test_missing_evidence_refuses_before_aws(self):
  r=receipt();del r['checks']['stale_owner'];(self.path/'acceptance.json').write_text(json.dumps(r));c=Fake(self.args)
  with self.assertRaises(ValueError):c.run()
  self.assertEqual(c.calls,[])
 def test_ecs_exec_uses_tty_and_reads_json(self):
  fake=self.path/'aws'
  fake.write_text('#!/usr/bin/env python3\nimport json,sys\nassert sys.stdin.isatty() and sys.stdout.isatty()\nprint("HANDOFF_BEGIN")\nprint(json.dumps({"protocol":1,"processing":True}))\nprint("HANDOFF_HTTP:200")\nprint("HANDOFF_END")\n')
  fake.chmod(0o755)
  with patch.dict(os.environ,PATH=str(self.path)+os.pathsep+os.environ['PATH']):
   result=Controller(self.args).api('task','/ready')
  self.assertEqual(result,{'protocol':1,'processing':True})
 def test_tagged_bootstrap_requires_confirmed_404_and_healthy_target(self):
  self.args.execute=False
  for bootstrap, healthy, error, allowed in (
      (True, True, ReadinessNotFound('404'), True),
      (False, True, ReadinessNotFound('404'), False),
      (True, False, ReadinessNotFound('404'), False),
      (True, True, RuntimeError('transport unavailable'), False)):
   with self.subTest(bootstrap=bootstrap,healthy=healthy,error=error):
    self.args.bootstrap=bootstrap
    self.args.journal=str(self.path/('journal-'+str(bootstrap)+str(healthy)+type(error).__name__))
    c=Fake(self.args)
    with patch.object(c,'api',side_effect=error), patch.object(c,'healthy',return_value=healthy):
     if allowed: self.assertTrue(c.run()['legacy'])
     else:
      with self.assertRaises(RuntimeError): c.run()
    self.assertFalse(any(call[:2]==('ecs','run-task') for call in c.calls))

 def test_first_upgrade_keeps_payments_running_until_registration_reopens(self):
  c=Fake(self.args);original=c.api
  def observe(task,path,body=None):
   status=original(task,path,body)
   if task=='old-task':status.pop('queueDiscovery')
   return status
  c.api=observe
  self.assertEqual(c.run()['stage'],'complete')
  self.assertLess(c.calls.index(('registration_window',True)),c.calls.index(('wait','prewarmed target health')))
  self.assertLess(c.calls.index(('wait','task retirement')),c.calls.index(('registration_window',False)))
  self.assertFalse(any('--desired-count' in call and call[call.index('--desired-count')+1]=='0' for call in c.calls))
 def test_rotated_incumbent_after_planning_and_during_pause(self):
  self.args.execute=False
  c=Fake(self.args)
  original_api, original_tasks, original_window = c.api, c.tasks, c.registration_window
  replacement=['old-task']
  def tasks(service=None):
   rows=original_tasks(service)
   for row in rows:
    if row['taskArn']=='old-task':row['taskArn']=replacement[0]
   return rows
  def api(arn,path,body=None):
   if arn.startswith('rotated') or arn=='old-task':
    self.assertEqual(arn,replacement[0])
    response=original_api('old-task',path,body)
    response.pop('queueDiscovery')
    return response
   return original_api(arn,path,body)
  def window(group,close):
   original_window(group,close)
   if close:replacement[0]='rotated-2'
  original_task=c.task
  def task(arn):
   if arn.startswith('rotated'):
    return dict(original_task('old-task'),taskArn=arn,lastStatus='STOPPED' if c.replaced or arn!=replacement[0] else 'RUNNING')
   return original_task(arn)
  c.task=task
  c.api,c.tasks,c.registration_window=api,tasks,window
  c.run()
  self.args.resume=self.args.execute=True
  replacement[0]='rotated-1'
  self.assertEqual(c.run()['stage'],'complete')
  self.assertEqual(c.state['previous_tasks'],['old-task','rotated-1','rotated-2'])
 def test_public_reopening_requires_same_service_task_after_probe(self):
  c=Fake(self.args)
  c.state['target_group']='tg'
  with patch.object(c,'completed_service_task',side_effect=['service-new','rotated-service']) as inventory, patch.object(c,'probe_registration',return_value=True):
   self.assertFalse(c.verify_registration_reopened('lb.test',80))
   self.assertEqual(inventory.call_count,2)
  with patch.object(c,'completed_service_task',return_value=False), patch.object(c,'probe_registration') as probe:
   self.assertFalse(c.verify_registration_reopened('lb.test',80))
   probe.assert_not_called()
 def test_extra_healthy_target_blocks_completion_and_completed_resume_without_pause(self):
  c=Fake(self.args)
  normal=c.aws
  def mixed(*args):
   response=normal(*args)
   if args[1]=='describe-target-health' and '--targets' not in args:
    response['TargetHealthDescriptions'].append({'Target':{'Id':'unexpected','Port':3000},'TargetHealth':{'State':'healthy'}})
   return response
  c.aws=mixed
  with self.assertRaisesRegex(RuntimeError,'sole accepted service'):c.run()
  self.assertEqual(c.state['stage'],'retiring')
  self.assertNotIn(('registration_window',True),c.calls)
  c.aws=normal
  self.assertEqual(c.run()['stage'],'complete')
  c.aws=mixed
  with self.assertRaisesRegex(RuntimeError,'sole accepted service'):c.run()
 def test_completed_resume_requires_retirement_in_inventory_and_historical_receipt(self):
  c=Fake(self.args);self.assertEqual(c.run()['stage'],'complete')
  c.warmStopped=False
  with self.assertRaisesRegex(RuntimeError,'sole accepted service'):c.run()
  c.state.pop('retired_tasks');c.save()
  with self.assertRaisesRegex(RuntimeError,'sole accepted service'):c.run()
  service_tasks=c.tasks
  c.tasks=lambda service=None:service_tasks(self.args.service)
  with self.assertRaisesRegex(RuntimeError,'sole accepted service'):c.run()
  c.warmStopped=True
  self.assertEqual(c.run()['stage'],'complete')
  self.assertIn('warm-task',c.state['retired_tasks'])
 def test_completed_resume_checks_each_previous_sender_even_with_incomplete_list(self):
  c=Fake(self.args);self.assertEqual(c.run()['stage'],'complete')
  c.save(previous_tasks=['old-task','rotated-out'])
  original=c.task
  for state in ('RUNNING','DEACTIVATING','STOPPED'):
   c.task=lambda arn:dict(original(arn),lastStatus=state) if arn=='rotated-out' else original(arn)
   if state=='STOPPED':
    self.assertEqual(c.run()['stage'],'complete')
    self.assertIn('rotated-out',c.state['retired_tasks'])
   else:
    with self.assertRaisesRegex(RuntimeError,'sole accepted service'):c.run()
 def test_recovery_preserves_confirmed_and_unconfirmed_senders_across_ancestors(self):
  first=Fake(self.args);first.run()
  prior=dict(id='ancestor',warm_task='ancestor-warm',previous_tasks=['ancestor-service'],
             retired_tasks=['ancestor-service'],warm_retired=True,legacy=False)
  first.save(recovery=prior)
  args=copy.copy(self.args);args.recover_journal=args.journal;args.journal=str(self.path/'next.json');args.execute=False
  rollback=Fake(args);rollback.active=SHA;rollback.replaced=rollback.warmStopped=True
  rollback.run()
  previous,warm,retired=retirement_inventory(rollback.state)
  self.assertTrue({'old-task','ancestor-service'}.issubset(previous))
  self.assertTrue({'warm-task','ancestor-warm'}.issubset(warm))
  self.assertTrue({'old-task','warm-task','ancestor-service','ancestor-warm'}.issubset(retired))
  rollback.save(warm_task='rollback-warm')
  original=rollback.task
  def expired(task):
   if task in retired:raise TaskNotFound(task)
   if task=='rollback-warm':return dict(original(task),lastStatus='STOPPED')
   return original(task)
  rollback.task=expired
  rollback.tasks=lambda service=None:[original('service-new')]
  self.assertEqual(rollback.completed_service_task('tg'),'service-new')
  rollback.state['recovery']['retired_tasks'].remove('old-task')
  rollback.state['retired_tasks'].remove('old-task')
  self.assertFalse(rollback.completed_service_task('tg'))
 def test_scoped_historical_production_verdict_recovers_only_recorded_retirements(self):
  c=Fake(self.args);c.run()
  proof={k:c.state[k] for k in ('id','cluster','region','service','warm_task','source','digest')}
  proof.update(lastStatus='STOPPED',evidence='production-verdict.json')
  file=self.path/proof['evidence']
  verdict=dict(passed=True,source=SHA,imageDigest=DIGEST,retiredTasks=[dict(taskArn=t,lastStatus='STOPPED') for t in ('old-task','warm-task')])
  file.write_text(json.dumps(verdict))
  self.assertEqual(historical_retirements(c.state,proof,self.args.acceptance),{'old-task','warm-task'})
  for change in [dict(source='foreign'),dict(imageDigest='foreign'),dict(passed=False),
                 dict(retiredTasks=[]),dict(retiredTasks=verdict['retiredTasks'][:1]),
                 dict(retiredTasks=verdict['retiredTasks']*2),dict(retiredTasks=None),
                 dict(retiredTasks=[dict(taskArn='elsewhere',lastStatus='STOPPED')]),
                 dict(retiredTasks=[dict(taskArn='old-task',lastStatus='DEACTIVATING')])]:
   file.write_text(json.dumps(dict(verdict,**change)))
   with self.assertRaises(ValueError):historical_retirements(c.state,proof,self.args.acceptance)
  for content in ('invalid','{}','[]'):
   file.write_text(content)
   with self.assertRaises(ValueError):historical_retirements(c.state,proof,self.args.acceptance)
  file.unlink()
  with self.assertRaises(ValueError):historical_retirements(c.state,proof,self.args.acceptance)
 def test_incompatible_rollback_preserves_incumbent(self):
  c=Fake(self.args);original=c.api
  def observe(task,path,body=None):
   status=original(task,path,body)
   if task=='warm-task':status.pop('queueDiscovery')
   return status
  c.api=observe
  with self.assertRaisesRegex(ValueError,'rollback images'):c.run()
  self.assertEqual(c.active,'old')
  self.assertNotIn(('api','warm-task','/handoff/activate'),c.calls)
 def test_normal_order_and_rollback_disabled(self):
  c=Fake(self.args);self.assertEqual(c.run()['stage'],'complete');self.assertTrue(c.warmStopped)
  pre=c.calls.index(('wait','prewarmed target health'));activate=c.calls.index(('api','warm-task','/handoff/activate'));self.assertLess(pre,activate)
  changes=[v for v in c.calls if '--deployment-configuration' in v];self.assertFalse(json.loads(changes[0][-1])['deploymentCircuitBreaker']['rollback'])
 def test_resume_after_warm_task_stopped(self):
  controller=Fake(self.args)
  save=controller.save
  def interrupt(**fields):
   if fields.get('stage')=='complete': raise InterruptedError()
   save(**fields)
  controller.save=interrupt
  with self.assertRaises(InterruptedError): controller.run()
  self.assertEqual(controller.state['stage'],'retiring')
  self.args.resume=True
  resumed=Fake(self.args)
  resumed.active,resumed.replaced,resumed.warmStopped,resumed.oldStopped=SHA,True,True,True
  self.assertEqual(resumed.run()['stage'],'complete')
  self.assertIn(('api','service-new','/ready'),resumed.calls)
  self.assertNotIn(('api','warm-task','/ready'),resumed.calls)
  self.assertFalse(any('update-service' in call or 'stop-task' in call for call in resumed.calls))
 def test_async_stop_keeps_retirement_pending(self):
  controller=Fake(self.args)
  original=controller.task
  def stopping(arn):
   task=original(arn)
   if arn=='warm-task' and controller.warmStopped:
    task.update(lastStatus='RUNNING',desiredStatus='STOPPED')
   return task
  controller.task=stopping
  with self.assertRaisesRegex(RuntimeError,'task retirement'): controller.run()
  self.assertEqual(controller.state['stage'],'retiring')
  with self.assertRaisesRegex(RuntimeError,'task retirement'): controller.run()
  self.assertEqual(sum('stop-task' in call for call in controller.calls),1)
  controller.task=original
  self.assertEqual(controller.run()['stage'],'complete')
 def test_resume_requires_existing_journal(self):
  self.args.resume=True
  with self.assertRaisesRegex(ValueError,'existing deployment journal'): Fake(self.args)
  self.assertFalse(Path(self.args.journal).exists())
 def test_lost_launch_response_recovers_only_the_original_task(self):
  c=Fake(self.args)
  c.fail='candidate readiness'
  with self.assertRaises(RuntimeError): c.run()
  c.state.pop('warm_task')
  c.save(stage='launching')
  self.args.resume=True
  resumed=Fake(self.args)
  service_tasks=resumed.tasks
  extra=dict(resumed.task('warm-task'),group='handoff-'+resumed.state['id'],startedBy=resumed.state['id'])
  resumed.tasks=lambda service=None: service_tasks(service or 'service') + ([] if service else [dict(extra, **resumed.task('warm-task'))])
  extra['group']='unrelated'
  with self.assertRaisesRegex(ValueError,'Unaccounted'): resumed.run()
  extra['group']='handoff-'+resumed.state['id']
  del extra['startedBy']
  with self.assertRaisesRegex(ValueError,'Unaccounted'): resumed.run()
  extra['startedBy']='another deployment'
  with self.assertRaisesRegex(ValueError,'Unaccounted'): resumed.run()
  extra['startedBy']=resumed.state['id']
  self.assertEqual(resumed.run()['stage'],'complete')
  self.assertEqual(resumed.state['warm_task'],'warm-task')
  self.assertFalse(any('run-task' in call for call in resumed.calls))
 def test_rollback_plan_after_interrupted_bootstrap_at_zero(self):
  self.args.bootstrap=True
  original=Fake(self.args,True)
  original.fail='active warm target health'
  with self.assertRaises(RuntimeError): original.run()
  self.assertEqual(original.state['stage'],'activated')
  args=copy.copy(self.args)
  args.journal=str(self.path/'rollback.json')
  args.recover_journal=self.args.journal
  args.execute=False
  recovery=Fake(args)
  recovery.aws=original.aws
  recovery.ready=original.ready
  self.assertFalse(recovery.run()['legacy'])
  self.assertEqual(recovery.state['previous_release'],SHA)
  self.assertEqual(recovery.state['recovery']['id'],original.state['id'])
  # Earlier controllers persisted the same reference without these two fields.
  reference=recovery.state['recovery'].copy()
  for key in ('previous_definition','previous_tasks'): reference.pop(key)
  recovery.save(recovery=reference)
  args.resume=True
  resumed=Fake(args)
  resumed.aws,resumed.ready=original.aws,original.ready
  self.assertFalse(resumed.run()['legacy'])
  self.assertEqual(resumed.state['recovery']['previous_definition'],original.state['previous_definition'])
  resumed.state['recovery']['source']='wrong'
  with self.assertRaisesRegex(ValueError,'Recovery journal changed'): resumed.run()
 def test_stopped_bootstrap_recovery_checks_durable_owner_before_activation(self):
  self.args.bootstrap=True
  original=Fake(self.args,True)
  original.fail='active warm target health'
  with self.assertRaises(RuntimeError): original.run()
  original.save(stage='activating')
  original.warmStopped=True
  args=copy.copy(self.args)
  args.journal=str(self.path/'recovery.json')
  args.recover_journal=self.args.journal
  recovery=Fake(args)
  recovery.aws=original.aws
  recovery.tasks=lambda service=None: []
  recovery.task=original.task
  recovery.active='unexpected'
  with self.assertRaisesRegex(ValueError,'Active release changed'): recovery.run()
  self.assertEqual(recovery.state['previous_release'],SHA)
  self.assertNotIn(('api','warm-task','/handoff/activate'),recovery.calls)
 def test_failed_prewarm_keeps_old(self):
  c=Fake(self.args);c.fail='candidate readiness'
  with self.assertRaises(RuntimeError):c.run()
  self.assertEqual(c.active,'old');self.assertFalse(c.oldStopped);self.assertFalse(c.warmStopped)
 def test_failed_service_keeps_active_warm_then_resume(self):
  c=Fake(self.args);c.fail='replacement service health'
  with self.assertRaises(RuntimeError):c.run()
  self.assertEqual(c.active,SHA);self.assertFalse(c.warmStopped)
  c.fail=None;self.assertEqual(c.run()['stage'],'complete')
  self.assertEqual(sum(call[:2]==('ecs','run-task') for call in c.calls),1)
  self.assertEqual(c.run()['stage'],'complete')
 def test_bootstrap_drains_before_activate(self):
  self.args.bootstrap=True;c=Fake(self.args,True);c.run();self.assertTrue(c.oldStopped)
  self.assertLess(c.calls.index(('wait','legacy tasks stopped')),c.calls.index(('api','warm-task','/handoff/activate')))
 def test_rotated_legacy_sender_must_stop_before_activation(self):
  self.args.bootstrap=True
  self.args.execute=False
  c=Fake(self.args,True)
  c.run()
  self.args.execute=True
  original=c.tasks
  rotated=dict(c.task('old-task'),taskArn='rotated')
  c.tasks=lambda service=None: ([rotated] if service else original(service)+[rotated])
  with self.assertRaisesRegex(RuntimeError,'legacy tasks stopped'): c.run()
  self.assertIn('rotated',c.state['previous_tasks'])
  self.assertIsNone(c.active)
 def test_pending_legacy_cannot_activate(self):
  self.args.bootstrap=True;c=Fake(self.args,True);c.unresolved=1
  with self.assertRaises(ValueError):c.run()
  self.assertFalse(c.oldStopped);self.assertIsNone(c.active)
 def test_legacy_requires_explicit_bootstrap(self):
  c=Fake(self.args,True)
  with self.assertRaises(ValueError):c.run()
  self.assertFalse(c.oldStopped)
 def test_resume_rejects_changed_target(self):
  c=Fake(self.args);c.save();self.args.resume=True;self.args.service='other'
  with self.assertRaises(ValueError):Controller(self.args)
 def test_dev_validation_cannot_target_production(self):
  self.args.dev_validation=True;c=Fake(self.args)
  with self.assertRaises(ValueError):c.run()
  self.assertEqual(c.calls,[])
 def test_dev_receipt_does_not_satisfy_production_acceptance(self):
  r=receipt();del r['checks']['dev_ecs_handoff'];del r['checks']['downstream_no_refund_or_duplicate_credit']
  validate_acceptance(r,SHA,DIGEST,dev_validation=True)
  with self.assertRaises(ValueError):validate_acceptance(r,SHA,DIGEST)
 def test_rejects_config_and_image_changes(self):
  with self.assertRaises(ValueError):validate_acceptance(receipt(),SHA,'sha256:'+'c'*64)
  a={'containerDefinitions':[{'name':'relayer','image':'old','environment':[]}]};b=copy.deepcopy(a);b['containerDefinitions'][0]['command']=['different']
  with self.assertRaises(ValueError):compatible_definition(a,b,'relayer')
if __name__=='__main__':unittest.main()
