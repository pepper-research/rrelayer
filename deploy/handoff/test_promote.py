import copy,json,tempfile,unittest
from pathlib import Path
from types import SimpleNamespace
from promote import Controller,REQUIRED,validate_acceptance,compatible_definition
SHA='a'*40;DIGEST='sha256:'+'b'*64

def receipt():return {'source':SHA,'image_digest':DIGEST,'protocol':1,'downstream_image_digest':DIGEST,'legacy_ingress_quiesced_evidence':'fixture ingress closed','checks':{k:{'passed':True,'evidence':'test fixture'} for k in REQUIRED}}

class Fake(Controller):
 def __init__(self,args,legacy=False):
  super().__init__(args);self.calls=[];self.active=None if legacy else 'old';self.enabled=not legacy;self.legacy=legacy;self.fail=None;self.oldStopped=False;self.warmStopped=False;self.replaced=False;self.unresolved=0
 def api(self,task,path,body=None):
  self.calls.append(('api',task,path))
  if path=='/handoff/activate':
   if self.legacy:assert self.oldStopped
   assert body['expected_release']==self.active;self.active=SHA;self.enabled=True
  return {'protocol':1,'release':SHA if task!='old-task' else 'old','activeRelease':self.active,'durableIntakeReady':self.enabled,'processing':self.enabled and (task!='old-task' or self.active=='old'),'unresolvedTransactions':self.unresolved}
 def ready(self,*args):return self.api(args[0],'/ready')
 def wait(self,predicate,label,seconds=600):
  self.calls.append(('wait',label))
  if self.fail==label:raise RuntimeError('injected '+label)
  result=predicate()
  if not result:raise RuntimeError('fixture condition false: '+label)
  return result
 def task(self,arn):return {'taskArn':arn,'taskDefinitionArn':'candidate:1' if arn!='old-task' else 'previous:1','lastStatus':'STOPPED' if (arn=='old-task' and self.oldStopped) or (arn=='warm-task' and self.warmStopped) else 'RUNNING'}
 def tasks(self,service=None):return [self.task('service-new' if self.replaced else 'old-task')] if service or 'warm_task' not in self.state else [self.task('service-new' if self.replaced else 'old-task'),self.task('warm-task')]
 def target(self,arn):return {'Id':arn,'Port':3000}
 def healthy(self,*args):return True
 def aws(self,*args):
  self.calls.append(args);op=args[1]
  if op=='describe-services':return {'services':[{'deploymentConfiguration':{'maximumPercent':100,'minimumHealthyPercent':0,'deploymentCircuitBreaker':{'enable':True,'rollback':True}},'desiredCount':0 if self.oldStopped and not self.replaced else 1,'taskDefinition':'candidate:1' if self.replaced else 'previous:1','loadBalancers':[{'containerPort':3000,'targetGroupArn':'tg'}],'networkConfiguration':{}}]}
  if op=='describe-task-definition':
   candidate=args[3]=='candidate:1';return {'taskDefinition':{'taskDefinitionArn':args[3],'family':'rrelayer','containerDefinitions':[{'name':'relayer','image':'image@'+DIGEST if candidate else 'image:old','environment':[{'name':'RRELAYER_RELEASE_ID','value':SHA if candidate else 'old'}] if candidate or not self.legacy else []}]}}
  if op=='describe-target-groups':return {'TargetGroups':[{'TargetType':'ip'}]}
  if op=='run-task':return {'tasks':[{'taskArn':'warm-task'}]}
  if op=='update-service':
   if '--desired-count' in args and args[args.index('--desired-count')+1]=='0':self.oldStopped=True
   if '--task-definition' in args:self.replaced=True;self.oldStopped=True
  if op=='stop-task':self.warmStopped=True
  if op=='describe-target-health':return {'TargetHealthDescriptions':[{'TargetHealth':{'State':'unused'}}]}
  return {}

class PromotionTests(unittest.TestCase):
 def setUp(self):
  self.temp=tempfile.TemporaryDirectory();self.path=Path(self.temp.name);(self.path/'acceptance.json').write_text(json.dumps(receipt()))
  self.args=SimpleNamespace(cluster='cluster',service='service',container='relayer',region='region',journal=str(self.path/'journal.json'),resume=False,source=SHA,digest=DIGEST,acceptance=str(self.path/'acceptance.json'),task_definition='candidate:1',bootstrap=False,recover_journal=None,execute=True)
 def tearDown(self):self.temp.cleanup()
 def test_missing_evidence_refuses_before_aws(self):
  r=receipt();del r['checks']['stale_owner'];(self.path/'acceptance.json').write_text(json.dumps(r));c=Fake(self.args)
  with self.assertRaises(ValueError):c.run()
  self.assertEqual(c.calls,[])
 def test_normal_order_and_rollback_disabled(self):
  c=Fake(self.args);self.assertEqual(c.run()['stage'],'complete');self.assertTrue(c.warmStopped)
  pre=c.calls.index(('wait','prewarmed target health'));activate=c.calls.index(('api','warm-task','/handoff/activate'));self.assertLess(pre,activate)
  changes=[v for v in c.calls if '--deployment-configuration' in v];self.assertFalse(json.loads(changes[0][-1])['deploymentCircuitBreaker']['rollback'])
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
