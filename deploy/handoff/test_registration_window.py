import json
import unittest
from copy import deepcopy
from unittest.mock import Mock

from registration_window import ACTIONS, CONDITIONS, matches, registration_window


class WindowTests(unittest.TestCase):
    def setUp(self):
        self.control = Mock()
        self.control.state = {'registration_window_owner': 'fixture'}
        self.control.wait.side_effect = lambda fn, _: self.assertTrue(fn())
        self.control.verify_registration_response.return_value = True
        self.control.aws.side_effect = self.aws
        self.rules = []
        self.tags = []
        self.actions = [{'Type': 'forward', 'TargetGroupArn': 'group'}]

    def aws(self, service, action, *args):
        if action == 'describe-target-groups':
            return {'TargetGroups': [{'LoadBalancerArns': ['lb']}]}
        if action == 'describe-listeners':
            return {'Listeners': [{'ListenerArn': 'listener', 'DefaultActions': self.actions, 'Protocol': 'HTTP', 'Port': 80}]}
        if action == 'describe-load-balancers':
            return {'LoadBalancers': [{'DNSName': 'lb.example'}]}
        if action == 'describe-rules':
            return {'Rules': [{'IsDefault': True}, *self.rules]}
        if action == 'create-rule':
            self.assertFalse(self.rules)
            self.tags = json.loads(args[args.index('--tags') + 1])
            self.rules.append({'RuleArn': 'rule', 'IsDefault': False, 'Priority': '1',
                               'Conditions': json.loads(args[args.index('--conditions') + 1]),
                               'Actions': json.loads(args[args.index('--actions') + 1])})
        elif action == 'describe-tags':
            return {'TagDescriptions': [{'Tags': self.tags}]}
        elif action == 'delete-rule':
            self.rules.clear()
        else:
            self.fail(action)
        return {}

    def test_only_registration_posts_pause_and_resume(self):
        for _ in range(2):
            registration_window(self.control, 'group', close=True)
        conditions = self.rules[0]['Conditions']
        self.assertEqual(conditions[0]['PathPatternConfig']['Values'],
                         ['/relayers/*/new', '/relayers/*/import', '/relayers/*/clone'])
        self.assertEqual(conditions[1]['HttpRequestMethodConfig']['Values'], ['POST'])
        self.assertEqual(self.rules[0]['Actions'][0]['FixedResponseConfig']['StatusCode'], '503')
        self.assertEqual(self.control.verify_registration_response.call_count, 2)
        for _ in range(2):
            registration_window(self.control, 'group', close=False)
        self.assertFalse(self.rules)

    def test_never_remove_another_deployments_rule(self):
        registration_window(self.control, 'group', close=True)
        self.control.state['registration_window_owner'] = 'unrelated'
        with self.assertRaisesRegex(ValueError, 'another deployment'):
            registration_window(self.control, 'group', close=False)
        self.assertEqual(len(self.rules), 1)

    def test_reject_shared_ingress_and_modified_rule(self):
        self.actions[0]['ForwardConfig'] = {'TargetGroups': [{'TargetGroupArn': 'shared'}]}
        with self.assertRaisesRegex(ValueError, 'exclusively'):
            registration_window(self.control, 'group', close=True)
        rule = dict(Priority='1', Conditions=deepcopy(CONDITIONS), Actions=ACTIONS)
        rule['Conditions'][0]['PathPatternConfig']['Values'].append('/transactions/*')
        self.assertFalse(matches(rule))
