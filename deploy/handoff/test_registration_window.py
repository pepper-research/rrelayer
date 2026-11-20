import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest
from copy import deepcopy
from unittest.mock import Mock

from promote import Controller
from registration_window import ACTIONS, CONDITIONS, matches, registration_window


class WindowTests(unittest.TestCase):
    def setUp(self):
        self.control = Mock()
        self.control.state = {'registration_window_owner': 'fixture'}
        self.control.wait.side_effect = lambda fn, _: self.assertTrue(fn())
        self.control.verify_registration_response.return_value = True
        self.control.verify_registration_reopened.return_value = True
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

    def test_rule_removal_and_resumed_open_require_public_forwarding(self):
        registration_window(self.control, 'group', close=True)
        self.control.verify_registration_reopened.return_value = False
        for _ in range(2):
            with self.assertRaises(AssertionError):
                registration_window(self.control, 'group', close=False)
            self.assertFalse(self.rules)
        self.control.verify_registration_reopened.return_value = True
        registration_window(self.control, 'group', close=False, verify_only=True)

    def test_public_probe_checks_all_nodes_and_routes_against_the_live_response(self):
        with tempfile.TemporaryDirectory() as directory:
            bin_path = Path(directory)
            for app, script in {
                'getent': 'echo "127.0.0.1 STREAM lb"; echo "127.0.0.2 STREAM lb"',
                'curl': """case "$*" in
  *127.0.0.2*clone*) echo "$LAST_NODE_RESPONSE" ;;
  *) echo 'Unauthorized 401' ;;
esac""",
            }.items():
                path = bin_path / app
                path.write_text('#!/bin/sh\n' + script)
                path.chmod(0o755)
            for reply, expected in [('maintenance 503', False), ('Unauthorized 401', True)]:
                env = dict(os.environ, PATH=directory + os.pathsep + os.environ['PATH'], LAST_NODE_RESPONSE=reply)
                def task_json(task, script, label):
                    return subprocess.run(['sh','-c',script], env=env, capture_output=True).returncode == 0
                self.control.task_json.side_effect = task_json
                self.assertEqual(Controller.probe_registration(self.control, 'steady', 'lb.test', 80, False), expected)

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
