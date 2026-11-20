"""Scope the first-upgrade maintenance response to the three registration POSTs."""
import json

PATHS = ['/relayers/*/new', '/relayers/*/import', '/relayers/*/clone']
MESSAGE = 'Relayer registration is paused during the sender upgrade. Retry after handoff.'
CONDITIONS = [
    {'Field': 'path-pattern', 'PathPatternConfig': {'Values': PATHS}},
    {'Field': 'http-request-method', 'HttpRequestMethodConfig': {'Values': ['POST']}},
]
ACTIONS = [{'Type': 'fixed-response', 'FixedResponseConfig': {
    'StatusCode': '503', 'ContentType': 'text/plain', 'MessageBody': MESSAGE,
}}]


def matches(rule):
    conditions = {c['Field']: c for c in rule['Conditions']}
    if set(conditions) != {'path-pattern', 'http-request-method'}:
        return False
    paths = conditions['path-pattern']['PathPatternConfig'].get('Values', [])
    methods = conditions['http-request-method']['HttpRequestMethodConfig'].get('Values', [])
    actions = rule['Actions']
    return (rule['Priority'] == '1' and sorted(paths) == sorted(PATHS) and methods == ['POST']
            and len(actions) == 1 and actions[0]['Type'] == 'fixed-response'
            and actions[0].get('FixedResponseConfig') == ACTIONS[0]['FixedResponseConfig'])


def registration_window(controller, group, close, verify_only=False):
    aws = controller.aws
    groups = aws('elbv2', 'describe-target-groups', '--target-group-arns', group)['TargetGroups']
    if len(groups) != 1 or len(groups[0]['LoadBalancerArns']) != 1:
        raise ValueError('Registration window requires the dedicated relayer load balancer')
    listeners = aws('elbv2', 'describe-listeners', '--load-balancer-arn',
                    groups[0]['LoadBalancerArns'][0])['Listeners']
    if len(listeners) != 1:
        raise ValueError('Unverified registration listener topology')
    listener = listeners[0]
    arn = listener['ListenerArn']
    if listener['Protocol'] != 'HTTP':
        raise ValueError('Unverified registration listener transport')
    def verify_response():
        lb = aws('elbv2', 'describe-load-balancers', '--load-balancer-arns', groups[0]['LoadBalancerArns'][0])['LoadBalancers'][0]
        probe = controller.verify_registration_response if close else controller.verify_registration_reopened
        controller.wait(lambda: probe(lb['DNSName'], listener['Port']),
                        'registration pause on public ingress' if close else 'registration reopened on public ingress')
    actions = listener['DefaultActions']
    if (len(actions) != 1 or actions[0]['Type'] != 'forward' or actions[0].get('TargetGroupArn') != group
            or any(g['TargetGroupArn'] != group for g in actions[0].get('ForwardConfig', {}).get('TargetGroups', []))
            or controller.state.get('registration_listener', arn) != arn):
        raise ValueError('Registration listener does not exclusively serve this relayer')
    rules = [r for r in aws('elbv2', 'describe-rules', '--listener-arn', arn)['Rules'] if not r['IsDefault']]
    owner = controller.state['registration_window_owner']
    if rules:
        if verify_only:
            raise ValueError('Expected registration ingress to be open')
        if len(rules) != 1 or not matches(rules[0]):
            raise ValueError('Unexpected routing rules on relayer listener')
        rule = rules[0]['RuleArn']
        tags = aws('elbv2', 'describe-tags', '--resource-arns', rule)['TagDescriptions']
        if len(tags) != 1 or not any(t == {'Key': 'rrelayer-handoff', 'Value': owner} for t in tags[0]['Tags']):
            raise ValueError('Registration rule belongs to another deployment')
        if close:
            verify_response()
        else:
            aws('elbv2', 'delete-rule', '--rule-arn', rule)
            controller.wait(lambda: all(r['IsDefault'] for r in
                            aws('elbv2', 'describe-rules', '--listener-arn', arn)['Rules']),
                            'registration rule removed')
            verify_response()
        return
    if close:
        controller.save(registration_listener=arn)
        aws('elbv2', 'create-rule', '--listener-arn', arn, '--priority', '1',
            '--conditions', json.dumps(CONDITIONS), '--actions', json.dumps(ACTIONS),
            '--tags', json.dumps([{'Key': 'rrelayer-handoff', 'Value': owner}]))
        controller.wait(lambda: any(not r['IsDefault'] and matches(r) for r in
                        aws('elbv2', 'describe-rules', '--listener-arn', arn)['Rules']),
                        'registration rule installed')
        verify_response()
    else:
        verify_response()
