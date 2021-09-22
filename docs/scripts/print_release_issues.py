#!/usr/bin/env python

# Output the issues fixed in a particular JIRA release.
# Can be used with any JIRA project but defaults to SYNPY.

# Examples:
# # output release issues in rst format
# print_release_issues.py py-2.4 rst

# # output release issues for SYNR synapser-0.10 github
# print_release_issues.py --project SYNR synapser-0.10 github


import argparse
import collections
import json
import urllib.request
import sys

JQL_ISSUE_URL = "https://sagebionetworks.jira.com/rest/api/2/search?jql=project={project}%20AND%20fixVersion={version}%20ORDER%20BY%20created%20ASC&startAt={start_at}"  # noqa
ISSUE_URL_PREFIX = "https://sagebionetworks.jira.com/browse/{key}"

RST_ISSUE_FORMAT = """-  [`{key} <{url}>`__] -
   {summary}"""
GITHUB_ISSUE_FORMAT = "-  \\[[{key}]({url})\\] - {summary}"

def _get_issues(project, version):

    start_at = 0
    issues_by_type = {}

    while True:
        response = urllib.request.urlopen(
            JQL_ISSUE_URL.format(
                project=project,
                version=version,
                start_at=start_at,
            )
        )
        response_json = json.loads(response.read())

        issues = response_json['issues']
        if not issues:
            break

        for issue in issues:
            issue_type = issue['fields']['issuetype']['name']
            issues_for_type = issues_by_type.setdefault(issue_type, [])
            issues_for_type.append(issue)

        start_at += len(issues)

    issue_types = sorted(issues_by_type)
    issues_by_type_ordered = collections.OrderedDict()
    for issue_type in issue_types:
        issues_by_type_ordered[issue_type] = issues_by_type[issue_type]

    return issues_by_type_ordered


def _pluralize_issue_type(issue_type):
    if issue_type == 'Bug':
        return 'Bug Fixes'
    elif issue_type == 'Story':
        return 'Stories'

    return issue_type + 's'


def print_issues(issues_by_type, issue_format, file=sys.stdout):
    for issue_type, issues in issues_by_type.items():
        issue_type_plural = _pluralize_issue_type(issue_type)
        print(issue_type_plural, file=file)
        print('-' * len(issue_type_plural), file=file)

        for issue in issues:
            issue_key = issue['key']
            issue_url = ISSUE_URL_PREFIX.format(key=issue_key)
            issue_summary = issue['fields']['summary']
            print(issue_format.format(key=issue_key, url=issue_url, summary=issue_summary), file=file)

        # newline
        print(file=file)


def main():
    """Builds the argument parser and returns the result."""

    parser = argparse.ArgumentParser(description='Generates release note issue list in desired format')

    parser.add_argument('version', help='The JIRA release version whose issues will be included in the release notes')
    parser.add_argument('format', help='The output format', choices=['github', 'rst'])
    parser.add_argument('--project', help='The JIRA project', default='SYNPY')

    args = parser.parse_args()
    issues = _get_issues(args.project, args.version)
    issue_format = RST_ISSUE_FORMAT if args.format == 'rst' else GITHUB_ISSUE_FORMAT
    print_issues(issues, issue_format)


if __name__ == '__main__':
    main() 
