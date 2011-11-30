from synapse.client import Synapse

s = Synapse('https://repo-alpha.sagebase.org/repo/v1','https://auth-alpha.sagebase.org/auth/v1', 30, False)
s.login('platform@sagebase.org', 'Grftac11')
e = s.startRestore("BackupDaemonJob88445-857150449942082859.zip")
#id = e["id"]
#f = s.monitorDaemonStatus(88453)
#print f

