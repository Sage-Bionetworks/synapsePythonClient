# Client

<!-- We are manually defining the members here in this case to control the ordering of
those memebrs. We are roughly grouping these for similar functionality. -->
::: synapseclient.Synapse
    options:
        members:
        - login
        - logout
        - get
        - store
        - move
        - delete
        - get_annotations
        - set_annotations
        - tableQuery
        - createColumns
        - getColumn
        - getColumns
        - getTableColumns
        - downloadTableColumns
        - get_acl
        - get_permissions
        - getPermissions
        - setPermissions
        - getProvenance
        - setProvenance
        - deleteProvenance
        - updateActivity
        - findEntityId
        - getChildren
        - getTeam
        - getTeamMembers
        - invite_to_team
        - get_membership_status
        - get_team_open_invitations
        - send_membership_invitation
        - submit
        - getConfigFile
        - setEndpoints
        - invalidateAPIKey
        - get_user_profile_by_username
        - get_user_profile_by_id
        - getUserProfile
        - is_certified
        - is_synapse_id
        - onweb
        - printEntity
        - get_available_services
        - service
        - clear_download_list
        - create_external_s3_file_handle
        - getMyStorageLocationSetting
        - createStorageLocationSetting
        - create_s3_storage_location
        - setStorageLocation
        - get_sts_storage_token
        - create_snapshot_version
        - getConfigFile
        - getEvaluation
        - getEvaluationByContentSource
        - getEvaluationByName
        - getProjectSetting
        - getSubmission
        - getSubmissions
        - getSubmissionBundles
        - getSubmissionStatus
        - getWiki
        - getWikiAttachments
        - getWikiHeaders
        - get_download_list
        - get_download_list_manifest
        - remove_from_download_list
        - md5Query
        - sendMessage
        - uploadFileHandle
        - restGET
        - restPOST
        - restPUT
        - restDELETE

## More information

See also the [Synapse API documentation](https://rest-docs.synapse.org/)
