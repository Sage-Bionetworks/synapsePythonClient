"""The purpose of this script is to demonstrate how to use the new OOP interface for teams.
The following actions are shown in this script:
1. Creating a Team
2. Instantiating a Team object from Synapse
3. Getting information about the members of a Team
4. Inviting a user to a Team
5. Checking on invitations to join a Team
6. Deleting a Team
"""

import asyncio

from synapseclient.models.team import Team
import synapseclient

from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.sdk.resources import SERVICE_NAME, Resource
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter

trace.set_tracer_provider(
    TracerProvider(resource=Resource(attributes={SERVICE_NAME: "oop_team"}))
)
trace.get_tracer_provider().add_span_processor(BatchSpanProcessor(OTLPSpanExporter()))
tracer = trace.get_tracer("my_tracer")

syn = synapseclient.Synapse(debug=True)
syn.login()


@tracer.start_as_current_span("Team")
async def new_team():
    # Create a team
    new_team = Team(
        name="python-client-test-team",
        description="testing OOP interface",
        can_public_join=False,
    )
    my_synapse_team = await new_team.create()
    print(my_synapse_team)
    # sleep for a bit to allow the team to be created
    await asyncio.sleep(10)
    # Instantiate a Team object from a Synapse team
    my_team = await Team().from_id(id=my_synapse_team.id)
    print(my_team)
    breakpoint()
    my_team = await Team().from_name(name=my_synapse_team.name)
    print(my_team)
    # Get information about the members of a Team
    members = await my_team.members()
    print(next(members))

    # Invite a user to a Team
    invite = await my_team.invite(
        user="test_account_synapse_client",
        message="testing OOP interface (do not accept)",
    )
    print(invite)

    # Get open invitations for the Team
    invitations = await my_team.open_invitations()
    print(next(invitations))

    # Delete the Team
    await my_team.delete()


asyncio.run(new_team())
